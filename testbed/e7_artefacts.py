"""
E7: turn one model answer into the artefacts that are deployed, recording every change.

The answer is split into artefacts exactly as the static validator does
(`kfbench.validate.extract`): bash blocks with `kafka-topics` commands, the
Flink configuration YAML (native) or Strimzi KafkaTopic + FlinkDeployment YAML
(k8s), and SQL blocks with CREATE TABLE statements. Nothing is fixed or
completed. The only changes are the mechanical substitutions a loopback
testbed needs (binary path, bootstrap address, object-store URIs, ports and
directories the testbed owns) and the measurement augmentation of the SQL job
(two columns plus a generated INSERT). Each one is returned in a list so the
result file says exactly what ran.
"""
from __future__ import annotations

import base64
import datetime as dt
import json
import math
import os
import random
import re
import shlex
import string
import sys
from dataclasses import dataclass, field
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
CODE_ROOT = Path(os.environ.get("STREAMING_ARCH_CODE_ROOT", HERE.parent))
sys.path.insert(0, str(CODE_ROOT / "src"))
from kfbench.catalog import FORMATS_KNOWN, get_catalog  # noqa: E402
from kfbench.parse import (_CREATE, _balanced, _logical_lines, code_blocks, flatten,  # noqa: E402
                           load_yaml_docs, parse_int, parse_memory_bytes, parse_topic_commands)
from kfbench.validate import Answer, extract  # noqa: E402

OPT = Path(os.environ.get("TUNING_OPT", HERE / "runtime"))
FLINK_HOME = OPT / "flink"
KAFKA_TOPICS = str(OPT / "kafka" / "bin" / "kafka-topics.sh")

SENT_COL, SEQ_COL = "__sent_ms", "__seq"


# ============================================================================ blocks
def classify_blocks(text: str):
    """Same block classification as kfbench.validate.extract."""
    blocks = code_blocks(text)
    sql = [b for b in blocks if b.lang == "sql" or re.search(r"\bCREATE\s+(TEMPORARY\s+)?TABLE\b", b.body, re.I)]
    sh = [b for b in blocks if b not in sql and re.search(r"kafka-topics", b.body)]
    yml = [b for b in blocks if b not in sql and b not in sh
           and (b.lang in ("yaml", "yml") or (b.lang == "" and ":" in b.body))]
    return sql, sh, yml


# ============================================================================ topics
SHELL_OPS = {"&&", ";", "|", "||", "&"}
REDIRECT = re.compile(r"^(\d?>>?|<|\d?>&\d|&>).*")


def native_topic_commands(sh_bodies: list[str]) -> tuple[list[dict], list[str]]:
    """kafka-topics commands of the answer, in order, as token lists (not yet bound to a testbed)."""
    script = "\n".join(sh_bodies)
    cmds = []
    for tc in parse_topic_commands(script):          # the validator's recognition of a topics command
        e = {"answer_cmd": tc.raw, "tokens": None, "error": None}
        try:
            toks = shlex.split(tc.raw, comments=True)
            i = next(j for j, t in enumerate(toks) if re.search(r"kafka-topics(\.sh)?$", t))
            e["tokens"] = toks[i:]
            e["prefix"] = toks[:i]
        except (ValueError, StopIteration) as exc:
            e["error"] = f"shell-parse: {exc}"
        cmds.append(e)
    ignored = [ln for ln in _logical_lines(script)
               if not re.search(r"(^|[/\s])kafka-topics(\.sh)?(\s|$)", ln)]
    return cmds, ignored


def bind_topic_command(cmd: dict, bootstrap: str) -> tuple[list[str] | None, list[dict]]:
    """Substitute the binary path and the --bootstrap-server value; nothing else."""
    subs = []
    if cmd["tokens"] is None:
        return None, subs
    toks = cmd["tokens"]
    if cmd.get("prefix"):
        subs.append({"kind": "dropped-command-prefix", "from": " ".join(cmd["prefix"])})
    if toks[0] != KAFKA_TOPICS:
        subs.append({"kind": "binary-path", "from": toks[0], "to": KAFKA_TOPICS})
    argv, j, rest = [KAFKA_TOPICS], 0, toks[1:]
    while j < len(rest):
        t = rest[j]
        if t in SHELL_OPS or REDIRECT.match(t):
            subs.append({"kind": "dropped-shell-suffix", "from": " ".join(rest[j:])})
            break
        if t == "--bootstrap-server" and j + 1 < len(rest) and not rest[j + 1].startswith("--"):
            subs.append({"kind": "bootstrap-server", "from": rest[j + 1], "to": bootstrap})
            argv += [t, bootstrap]
            j += 2
            continue
        if t.startswith("--bootstrap-server="):
            subs.append({"kind": "bootstrap-server", "from": t.split("=", 1)[1], "to": bootstrap})
            argv.append("--bootstrap-server=" + bootstrap)
            j += 1
            continue
        argv.append(t)
        j += 1
    return argv, subs


def _topic_cfg_value(v) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(_topic_cfg_value(x) for x in v) + "]"
    return str(v)


def k8s_topic_translations(docs: list, expected_cluster: str | None, crds: dict) -> list[dict]:
    """Strimzi KafkaTopic -> equivalent kafka-topics.sh --create (missing fields -> broker defaults).

    A resource the Topic Operator would not reconcile (apiVersion not served, or a
    strimzi.io/cluster label other than the cluster's) creates no topic.
    """
    out = []
    for d in docs:
        if not isinstance(d, dict) or d.get("kind") != "KafkaTopic":
            continue
        meta, spec = d.get("metadata") or {}, d.get("spec") or {}
        name = spec.get("topicName") or meta.get("name")
        e = {"resource": f"KafkaTopic/{meta.get('name')}", "topic": name, "tokens": None, "skipped": None}
        api = str(d.get("apiVersion", ""))
        label = (meta.get("labels") or {}).get("strimzi.io/cluster")
        if (api, "KafkaTopic") not in crds:
            e["skipped"] = f"apiVersion {api!r} is not served by Strimzi 0.48: the API server rejects the resource"
        elif expected_cluster is not None and label != expected_cluster:
            e["skipped"] = (f"label strimzi.io/cluster={label!r} does not select cluster {expected_cluster!r}: "
                            "the Topic Operator ignores the resource")
        elif not name:
            e["skipped"] = "no metadata.name / spec.topicName"
        else:
            toks = [KAFKA_TOPICS, "--create", "--topic", str(name)]
            if spec.get("partitions") is not None:
                toks += ["--partitions", str(spec["partitions"])]
            if spec.get("replicas") is not None:
                toks += ["--replication-factor", str(spec["replicas"])]
            for k, v in (spec.get("config") or {}).items():
                toks += ["--config", f"{k}={_topic_cfg_value(v)}"]
            e["tokens"] = toks
        out.append(e)
    return out


# ============================================================================ Flink configuration
_SHIPPED_CONF = None


def shipped_conf():
    """Load the installed Flink defaults only when an E7 run is requested."""
    global _SHIPPED_CONF
    if _SHIPPED_CONF is None:
        path = FLINK_HOME / "conf" / "config.yaml"
        if not path.is_file():
            raise FileNotFoundError(
                f"Flink configuration not found at {path}. Set TUNING_OPT to the runtime installation root.")
        _SHIPPED_CONF = flatten(yaml.safe_load(path.read_text()))
    return _SHIPPED_CONF
TM_MEM_KEYS = ("taskmanager.memory.process.size", "taskmanager.memory.flink.size", "taskmanager.memory.task.heap.size",
               "taskmanager.memory.managed.size", "taskmanager.heap.size", "taskmanager.heap.mb")
JM_MEM_KEYS = ("jobmanager.memory.process.size", "jobmanager.memory.flink.size", "jobmanager.memory.heap.size",
               "jobmanager.heap.size", "jobmanager.heap.mb")
# Keys the loopback testbed must own: addresses, ports, temp dirs, JVM options, metric reporters.
OWNED_EXACT = {"io.tmp.dirs", "taskmanager.tmp.dirs", "taskmanager.resource-id", "web.tmpdir"}
OWNED_PREFIX = ("env.", "metrics.reporter.", "metrics.reporters")
OWNED_SUFFIX = re.compile(r"\.(port|ports|bind-port|bind-host|bind-address|address|host)$")
OWNED_SUFFIX_ROOTS = ("jobmanager", "taskmanager", "rest", "web", "blob", "query", "metrics", "historyserver",
                      "sql-gateway", "high-availability")
OBJECT_STORE = re.compile(r"\b(s3a|s3p|s3n|s3|hdfs|gs|wasbs|wasb|abfss|abfs|oss|viewfs)://([^\s,;'\"]*)", re.I)
PATHY_KEY = re.compile(r"(dir|dirs|path|paths|localdir|storagedir|base-path)$", re.I)
HA_KEYS = ("high-availability", "high-availability.type", "high-availability.mode")


def is_owned(key: str, track: str) -> bool:
    k = key.lower()
    if k in OWNED_EXACT or k.startswith(OWNED_PREFIX):
        return True
    if OWNED_SUFFIX.search(k) and k.split(".", 1)[0] in OWNED_SUFFIX_ROOTS:
        return True
    if track == "k8s" and (k == "execution.target" or k.startswith("$internal.")):
        return True                      # set by the Flink Kubernetes Operator itself
    return False


def _relocate_value(key: str, v, run_dir: str, subs: list):
    if isinstance(v, list):
        return [_relocate_value(key, x, run_dir, subs) for x in v]
    if not isinstance(v, str):
        return v
    new = OBJECT_STORE.sub(lambda m: f"file://{run_dir}/objstore/{m.group(2).lstrip('/')}", v)
    if new != v:
        subs.append({"artefact": "flink-conf", "kind": "object-store-uri", "key": key, "from": v, "to": new,
                     "reason": "no object store on the testbed"})
        return new
    if PATHY_KEY.search(key) and (v.startswith("/") or v.startswith("file:/")):
        parts = re.split(r"([,;])", v)
        out = []
        for p in parts:
            s = p.strip()
            if s.startswith("file:"):
                path = re.sub(r"^file:/*", "/", s)
                out.append(f"file://{run_dir}/localfs{path}")
            elif s.startswith("/"):
                out.append(f"{run_dir}/localfs{s}")
            else:
                out.append(p)
        new = "".join(out)
        if new != v:
            subs.append({"artefact": "flink-conf", "kind": "local-path", "key": key, "from": v, "to": new,
                         "reason": "the testbed writes only inside its run directory"})
        return new
    return v


def k8s_memory_to_flink(v) -> str | None:
    b = parse_memory_bytes(v, k8s=True)
    if b is None:
        return None
    return f"{int(b // (1 << 20))}m"


def build_flink_conf(answer_conf: dict, k8s_over: dict, track: str, run_dir: str, rpc_port: int, rest_port: int):
    """Shipped config.yaml defaults + answer keys (verbatim except owned/URI/path keys) + testbed-owned keys."""
    subs: list[dict] = []
    conf = dict(shipped_conf())
    ans = dict(answer_conf)
    ans.update({k: v for k, v in k8s_over.items()})
    ha = next((ans[k] for k in HA_KEYS if k in ans), None)
    if ha is not None and str(ha).strip().lower() not in ("none", "", "false"):
        for k in [k for k in ans if k.startswith("high-availability")]:
            subs.append({"artefact": "flink-conf", "kind": "dropped-ha", "key": k, "from": ans.pop(k),
                         "reason": "no ZooKeeper / Kubernetes HA services on the testbed; the JobManager is never "
                                   "killed, so HA is not exercised"})
    for k in list(ans):
        if is_owned(k, track):
            subs.append({"artefact": "flink-conf", "kind": "testbed-owned", "key": k, "from": ans.pop(k),
                         "reason": "address/port/temp-dir/JVM/reporter keys are owned by the loopback testbed"})
    for k in list(ans):
        ans[k] = _relocate_value(k, ans[k], run_dir, subs)
    if any(k in ans for k in TM_MEM_KEYS):
        conf.pop("taskmanager.memory.process.size", None)
    if any(k in ans for k in JM_MEM_KEYS):
        conf.pop("jobmanager.memory.process.size", None)
    conf.update(ans)
    owned = {
        "jobmanager.rpc.address": "127.0.0.1", "jobmanager.rpc.port": rpc_port, "jobmanager.bind-host": "127.0.0.1",
        "taskmanager.host": "127.0.0.1", "taskmanager.bind-host": "127.0.0.1",
        "rest.address": "127.0.0.1", "rest.bind-address": "127.0.0.1", "rest.port": rest_port,
        "io.tmp.dirs": f"{run_dir}/flink-io",
        "env.java.opts.all": shipped_conf()["env.java.opts.all"],
    }
    conf.update(owned)
    return conf, subs


# ============================================================================ SQL
@dataclass
class Col:
    name: str
    raw_name: str
    type: str | None
    kind: str                # physical | computed | metadata
    not_null: bool = False
    virtual: bool = False
    pk: bool = False
    text: str = ""


@dataclass
class Table:
    name: str                # identifier as written
    plain: str               # unquoted
    options: dict
    items: list              # column / constraint texts in order
    cols: list
    constraints: list
    pk_cols: list
    start: int
    cols_lp: int
    cols_rp: int
    with_lp: int
    with_rp: int


_ANGLE_TYPE = re.compile(r"(ARRAY|MAP|MULTISET|ROW)\s*$", re.I)


def split_top(s: str, sep: str = ",") -> list[str]:
    parts, cur, depth, angle, q, i = [], [], 0, 0, None, 0
    while i < len(s):
        ch = s[i]
        if q:
            cur.append(ch)
            if ch == q:
                if i + 1 < len(s) and s[i + 1] == q:
                    cur.append(q)
                    i += 2
                    continue
                q = None
        elif ch in "'`\"":
            q = ch
            cur.append(ch)
        elif ch == "(":
            depth += 1
            cur.append(ch)
        elif ch == ")":
            depth -= 1
            cur.append(ch)
        elif ch == "<" and _ANGLE_TYPE.search("".join(cur[-12:])):
            angle += 1
            cur.append(ch)
        elif ch == ">" and angle > 0:
            angle -= 1
            cur.append(ch)
        elif ch == sep and depth == 0 and angle == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
        i += 1
    if "".join(cur).strip():
        parts.append("".join(cur))
    return parts


_STOP = re.compile(r"\s(NOT\s+NULL|NULL|METADATA|COMMENT|PRIMARY\s+KEY|CONSTRAINT)\b", re.I)


def _unquote(n: str) -> str:
    n = n.strip()
    if len(n) >= 2 and n[0] == n[-1] and n[0] in "`\"":
        return n[1:-1].replace(n[0] * 2, n[0])
    return n


def parse_columns(text: str):
    items = [x.strip() for x in split_top(text) if x.strip()]
    cols, cons, pk = [], [], []
    for it in items:
        if re.match(r"(WATERMARK|PRIMARY\s+KEY|CONSTRAINT|PERIOD\s+FOR)\b", it, re.I):
            cons.append(it)
            m = re.search(r"PRIMARY\s+KEY\s*\(([^)]*)\)", it, re.I)
            if m:
                pk += [_unquote(x) for x in m.group(1).split(",") if x.strip()]
            continue
        m = re.match(r"(`(?:[^`]|``)+`|\"[^\"]+\"|[^\s(]+)\s*(.*)$", it, re.S)
        if not m:
            cons.append(it)
            continue
        raw, rest = m.group(1), m.group(2)
        if re.match(r"AS\b", rest, re.I):
            cols.append(Col(_unquote(raw), raw, None, "computed", text=it))
            continue
        # the type ends at the first column-constraint keyword outside brackets
        cut = len(rest)
        for sm in _STOP.finditer(" " + rest):
            pre = rest[:max(0, sm.start())]
            if pre.count("(") == pre.count(")") and pre.count("<") <= pre.count(">"):
                cut = max(0, sm.start())
                break
        typ, tail = rest[:cut].strip(), rest[cut:]
        c = Col(_unquote(raw), raw, typ, "metadata" if re.search(r"\bMETADATA\b", tail, re.I) else "physical",
                not_null=bool(re.search(r"\bNOT\s+NULL\b", tail, re.I)),
                virtual=bool(re.search(r"\bVIRTUAL\b", tail, re.I)),
                pk=bool(re.search(r"\bPRIMARY\s+KEY\b", tail, re.I)), text=it)
        if c.pk:
            pk.append(c.name)
        cols.append(c)
    return items, cols, cons, pk


def find_tables(sql: str) -> tuple[str, list[Table], list[str]]:
    """CREATE TABLE statements with their spans (comment-stripped text, as in kfbench.parse)."""
    text = re.sub(r"--[^\n]*", "", sql)
    tables, spans = [], []
    for m in _CREATE.finditer(text):
        lp = m.end() - 1
        rp = _balanced(text, lp)
        if rp < 0:
            continue
        w = re.match(r"\s*(?:COMMENT\s+'[^']*'\s*)?(?:PARTITIONED\s+BY\s*\([^)]*\)\s*)?WITH\s*\(", text[rp + 1:], re.I)
        if not w:
            continue
        wlp = rp + 1 + w.end() - 1
        wrp = _balanced(text, wlp)
        if wrp < 0:
            continue
        body = text[wlp + 1:wrp]
        opts = {k.replace("''", "'").strip(): v.replace("''", "'").strip()
                for k, v in re.findall(r"'((?:[^']|'')*)'\s*=\s*'((?:[^']|'')*)'", body)}
        items, cols, cons, pk = parse_columns(text[lp + 1:rp])
        tables.append(Table(m.group(1), _unquote(m.group(1).split(".")[-1]) if "." in m.group(1) else _unquote(m.group(1)),
                            opts, items, cols, cons, pk, m.start(), lp, rp, wlp, wrp))
        spans.append((m.start(), wrp + 1))
    # everything that is not a CREATE TABLE statement is reported, not executed
    rest, prev = [], 0
    for a, b in spans:
        rest.append(text[prev:a])
        prev = b
    rest.append(text[prev:])
    ignored = [s.strip() for s in ";".join(rest).split(";") if s.strip()]
    return text, tables, ignored


def table_for(tables: list[Table], topic: str, role: str) -> Table | None:
    """Same matching as kfbench.validate.Answer.table_for: by the 'topic' option."""
    cands = [t for t in tables if t.options.get("topic") == topic]
    if role == "sink":
        cands.sort(key=lambda t: not any(k.startswith("sink.") for k in t.options))
    return cands[0] if cands else None


def _q(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _family(t: str | None) -> str:
    if t is None:
        return "?"
    u = t.strip().upper()
    if re.match(r"(VARCHAR|CHAR|STRING|CHARACTER)\b", u):
        return "str"
    if re.match(r"(TINYINT|SMALLINT|INTEGER|INT|BIGINT|FLOAT|DOUBLE|REAL|DECIMAL|DEC|NUMERIC)\b", u):
        return "num"
    if re.match(r"(TIMESTAMP|TIMESTAMP_LTZ|DATE|TIME)\b", u):
        return "time"
    if u.startswith("BOOLEAN"):
        return "bool"
    return u.split("(")[0].split("<")[0].strip()


def _norm_type(t: str | None) -> str:
    return re.sub(r"\s+", " ", (t or "").strip().upper())


def _default_literal(c: Col) -> str:
    f = _family(c.type)
    if f == "str":
        return "''"
    if f == "num":
        return f"CAST(0 AS {c.type})"
    if f == "bool":
        return "FALSE"
    u = _norm_type(c.type)
    if u.startswith("DATE"):
        return "CURRENT_DATE"
    if u.startswith("TIME") and not u.startswith("TIMESTAMP"):
        return "CURRENT_TIME"
    if u.startswith("TIMESTAMP"):
        return f"CAST(CURRENT_TIMESTAMP AS {c.type})"
    return f"CAST(NULL AS {c.type})"


def build_job_sql(text: str, tables: list[Table], src: Table, snk: Table, bootstrap: str):
    """Answer's CREATE TABLEs (bootstrap substituted, measurement columns added to source and sink) + INSERT."""
    subs, aug = [], []

    def render(t: Table) -> str:
        stmt_head = text[t.start:t.cols_lp + 1]
        items = list(t.items)
        if t is src or t is snk:
            extra = [f"{_q(SENT_COL)} BIGINT", f"{_q(SEQ_COL)} BIGINT"]
            pos = next((i for i, it in enumerate(items) if it in t.constraints), len(items))
            items[pos:pos] = extra
            aug.append({"kind": "measurement-columns", "table": t.plain, "role": "source" if t is src else "sink",
                        "added": extra, "position": "before first WATERMARK/PRIMARY KEY clause" if pos < len(t.items)
                        else "after the last column"})
        cols = ",\n    ".join(items)
        mid = text[t.cols_rp:t.with_lp + 1]
        body = text[t.with_lp + 1:t.with_rp]

        def rep(m):
            if m.group(2) != bootstrap:
                subs.append({"artefact": "sql", "kind": "bootstrap-server", "table": t.plain,
                             "from": m.group(2), "to": bootstrap})
            return m.group(1) + bootstrap + m.group(3)
        body = re.sub(r"('properties\.bootstrap\.servers'\s*=\s*')((?:[^']|'')*)(')", rep, body)
        return f"{stmt_head}\n    {cols}\n{mid}{body});"

    stmts = [render(t) for t in tables]
    src_cols = {c.name: c for c in src.cols}
    targets = [c for c in snk.cols if c.kind == "physical" or (c.kind == "metadata" and not c.virtual)]
    names, exprs = [], []
    for c in targets:
        s = src_cols.get(c.name)
        names.append(_q(c.name))
        if s is None:
            if c.not_null or c.name in snk.pk_cols:
                e = _default_literal(c)
                aug.append({"kind": "constant-for-not-null-sink-column", "column": c.name, "expr": e,
                            "reason": "no same-named source column and the column is NOT NULL / PRIMARY KEY"})
            else:
                e = f"CAST(NULL AS {c.type})"
        elif s.type is None or _norm_type(s.type) == _norm_type(c.type):
            e = _q(s.name)
        elif _family(s.type) == _family(c.type) or _family(c.type) == "str" or \
                {_family(s.type), _family(c.type)} <= {"num"}:
            e = f"CAST({_q(s.name)} AS {c.type})"
        else:
            e = f"TRY_CAST({_q(s.name)} AS {c.type})"
        exprs.append(e)
    names += [_q(SENT_COL), _q(SEQ_COL)]
    exprs += [_q(SENT_COL), _q(SEQ_COL)]
    insert = (f"INSERT INTO {snk.name} ({', '.join(names)})\nSELECT " + ",\n       ".join(exprs)
              + f"\nFROM {src.name};")
    aug.append({"kind": "generated-insert", "sql": insert})
    return "\n\n".join(stmts + [insert]) + "\n", subs, aug


# ============================================================================ formats
JAR_FORMATS = {"json", "debezium-json", "canal-json", "maxwell-json", "ogg-json", "csv", "raw"}


def available_formats() -> set[str]:
    lib = {p.name for p in (FLINK_HOME / "lib").glob("*.jar")}
    out = {"raw"}                                   # built into flink-table-runtime
    if any(n.startswith("flink-json") for n in lib):
        out |= {"json", "debezium-json", "canal-json", "maxwell-json", "ogg-json"}
    if any(n.startswith("flink-csv") for n in lib):
        out.add("csv")
    for f in ("avro", "avro-confluent", "debezium-avro-confluent", "protobuf", "parquet", "orc"):
        if any(n.startswith(f"flink-{f}") or n.startswith(f"flink-sql-{f}") for n in lib):
            out.add(f)
    return out


def table_formats(t: Table) -> dict:
    o = t.options
    return {k: o[k] for k in ("format", "value.format", "key.format") if k in o}


# ============================================================================ record templates
TYPE_RE = re.compile(r"\s*([A-Za-z_]+(?:\s+(?:PRECISION|VARYING))?)\s*(\(([^)]*)\))?(.*)$", re.S | re.I)


def _gen(t: str, rnd: random.Random, ts_iso: bool, now: float, key_pool: list[str] | None = None) -> str:
    """JSON text of a random value of SQL type t."""
    t = re.sub(r"\bNOT\s+NULL\b", "", t, flags=re.I).strip()
    u = t.upper()
    for kw in ("ARRAY", "MULTISET"):
        if u.startswith(kw) and "<" in t:
            inner = t[t.index("<") + 1:t.rindex(">")]
            if kw == "MULTISET":
                return "{" + json.dumps(_gen(inner, rnd, ts_iso, now)) + ":1}"
            return "[" + ",".join(_gen(inner, rnd, ts_iso, now) for _ in range(2)) + "]"
    if u.startswith("MAP") and "<" in t:
        k, v = split_top(t[t.index("<") + 1:t.rindex(">")])[:2]
        kv = json.loads(_gen(k, rnd, ts_iso, now))
        return "{" + json.dumps(str(kv)) + ":" + _gen(v, rnd, ts_iso, now) + "}"
    if u.startswith("ROW") and ("<" in t or "(" in t):
        o, c = ("<", ">") if "<" in t and (t.index("<") < t.index("(") if "(" in t else True) else ("(", ")")
        fields = split_top(t[t.index(o) + 1:t.rindex(c)])
        parts = []
        for f in fields:
            m = re.match(r"\s*(`[^`]+`|\S+)\s+(.*)$", f, re.S)
            if m:
                parts.append(json.dumps(_unquote(m.group(1))) + ":" + _gen(m.group(2), rnd, ts_iso, now))
        return "{" + ",".join(parts) + "}"
    m = TYPE_RE.match(t)
    base = (m.group(1) if m else t).upper()
    args = [a.strip() for a in (m.group(3) or "").split(",") if a.strip()] if m else []
    tail = (m.group(4) or "").upper() if m else ""
    if base in ("STRING", "VARCHAR", "CHAR", "CHARACTER", "CHARACTER VARYING", "TEXT"):
        if key_pool:
            return json.dumps(rnd.choice(key_pool))
        n = min(int(args[0]), 12) if args and args[0].isdigit() else 12
        return json.dumps("".join(rnd.choice(string.ascii_letters + string.digits) for _ in range(max(1, n))))
    if base == "BOOLEAN":
        return rnd.choice(["true", "false"])
    if base == "TINYINT":
        return str(rnd.randint(0, 127))
    if base == "SMALLINT":
        return str(rnd.randint(0, 32767))
    if base in ("INT", "INTEGER"):
        return str(rnd.randint(0, 2**31 - 1))
    if base == "BIGINT":
        return str(rnd.randint(0, 2**53))
    if base in ("FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION"):
        return f"{rnd.uniform(0, 10000):.4f}"
    if base in ("DECIMAL", "DEC", "NUMERIC"):
        p = int(args[0]) if args and args[0].isdigit() else 10
        s = int(args[1]) if len(args) > 1 and args[1].isdigit() else 0
        hi = min(10 ** max(0, p - s) - 1, 10**6)
        return f"{rnd.uniform(0, hi):.{s}f}" if s else str(rnd.randint(0, max(0, hi)))
    if base == "DATE":
        return json.dumps(dt.datetime.fromtimestamp(now, dt.timezone.utc).strftime("%Y-%m-%d"))
    if base == "TIME":
        return json.dumps(dt.datetime.fromtimestamp(now, dt.timezone.utc).strftime("%H:%M:%S"))
    if base in ("TIMESTAMP", "TIMESTAMP_LTZ"):
        ltz = base == "TIMESTAMP_LTZ" or "LOCAL TIME ZONE" in tail
        x = dt.datetime.fromtimestamp(now - rnd.uniform(0, 5), dt.timezone.utc)
        s = x.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] if ts_iso else x.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return json.dumps(s + ("Z" if ltz else ""))
    if base in ("BYTES", "BINARY", "VARBINARY"):
        return json.dumps(base64.b64encode(rnd.randbytes(8)).decode())
    # unknown / invalid type names (e.g. JSON): a string (the DDL itself decides whether this is valid)
    return json.dumps("".join(rnd.choice(string.ascii_lowercase) for _ in range(8)))


def make_templates(src: Table, event_bytes: int | None, n: int, seed: int, key_hint: str | None):
    """Pre-serialised source records: (Kafka key text or None, JSON value prefix without measurement fields).

    Values are random per SQL type. One STRING column is padded so that the value (including the two
    measurement fields) is ~event_bytes: a non-key STRING column if there is one, else the column named in
    the prompt as the business key (its random identifier is kept as a prefix). A Kafka record key is
    written only when the source table declares key fields (key.format/key.fields or upsert-kafka PK);
    otherwise records have no key and the producer's default partitioner spreads them.
    """
    rnd = random.Random(seed)
    o = src.options
    val_fmt = o.get("value.format") or o.get("format")
    ts_iso = (o.get("json.timestamp-format.standard") or o.get("value.json.timestamp-format.standard")
              or "SQL").upper().startswith("ISO")
    key_fmt = o.get("key.format")
    prefix = o.get("key.fields-prefix", "")
    key_fields = [x.strip() for x in o.get("key.fields", "").split(";") if x.strip()]
    if o.get("connector") == "upsert-kafka" and not key_fields:
        key_fields = list(src.pk_cols)
    except_key = o.get("value.fields-include", "ALL").upper() == "EXCEPT_KEY"
    phys = [c for c in src.cols if c.kind == "physical" and c.name not in (SENT_COL, SEQ_COL)]
    value_cols = [c for c in phys if not (except_key and c.name in key_fields)]
    hint_col = key_hint if key_hint in {c.name for c in phys} else None
    declared = set(key_fields) | set(src.pk_cols)
    pooled = declared | ({hint_col} if hint_col else set())     # columns drawn from a pool of 1000 ids

    def cap(c):
        m = re.search(r"\((\d+)\)", c.type or "")
        return int(m.group(1)) if m and not (c.type or "").upper().startswith("STRING") else 1 << 30
    strings = [c for c in value_cols if _family(c.type) == "str" and c.name not in declared]
    strings.sort(key=lambda c: (c.name == hint_col, -min(cap(c), 1 << 30)))
    pad = strings[0] if strings else None
    key_pool = [f"k{rnd.randrange(10**8):08d}" for _ in range(1000)]
    # the generator appends '"__sent_ms":<ms>,"__seq":<n>}' to each prefix at send time
    suffix_len = len(f'"{SENT_COL}":1731234567890,"{SEQ_COL}":12345678}}')
    now = __import__("time").time()
    out, sizes, short = [], [], 0
    for _ in range(n):
        vals = {c.name: _gen(c.type or "STRING", rnd, ts_iso, now,
                             key_pool if (c.name in pooled and _family(c.type) == "str") else None)
                for c in phys}
        if pad is not None:
            body0 = "{" + ",".join(json.dumps(c.name) + ":" + vals[c.name] for c in value_cols) + ","
            want = max(0, (event_bytes or 0) - (len(body0.encode()) + suffix_len))
            base_val = json.loads(vals[pad.name])
            room = max(0, cap(pad) - len(base_val))
            if want > room:
                short += 1
            vals = dict(vals)
            vals[pad.name] = json.dumps(base_val + "x" * min(want, room))
        body = "{" + ",".join(json.dumps(c.name) + ":" + vals[c.name] for c in value_cols) + ("," if value_cols else "")
        key = None
        if key_fmt and key_fields:
            if key_fmt == "json":
                key = "{" + ",".join(json.dumps(f[len(prefix):] if prefix and f.startswith(prefix) else f) + ":"
                                     + vals.get(f, "null") for f in key_fields) + "}"
            elif key_fmt == "raw":
                v = vals.get(key_fields[0], '""')
                key = json.loads(v) if v.startswith('"') else v
            elif key_fmt == "csv":
                key = ",".join(str(json.loads(vals.get(f, '""'))) for f in key_fields)
        out.append({"k": key, "v": body})
        sizes.append(len(body.encode()) + suffix_len)
    info = {"value_format": val_fmt, "key_format": key_fmt, "kafka_key": (f"key fields {key_fields} ({key_fmt})"
                                                                           if key_fmt and key_fields else "none"),
            "pooled_id_columns": sorted(pooled), "pad_column": pad.name if pad else None,
            "pad_short_records": short, "value_columns": [c.name for c in value_cols],
            "target_event_bytes": event_bytes, "mean_value_bytes": round(sum(sizes) / max(1, len(sizes)), 1),
            "timestamp_standard": "ISO-8601" if ts_iso else "SQL"}
    return out, info


# ============================================================================ the plan
@dataclass
class Plan:
    track: str
    topic_cmds: list = field(default_factory=list)          # native: parsed commands
    topic_translations: list = field(default_factory=list)  # k8s: translated KafkaTopics
    ignored_commands: list = field(default_factory=list)
    flink_answer_conf: dict = field(default_factory=dict)
    k8s_over: dict = field(default_factory=dict)
    translations: list = field(default_factory=list)
    n_taskmanagers: int = 3
    flink_conf_error: str | None = None
    sql_text: str = ""
    tables: list = field(default_factory=list)
    ignored_sql: list = field(default_factory=list)
    src: Table | None = None
    snk: Table | None = None
    job_error: str | None = None           # why no job can be built from the answer (a model failure)
    untestable: str | None = None          # why the deployment cannot be judged on this testbed
    notes: list = field(default_factory=list)
    validator: dict = field(default_factory=dict)


def _expected_cluster(scenario: dict) -> str | None:
    for e in scenario.get("expect", []):
        if e.get("type") == "topic_label":
            return e.get("value")
    return None


def _tm_budget(scenario: dict) -> int:
    for e in scenario.get("expect", []):
        if e.get("type") == "tm_memory_max":
            return int(e["bytes"])
    return 8 << 30


def plan_answer(text: str, scenario: dict, finish_reason: str | None = None) -> Plan:
    track = scenario["track"]
    cat = get_catalog()
    p = Plan(track=track)
    sql_b, sh_b, yml_b = classify_blocks(text)

    # same effective-value semantics as the static validator (used for the memory budget)
    ans = Answer(text or "", track, cat)
    extract(ans, finish_reason)
    p.validator = {"n_findings": len(ans.findings),
                   "findings_L1_L3": [f"{f.level}:{f.category}:{f.message}"[:200] for f in ans.findings
                                      if f.level in ("L1", "L2", "L3")][:20]}

    # ---- topics + Flink configuration
    if track == "native":
        p.topic_cmds, p.ignored_commands = native_topic_commands([b.body for b in sh_b])
        conf = None
        for b in yml_b:
            docs, dups, err = load_yaml_docs(b.body)
            if err:
                p.flink_conf_error = f"the answer's Flink configuration is not valid YAML: {err}"
                break
            if docs and isinstance(docs[0], dict):
                conf = flatten(docs[0])
                if dups:
                    p.notes.append(f"duplicate YAML keys {dups}: the last value is used (as the validator does)")
                if len(yml_b) > 1:
                    p.notes.append(f"{len(yml_b)} YAML blocks; the first mapping is deployed (as the validator does)")
                break
        if conf is None and p.flink_conf_error is None:
            p.notes.append("no Flink configuration in the answer: the shipped config.yaml defaults are used")
        p.flink_answer_conf = conf or {}
        p.n_taskmanagers = 3
    else:
        docs_all = []
        for b in yml_b:
            docs, dups, err = load_yaml_docs(b.body)
            if err:
                p.notes.append(f"YAML error in a k8s block (documents before the error are used): {err}")
            docs_all += [d for d in docs if isinstance(d, dict)]
        p.topic_translations = k8s_topic_translations(docs_all, _expected_cluster(scenario), cat.crds)
        for t in p.topic_translations:
            p.translations.append({"artefact": "topics", "from": t["resource"],
                                   "to": " ".join(t["tokens"]) if t["tokens"] else None, "skipped": t["skipped"]})
        fds = [d for d in docs_all if d.get("kind") == "FlinkDeployment"]
        if not fds:
            p.flink_conf_error = "no FlinkDeployment resource: no Flink cluster would be created"
        else:
            fd = fds[0]
            api = str(fd.get("apiVersion", ""))
            if (api, "FlinkDeployment") not in cat.crds:
                p.flink_conf_error = (f"FlinkDeployment apiVersion {api!r} is not served by the Flink Kubernetes "
                                      "Operator 1.13: the API server rejects the resource")
            spec = fd.get("spec") or {}
            fc = spec.get("flinkConfiguration") or {}
            p.flink_answer_conf = flatten(fc) if isinstance(fc, dict) else {}
            p.translations.append({"artefact": "flink-conf", "from": "FlinkDeployment.spec.flinkConfiguration",
                                   "to": "config.yaml keys (verbatim)"})
            job = spec.get("job") or {}
            if job.get("parallelism") is not None:
                p.k8s_over["parallelism.default"] = job["parallelism"]
                p.translations.append({"artefact": "flink-conf", "from": f"spec.job.parallelism={job['parallelism']}",
                                       "to": f"parallelism.default={job['parallelism']}"})
            for role, key in (("taskManager", "taskmanager.memory.process.size"),
                              ("jobManager", "jobmanager.memory.process.size")):
                mem = ((spec.get(role) or {}).get("resource") or {}).get("memory")
                if mem is not None:
                    fm = k8s_memory_to_flink(mem)
                    if fm:
                        p.k8s_over[key] = fm
                    p.translations.append({"artefact": "flink-conf", "from": f"spec.{role}.resource.memory={mem}",
                                           "to": f"{key}={fm}" if fm else "unparsable: not applied"})
            cpu = ((spec.get("taskManager") or {}).get("resource") or {}).get("cpu")
            if cpu is not None:
                p.translations.append({"artefact": "flink-conf", "from": f"spec.taskManager.resource.cpu={cpu}",
                                       "to": "not applied: every TaskManager is pinned to the testbed's 4 cores"})
            slots = parse_int({**p.flink_answer_conf, **p.k8s_over}.get("taskmanager.numberOfTaskSlots")) or 1
            par = parse_int({**p.flink_answer_conf, **p.k8s_over}.get("parallelism.default")) or 1
            replicas = parse_int((spec.get("taskManager") or {}).get("replicas"))
            want = replicas if replicas else math.ceil(par / max(1, slots))
            p.n_taskmanagers = max(1, min(3, want))
            p.translations.append({"artefact": "taskmanagers",
                                   "from": (f"spec.taskManager.replicas={replicas}" if replicas else
                                            f"ceil(parallelism {par} / {slots} slots) = {want} pods"),
                                   "to": f"{p.n_taskmanagers} TaskManager processes (the prompt allows at most 3 pods)"})

    # ---- SQL
    sql_text = "\n".join(b.body for b in sql_b)
    stripped, tables, ignored = find_tables(sql_text)
    p.sql_text, p.tables, p.ignored_sql = stripped, tables, ignored
    spec = scenario.get("spec") or {}
    if not spec.get("src") or not spec.get("sink"):
        p.untestable = (f"scenario family {scenario.get('family')!r} is not a single source->sink pipeline; "
                        "the E7 driver deploys only per-event source->sink jobs")
        return p
    p.src, p.snk = table_for(tables, spec["src"], "source"), table_for(tables, spec["sink"], "sink")
    if not tables:
        p.job_error = "the answer has no CREATE TABLE statement"
    elif p.src is None:
        p.job_error = f"no CREATE TABLE reads topic {spec['src']!r}; no job can be built"
    elif p.snk is None:
        p.job_error = f"no CREATE TABLE writes topic {spec['sink']!r}; no job can be built"
    elif p.src is p.snk:
        p.job_error = "the same table is matched as source and sink"

    # ---- untestable on this testbed (not a model failure)
    avail = available_formats()
    if p.job_error is None:
        for role, t in (("source", p.src), ("sink", p.snk)):
            for k, f in table_formats(t).items():
                if f in FORMATS_KNOWN and f not in avail:
                    p.untestable = f"{role} table {t.plain}: '{k}'='{f}' needs a format jar that is not on the testbed"
                    return p
        vf = p.src.options.get("value.format") or p.src.options.get("format")
        if vf is not None and vf in avail and vf != "json":
            p.untestable = f"source format '{vf}': the load generator writes plain JSON records only"
            return p
        sf = p.snk.options.get("value.format") or p.snk.options.get("format")
        if sf is not None and sf in avail and not sf.endswith("json"):
            p.untestable = f"sink format '{sf}': the measurement reader parses JSON records only"
            return p
    if track == "native":
        raw = ans.flink_get("taskmanager.memory.process.size")
        v = parse_memory_bytes(raw)
    else:
        raw = ans.k8s_tm.get("memory")
        v = parse_memory_bytes(raw, k8s=True)
    budget = _tm_budget(scenario)
    if v is not None and v > budget:
        p.untestable = (f"TaskManager memory {raw!r} exceeds the per-TaskManager budget of {budget / 2**30:.0f} GiB; "
                        "the testbed has no per-node cgroup memory limit (TESTBED.md), so the consequence "
                        "(OOM kill / unschedulable pod) cannot be reproduced")
    return p
