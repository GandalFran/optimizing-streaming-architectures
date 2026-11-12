"""Deterministic extraction and parsing of model outputs into configuration artefacts."""
from __future__ import annotations

import re
import shlex
from dataclasses import dataclass, field

import yaml

FENCE = re.compile(r"```[ \t]*([A-Za-z0-9_+-]*)[^\n]*\n(.*?)(?:```|\Z)", re.S)


@dataclass
class Block:
    lang: str
    body: str
    closed: bool


def code_blocks(text: str) -> list[Block]:
    out = []
    for m in FENCE.finditer(text):
        closed = text[m.end() - 3:m.end()] == "```"
        out.append(Block(m.group(1).lower(), m.group(2), closed))
    return out


# ----------------------------------------------------------------------------- units
_DUR_UNITS = {
    "ns": 1e-6, "nano": 1e-6, "nanos": 1e-6, "nanosecond": 1e-6, "nanoseconds": 1e-6,
    "us": 1e-3, "µs": 1e-3, "micro": 1e-3, "micros": 1e-3, "microsecond": 1e-3, "microseconds": 1e-3,
    "ms": 1, "milli": 1, "millis": 1, "millisecond": 1, "milliseconds": 1,
    "s": 1000, "sec": 1000, "secs": 1000, "second": 1000, "seconds": 1000,
    "m": 60000, "min": 60000, "mins": 60000, "minute": 60000, "minutes": 60000,
    "h": 3600000, "hour": 3600000, "hours": 3600000,
    "d": 86400000, "day": 86400000, "days": 86400000,
}


def parse_duration_ms(v) -> float | None:
    """Flink TimeUtils.parseDuration semantics: '<number>[ ]<unit>', a bare number means milliseconds."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().lower()
    if s.startswith("pt"):  # ISO-8601, as printed by java.time.Duration
        m = re.fullmatch(r"pt(?:(\d+(?:\.\d+)?)h)?(?:(\d+(?:\.\d+)?)m)?(?:(\d+(?:\.\d+)?)s)?", s)
        if not m:
            return None
        h, mi, se = (float(x) if x else 0.0 for x in m.groups())
        return ((h * 60 + mi) * 60 + se) * 1000
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([a-zµ]*)", s)
    if not m:
        return None
    num, unit = float(m.group(1)), m.group(2)
    if unit == "":
        return num
    return num * _DUR_UNITS[unit] if unit in _DUR_UNITS else None


_MEM_UNITS = {"": 1, "b": 1, "bytes": 1, "k": 1 << 10, "kb": 1 << 10, "kibibytes": 1 << 10,
              "m": 1 << 20, "mb": 1 << 20, "mebibytes": 1 << 20, "g": 1 << 30, "gb": 1 << 30, "gibibytes": 1 << 30,
              "t": 1 << 40, "tb": 1 << 40, "tebibytes": 1 << 40,
              "ki": 1 << 10, "mi": 1 << 20, "gi": 1 << 30, "ti": 1 << 40}


def parse_memory_bytes(v, k8s: bool = False) -> float | None:
    """Flink MemorySize.parse (binary units) or Kubernetes quantities (Mi/Gi, and decimal M/G when k8s=True)."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([A-Za-z]*)", s)
    if not m:
        return None
    num, unit = float(m.group(1)), m.group(2)
    if k8s and unit in ("K", "M", "G", "T"):
        return num * {"K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12}[unit]
    u = unit.lower()
    return num * _MEM_UNITS[u] if u in _MEM_UNITS else None


def parse_int(v) -> int | None:
    try:
        if isinstance(v, bool):
            return None
        f = float(str(v).strip().replace("_", ""))
        return int(f) if f.is_integer() else None
    except (TypeError, ValueError):
        return None


# ----------------------------------------------------------------------------- YAML
class _UniqueKeyLoader(yaml.SafeLoader):
    """SafeLoader that records duplicate mapping keys instead of silently keeping the last one."""
    duplicates: list[str]


def _construct_mapping(loader, node, deep=False):
    keys, mapping = set(), {}
    for k_node, v_node in node.value:
        k = loader.construct_object(k_node, deep=deep)
        if isinstance(k, (list, dict)):
            k = str(k)
        if k in keys:
            loader.duplicates.append(str(k))
        keys.add(k)
        mapping[k] = loader.construct_object(v_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping)


def load_yaml_docs(text: str) -> tuple[list, list[str], str | None]:
    """Returns (documents, duplicate_keys, error)."""
    loader = _UniqueKeyLoader(text)
    loader.duplicates = []
    docs = []
    try:
        while loader.check_data():
            docs.append(loader.get_data())
    except yaml.YAMLError as e:
        return docs, loader.duplicates, f"{type(e).__name__}: {str(e).splitlines()[0]}"
    finally:
        loader.dispose()
    return [d for d in docs if d is not None], loader.duplicates, None


def flatten(d, prefix="") -> dict:
    """Flink config.yaml (nested) and flink-conf.yaml (flat) both map to dotted keys."""
    out = {}
    if not isinstance(d, dict):
        return out
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else str(k)
        if isinstance(v, dict) and v:
            out.update(flatten(v, key))
        else:
            out[key] = v
    return out


# ----------------------------------------------------------------------------- kafka-topics.sh
@dataclass
class TopicCmd:
    raw: str
    action: str | None = None
    topic: str | None = None
    partitions: str | None = None
    replication_factor: str | None = None
    configs: dict = field(default_factory=dict)
    options: list = field(default_factory=list)
    bootstrap: str | None = None
    errors: list = field(default_factory=list)


def _logical_lines(script: str) -> list[str]:
    script = re.sub(r"\\\s*\n", " ", script)
    lines = []
    for ln in script.splitlines():
        ln = ln.strip()
        if ln.startswith("$ "):
            ln = ln[2:]
        if not ln or ln.startswith("#"):
            continue
        lines.append(ln)
    return lines


def parse_topic_commands(script: str) -> list[TopicCmd]:
    cmds = []
    for ln in _logical_lines(script):
        if not re.search(r"(^|[/\s])kafka-topics(\.sh)?(\s|$)", ln):
            continue
        try:
            toks = shlex.split(ln, comments=True)
        except ValueError as e:
            c = TopicCmd(raw=ln)
            c.errors.append(f"shell-parse: {e}")
            cmds.append(c)
            continue
        i = next((j for j, t in enumerate(toks) if re.search(r"kafka-topics(\.sh)?$", t)), None)
        c = TopicCmd(raw=ln)
        toks = toks[i + 1:]
        j = 0
        while j < len(toks):
            t = toks[j]
            j += 1
            if t in ("&&", ";", "|", "||"):
                break
            if not t.startswith("--"):
                c.errors.append(f"unexpected token {t!r}")
                continue
            name, eq, inline = t.partition("=")
            c.options.append(name)
            if name in TOPICS_CLI_FLAGS:
                if name in ("--create", "--alter", "--delete", "--describe", "--list"):
                    c.action = name[2:]
                continue
            if eq:
                val = inline
            elif j < len(toks) and not toks[j].startswith("--"):
                val = toks[j]
                j += 1
            else:
                val = None
                c.errors.append(f"option {name} without value")
            if name == "--topic":
                c.topic = val
            elif name == "--partitions":
                c.partitions = val
            elif name == "--replication-factor":
                c.replication_factor = val
            elif name == "--bootstrap-server":
                c.bootstrap = val
            elif name == "--config":
                if val and "=" in val:
                    k, _, v = val.partition("=")
                    c.configs[k.strip()] = v.strip()
                else:
                    c.errors.append(f"malformed --config {val!r}")
        cmds.append(c)
    return cmds


# Options of kafka-topics.sh (4.1) that take no argument.
TOPICS_CLI_FLAGS = {"--create", "--alter", "--delete", "--describe", "--list", "--if-not-exists", "--if-exists",
                    "--help", "--version", "--exclude-internal", "--under-replicated-partitions",
                    "--unavailable-partitions", "--at-min-isr-partitions", "--under-min-isr-partitions",
                    "--topics-with-overrides", "--exclude-internal"}


# ----------------------------------------------------------------------------- Flink SQL DDL
@dataclass
class TableDDL:
    name: str
    columns: str
    options: dict
    has_primary_key: bool
    has_watermark: bool


_CREATE = re.compile(r"CREATE\s+(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([`\"\w.]+)\s*\(", re.I)


def _balanced(text: str, start: int) -> int:
    depth, i, q = 0, start, None
    while i < len(text):
        ch = text[i]
        if q:
            if ch == q:
                q = None
        elif ch in "'`\"":
            q = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def strip_sql_comments(sql: str) -> str:
    """Removes '-- ...' and '/* ... */' comments outside string literals and quoted identifiers (v1.3: block comments,
    which Flink SQL accepts, were previously reported as unparsable option text)."""
    out, i, q = [], 0, None
    while i < len(sql):
        ch = sql[i]
        if q:
            out.append(ch)
            if ch == q:
                q = None
            i += 1
        elif ch in "'`\"":
            q = ch
            out.append(ch)
            i += 1
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            i = len(sql) if j < 0 else j
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            i = len(sql) if j < 0 else j + 2
            out.append(" ")
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def parse_create_tables(sql: str) -> tuple[list[TableDDL], list[str]]:
    tables, errors = [], []
    sql = strip_sql_comments(sql)
    for m in _CREATE.finditer(sql):
        lp = m.end() - 1
        rp = _balanced(sql, lp)
        if rp < 0:
            errors.append(f"unbalanced column list in table {m.group(1)}")
            continue
        cols = sql[lp + 1:rp]
        rest = sql[rp + 1:]
        w = re.match(r"\s*(?:COMMENT\s+'[^']*'\s*)?(?:PARTITIONED\s+BY\s*\([^)]*\)\s*)?WITH\s*\(", rest, re.I)
        opts = {}
        if not w:
            errors.append(f"table {m.group(1)} has no WITH clause")
        else:
            wp = rp + 1 + w.end() - 1
            we = _balanced(sql, wp)
            if we < 0:
                errors.append(f"unbalanced WITH clause in table {m.group(1)}")
                continue
            body = sql[wp + 1:we]
            pairs = re.findall(r"'((?:[^']|'')*)'\s*=\s*'((?:[^']|'')*)'", body)
            leftovers = re.sub(r"'((?:[^']|'')*)'\s*=\s*'((?:[^']|'')*)'", "", body).replace(",", "").strip()
            if leftovers:
                errors.append(f"unparsable option text in {m.group(1)}: {leftovers[:60]!r}")
            for k, v in pairs:
                opts[k.replace("''", "'").strip()] = v.replace("''", "'").strip()
        tables.append(TableDDL(m.group(1).strip("`\""), cols, opts,
                               bool(re.search(r"PRIMARY\s+KEY", cols, re.I)),
                               bool(re.search(r"WATERMARK\s+FOR", cols, re.I))))
    return tables, errors
