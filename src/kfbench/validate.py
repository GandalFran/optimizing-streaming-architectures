"""Layered deterministic validator for Kafka/Flink configuration answers.

Levels (cumulative):
  L1  format & syntax     every requested artefact is present, parseable and not truncated
  L2  schema              every key / option / CLI flag / CRD field exists for the target versions and values have
                          the documented type, enum or range
  L3  version             nothing that the target versions (Kafka 4.1, Flink 1.20, Strimzi 0.48, Flink Operator 1.13)
                          have removed; deprecated keys are reported as warnings, not failures
  L4  requirements        the scenario's machine-checkable expectations of kind `correctness` or `resource` hold
                          on the *effective* configuration (explicit value, else the documented default)
Expectations of kind `policy` are reported separately and never affect validity.
"""
from __future__ import annotations

import math
import re
from dataclasses import asdict, dataclass

from .catalog import CONNECTORS_AVAILABLE, FORMATS_KNOWN, Catalog, get_catalog
from .parse import (code_blocks, flatten, load_yaml_docs, parse_create_tables, parse_duration_ms, parse_int,
                    parse_memory_bytes, parse_topic_commands, strip_sql_comments)

BROKER_TXN_MAX_TIMEOUT_MS = 900_000      # Kafka broker default transaction.max.timeout.ms (15 min)
FLINK_KAFKA_SINK_DEFAULT_TXN_MS = 3_600_000  # KafkaSinkBuilder default transaction.timeout.ms (1 h)
REMOVED_KAFKA_CLI = {"--zookeeper": "removed from kafka-topics.sh in Kafka 3.0 (KIP-604)"}


@dataclass
class Finding:
    level: str        # L1 | L2 | L3 | warn
    category: str
    artefact: str
    message: str


class Answer:
    """Artefacts extracted from one model answer, with effective-value helpers."""

    def __init__(self, text: str, track: str, cat: Catalog):
        self.cat, self.track, self.text = cat, track, text
        self.findings: list[Finding] = []
        self.topics: dict[str, dict] = {}
        self.flink_raw: dict = {}
        self.flink: dict = {}
        self.tables = []
        self.k8s_tm = {}
        self.k8s_parallelism = None
        self.k8s_flink_version = None
        self.topic_labels: dict[str, str | None] = {}
        self.duplicates = 0
        self.blocks = code_blocks(text)

    def add(self, level, category, artefact, message):
        self.findings.append(Finding(level, category, artefact, message))

    # ------------------------------------------------------------------ effective values
    def flink_get(self, key):
        """Effective Flink value: explicit (incl. deprecated aliases), else catalog default; None = unset."""
        if key in self.flink:
            return self.flink[key]
        if key == "parallelism.default" and self.k8s_parallelism is not None:
            return self.k8s_parallelism
        return self.cat.flink_default(key)

    def topic_get(self, name, key):
        t = self.topics.get(name)
        if t is None:
            return None
        if key in ("partitions", "replication-factor"):
            v = t.get(key)
            return parse_int(v) if v is not None else self.cat.topic_default(key)
        v = t["configs"].get(key)
        return v if v is not None else self.cat.topic_default(key)

    def table_for(self, topic, role=None):
        cands = [t for t in self.tables if t.options.get("topic") == topic]
        if role == "sink":  # prefer tables that look like sinks when a topic is both read and written
            cands.sort(key=lambda t: not any(k.startswith("sink.") for k in t.options))
        return cands[0] if cands else None

    def parallelism(self) -> int:
        p = parse_int(self.flink_get("parallelism.default")) or 1
        for t in self.tables:
            for k in ("scan.parallelism", "sink.parallelism"):
                if k in t.options and parse_int(t.options[k]):
                    p = max(p, parse_int(t.options[k]))
        return p


# =================================================================================== L1–L3 per artefact
def _check_value(ans: Answer, art: str, key: str, value, opt: dict, enum_style="flink"):
    t = (opt or {}).get("type") or ""
    s = None if value is None else str(value).strip()
    bad = False
    if s is None or s == "":
        bad = s == ""
    elif t == "java.time.Duration":
        bad = parse_duration_ms(value) is None
    elif t.endswith("MemorySize"):
        bad = parse_memory_bytes(value) is None
    elif t in ("java.lang.Integer", "java.lang.Long"):
        bad = parse_int(value) is None
    elif t == "java.lang.Boolean":
        bad = s.lower() not in ("true", "false")
    elif t in ("java.lang.Double", "java.lang.Float"):
        try:
            float(s)
        except ValueError:
            bad = True
    elif opt and opt.get("enum") and not opt.get("is_list"):
        norm = s.replace("-", "_").upper() if enum_style == "table" else s.upper()
        bad = norm not in {e.upper() for e in opt["enum"]}
    if bad:
        ans.add("L2", "invalid-value", art, f"{key}={value!r} is not a valid {t.rsplit('.', 1)[-1] or 'value'}")


def _kafka_valid_values_ok(rec: dict, value: str) -> bool:
    vv = (rec or {}).get("valid_values") or ""
    t = (rec or {}).get("type") or ""
    s = str(value).strip()
    if t in ("int", "long", "short"):
        if parse_int(s) is None:
            return False
        m = re.fullmatch(r"\[(-?\d+),\.\.\.(?:,(-?\d+))?\]", vv.replace(" ", ""))
        if m:
            lo, hi = int(m.group(1)), m.group(2)
            return parse_int(s) >= lo and (hi is None or parse_int(s) <= int(hi))
        return True
    if t == "double":
        try:
            float(s)
            return True
        except ValueError:
            return False
    if t == "boolean":
        return s.lower() in ("true", "false")
    m = re.fullmatch(r"\[(.*)\]", vv.strip())
    if m and "..." not in vv and t in ("string", "list"):
        allowed = {x.strip().lower() for x in m.group(1).split(",")}
        vals = [x.strip().lower() for x in s.split(",")] if t == "list" else [s.lower()]
        return all(v in allowed for v in vals) if allowed else True
    return True


def _check_topic_config(ans: Answer, art: str, key: str, value):
    rec = ans.cat.kafka_topic.get(key)
    if rec is None:
        if key in ans.cat.kafka_broker:
            ans.add("L2", "broker-key-as-topic-config", art, f"{key} is a broker setting, not a topic config")
        else:
            ans.add("L2", "unknown-key", art, f"unknown topic config {key}")
        return
    if not _kafka_valid_values_ok(rec, value):
        ans.add("L2", "invalid-value", art, f"{key}={value!r} outside valid values {rec.get('valid_values')}")


def _native_topics(ans: Answer, script: str):
    cmds = parse_topic_commands(script)
    creates = [c for c in cmds if c.action == "create"]
    if not creates:
        ans.add("L1", "missing-artefact", "topics", "no 'kafka-topics.sh --create' command found")
        return
    for c in cmds:
        for e in c.errors:
            ans.add("L1" if e.startswith("shell-parse") else "L2", "cli-syntax", "topics", e)
        for o in c.options:
            if o in REMOVED_KAFKA_CLI:
                ans.add("L3", "removed-option", "topics", f"{o}: {REMOVED_KAFKA_CLI[o]}")
            elif o not in ans.cat.kafka_topics_cli:
                ans.add("L2", "unknown-cli-option", "topics", f"kafka-topics.sh has no option {o}")
        if c.action == "create":
            if "--bootstrap-server" not in c.options and "--bootstrap-controller" not in c.options \
                    and "--zookeeper" not in c.options:
                ans.add("L2", "missing-required-option", "topics", "--create requires --bootstrap-server")
            if not c.topic:
                ans.add("L2", "missing-required-option", "topics", "--create without --topic")
                continue
            for k, v in (("partitions", c.partitions), ("replication-factor", c.replication_factor)):
                if v is not None and (parse_int(v) is None or parse_int(v) < 1):
                    ans.add("L2", "invalid-value", "topics", f"--{k} {v!r}")
            for k, v in c.configs.items():
                _check_topic_config(ans, "topics", k, v)
            ans.topics[c.topic] = {"partitions": c.partitions, "replication-factor": c.replication_factor,
                                   "configs": dict(c.configs)}
        elif c.action == "alter" and c.topic in ans.topics and c.partitions:
            ans.topics[c.topic]["partitions"] = c.partitions


def _flink_conf(ans: Answer, conf: dict, art: str, k8s: bool):
    for key, value in conf.items():
        key = str(key)
        st = ans.cat.flink_key_status(key)
        if st == "unknown":
            ans.add("L2", "unknown-key", art, f"{key} is not a Flink 1.20 configuration option")
            continue
        if st == "table-option":
            ans.add("L2", "misplaced-key", art, f"{key} is a SQL connector option; it has no effect in the cluster configuration")
            continue
        if st == "operator" and not k8s:
            ans.add("L2", "misplaced-key", art, f"{key} is a Kubernetes-operator option; it has no effect here")
            continue
        if st == "deprecated":
            ans.add("warn", "deprecated-key", art, f"{key} is deprecated in Flink 1.20")
        if st == "operator" and key in ans.cat.operator_deprecated:
            ans.add("warn", "deprecated-key", art, f"{key} is deprecated in Flink Kubernetes Operator 1.13")
        canon = ans.cat.flink_deprecated.get(key, key)
        if st in ("ok", "deprecated"):
            _check_value(ans, art, key, value, ans.cat.flink_option(key))
        ans.flink[canon] = value
        ans.flink_raw[key] = value


def _sql(ans: Answer, sql: str):
    tables, errors = parse_create_tables(sql)
    for e in errors:
        ans.add("L1", "sql-syntax", "sql", e)
    if not tables:
        ans.add("L1", "missing-artefact", "sql", "no CREATE TABLE statement found")
        return
    cat = ans.cat
    for t in tables:
        o = t.options
        conn = o.get("connector")
        art = f"sql:{t.name}"
        if conn is None:
            ans.add("L2", "missing-required-option", art, "no 'connector' option")
            continue
        if conn not in CONNECTORS_AVAILABLE:
            ans.add("L2", "unknown-connector", art, f"connector '{conn}' does not exist in Flink 1.20 / kafka connector 3.3")
            continue
        if conn not in ("kafka", "upsert-kafka"):
            continue
        fmts = {o[k] for k in ("format", "value.format", "key.format") if k in o}
        for f in fmts:
            if f not in FORMATS_KNOWN:
                ans.add("L2", "unknown-format", art, f"format '{f}' does not exist")
        for k, v in o.items():
            if k.startswith("properties."):
                sub = k[len("properties."):]
                if sub not in cat.kafka_client_keys:
                    ans.add("L2", "unknown-key", art, f"'{k}': {sub} is not a Kafka client property")
                continue
            if k in cat.table_options:
                _check_value(ans, art, k, v, cat.table_options[k], enum_style="table")
                continue
            m = re.fullmatch(r"(?:(key|value)\.)?([a-z-]+)\.(.+)", k)
            if m and m.group(2) in fmts:
                fopts = cat.format_options.get(m.group(2))
                if fopts is not None and m.group(3) not in fopts:
                    ans.add("L2", "unknown-key", art, f"'{k}' is not an option of format {m.group(2)}")
                continue
            ans.add("L2", "unknown-key", art, f"'{k}' is not an option of connector {conn}")
        _column_types(ans, art, t.columns)
        if conn == "kafka" and t.has_primary_key and not (fmts & CHANGELOG_FORMATS):
            # runtime-verified (E7): "The Kafka table ... with 'json' format doesn't support defining PRIMARY KEY
            # constraint on the table, because it can't guarantee the semantic of primary key."
            ans.add("L2", "invalid-value", art, "PRIMARY KEY on an insert-only 'kafka' table is rejected (use upsert-kafka)")
        req = ["topic", "properties.bootstrap.servers"] if conn == "upsert-kafka" else ["properties.bootstrap.servers"]
        if conn == "kafka" and "topic" not in o and "topic-pattern" not in o:
            req.append("topic")
        for k in req:
            if k not in o:
                ans.add("L2", "missing-required-option", art, f"connector {conn} requires '{k}'")
        if conn == "kafka" and not ({"format", "value.format"} & set(o)):
            ans.add("L2", "missing-required-option", art, "connector kafka requires 'format' or 'value.format'")
        if conn == "upsert-kafka":
            for k in ("key.format", "value.format"):
                if k not in o:
                    ans.add("L2", "missing-required-option", art, f"upsert-kafka requires '{k}'")
            if not t.has_primary_key:
                ans.add("L2", "missing-required-option", art, "upsert-kafka requires a PRIMARY KEY")
    ans.tables = tables


CHANGELOG_FORMATS = {"debezium-json", "canal-json", "maxwell-json", "ogg-json", "debezium-avro-confluent"}
# Flink SQL data types (Flink 1.20 "Data Types" and the Calcite-based parser); anything else fails at DDL validation.
SQL_TYPES = {"CHAR", "CHARACTER", "VARCHAR", "STRING", "BOOLEAN", "BINARY", "VARBINARY", "BYTES", "DECIMAL", "DEC",
             "NUMERIC", "TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT", "FLOAT", "REAL", "DOUBLE", "DATE", "TIME",
             "TIMESTAMP", "TIMESTAMP_LTZ", "INTERVAL", "ARRAY", "MAP", "MULTISET", "ROW", "RAW"}


def _split_top(s: str):
    parts, depth, cur, q = [], 0, "", None
    for ch in s:
        if q:
            q = None if ch == q else q
        elif ch in "'`\"":
            q = ch
        elif ch in "(<":
            depth += 1
        elif ch in ")>":
            depth -= 1
        if ch == "," and depth == 0 and not q:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    parts.append(cur)
    return [p.strip() for p in parts if p.strip()]


def _column_types(ans: Answer, art: str, columns: str):
    """v1.2 (found by E7: e.g. columns typed JSON fail with 'Unknown identifier'): every physical or metadata column
    must have a Flink SQL data type; computed columns and constraints are skipped."""
    for part in _split_top(columns):
        if re.match(r"(PRIMARY\s+KEY|WATERMARK|CONSTRAINT)\b", part, re.I):
            continue
        m = re.match(r"(`[^`]+`|\"[^\"]+\"|[A-Za-z_][\w$]*)\s+(.*)", part, re.S)
        if not m:
            continue
        rest = m.group(2).strip()
        if re.match(r"AS\b", rest, re.I):
            continue
        tm = re.match(r"([A-Za-z_]+)", rest)
        if tm and tm.group(1).upper() not in SQL_TYPES:
            ans.add("L2", "invalid-value", art, f"column {m.group(1)}: '{tm.group(1)}' is not a Flink SQL data type")


# ---------------------------------------------------------------- Kubernetes structural schema check
def _schema_walk(ans: Answer, art: str, node, schema: dict, path: str):
    if schema is None:
        return
    if schema.get("x-kubernetes-int-or-string"):
        if not isinstance(node, (int, str)):
            ans.add("L2", "crd-type", art, f"{path}: expected int-or-string")
        return
    t = schema.get("type")
    tmap = {"object": dict, "array": list, "string": str, "boolean": bool}
    if t in tmap and not isinstance(node, tmap[t]):
        if not (t == "string" and isinstance(node, (int, float)) and not isinstance(node, bool)):
            ans.add("L2", "crd-type", art, f"{path}: expected {t}, got {type(node).__name__}")
            return
    if t == "integer" and (isinstance(node, bool) or not isinstance(node, int)):
        ans.add("L2", "crd-type", art, f"{path}: expected integer")
        return
    if t == "number" and (isinstance(node, bool) or not isinstance(node, (int, float))):
        ans.add("L2", "crd-type", art, f"{path}: expected number")
        return
    if "enum" in schema and node not in schema["enum"]:
        ans.add("L2", "crd-enum", art, f"{path}: {node!r} not in {schema['enum'][:8]}")
    if "minimum" in schema and isinstance(node, (int, float)) and node < schema["minimum"]:
        ans.add("L2", "crd-range", art, f"{path}: {node} < {schema['minimum']}")
    if isinstance(node, dict):
        props = schema.get("properties") or {}
        for r in schema.get("required", []):
            if r not in node:
                ans.add("L2", "crd-required", art, f"{path}.{r} is required")
        for k, v in node.items():
            if k in props:
                _schema_walk(ans, art, v, props[k], f"{path}.{k}")
            elif isinstance(schema.get("additionalProperties"), dict):
                _schema_walk(ans, art, v, schema["additionalProperties"], f"{path}.{k}")
            elif schema.get("additionalProperties") is True or schema.get("x-kubernetes-preserve-unknown-fields"):
                pass
            elif props or schema.get("additionalProperties") is False:
                ans.add("L2", "unknown-field", art, f"{path}.{k} is not a field of this resource")
    elif isinstance(node, list) and "items" in schema:
        for i, v in enumerate(node):
            _schema_walk(ans, art, v, schema["items"], f"{path}[{i}]")


def _k8s(ans: Answer, docs: list):
    cat = ans.cat
    kt = [d for d in docs if isinstance(d, dict) and d.get("kind") == "KafkaTopic"]
    fd = [d for d in docs if isinstance(d, dict) and d.get("kind") == "FlinkDeployment"]
    if not kt:
        ans.add("L1", "missing-artefact", "kafkatopic", "no Strimzi KafkaTopic resource found")
    if not fd:
        ans.add("L1", "missing-artefact", "flinkdeployment", "no FlinkDeployment resource found")
    for d in kt + fd:
        kind, api = d.get("kind"), str(d.get("apiVersion", ""))
        schema = cat.crds.get((api, kind))
        art = kind.lower()
        if schema is None:
            known = [a for (a, k) in cat.crds if k == kind]
            ans.add("L3", "unsupported-api-version", art, f"{kind} apiVersion {api!r} not served (served: {known})")
            continue
        meta = d.get("metadata") or {}
        _schema_walk(ans, art, {k: v for k, v in d.items() if k not in ("apiVersion", "kind", "metadata")}, schema, kind)
        if kind == "KafkaTopic":
            spec = d.get("spec") if isinstance(d.get("spec"), dict) else {}
            meta = meta if isinstance(meta, dict) else {}
            name = spec.get("topicName") or meta.get("name")
            cfg = spec.get("config") if isinstance(spec.get("config"), dict) else {}
            for k, v in cfg.items():
                _check_topic_config(ans, art, str(k), v)
            ans.topics[name] = {"partitions": spec.get("partitions"), "replication-factor": spec.get("replicas"),
                                "configs": {str(k): v for k, v in cfg.items()}}
            labels = meta.get("labels") if isinstance(meta.get("labels"), dict) else {}
            ans.topic_labels[name] = labels.get("strimzi.io/cluster")
    if fd:
        spec = fd[0].get("spec") if isinstance(fd[0].get("spec"), dict) else {}
        conf = spec.get("flinkConfiguration") or {}
        conf = flatten(conf) if isinstance(conf, dict) else {}
        _flink_conf(ans, conf, "flinkdeployment", k8s=True)
        job = spec.get("job") if isinstance(spec.get("job"), dict) else {}
        tm = spec.get("taskManager") if isinstance(spec.get("taskManager"), dict) else {}
        ans.k8s_parallelism = parse_int(job.get("parallelism"))
        ans.k8s_tm = tm.get("resource") if isinstance(tm.get("resource"), dict) else {}
        ans.k8s_flink_version = spec.get("flinkVersion")
        if ans.k8s_flink_version and ans.k8s_flink_version != "v1_20":
            ans.add("L3", "version-mismatch", "flinkdeployment", f"flinkVersion {ans.k8s_flink_version} but the cluster runs Flink 1.20")
        if len(fd) > 1:
            ans.add("warn", "ambiguous", "flinkdeployment", f"{len(fd)} FlinkDeployment resources; the first is evaluated")
        _operator_admission(ans, fd[0], spec, job, tm)


# ------------------------------------------------------------------ Flink Kubernetes Operator 1.13 admission (v1.3)
DEPLOYMENT_NAME = re.compile(r"[a-z]([-a-z\d]{0,43}[a-z\d])?")
FORBIDDEN_OPERATOR_KEYS = ("kubernetes.namespace", "kubernetes.cluster-id", "high-availability.cluster-id")
# (key, fallback keys, minimum, maximum) of DefaultValidator.validateAutoScalerFlinkConfiguration; None = unbounded,
# "target" = the effective job.autoscaler.utilization.target
AUTOSCALER_RANGES = [
    ("job.autoscaler.scale-down.max-factor", 0.0, None), ("job.autoscaler.scale-up.max-factor", 0.0, None),
    ("job.autoscaler.utilization.target", 0.0, 1.0), ("job.autoscaler.target.utilization.boundary", 0.0, None),
    ("job.autoscaler.utilization.max", "target", 1.0), ("job.autoscaler.utilization.min", 0.0, "target"),
    ("job.autoscaler.observed-scalability.coefficient-min", 0.01, 1.0)]


def _operator_get(ans: Answer, key: str):
    """Value of an operator option given under its key or one of its fallback keys (None if absent)."""
    keys = [key] + ans.cat.operator_fallbacks.get(key, [])
    return next((ans.flink_raw[k] for k in keys if k in ans.flink_raw), None)


def _operator_admission(ans: Answer, doc: dict, spec: dict, job: dict, tm: dict):
    """Admission checks of the Flink Kubernetes Operator 1.13 (DefaultValidator.validateDeployment, release-1.13
    source) that can be decided from the FlinkDeployment alone; the operator's default configuration (Helm chart
    conf/flink-conf.yaml) sets only the slot count and the default parallelism. A violation means that the operator
    rejects the resource. Added in v1.3 after an expert review of the reference answers."""
    art = "flinkdeployment"

    def bad(msg):
        ans.add("L2", "operator-rejects", art, msg)

    meta = doc.get("metadata") if isinstance(doc.get("metadata"), dict) else {}
    labels = meta.get("labels") if isinstance(meta.get("labels"), dict) else {}
    if str(labels.get("flink.apache.org/canary", "")).lower() == "true":
        return  # canary resources are handled by the operator's health check before any validation
    name = str(meta.get("name", ""))
    if not DEPLOYMENT_NAME.fullmatch(name):
        bad(f"FlinkDeployment name {name!r} is invalid (lower-case alphanumerics or '-', at most 45 characters)")
    if not spec.get("flinkVersion"):
        bad("spec.flinkVersion must be defined")
    if spec.get("serviceAccount") is None:
        bad("spec.serviceAccount must be defined")
    for k in FORBIDDEN_OPERATOR_KEYS:
        if k in ans.flink_raw:
            bad(f"forbidden Flink configuration key {k}")

    def true(v):
        return str(v).strip().lower() == "true"
    ha = str(ans.flink_get("high-availability.type") or "NONE").strip().lower()
    if true(_operator_get(ans, "kubernetes.operator.deployment.rollback.enabled")) and ha in ("none", ""):
        bad("HA must be enabled for rollback support")
    rec = _operator_get(ans, "kubernetes.operator.jm-deployment-recovery.enabled")
    if true(_operator_get(ans, "kubernetes.operator.cluster.health-check.enabled")) and rec is not None and not true(rec):
        bad("deployment recovery must be enabled for the job health check")
    if job:
        mode = str(job.get("upgradeMode", "stateless")).strip().lower()
        has_ckpt = ans.flink_get("execution.checkpointing.dir") not in (None, "")
        has_sp = ans.flink_get("execution.checkpointing.savepoint-dir") not in (None, "")
        if mode != "stateless" and not has_ckpt:
            bad(f"a checkpoint directory (execution.checkpointing.dir) is required for upgradeMode {mode}")
        if not has_sp:
            if mode == "savepoint":
                bad("upgradeMode savepoint requires execution.checkpointing.savepoint-dir")
            elif job.get("savepointTriggerNonce") is not None:
                bad("a manual savepoint trigger requires execution.checkpointing.savepoint-dir")
            elif _operator_get(ans, "kubernetes.operator.periodic.savepoint.interval") is not None:
                bad("periodic savepoints require execution.checkpointing.savepoint-dir")
            elif _operator_get(ans, "kubernetes.operator.job.upgrade.last-state.max.allowed.checkpoint.age") is not None:
                bad("the last-state checkpoint age limit requires execution.checkpointing.savepoint-dir")
        replicas = tm.get("replicas") if isinstance(tm, dict) else None
        if replicas is not None and (parse_int(replicas) or 0) < 1:
            bad("TaskManager replicas must be larger than 0")
        elif replicas is None and job.get("parallelism") is not None and (parse_int(job.get("parallelism")) or 0) < 1:
            bad("job parallelism must be larger than 0")
    for comp, prefix, alt in (("jobManager", "jobmanager", ("jobmanager.memory.process.size", "jobmanager.memory.flink.size",
                                                             "jobmanager.memory.heap.size")),
                              ("taskManager", "taskmanager", ("taskmanager.memory.process.size",
                                                              "taskmanager.memory.flink.size"))):
        c = spec.get(comp) if isinstance(spec.get(comp), dict) else {}
        res = c.get("resource") if isinstance(c.get("resource"), dict) else {}
        mem = res.get("memory")
        if mem is not None and parse_memory_bytes(mem, k8s=True) is None:
            bad(f"{comp} resource memory {mem!r} cannot be parsed")
        defined = mem not in (None, "") or any(k in ans.flink for k in alt) or (
            prefix == "taskmanager" and "taskmanager.memory.task.heap.size" in ans.flink
            and "taskmanager.memory.managed.size" in ans.flink)
        if not defined:
            bad(f"{comp} memory must be defined (spec.{comp}.resource.memory or the {prefix}.memory options)")
    if true(_operator_get(ans, "job.autoscaler.enabled")):
        target = _operator_get(ans, "job.autoscaler.utilization.target")
        try:
            target = float(target) if target is not None else 0.7
        except ValueError:
            target = None
        for key, lo, hi in AUTOSCALER_RANGES:
            v = _operator_get(ans, key)
            if v is None:
                continue
            lo = target if lo == "target" else lo
            hi = target if hi == "target" else hi
            try:
                x = float(v)
            except ValueError:
                bad(f"invalid value in the autoscaler option {key}")
                continue
            if (lo is not None and x < lo) or (hi is not None and x > hi):
                bad(f"autoscaler option {key}={v} outside [{lo}, {hi}]")


# =================================================================================== extraction
def extract(ans: Answer, finish_reason: str | None):
    if finish_reason == "length":
        ans.add("L1", "truncated", "answer", "generation stopped at the token limit")
    blocks = ans.blocks
    if any(not b.closed for b in blocks) and finish_reason != "length":
        ans.add("L1", "syntax", "answer", "unterminated code fence")
    sql_blocks = [b for b in blocks if b.lang == "sql" or re.search(r"\bCREATE\s+(TEMPORARY\s+)?TABLE\b", b.body, re.I)]
    sh_blocks = [b for b in blocks if b not in sql_blocks and re.search(r"kafka-topics", b.body)]
    yaml_blocks = [b for b in blocks if b not in sql_blocks and b not in sh_blocks
                   and (b.lang in ("yaml", "yml") or (b.lang == "" and ":" in b.body))]
    if ans.track == "native":
        if sh_blocks:
            _native_topics(ans, "\n".join(b.body for b in sh_blocks))
        else:
            ans.add("L1", "missing-artefact", "topics", "no code block with kafka-topics commands")
        conf_done = False
        for b in yaml_blocks:
            docs, dups, err = load_yaml_docs(b.body)
            if err:
                ans.add("L1", "syntax", "flink-conf", err)
                conf_done = True
                break
            if docs and isinstance(docs[0], dict):
                ans.duplicates += len(dups)
                for d in dups:
                    ans.add("warn", "duplicate-key", "flink-conf", f"duplicate YAML key {d}")
                _flink_conf(ans, flatten(docs[0]), "flink-conf", k8s=False)
                conf_done = True
                if len([x for x in yaml_blocks]) > 1:
                    ans.add("warn", "ambiguous", "flink-conf", f"{len(yaml_blocks)} YAML blocks; the first mapping is evaluated")
                break
        if not conf_done:
            ans.add("L1", "missing-artefact", "flink-conf", "no YAML Flink configuration block")
    else:
        docs_all = []
        for b in yaml_blocks:
            docs, dups, err = load_yaml_docs(b.body)
            if err:
                ans.add("L1", "syntax", "k8s-yaml", err)
            ans.duplicates += len(dups)
            for d in dups:
                ans.add("warn", "duplicate-key", "k8s-yaml", f"duplicate YAML key {d}")
            docs_all += [d for d in docs if isinstance(d, dict)]
        _k8s(ans, docs_all)
    if sql_blocks:
        sql = strip_sql_comments("\n".join(b.body for b in sql_blocks))  # v1.3: commented-out SET/DDL is ignored
        # Flink SQL scripts may configure the job with SET 'key' = 'value'; these apply on top of the cluster config
        sets = dict(re.findall(r"^\s*SET\s+'([^']+)'\s*=\s*'([^']*)'\s*;", sql, re.M | re.I))
        if sets:
            _flink_conf(ans, sets, "sql-set", k8s=ans.track == "k8s")
        _sql(ans, sql)
    else:
        ans.add("L1", "missing-artefact", "sql", "no SQL block with CREATE TABLE statements")


# =================================================================================== L4 expectations
def _cmp(a, op, b) -> bool:
    if op == "set":
        return a is not None and str(a).strip() != ""
    if a is None:
        return False
    if op == "==":
        return a == b
    if op == "!=":
        return a != b
    if op == "<=":
        return a <= b
    if op == ">=":
        return a >= b
    if op == "<":
        return a < b
    if op == ">":
        return a > b
    if op == "in":
        return a in b
    raise ValueError(op)


def _norm_flink(cat: Catalog, key: str, v):
    if v is None:
        return None
    opt = cat.flink_option(key) or {}
    t = opt.get("type") or ""
    if t == "java.time.Duration":
        return parse_duration_ms(v)
    if t.endswith("MemorySize"):
        return parse_memory_bytes(v)
    if t in ("java.lang.Integer", "java.lang.Long"):
        return parse_int(v)
    if t in ("java.lang.Float", "java.lang.Double"):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None
    if t == "java.lang.Boolean":
        return str(v).strip().lower() == "true"
    return str(v).strip().lower()


def _norm_topic(key: str, v):
    if v is None:
        return None
    if key.endswith(".ms") or key.endswith(".bytes") or key in ("min.insync.replicas", "partitions",
                                                                 "replication-factor", "max.message.bytes"):
        return parse_int(v)
    return str(v).strip().lower()


def _norm_value(v):
    return v.lower() if isinstance(v, str) else v


def evaluate_expectation(ans: Answer, e: dict) -> tuple[bool, str]:
    cat, t = ans.cat, e["type"]
    if t == "topic_exists":
        ok = e["topic"] in ans.topics
        return ok, "" if ok else f"topic {e['topic']} is not created"
    if t.startswith("topic_") and e["topic"] not in ans.topics:
        return False, f"topic {e['topic']} is not created"
    if t == "topic_partitions_min":
        v = ans.topic_get(e["topic"], "partitions")
        return (v is not None and v >= e["value"]), f"partitions={v}, required>={e['value']}"
    if t == "topic_rf":
        v = ans.topic_get(e["topic"], "replication-factor")
        ok = v is not None and v >= e.get("min", 1) and v <= e.get("max", 10**9)
        return ok, f"replication factor={v}, required in [{e.get('min', 1)},{e.get('max', '∞')}]"
    if t == "topic_config":
        v = _norm_topic(e["key"], ans.topic_get(e["topic"], e["key"]))
        return _cmp(v, e["op"], _norm_value(e.get("value"))), f"{e['topic']}:{e['key']}={v} {e['op']} {e.get('value')}"
    if t == "topic_label":
        v = ans.topic_labels.get(e["topic"])
        return v == e["value"], f"strimzi.io/cluster={v}"
    if t == "flink":
        raw = ans.flink_get(e["key"])
        v = _norm_flink(cat, e["key"], raw)
        target = _norm_flink(cat, e["key"], e.get("value")) if e.get("value") is not None else None
        if e["op"] == "in":
            target = [_norm_flink(cat, e["key"], x) for x in e["value"]]
        return _cmp(v, e["op"], target), f"{e['key']}={raw!r} {e['op']} {e.get('value')!r}"
    if t == "flink_prefix":  # e.g. checkpoint directory on durable storage
        raw = ans.flink_get(e["key"])
        ok = raw is not None and str(raw).startswith(tuple(e["prefixes"]))
        return ok, f"{e['key']}={raw!r}"
    if t in ("table", "table_connector", "table_pk", "eos_sink", "txn_timeout"):
        tb = ans.table_for(e["topic"], e.get("role"))
        if tb is None:
            return False, f"no CREATE TABLE reads/writes topic {e['topic']}"
        o = tb.options
        if t == "table_connector":
            return o.get("connector") in e["value"], f"connector={o.get('connector')}"
        if t == "table_pk":
            return tb.has_primary_key, "PRIMARY KEY present" if tb.has_primary_key else "no PRIMARY KEY"
        if t == "table":
            v = o.get(e["key"])
            if v is None and e.get("default") is not None:
                v = e["default"]
            if e["op"] in ("<=", ">=", "<", ">"):
                vv = parse_duration_ms(v) if e.get("unit") == "duration" else parse_int(v)
                return _cmp(vv, e["op"], e["value"]), f"{e['key']}={v!r} {e['op']} {e['value']}"
            vn = str(v).strip().lower().replace("_", "-") if v is not None else None
            tv = e.get("value")
            tv = [str(x).lower() for x in tv] if isinstance(tv, list) else (str(tv).lower() if tv is not None else None)
            return _cmp(vn, e["op"], tv), f"{e['key']}={v!r} {e['op']} {e.get('value')!r}"
        if t == "eos_sink":
            dg = str(o.get("sink.delivery-guarantee", "at-least-once")).lower().replace("_", "-")
            ok = dg == "exactly-once" and bool(o.get("sink.transactional-id-prefix"))
            return ok, f"sink.delivery-guarantee={dg}, transactional-id-prefix={o.get('sink.transactional-id-prefix')!r}"
        if t == "txn_timeout":
            raw = o.get("properties.transaction.timeout.ms")
            v = parse_int(raw) if raw is not None else FLINK_KAFKA_SINK_DEFAULT_TXN_MS
            interval = parse_duration_ms(ans.flink_get("execution.checkpointing.interval"))
            ok = v is not None and v <= BROKER_TXN_MAX_TIMEOUT_MS and (interval is None or v > interval)
            return ok, (f"transaction.timeout.ms={'(default) ' if raw is None else ''}{v}, broker max "
                        f"{BROKER_TXN_MAX_TIMEOUT_MS}, checkpoint interval {interval}")
    if t == "any_of":
        res = [evaluate_expectation(ans, dict(sub, kind=e["kind"], category=e["category"])) for sub in e["options"]]
        return any(r[0] for r in res), " | ".join(r[1] for r in res)
    if t == "table_watermark":
        tb = ans.table_for(e["topic"])
        if tb is None:
            return False, f"no CREATE TABLE reads topic {e['topic']}"
        return tb.has_watermark, "WATERMARK defined" if tb.has_watermark else "no WATERMARK clause"
    if t == "table_any":  # first present key among e['keys'] compared with value
        tb = ans.table_for(e["topic"], e.get("role"))
        if tb is None:
            return False, f"no CREATE TABLE reads/writes topic {e['topic']}"
        v = next((tb.options[k] for k in e["keys"] if k in tb.options), None)
        return (v is not None and v.lower() == str(e["value"]).lower()), f"{'/'.join(e['keys'])}={v!r}"
    if t == "distinct_option":
        tbs = [tb for tb in ans.tables if tb.options.get("topic") == e["topic"]]
        vals = [tb.options.get(e["key"]) for tb in tbs]
        ok = len(tbs) >= e.get("min_tables", 2) and None not in vals and len(set(vals)) == len(vals)
        return ok, f"{len(tbs)} tables on {e['topic']} with {e['key']}={vals}"
    if t == "tables_reference_topics":
        missing = sorted({tb.options.get("topic") for tb in ans.tables
                          if tb.options.get("connector") in ("kafka", "upsert-kafka") and tb.options.get("topic")
                          and tb.options.get("topic") not in ans.topics})
        return not missing, f"tables use topics that are not created: {missing}" if missing else ""
    if t == "parallelism_min":
        p = ans.parallelism()
        return p >= e["value"], f"effective parallelism={p}, required>={e['value']}"
    if t == "slots_capacity":
        slots = parse_int(ans.flink_get("taskmanager.numberOfTaskSlots")) or 1
        p = ans.parallelism()
        if ans.track == "k8s":
            pods = math.ceil(p / slots)
            return pods <= e["tms"], f"parallelism {p} / {slots} slots per TM = {pods} TaskManager pods (max {e['tms']})"
        cap = e["tms"] * slots
        return p <= cap, f"parallelism {p} vs {e['tms']} TMs x {slots} slots = {cap}"
    if t == "slots_per_core":
        slots = parse_int(ans.flink_get("taskmanager.numberOfTaskSlots")) or 1
        return slots <= e["cores"], f"{slots} slots per TaskManager on {e['cores']} cores"
    if t == "tm_memory_max":
        if ans.track == "k8s":
            raw = ans.k8s_tm.get("memory")
            v = parse_memory_bytes(raw, k8s=True)
        else:
            raw = ans.flink_get("taskmanager.memory.process.size")
            v = parse_memory_bytes(raw)
        return (v is None or v <= e["bytes"]), f"TaskManager memory={raw!r} (max {e['bytes'] / 2**30:.0f} GiB)"
    if t == "tm_cpu_max":
        raw = ans.k8s_tm.get("cpu")
        try:
            v = float(str(raw).rstrip("m")) / (1000 if str(raw).endswith("m") else 1) if raw is not None else None
        except ValueError:
            v = None
        return (v is None or v <= e["cores"]), f"TaskManager cpu={raw!r} (max {e['cores']})"
    raise ValueError(f"unknown expectation type {t}")


# =================================================================================== runtime-derived rule (v1.1)
def _source_group_id(ans: Answer, scenario: dict):
    """A Kafka SQL source in 'group-offsets' startup mode (the connector default) without properties.group.id fails at
    job start with flink-sql-connector-kafka 3.3.0-1.20 ("Property group.id is required when using committed offset
    for offsets initializer"), although the documentation states that an id is generated. Found by the executable
    validation (E7) and added as validator v1.1; sources are the scenario's source topics."""
    sources = {e["topic"] for e in scenario["expect"] if e.get("type") == "table_connector" and e.get("role") == "source"}
    for t in ans.tables:
        o = t.options
        if o.get("connector") != "kafka" or o.get("topic") not in sources:
            continue
        mode = str(o.get("scan.startup.mode", "group-offsets")).strip().lower()
        if mode == "group-offsets" and not o.get("properties.group.id"):
            ans.add("L2", "missing-required-option", f"sql:{t.name}",
                    "Kafka source in group-offsets startup mode (the default) requires 'properties.group.id' "
                    "(runtime-verified with connector 3.3.0-1.20)")


VALIDATOR_VERSION = "1.3"


# =================================================================================== entry point
def validate(text: str, scenario: dict, finish_reason: str | None = None, cat: Catalog | None = None) -> dict:
    cat = cat or get_catalog()
    ans = Answer(text or "", scenario["track"], cat)
    try:
        extract(ans, finish_reason)
        _source_group_id(ans, scenario)
    except Exception as ex:  # structurally malformed artefact: record, never crash
        ans.add("L2", "malformed-structure", "answer", f"{type(ex).__name__}: {ex}")
    lv = {lvl: not any(f.level == lvl for f in ans.findings) for lvl in ("L1", "L2", "L3")}
    exp_res = {}
    for e in scenario["expect"]:
        try:
            ok, msg = evaluate_expectation(ans, e)
        except Exception as ex:  # a malformed answer must never crash the validator
            ok, msg = False, f"evaluation error: {type(ex).__name__}: {ex}"
        exp_res[e["id"]] = {"ok": bool(ok), "kind": e["kind"], "category": e["category"], "msg": msg}
    l4 = all(r["ok"] for r in exp_res.values() if r["kind"] in ("correctness", "resource"))
    out = {
        "L1": lv["L1"],
        "L2": lv["L1"] and lv["L2"],
        "L3": lv["L1"] and lv["L2"] and lv["L3"],
    }
    out["L4"] = out["L3"] and l4
    out["valid"] = out["L4"]
    out["requirements_ok"] = l4  # requirement satisfaction regardless of L1–L3 (diagnostic only)
    out["policy_ok"] = all(r["ok"] for r in exp_res.values() if r["kind"] == "policy")
    out["expectations"] = exp_res
    out["findings"] = [asdict(f) for f in ans.findings]
    out["n_unknown_keys"] = sum(f.category in ("unknown-key", "unknown-cli-option", "unknown-field", "misplaced-key",
                                               "broker-key-as-topic-config", "unknown-connector", "unknown-format")
                                for f in ans.findings)
    out["n_deprecated"] = sum(f.category == "deprecated-key" for f in ans.findings)
    out["n_duplicate_keys"] = ans.duplicates
    out["truncated"] = finish_reason == "length"
    out["validator_version"] = VALIDATOR_VERSION
    return out
