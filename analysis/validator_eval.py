"""Evaluation of the static validator itself (reviewer 3, M2: coverage and false positives).

Four parts, all deterministic:
  A. reference answers (bench/kfbench-v1-reference.jsonl): every scenario has an answer that passes L1-L4;
  B. semantics-preserving rewrites of every reference answer (other accepted spellings of the same configuration:
     flat instead of nested config.yaml keys, other duration and memory units, comments, unquoted identifiers,
     lower-case SQL keywords, reordered CLI options, string-typed Strimzi config values, enum case); each rewrite must
     remain valid -> false rejections caused by surface form;
  C. fault injection: single-defect mutants of every reference answer, one defect class at a time (format/syntax,
     schema, version, requirements); each mutant must be rejected -> detection per defect class;
  D. official artefacts of the target versions, which are valid by construction except where noted: the Flink 1.20.5
     distribution's config.yaml (active keys and the keys of its commented examples), the kafka-topics.sh --create
     commands of the Kafka 4.1.2 documentation, the KafkaTopic examples of Strimzi 0.48.0, the FlinkDeployment
     examples of Flink Kubernetes Operator 1.13 and the CREATE TABLE examples of the Flink Kafka connector 3.3
     documentation (catalog/official-examples/). Each artefact is inserted into a reference answer and only the
     L1-L3 findings attributed to it are counted; every rejection is listed with its reason.

  python analysis/validator_eval.py                 # -> results/validator_eval.json
  python analysis/validator_eval.py --src <dir with the previous kfbench package> --out results/validator_eval_v1.2.json
"""
from __future__ import annotations

import collections
import copy
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# --src DIR evaluates another copy of the validator package (e.g. the previous version) and --out names the output
_args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
sys.path.insert(0, _args.get("--src", str(ROOT / "src")))
import yaml  # noqa: E402

from kfbench.parse import code_blocks, flatten  # noqa: E402
from kfbench.validate import VALIDATOR_VERSION, validate  # noqa: E402

BENCH = {json.loads(l)["id"]: json.loads(l) for l in (ROOT / "bench/kfbench-v1.jsonl").open()}
REF = {json.loads(l)["id"]: json.loads(l)["text"] for l in (ROOT / "bench/kfbench-v1-reference.jsonl").open()}
EX = ROOT / "catalog/official-examples"


def first_failing(r: dict) -> str | None:
    for lvl in ("L1", "L2", "L3", "L4"):
        if not r[lvl]:
            return lvl
    return None


def blocks(text):
    """(lang, body, start, end) of each fenced block."""
    return [(m.group(1).lower(), m.group(2), m.start(2), m.end(2))
            for m in re.finditer(r"```([A-Za-z0-9_+-]*)[^\n]*\n(.*?)```", text, re.S)]


def replace_block(text, idx, new_body):
    b = blocks(text)[idx]
    return text[:b[2]] + new_body + text[b[3]:]


def block_index(text, kind):
    """Index of the topics (bash / KafkaTopic), flink (config.yaml / FlinkDeployment) or sql block."""
    for i, (lang, body, _, _) in enumerate(blocks(text)):
        if kind == "sql" and lang == "sql":
            return i
        if kind == "topics" and (lang == "bash" or "kind: KafkaTopic" in body):
            return i
        if kind == "flink" and lang == "yaml" and "KafkaTopic" not in body:
            return i
    return None


def sub1(pattern, repl, text, flags=0):
    """Replace the first match only; None if the pattern does not occur (operator not applicable)."""
    new, n = re.subn(pattern, repl, text, count=1, flags=flags)
    return new if n and new != text else None


def expect_types(sc):
    return {e["type"] for e in sc["expect"]}


def has_expect(sc, pred):
    return any(pred(e) for e in sc["expect"])


def eos(sc):
    return has_expect(sc, lambda e: e["type"] == "eos_sink")


def sink_topic(sc):
    return next(e["topic"] for e in sc["expect"] if e["type"] in ("table_connector", "table", "eos_sink")
                and e.get("role") == "sink")


# =========================================================================================== B. neutral rewrites
def nr_flat_config(t, sc):
    if sc["track"] != "native":
        return None
    i = block_index(t, "flink")
    d = flatten(yaml.safe_load(blocks(t)[i][1]))
    return replace_block(t, i, "\n".join(f"{k}: {v}" for k, v in d.items()) + "\n")


def nr_duration_units(t, sc):
    def conv(m):
        n, u = int(m.group(1)), m.group(2)
        ms = n * {"ms": 1, "s": 1000, "min": 60000, "h": 3600000}[u]
        return f"{ms}ms" if ms < 1000 else (f"{ms // 1000}s" if ms % 1000 == 0 else f"{ms}ms")
    return re.sub(r"\b(\d+) (ms|s|min|h)\b", conv, t) if re.search(r"\b\d+ (ms|s|min|h)\b", t) else None


def nr_memory_units(t, sc):
    return sub1(r"\b8g\b", "8192m", t) or sub1(r'"8Gi"', '"8192Mi"', t)


def nr_comments(t, sc):
    out = t
    for i, (lang, body, _, _) in reversed(list(enumerate(blocks(t)))):
        c = {"bash": "# create the topics\n", "yaml": "# generated configuration\n", "sql": "-- source and sink tables\n"}[lang]
        out = replace_block(out, i, c + body)
    return out


def nr_unquoted_identifiers(t, sc):
    i = block_index(t, "sql")
    return replace_block(t, i, blocks(t)[i][1].replace("`", ""))


def nr_sql_keyword_case(t, sc):
    i = block_index(t, "sql")
    body = blocks(t)[i][1]
    for kw in ("CREATE TABLE", "WITH (", "PRIMARY KEY", "NOT ENFORCED", "WATERMARK FOR", "INTERVAL", "STRING",
               "TIMESTAMP(3)", "DOUBLE", "BIGINT"):
        body = body.replace(kw, kw.lower())
    return replace_block(t, i, body)


def nr_cli_order(t, sc):
    if sc["track"] != "native":
        return None
    return re.sub(r"--bootstrap-server (\S+) --create --topic (\S+)", r"--create --topic \2 --bootstrap-server \1", t)


def nr_cli_equals(t, sc):
    if sc["track"] != "native":
        return None
    return re.sub(r"--(partitions|replication-factor|topic) (\S+)", r"--\1=\2", t)


def nr_strimzi_string_values(t, sc):
    """Strimzi KafkaTopic spec.config accepts strings for numeric topic configs (x-kubernetes-preserve-unknown-fields)."""
    if sc["track"] != "k8s":
        return None
    i = block_index(t, "topics")
    body = blocks(t)[i][1]
    new = re.sub(r"(\n {6}[a-z.]+): (\d+)(?=\n)", r'\1: "\2"', body)
    return replace_block(t, i, new) if new != body else None


def nr_enum_case(t, sc):
    return sub1(r"\b(EXACTLY_ONCE|AT_LEAST_ONCE)\b", lambda m: m.group(1).lower(), t)


def nr_deprecated_alias(t, sc):
    """Deprecated but still supported alias of Flink 1.20 (reported as a warning, never as a failure)."""
    if sc["track"] == "native":
        t = nr_flat_config(t, sc)  # the nested form cannot rename one key in place
    return sub1(r"execution\.checkpointing\.dir", "state.checkpoints.dir", t)


NEUTRAL = {"flat config.yaml keys": nr_flat_config, "other duration units": nr_duration_units,
           "other memory units": nr_memory_units, "comments in every block": nr_comments,
           "unquoted SQL identifiers": nr_unquoted_identifiers, "lower-case SQL keywords": nr_sql_keyword_case,
           "reordered CLI options": nr_cli_order, "CLI options as --opt=value": nr_cli_equals,
           "string-typed Strimzi config values": nr_strimzi_string_values, "lower-case enum values": nr_enum_case,
           "deprecated Flink alias": nr_deprecated_alias}


# =========================================================================================== C. fault injection
def m_drop_sql(t, sc):
    i = block_index(t, "sql")
    b = blocks(t)[i]
    return t[:b[2] - 7] + t[b[3] + 3:]


def m_yaml_syntax(t, sc):
    i = block_index(t, "flink")
    return replace_block(t, i, blocks(t)[i][1] + "  broken: [unclosed\n")


def m_truncated(t, sc):
    return ("TRUNC", t[: int(len(t) * 0.6)])


def m_sql_unbalanced(t, sc):
    return sub1(r"\n\);", "\n;", t)


def m_flink_key_typo(t, sc):
    for a, b in (("execution.checkpointing.interval", "execution.checkpointing.interval.ms"),
                 ("interval: ", "interval-ms: "),
                 ("taskmanager.numberOfTaskSlots", "taskmanager.numberOfSlots"), ("numberOfTaskSlots", "numberOfSlots")):
        r = sub1(re.escape(a), b, t)
        if r:
            return r
    return None


def m_topic_key_typo(t, sc):
    return sub1(r"retention\.ms", "retention.time.ms", t)


def m_broker_key(t, sc):
    return sub1(r"(?<![\w.])retention\.ms", "log.retention.ms", t)


def m_value_type(t, sc):
    return sub1(r"(interval: \"?)\d+ ?(ms|s)\b", r"\1ten seconds", t)


def m_enum_value(t, sc):
    return sub1(r"\b(EXACTLY_ONCE|AT_LEAST_ONCE)\b", "EXACTLY_ONE", t)


def m_out_of_range(t, sc):
    return sub1(r"min\.insync\.replicas[=:] ?\d", lambda m: m.group(0)[:-1] + "0", t) or \
        sub1(r"(--partitions |partitions: )\d+", r"\g<1>0", t)


def m_connector_option(t, sc):
    return sub1(r"'sink\.delivery-guarantee'", "'sink.semantic'", t)


def m_connector_name(t, sc):
    return sub1(r"'connector' = 'kafka'", "'connector' = 'kafka-0.11'", t)


def m_format(t, sc):
    return sub1(r"'format' = 'json'", "'format' = 'jsonl'", t)


def m_client_property(t, sc):
    return sub1(r"'properties\.auto\.offset\.reset'", "'properties.auto.offset.reset.policy'", t)


def m_crd_field(t, sc):
    if sc["track"] != "k8s":
        return None
    return sub1(r"\n  replicas: ", "\n  replication: ", t)


def m_crd_type(t, sc):
    if sc["track"] != "k8s":
        return None
    return sub1(r"\n    parallelism: \d+", "\n    parallelism: auto", t)


def m_misplaced(t, sc):
    i = block_index(t, "flink")
    body = blocks(t)[i][1]
    if sc["track"] == "native":
        return replace_block(t, i, body + "sink.delivery-guarantee: exactly-once\n")
    return sub1(r"(flinkConfiguration:\n)", r'\1    sink.delivery-guarantee: "exactly-once"\n', t)


def m_cli_option(t, sc):
    if sc["track"] != "native":
        return None
    return sub1(r"--replication-factor", "--replicas", t)


def m_missing_required(t, sc):
    return sub1(r"\n  'properties\.bootstrap\.servers' = '[^']*',", "", t)


def m_group_id(t, sc):
    return sub1(r"\n  'properties\.group\.id' = '[^']*',", "", t)


def m_zookeeper(t, sc):
    if sc["track"] != "native":
        return None
    return sub1(r"--bootstrap-server kafka:9092", "--zookeeper zookeeper:2181", t)


def m_api_version(t, sc):
    if sc["track"] != "k8s":
        return None
    # Strimzi 0.48 serves KafkaTopic v1beta2, v1beta1 and v1alpha1; "v1" is not served by this version
    return sub1(r"kafka\.strimzi\.io/v1beta2", "kafka.strimzi.io/v1", t)


def m_flink_version(t, sc):
    if sc["track"] != "k8s":
        return None
    return sub1(r"flinkVersion: v1_20", "flinkVersion: v1_17", t)


def m_removed_topic_config(t, sc):
    """message.format.version was removed in Kafka 4.0 (KIP-724)."""
    if sc["track"] == "native":
        return sub1(r"(--partitions \d+ --replication-factor \d+)", r"\1 --config message.format.version=2.8", t)
    return sub1(r"(\n  config:)", r'\1\n      message.format.version: "2.8"', t)


# --- requirement (L4) defects -------------------------------------------------------------------------------------
def m_retention(t, sc):
    if not has_expect(sc, lambda e: e.get("key") == "retention.ms"):
        return None
    return sub1(r"(retention\.ms[=:] ?)(\d+)", lambda m: m.group(1) + str(int(m.group(2)) // 2), t)


def m_min_isr_removed(t, sc):
    if not has_expect(sc, lambda e: e.get("key") == "min.insync.replicas"):
        return None
    return sub1(r" \\\n  --config min\.insync\.replicas=2", "", t) or sub1(r"\n      min\.insync\.replicas: 2", "", t)


def m_rf_one(t, sc):
    top = next((e["topic"] for e in sc["expect"] if e["type"] == "topic_rf" and e.get("min")), None)
    if top is None:
        return None
    if sc["track"] == "native":
        return sub1(rf"(--topic {re.escape(top)} --partitions \d+ --replication-factor )3", r"\g<1>1", t)
    return sub1(rf"(name: {re.escape(top)}\n(?:.*\n){{4}}  replicas: )3", r"\g<1>1", t)


def m_rf_above_brokers(t, sc):
    return sub1(r"(--replication-factor |replicas: )3", r"\g<1>4", t)


def m_eos_downgrade(t, sc):
    if not eos(sc):
        return None
    return sub1(r"'sink\.delivery-guarantee' = 'exactly-once'", "'sink.delivery-guarantee' = 'at-least-once'", t)


def m_no_txn_prefix(t, sc):
    if not eos(sc):
        return None
    return sub1(r",\n  'sink\.transactional-id-prefix' = '[^']*'", "", t) or \
        sub1(r",\s*'sink\.transactional-id-prefix'\s*=\s*'[^']*'", "", t)


def m_txn_timeout(t, sc):
    if not eos(sc):
        return None
    return sub1(r"'properties\.transaction\.timeout\.ms'\s*=\s*'600000'", "'properties.transaction.timeout.ms' = '3600000'", t)


def m_ckpt_mode(t, sc):
    if not eos(sc):
        return None
    return sub1(r"EXACTLY_ONCE", "AT_LEAST_ONCE", t)


def m_ckpt_interval_slo(t, sc):
    e = next((e for e in sc["expect"] if e["id"] == "eos_latency" or (e.get("key") == "execution.checkpointing.interval"
                                                                       and e.get("op") == "<=")), None)
    if e is None:
        return None
    return sub1(r"(interval: \"?)(\d+) ?(ms|s)\b", lambda m: m.group(1) + str(int(m.group(2)) * 10) + " " + m.group(3), t)


def m_no_checkpointing(t, sc):
    return sub1(r"\n\s*interval: [^\n]*", "", t) or sub1(r"\n\s*execution\.checkpointing\.interval: [^\n]*", "", t)


def m_partitions_low(t, sc):
    need = max([e["value"] for e in sc["expect"] if e["type"] == "topic_partitions_min"] or [1])
    if need <= 1:
        return None
    first = next(e["topic"] for e in sc["expect"] if e["type"] == "topic_partitions_min" and e["value"] == need)
    if sc["track"] == "native":
        return sub1(rf"(--topic {re.escape(first)} --partitions )\d+", r"\g<1>1", t)
    return sub1(rf"(name: {re.escape(first)}\n(?:.*\n){{3}}  partitions: )\d+", r"\g<1>1", t)


def reactive(t):
    """Reactive mode derives the parallelism from the available slots and ignores parallelism.default."""
    return "REACTIVE" in t


def m_parallelism_low(t, sc):
    need = next(e["value"] for e in sc["expect"] if e["type"] == "parallelism_min")
    if need <= 1 or reactive(t):
        return None
    return sub1(r"(default: |\n    parallelism: )\d+", r"\g<1>1", t)


def m_parallelism_high(t, sc):
    if reactive(t):
        return None
    return sub1(r"(default: |\n    parallelism: )\d+", r"\g<1>13", t)


def m_tm_memory(t, sc):
    return sub1(r"\b8g\b", "16g", t) or sub1(r'"8Gi"', '"16Gi"', t)


def m_tm_cpu(t, sc):
    if sc["track"] != "k8s":
        return None
    return sub1(r'(memory: "8Gi"\n      cpu: )4', r"\g<1>8", t)


def m_topic_missing(t, sc):
    s = sink_topic(sc)
    if sc["track"] == "native":
        return sub1(rf"\nkafka-topics\.sh [^\n]*--topic {re.escape(s)} [^\n]*(\\\n[^\n]*)*", "", t)
    return sub1(rf"\n---\napiVersion: kafka\.strimzi\.io/v1beta2\nkind: KafkaTopic\nmetadata:\n  name: {re.escape(s)}\n(?:  .*\n|    .*\n|      .*\n|spec:\n)*?(?=---|```)", "\n", t)


def m_table_topic(t, sc):
    s = sink_topic(sc)
    return sub1(rf"'topic' = '{re.escape(s)}'", f"'topic' = '{s}-out'", t)


def m_no_savepoint_dir(t, sc):
    """upgradeMode savepoint without a savepoint directory: the operator rejects the FlinkDeployment."""
    if sc["track"] != "k8s":
        return None
    return sub1(r"\n    execution\.checkpointing\.savepoint-dir: [^\n]*", "", t)


def m_deployment_name(t, sc):
    if sc["track"] != "k8s":
        return None
    return sub1(r"(kind: FlinkDeployment\nmetadata:\n  name: )([a-z0-9-]+)", r"\g<1>Job_\2", t)


def m_no_service_account(t, sc):
    if sc["track"] != "k8s":
        return None
    return sub1(r"\n  serviceAccount: [^\n]*", "", t)


def m_cluster_label(t, sc):
    if sc["track"] != "k8s":
        return None
    return sub1(r"strimzi\.io/cluster: my-cluster", "strimzi.io/cluster: kafka-cluster", t)


FAMILY_KEYS = {  # family-specific settings of the out-of-distribution references; one mutant removes each
    "cdc-upsert": ["'format' = 'debezium-json'", "cleanup.policy", "PRIMARY KEY"],
    "interval-join": ["WATERMARK FOR"],
    "dedup-large-state": ["state.backend.type", "table.exec.state.ttl"],
    "backpressure-checkpoints": ["unaligned"],
    "idle-partitions": ["idle-timeout", "WATERMARK FOR"],
    "large-messages": ["max.message.bytes", "max.request.size"],
    "compression-size-retention": ["compression.type", "retention.bytes"],
    "tiered-storage": ["remote.storage.enable", "local.retention.ms"],
    "independent-consumers": ["'properties.group.id' = 'merchant-stats'"],
    "eos-tight-latency": [],
    "restart-ha": ["restart-strategy.type", "high-availability.type"],
    "rocksdb-memory": ["state.backend.type", "rocksdb.memory.managed", "managed.size"],
    "mixed-retention": [],
    "compact-delete": ["cleanup.policy", "delete.retention.ms"],
    "checkpoint-retention": ["num-retained", "externalized-checkpoint-retention"],
    "elasticity": ["scheduler-mode", "job.autoscaler.enabled"],
}


def equals_default(t, sc, k):
    """True when the setting removed by a family mutant is set to its documented default, so that the effective
    configuration (which the validator checks) does not change and the mutant is not a defect."""
    from kfbench.parse import parse_topic_commands
    from kfbench.catalog import get_catalog
    cat = get_catalog()
    i = block_index(t, "flink")
    body = blocks(t)[i][1]
    conf = {}
    for d in yaml.safe_load_all(body):
        if isinstance(d, dict):
            conf.update(flatten(d.get("spec", {}).get("flinkConfiguration", {}) if d.get("kind") else d))
    for key, v in conf.items():
        if key.endswith(k) and cat.flink_default(key) is not None:
            return str(cat.flink_default(key)).lower() == str(v).lower()
    for c in parse_topic_commands(blocks(t)[block_index(t, "topics")][1]) if sc["track"] == "native" else []:
        for key, v in c.configs.items():
            if key == k and cat.topic_default(key) is not None:
                return str(cat.topic_default(key)) == str(v)
    for key, v in re.findall(r"\n {6}([a-z.]+): \"?([^\"\n]+)", t) if sc["track"] == "k8s" else []:
        if key == k and cat.topic_default(key) is not None:
            return str(cat.topic_default(key)) == str(v)
    return False


def family_mutants(t, sc):
    out = []
    if sc["track"] == "native":
        t = nr_flat_config(t, sc)  # remove flat keys: deleting one line of a nested mapping could change its parent
    for k in FAMILY_KEYS.get(sc.get("family"), []):
        lines = t.split("\n")
        hit = [i for i, ln in enumerate(lines) if k in ln and not ln.strip().startswith(("CREATE", "-- "))]
        if not hit:
            continue
        i = hit[0]
        if k == "PRIMARY KEY":  # drop the PK of the sink table (the last one in the answer)
            i = hit[-1]
            lines[i - 1] = lines[i - 1].rstrip(",")
        ln = lines[i]
        if "--config" in ln:
            new = re.sub(r"\s*--config " + re.escape(k) + r"=\S+", "", ln)
            lines[i] = new
            if lines[i].strip() == "\\":
                del lines[i]
            elif lines[i - 1].endswith("\\") and not lines[i].strip():
                del lines[i]
        else:
            del lines[i]
        new = "\n".join(lines)
        new = re.sub(r" \\\n(?=\n|kafka-topics|```)", "\n", new)
        new = re.sub(r",\n\)", "\n)", new)
        out.append((k, new))
    return out


FAULTS = {  # name -> (group, expected level, operator)
    "artefact missing (SQL block)": ("format", "L1", m_drop_sql),
    "YAML syntax error": ("format", "L1", m_yaml_syntax),
    "answer truncated at the token limit": ("format", "L1", m_truncated),
    "unbalanced SQL statement": ("format", "L1", m_sql_unbalanced),
    "misspelled Flink option": ("schema", "L2", m_flink_key_typo),
    "misspelled topic config": ("schema", "L2", m_topic_key_typo),
    "broker setting used as topic config": ("schema", "L2", m_broker_key),
    "value of the wrong type": ("schema", "L2", m_value_type),
    "invalid enumeration value": ("schema", "L2", m_enum_value),
    "value out of the documented range": ("schema", "L2", m_out_of_range),
    "unknown connector option": ("schema", "L2", m_connector_option),
    "unknown connector": ("schema", "L2", m_connector_name),
    "unknown format": ("schema", "L2", m_format),
    "unknown Kafka client property": ("schema", "L2", m_client_property),
    "unknown CRD field": ("schema", "L2", m_crd_field),
    "CRD field of the wrong type": ("schema", "L2", m_crd_type),
    "connector option in the cluster configuration": ("schema", "L2", m_misplaced),
    "unknown kafka-topics.sh option": ("schema", "L2", m_cli_option),
    "savepoint upgrade mode without savepoint directory": ("schema", "L2", m_no_savepoint_dir),
    "invalid FlinkDeployment name": ("schema", "L2", m_deployment_name),
    "FlinkDeployment without service account": ("schema", "L2", m_no_service_account),
    "required connector option missing": ("schema", "L2", m_missing_required),
    "source without group.id (runtime-derived rule)": ("schema", "L2", m_group_id),
    "topic config removed in Kafka 4.0": ("version", "L2", m_removed_topic_config),
    "--zookeeper (removed in Kafka 3.0)": ("version", "L3", m_zookeeper),
    "CRD apiVersion not served": ("version", "L3", m_api_version),
    "flinkVersion other than the cluster's": ("version", "L3", m_flink_version),
    "retention differs from the requirement": ("requirements", "L4", m_retention),
    "min.insync.replicas missing (durability)": ("requirements", "L4", m_min_isr_removed),
    "replication factor 1 (durability)": ("requirements", "L4", m_rf_one),
    "replication factor above the broker count": ("requirements", "L4", m_rf_above_brokers),
    "at-least-once sink for exactly-once": ("requirements", "L4", m_eos_downgrade),
    "transactional-id prefix missing": ("requirements", "L4", m_no_txn_prefix),
    "transaction timeout above the broker maximum": ("requirements", "L4", m_txn_timeout),
    "AT_LEAST_ONCE checkpoints for exactly-once": ("requirements", "L4", m_ckpt_mode),
    "checkpoint interval above the latency objective": ("requirements", "L4", m_ckpt_interval_slo),
    "checkpointing disabled": ("requirements", "L4", m_no_checkpointing),
    "too few partitions for the input rate": ("requirements", "L4", m_partitions_low),
    "parallelism too low for the input rate": ("requirements", "L4", m_parallelism_low),
    "parallelism above the slot capacity": ("requirements", "L4", m_parallelism_high),
    "TaskManager memory above the limit": ("requirements", "L4", m_tm_memory),
    "TaskManager CPU above the limit": ("requirements", "L4", m_tm_cpu),
    "topic used by the job not created": ("requirements", "L4", m_topic_missing),
    "table reads or writes the wrong topic": ("requirements", "L4", m_table_topic),
    "KafkaTopic bound to another cluster": ("requirements", "L4", m_cluster_label),
}


# =========================================================================================== D. official artefacts
def official_flink_keys():
    lines = (EX / "flink-1.20.5-config.yaml").read_text().splitlines()
    active = flatten(yaml.safe_load("\n".join(lines)))
    yl = []
    for ln in lines:
        m = re.match(r"^#( +)([a-zA-Z][\w.-]*:(?: .*)?)$", ln)
        if m and len(m.group(1)) % 2 == 1:
            yl.append(" " * (len(m.group(1)) - 1) + m.group(2))
    commented = flatten(yaml.safe_load("\n".join(yl)))
    commented = {k: (v[0] if isinstance(v, list) else v) for k, v in commented.items()}  # "[A, B]" lists the choices
    return [("config.yaml (active)", k, v) for k, v in active.items()] + \
           [("config.yaml (commented examples)", k, v) for k, v in commented.items()]


def official_topic_commands():
    out = []
    for f in sorted((EX / "kafka-docs/site-docs").rglob("*.md")):
        s = re.sub(r"\\\n\s*", " ", f.read_text(errors="replace"))  # join continuation lines
        for m in re.finditer(r"\$ bin/kafka-topics\.sh [^\n]*--create[^\n]*", s):
            cmd = re.sub(r"^\$ bin/", "", m.group(0)).strip()
            cmd = re.sub(r"localhost:9092", "kafka:9092", cmd)
            if "--config x=y" in cmd:  # placeholder in the documentation, not a configuration
                continue
            out.append((str(f.relative_to(EX / "kafka-docs/site-docs")), cmd))
    return out


def official_docs(kind):
    out = []
    root = EX / ("strimzi" if kind == "KafkaTopic" else "flink-operator")
    for f in sorted(root.rglob("*.yaml")):
        if "/install/" in str(f) or "Crd" in f.name:
            continue
        try:
            docs = [d for d in yaml.safe_load_all(f.read_text()) if isinstance(d, dict) and d.get("kind") == kind]
        except yaml.YAMLError:
            continue
        out += [(str(f.relative_to(root)), d) for d in docs]
    return out


def official_tables():
    out = []
    for name in ("kafka.md", "upsert-kafka.md"):
        s = (EX / "connector" / name).read_text()
        for m in re.finditer(r"```sql\n(.*?)```", s, re.S):
            for t in re.findall(r"CREATE TABLE.*?\)\s*(?:;|$)(?=\s*(?:CREATE|--|INSERT|$))", m.group(1), re.S):
                t = t.replace("'...'", "'kafka:9092'")  # elided bootstrap servers
                if re.search(r"^\s*\.\.\.\s*$", t, re.M):
                    # the documentation elides the other options with a line "..."; use those of its first example
                    fill = "'topic' = 'user_behavior',\n  'properties.bootstrap.servers' = 'kafka:9092',"
                    if "upsert-kafka" not in t:
                        fill += "\n  'properties.group.id' = 'testGroup',\n  'scan.startup.mode' = 'earliest-offset',"
                    if not re.search(r"'(value\.)?format'", t):
                        fill += "\n  'format' = 'csv',"
                    t = re.sub(r"^\s*\.\.\.\s*$", "  " + fill, t, flags=re.M)
                out.append((name, t.strip().rstrip(";") + ";"))
    return out


# Rejections of official artefacts that are correct, with the reason (checked by hand against the software):
EXPECTED_REJECTIONS = {
    "classloader.resolve.order": "Flink's commented template nests classloader.resolve-order as classloader.resolve.order, "
                                 "which is not an option of Flink 1.20 (Flink ignores unknown keys)",
    "historyserver.archive.fs.fs.refresh-interval": "the template nests historyserver.archive.fs.refresh-interval under "
                                                    "archive.fs a second time; the resulting key does not exist",
    "beam-example": "the example targets Flink 1.19 (flinkVersion v1_19), not the 1.20 cluster of the benchmark",
}


def lvl_findings(r, art_pred):
    return [f for f in r["findings"] if f["level"] in ("L1", "L2", "L3") and art_pred(f["artefact"])]


def run_official():
    nat, k8s = BENCH["ID-001"], next(s for s in BENCH.values() if s["split"] == "ID" and s["track"] == "k8s")
    tn, tk = REF[nat["id"]], REF[k8s["id"]]
    items = []
    for src, k, v in official_flink_keys():
        for track, sc, t in (("native", nat, tn), ("k8s", k8s, tk)):
            if track == "native":
                i = block_index(t, "flink")
                body = blocks(t)[i][1] + yaml.safe_dump({k: v}, default_flow_style=False, width=10_000)
                ans = replace_block(t, i, body)
            else:
                ans = t.replace("  flinkConfiguration:\n", f"  flinkConfiguration:\n    {k}: {json.dumps(str(v).lower() if isinstance(v, bool) else str(v))}\n", 1)
            r = validate(ans, sc, "stop")
            fs = lvl_findings(r, lambda a: a in ("flink-conf", "flinkdeployment"))
            items.append({"source": src, "track": track, "item": f"{k}: {v}", "rejected": bool(fs),
                          "findings": [f"{f['level']} {f['category']}: {f['message']}" for f in fs],
                          "warnings": [f["message"] for f in r["findings"] if f["level"] == "warn"]})
    for src, cmd in official_topic_commands():
        i = block_index(tn, "topics")
        ans = replace_block(tn, i, blocks(tn)[i][1] + cmd + "\n")
        r = validate(ans, nat, "stop")
        fs = lvl_findings(r, lambda a: a == "topics")
        items.append({"source": f"Kafka 4.1.2 docs: {src}", "track": "native", "item": " ".join(cmd.split()),
                      "rejected": bool(fs), "findings": [f"{f['level']} {f['category']}: {f['message']}" for f in fs]})
    for src, d in official_docs("KafkaTopic"):
        i = block_index(tk, "topics")
        ans = replace_block(tk, i, blocks(tk)[i][1] + "---\n" + yaml.safe_dump(d, sort_keys=False))
        r = validate(ans, k8s, "stop")
        fs = lvl_findings(r, lambda a: a == "kafkatopic")
        items.append({"source": f"Strimzi 0.48.0 {src}", "track": "k8s", "item": d["metadata"]["name"],
                      "rejected": bool(fs), "findings": [f"{f['level']} {f['category']}: {f['message']}" for f in fs]})
    for src, d in official_docs("FlinkDeployment"):
        i = block_index(tk, "flink")
        ans = replace_block(tk, i, yaml.safe_dump(d, sort_keys=False))
        r = validate(ans, k8s, "stop")
        fs = lvl_findings(r, lambda a: a == "flinkdeployment")
        items.append({"source": f"Flink Kubernetes Operator 1.13 {src}", "track": "k8s",
                      "item": f"{d['metadata']['name']} (flinkVersion {d.get('spec', {}).get('flinkVersion')})",
                      "rejected": bool(fs), "findings": [f"{f['level']} {f['category']}: {f['message']}" for f in fs]})
    for src, ddl in official_tables():
        i = block_index(tn, "sql")
        ans = replace_block(tn, i, blocks(tn)[i][1] + "\n" + ddl + "\n")
        r = validate(ans, nat, "stop")
        name = re.search(r"CREATE TABLE\s+([`\w]+)", ddl).group(1).strip("`")
        fs = lvl_findings(r, lambda a: a == f"sql:{name}" or a == "sql")
        items.append({"source": f"Flink Kafka connector 3.3 docs: {src}", "track": "native",
                      "item": " ".join(ddl.split())[:160], "rejected": bool(fs),
                      "findings": [f"{f['level']} {f['category']}: {f['message']}" for f in fs]})
    return items


# =========================================================================================== main
def main():
    out = {"validator_version": VALIDATOR_VERSION, "n_scenarios": len(BENCH)}
    # A
    ref = collections.defaultdict(lambda: [0, 0])
    for i, sc in BENCH.items():
        r = validate(REF[i], sc, "stop")
        ref[sc["split"]][0] += 1
        ref[sc["split"]][1] += int(r["valid"] and r["policy_ok"])
        assert r["valid"], (i, first_failing(r), r["findings"][:3])
    out["reference"] = {k: {"n": v[0], "valid": v[1]} for k, v in ref.items()}
    out["reference"]["all"] = {"n": sum(v[0] for v in ref.values()), "valid": sum(v[1] for v in ref.values())}
    # B
    neutral = {}
    for name, op in NEUTRAL.items():
        n = ok = 0
        fails = []
        for i, sc in BENCH.items():
            t = op(REF[i], sc)
            if t is None or t == REF[i]:
                continue
            n += 1
            r = validate(t, sc, "stop")
            if r["valid"]:
                ok += 1
            else:
                fails.append({"id": i, "level": first_failing(r),
                              "findings": [f["message"] for f in r["findings"] if f["level"] != "warn"][:3],
                              "expectations": [k for k, v in r["expectations"].items() if not v["ok"]][:3]})
        neutral[name] = {"n": n, "still_valid": ok, "examples_rejected": fails[:5]}
    out["neutral"] = neutral
    # C
    faults = {}
    for name, (group, exp, op) in FAULTS.items():
        n = det = at_level = 0
        missed = []
        for i, sc in BENCH.items():
            res = op(REF[i], sc)
            if res is None:
                continue
            fr = "stop"
            if isinstance(res, tuple):
                fr, res = "length", res[1]
            if res == REF[i]:
                continue
            n += 1
            r = validate(res, sc, fr)
            if not r["valid"]:
                det += 1
                at_level += first_failing(r) == exp
            else:
                missed.append(i)
        faults[name] = {"group": group, "expected_level": exp, "n": n, "detected": det, "at_expected_level": at_level,
                        "missed": missed[:10]}
    fam_n = fam_det = 0
    fam_missed, fam_default = [], []
    for i, sc in BENCH.items():
        if sc["split"] != "OOD":
            continue
        for k, t in family_mutants(REF[i], sc):
            r = validate(t, sc, "stop")
            if r["valid"] and equals_default(REF[i] if sc["track"] == "k8s" else nr_flat_config(REF[i], sc), sc, k):
                fam_default.append(f"{i}: {k}")
                continue
            fam_n += 1
            if not r["valid"]:
                fam_det += 1
            else:
                fam_missed.append(f"{i}: {k}")
    faults["family-specific setting removed (OOD)"] = {"group": "requirements", "expected_level": "L4", "n": fam_n,
                                                       "detected": fam_det, "at_expected_level": None,
                                                       "missed": fam_missed,
                                                       "not_a_defect_equals_default": fam_default}
    out["faults"] = faults
    g = collections.defaultdict(lambda: [0, 0, 0])
    for v in faults.values():
        g[v["group"]][0] += v["n"]
        g[v["group"]][1] += v["detected"]
        g[v["group"]][2] += 1
    out["fault_groups"] = {k: {"classes": v[2], "n": v[0], "detected": v[1]} for k, v in g.items()}
    # D
    items = run_official()
    for it in items:
        why = next((v for k, v in EXPECTED_REJECTIONS.items() if it["item"].startswith(k)), None)
        it["expected_rejection"] = why
        it["false_rejection"] = bool(it["rejected"] and why is None)
        it["missed_expected_rejection"] = bool(why and not it["rejected"])
    out["official"] = items
    agg = collections.defaultdict(lambda: [0, 0])
    for it in items:
        key = it["source"].split(":")[0].split(" examples")[0]
        key = re.sub(r" (examples/|packaging/).*| strimzi-0.48.0/.*| examples__.*", "", key)
        agg[key][0] += 1
        agg[key][1] += it["rejected"]
    out["official_summary"] = {k: {"n": v[0], "rejected": v[1]} for k, v in agg.items()}
    path = Path(_args.get("--out", ROOT / "results/validator_eval.json"))
    path.write_text(json.dumps(out, indent=1))
    print(json.dumps({"reference": out["reference"], "neutral": {k: (v["n"], v["still_valid"]) for k, v in neutral.items()},
                      "fault_groups": out["fault_groups"], "official": out["official_summary"]}, indent=1))
    for k, v in faults.items():
        if v["detected"] < v["n"] or (v["at_expected_level"] is not None and v["at_expected_level"] < v["detected"]) or v["n"] == 0:
            print("FAULT", k, v["n"], v["detected"], v["at_expected_level"], v["missed"][:5])
    for k, v in neutral.items():
        if v["still_valid"] < v["n"]:
            print("NEUTRAL", k, v["n"], v["still_valid"], v["examples_rejected"][:2])
    for it in items:
        if it["rejected"]:
            print("OFFICIAL", it["source"], "|", it["item"][:90], "|", it["findings"][:2])


if __name__ == "__main__":
    main()
