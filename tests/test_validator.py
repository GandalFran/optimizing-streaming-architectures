"""Unit tests for the layered validator: one positive answer per track and targeted negative cases per rule."""
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from kfbench.validate import validate  # noqa: E402

BENCH = {json.loads(l)["id"]: json.loads(l) for l in open(ROOT / "bench" / "kfbench-v1.jsonl")}
CASE_N, CASE_K = BENCH["CASE-banking-native"], BENCH["CASE-banking-k8s"]

GOLD_NATIVE = """
```bash
kafka-topics.sh --bootstrap-server kafka:9092 --create --topic banking-transactions --partitions 12 \\
  --replication-factor 3 --config min.insync.replicas=2 --config retention.ms=604800000
kafka-topics.sh --bootstrap-server kafka:9092 --create --topic banking-transactions-validated --partitions 12 \\
  --replication-factor 3 --config min.insync.replicas=2 --config retention.ms=604800000
```
```yaml
execution:
  checkpointing:
    interval: 100 ms
    mode: EXACTLY_ONCE
    dir: s3://flink-state/checkpoints
taskmanager:
  numberOfTaskSlots: 4
  memory:
    process.size: 8g
parallelism:
  default: 3
```
```sql
CREATE TABLE src (account_id STRING, amount DOUBLE) WITH (
  'connector' = 'kafka', 'topic' = 'banking-transactions', 'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'validator', 'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest', 'format' = 'json');
CREATE TABLE snk (account_id STRING, amount DOUBLE) WITH (
  'connector' = 'kafka', 'topic' = 'banking-transactions-validated', 'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json', 'sink.delivery-guarantee' = 'exactly-once', 'sink.transactional-id-prefix' = 'bank-val',
  'properties.transaction.timeout.ms' = '600000');
```
"""

GOLD_K8S = """
```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: banking-transactions
  labels: {strimzi.io/cluster: my-cluster}
spec:
  partitions: 12
  replicas: 3
  config: {min.insync.replicas: 2, retention.ms: 604800000}
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: banking-transactions-validated
  labels: {strimzi.io/cluster: my-cluster}
spec:
  partitions: 12
  replicas: 3
  config: {min.insync.replicas: 2, retention.ms: 604800000}
```
```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: banking
spec:
  image: flink:1.20
  flinkVersion: v1_20
  serviceAccount: flink
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
    execution.checkpointing.interval: "100 ms"
    execution.checkpointing.mode: EXACTLY_ONCE
    execution.checkpointing.dir: s3://flink-state/checkpoints
    execution.checkpointing.savepoint-dir: s3://flink-state/savepoints
  jobManager:
    resource: {memory: "2048m", cpu: 1}
  taskManager:
    resource: {memory: "8Gi", cpu: 4}
  job:
    jarURI: local:///opt/flink/usrlib/sql-runner.jar
    parallelism: 3
    upgradeMode: savepoint
```
```sql
CREATE TABLE src (account_id STRING) WITH ('connector'='kafka','topic'='banking-transactions',
 'properties.bootstrap.servers'='my-cluster-kafka-bootstrap:9092','properties.group.id'='bank-validator',
 'scan.startup.mode'='group-offsets','properties.auto.offset.reset'='earliest','format'='json');
CREATE TABLE snk (account_id STRING) WITH ('connector'='kafka','topic'='banking-transactions-validated',
 'properties.bootstrap.servers'='my-cluster-kafka-bootstrap:9092','format'='json',
 'sink.delivery-guarantee'='exactly-once','sink.transactional-id-prefix'='bank',
 'properties.transaction.timeout.ms'='600000');
```
"""


def failing(r):
    return sorted(k for k, v in r["expectations"].items() if not v["ok"])


def cats(r, level=None):
    return sorted({f["category"] for f in r["findings"] if level is None or f["level"] == level})


def test_gold_native():
    r = validate(GOLD_NATIVE, CASE_N, "stop")
    assert r["valid"], (r["findings"], failing(r))
    assert r["policy_ok"]


def test_gold_k8s():
    r = validate(GOLD_K8S, CASE_K, "stop")
    assert r["valid"], (r["findings"], failing(r))


def test_truncation_fails_l1():
    r = validate(GOLD_NATIVE, CASE_N, "length")
    assert not r["L1"] and "truncated" in cats(r, "L1")


def test_missing_txn_timeout_and_latency():
    a = GOLD_NATIVE.replace(",\n  'properties.transaction.timeout.ms' = '600000'", "").replace("interval: 100 ms", "interval: 5 s")
    r = validate(a, CASE_N, "stop")
    assert r["L3"] and not r["valid"]
    assert "txn_timeout:banking-transactions-validated" in failing(r) and "eos_latency" in failing(r)


def test_compression_type_is_a_valid_topic_config():  # the original paper called this a hallucination
    a = GOLD_NATIVE.replace("--config retention.ms=604800000\n", "--config retention.ms=604800000 --config compression.type=lz4\n", 1)
    r = validate(a, CASE_N, "stop")
    assert r["L2"], r["findings"]


def test_zookeeper_flag_is_removed_in_kafka_4():
    a = GOLD_NATIVE.replace("kafka-topics.sh --bootstrap-server kafka:9092", "kafka-topics.sh --zookeeper localhost:2181")
    r = validate(a, CASE_N, "stop")
    assert r["L2"] and not r["L3"] and "removed-option" in cats(r, "L3")


def test_original_finetuned_case_study_output():
    """Output of the original adapter (case_study_output.txt), fenced so that only content is judged."""
    fixture = ROOT / "tests" / "fixtures" / "original_finetuned_case_study.txt"
    ft = fixture.read_text().split("=== FINE-TUNED MODEL (Ours) ===")[1]
    cmd, conf = ft.split("# Flink Checkpointing Configuration")
    text = f"```bash\n{cmd}\n```\n```yaml\n{conf.strip()}\n```\n```sql\nCREATE TABLE x (a INT) WITH ('connector'='kafka');\n```"
    r = validate(text, CASE_N, "stop")
    assert "removed-option" in cats(r, "L3")              # --zookeeper
    assert "unknown-key" in cats(r, "L2")                 # checkpointing.enabled, savepoint-trigger-*, ...
    assert r["n_duplicate_keys"] > 0                      # repeated savepoint-trigger-condition blocks
    assert "ckpt_enabled" in failing(r)                   # no checkpoint interval => checkpointing disabled


def test_unknown_flink_key_and_misplaced_option():
    a = GOLD_NATIVE.replace("    mode: EXACTLY_ONCE\n", "    mode: EXACTLY_ONCE\n    guarantee: strict\n").replace(
        "parallelism:\n  default: 3", "parallelism:\n  default: 3\nsink.delivery-guarantee: exactly-once")
    r = validate(a, CASE_N, "stop")
    assert not r["L2"] and {"unknown-key", "misplaced-key"} <= set(cats(r, "L2"))


def test_invalid_enum_value():
    r = validate(GOLD_NATIVE.replace("mode: EXACTLY_ONCE", "mode: exactly-once"), CASE_N, "stop")
    assert "invalid-value" in cats(r, "L2")


def test_durability_and_partitions():
    a = GOLD_NATIVE.replace("--replication-factor 3", "--replication-factor 1").replace("--partitions 12", "--partitions 5")
    r = validate(a, CASE_N, "stop")
    f = failing(r)
    assert "rf_min:banking-transactions" in f and "partitions:banking-transactions" in f


def test_slots_capacity_default_one_slot():
    a = GOLD_NATIVE.replace("  numberOfTaskSlots: 4\n", "").replace("default: 3", "default: 6")
    r = validate(a, CASE_N, "stop")
    assert "slots" in failing(r)


def test_retention_default_is_seven_days():
    a = GOLD_NATIVE.replace(" --config retention.ms=604800000", "")
    r = validate(a, CASE_N, "stop")
    assert "retention:banking-transactions" not in failing(r)


def test_k8s_unknown_field_and_label():
    a = GOLD_K8S.replace("  replicas: 3\n  config", "  replicas: 3\n  retentionDays: 7\n  config", 1).replace(
        "labels: {strimzi.io/cluster: my-cluster}", "labels: {app: kafka}", 1)
    r = validate(a, CASE_K, "stop")
    assert "unknown-field" in cats(r, "L2") and "label:banking-transactions" in failing(r)


def test_k8s_flink_version_mismatch():
    r = validate(GOLD_K8S.replace("v1_20", "v1_17"), CASE_K, "stop")
    assert not r["L3"] and "version-mismatch" in cats(r, "L3")


def test_sql_set_statements_configure_the_job():
    a = GOLD_NATIVE.replace("    interval: 100 ms\n", "").replace("```sql\n", "```sql\nSET 'execution.checkpointing.interval' = '100 ms';\n")
    r = validate(a, CASE_N, "stop")
    assert r["valid"], failing(r)


def test_source_without_group_id_in_group_offsets_mode():  # v1.1, found by the testbed
    a = GOLD_NATIVE.replace("'properties.group.id' = 'validator', ", "")
    r = validate(a, CASE_N, "stop")
    assert not r["L2"] and "missing-required-option" in cats(r, "L2")
    b = a.replace("'scan.startup.mode' = 'group-offsets'", "'scan.startup.mode' = 'earliest-offset'")
    assert validate(b, CASE_N, "stop")["valid"]


def test_sql_types_and_primary_key_on_kafka():  # v1.2, found by the testbed
    a = GOLD_NATIVE.replace("CREATE TABLE src (account_id STRING, amount DOUBLE)", "CREATE TABLE src (account_id STRING, payload JSON)")
    assert "invalid-value" in cats(validate(a, CASE_N, "stop"), "L2")
    b = GOLD_NATIVE.replace("CREATE TABLE snk (account_id STRING, amount DOUBLE)",
                            "CREATE TABLE snk (account_id STRING, amount DOUBLE, PRIMARY KEY (account_id) NOT ENFORCED)")
    assert "invalid-value" in cats(validate(b, CASE_N, "stop"), "L2")
    c = GOLD_NATIVE.replace("amount DOUBLE)", "amount DECIMAL(10, 2), ts TIMESTAMP(3), m MAP<STRING, INT>, total AS amount * 2)", 1)
    assert validate(c, CASE_N, "stop")["valid"], failing(validate(c, CASE_N, "stop"))


def test_missing_artefacts():
    r = validate("Here is a description without code.", CASE_N, "stop")
    assert not r["L1"] and "missing-artefact" in cats(r, "L1")


def test_sql_comments_are_ignored():  # v1.3, found by the validator evaluation (official connector example)
    a = GOLD_NATIVE.replace("'format' = 'json', 'sink.delivery-guarantee'",
                            "/* sink options */ 'format' = 'json', -- one record per line\n  'sink.delivery-guarantee'")
    assert validate(a, CASE_N, "stop")["valid"], failing(validate(a, CASE_N, "stop"))
    b = GOLD_NATIVE.replace("'sink.transactional-id-prefix' = 'bank-val'", "'sink.transactional-id-prefix' = 'bank--val'")
    assert validate(b, CASE_N, "stop")["valid"]  # '--' inside a string literal is not a comment


def test_deprecated_and_fallback_operator_keys():  # v1.3, operator options by reflection from the 1.13.0 jars
    a = GOLD_K8S.replace("    execution.checkpointing.mode: EXACTLY_ONCE\n",
                         "    execution.checkpointing.mode: EXACTLY_ONCE\n    job.autoscaler.enabled: \"true\"\n"
                         "    job.autoscaler.target.utilization.boundary: \"0.2\"\n"
                         "    kubernetes.operator.job.autoscaler.target.utilization: \"0.6\"\n")
    r = validate(a, CASE_K, "stop")
    assert r["valid"], failing(r)
    assert "deprecated-key" in {f["category"] for f in r["findings"] if f["level"] == "warn"}
    b = a.replace("job.autoscaler.enabled", "job.autoscaler.enable")
    assert "unknown-key" in cats(validate(b, CASE_K, "stop"), "L2")


def test_undefined_api_version():  # no served version of the installed CRDs is named "v1"
    a = GOLD_K8S.replace("kafka.strimzi.io/v1beta2", "kafka.strimzi.io/v1", 1)
    assert "unsupported-api-version" in cats(validate(a, CASE_K, "stop"), "L3")



def test_operator_admission_rules():  # v1.3, ported from the operator's DefaultValidator (release-1.13)
    r = validate(GOLD_K8S.replace("    execution.checkpointing.savepoint-dir: s3://flink-state/savepoints\n", ""), CASE_K, "stop")
    assert "operator-rejects" in cats(r, "L2")  # upgradeMode savepoint without a savepoint directory
    assert validate(GOLD_K8S.replace("upgradeMode: savepoint", "upgradeMode: stateless").replace(
        "    execution.checkpointing.savepoint-dir: s3://flink-state/savepoints\n", ""), CASE_K, "stop")["valid"]
    assert "operator-rejects" in cats(validate(GOLD_K8S.replace("name: banking\n", "name: Banking_Job\n"), CASE_K, "stop"), "L2")
    assert "operator-rejects" in cats(validate(GOLD_K8S.replace("  serviceAccount: flink\n", ""), CASE_K, "stop"), "L2")
    a = GOLD_K8S.replace("    execution.checkpointing.mode: EXACTLY_ONCE\n", "    execution.checkpointing.mode: EXACTLY_ONCE\n"
                         "    job.autoscaler.enabled: \"true\"\n    job.autoscaler.utilization.target: \"1.5\"\n")
    assert "operator-rejects" in cats(validate(a, CASE_K, "stop"), "L2")
    b = GOLD_K8S.replace("    resource: {memory: \"8Gi\", cpu: 4}\n", "    resource: {cpu: 4}\n")
    assert "operator-rejects" in cats(validate(b, CASE_K, "stop"), "L2")  # no TaskManager memory anywhere


def test_commented_out_set_statement_is_ignored():  # v1.3
    a = GOLD_NATIVE.replace("    interval: 100 ms\n", "") + "\n```sql\n/* SET 'execution.checkpointing.interval' = '100 ms'; */\n```\n"
    assert "ckpt_enabled" in failing(validate(a, CASE_N, "stop"))


if __name__ == "__main__":
    fails = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            try:
                fn()
                print("PASS", name)
            except AssertionError as e:
                fails += 1
                print("FAIL", name, str(e)[:1500])
    sys.exit(1 if fails else 0)
