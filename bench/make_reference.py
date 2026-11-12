"""Reference answers for KFConfBench (one per scenario).

Purpose: show that every scenario admits an answer that passes all four validator levels (the requirement set of each
scenario is satisfiable and the validator accepts a correct answer), give the executable validation (E7) a positive
control for every testbed scenario, and provide the seeds for the validator's fault-injection test
(analysis/validator_eval.py).

How the answers are written:
  * grid scenarios (ID): generated from the scenario parameters with the rules a practitioner would apply and the
    sizing assumptions stated in the prompt (5 MB/s per partition, 20,000 events/s per source subtask); the
    configuration follows the Kafka 4.1 / Flink 1.20 / Flink Kafka connector 3.3 documentation;
  * case-study scenarios: the two reference answers deployed in E7 (runs/e7/gold/), verbatim;
  * out-of-distribution scenarios: written by hand per family from the prompt (FAMILIES below).
The answers were written from the prompts and the documentation; when the validator rejected one, the cause was
investigated and the decision recorded in the docstring of validator_eval.py (never by changing the validator to
accept the answer).

  python bench/make_reference.py            # -> bench/kfbench-v1-reference.jsonl
"""
from __future__ import annotations

import json
import math
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PART_BPS, SUBTASK_EPS, DAY = 5_000_000, 20_000, 86_400_000
NATIVE_BOOT, K8S_BOOT, CLUSTER = "kafka:9092", "my-cluster-kafka-bootstrap:9092", "my-cluster"
CKPT_DIR = "s3://flink-state/checkpoints"


# ------------------------------------------------------------------------------------------------ rendering helpers
def nest(flat: dict) -> dict:
    """Nested YAML mapping for Flink's config.yaml; a key that is both a leaf and a prefix stays flat."""
    out: dict = {}
    keys = sorted(flat)
    for k in keys:
        prefix_of_other = any(o.startswith(k + ".") for o in keys)
        parts = k.split(".")
        node = out
        ok = not prefix_of_other
        for p in parts[:-1]:
            if not ok:
                break
            if p not in node:
                node[p] = {}
            elif not isinstance(node[p], dict):
                ok = False
                break
            node = node[p]
        if ok and parts[-1] not in node:
            node[parts[-1]] = flat[k]
        else:
            out[k] = flat[k]
    return out


def yaml_dump(d: dict, indent: int = 0) -> str:
    lines = []
    for k, v in d.items():
        if isinstance(v, dict):
            lines.append(" " * indent + f"{k}:")
            lines.append(yaml_dump(v, indent + 2))
        else:
            lines.append(" " * indent + f"{k}: {v}")
    return "\n".join(lines)


def q(v) -> str:
    return '"' + str(v) + '"'


class Topic:
    def __init__(self, name, partitions, rf=3, **configs):
        self.name, self.partitions, self.rf = name, partitions, rf
        self.configs = {k.replace("_", "."): v for k, v in configs.items()}


class Table:
    def __init__(self, name, columns, options, pk=None, watermark=None):
        self.name, self.columns, self.options, self.pk, self.watermark = name, columns, options, pk, watermark

    def ddl(self) -> str:
        cols = [f"  `{c}` {t}" for c, t in self.columns]
        if self.watermark:
            cols.append(f"  WATERMARK FOR `{self.watermark}` AS `{self.watermark}` - INTERVAL '5' SECOND")
        if self.pk:
            cols.append(f"  PRIMARY KEY (`{self.pk}`) NOT ENFORCED")
        opts = ",\n".join(f"  '{k}' = '{v}'" for k, v in self.options.items())
        return f"CREATE TABLE {self.name} (\n" + ",\n".join(cols) + f"\n) WITH (\n{opts}\n);"


def render(track: str, topics: list[Topic], flink: dict, tables: list[Table], parallelism: int, why: str,
           job_name: str = "pipeline") -> str:
    if track == "native":
        cmds = []
        for t in topics:
            c = (f"kafka-topics.sh --bootstrap-server {NATIVE_BOOT} --create --topic {t.name} "
                 f"--partitions {t.partitions} --replication-factor {t.rf}")
            for k, v in t.configs.items():
                c += f" \\\n  --config {k}={v}"
            cmds.append(c)
        conf = dict(flink)
        conf.setdefault("taskmanager.numberOfTaskSlots", 4)
        conf.setdefault("taskmanager.memory.process.size", "8g")
        conf["parallelism.default"] = parallelism
        blocks = ["```bash\n" + "\n".join(cmds) + "\n```", "```yaml\n" + yaml_dump(nest(conf)) + "\n```"]
    else:
        docs = []
        for t in topics:
            cfg = "".join(f"\n      {k}: {v if isinstance(v, int) else q(v)}" for k, v in t.configs.items())
            docs.append(f"apiVersion: kafka.strimzi.io/v1beta2\nkind: KafkaTopic\nmetadata:\n  name: {t.name}\n"
                        f"  labels:\n    strimzi.io/cluster: {CLUSTER}\nspec:\n  partitions: {t.partitions}\n"
                        f"  replicas: {t.rf}" + (f"\n  config:{cfg}" if cfg else ""))
        conf = dict(flink)
        conf.setdefault("taskmanager.numberOfTaskSlots", 4)
        # upgradeMode savepoint: the operator requires a savepoint directory (DefaultValidator, release-1.13)
        conf.setdefault("execution.checkpointing.savepoint-dir", "s3://flink-state/savepoints")
        fc = "".join(f"\n    {k}: {q(v)}" for k, v in conf.items())
        fd = (f"apiVersion: flink.apache.org/v1beta1\nkind: FlinkDeployment\nmetadata:\n  name: {job_name}\nspec:\n"
              f"  image: flink-sql-runner:1.20  # flink:1.20 with sql-runner.jar and the SQL script in /opt/flink/usrlib\n"
              f"  flinkVersion: v1_20\n  serviceAccount: flink\n  flinkConfiguration:{fc}\n"
              f"  jobManager:\n    resource:\n      memory: \"2048m\"\n      cpu: 1\n"
              f"  taskManager:\n    resource:\n      memory: \"8Gi\"\n      cpu: 4\n"
              f"  job:\n    jarURI: local:///opt/flink/usrlib/sql-runner.jar\n"
              f"    args: [\"/opt/flink/usrlib/sql-scripts/pipeline.sql\"]\n    parallelism: {parallelism}\n"
              f"    upgradeMode: savepoint")
        blocks = ["```yaml\n" + "\n---\n".join(docs) + "\n```", "```yaml\n" + fd + "\n```"]
    blocks.append("```sql\n" + "\n\n".join(t.ddl() for t in tables) + "\n```")
    return "\n\n".join(blocks) + "\n\n" + why.strip() + "\n"


def boot(track):
    return NATIVE_BOOT if track == "native" else K8S_BOOT


def src_table(name, topic, track, columns, group, fmt="json", **extra):
    o = {"connector": "kafka", "topic": topic, "properties.bootstrap.servers": boot(track),
         "properties.group.id": group, "scan.startup.mode": "group-offsets", "properties.auto.offset.reset": "earliest",
         "format": fmt}
    o.update({k.replace("__", "."): v for k, v in extra.items()})
    return o


def sink_opts(topic, track, eos: bool, prefix: str, **extra):
    o = {"connector": "kafka", "topic": topic, "properties.bootstrap.servers": boot(track), "format": "json"}
    if eos:
        # read_committed consumers see a transaction when the checkpoint that closes it completes; the transaction
        # timeout must exceed the checkpoint interval and stay below the broker's transaction.max.timeout.ms (15 min)
        o.update({"sink.delivery-guarantee": "exactly-once", "sink.transactional-id-prefix": prefix,
                  "properties.transaction.timeout.ms": "600000"})
    else:
        o["sink.delivery-guarantee"] = "at-least-once"
    o.update(extra)
    return o


def parts(eps, event_bytes):
    return max(1, math.ceil(eps * event_bytes / PART_BPS))


def par(eps):
    return max(1, math.ceil(eps / SUBTASK_EPS))


def dur(ms: int) -> str:
    return f"{ms // 1000} s" if ms >= 1000 and ms % 1000 == 0 else f"{ms} ms"


def ckpt(eos: bool, slo_ms: int | None, alo_interval="10 s", interval_ms: int | None = None):
    """Checkpoint interval: with an exactly-once Kafka sink, results become visible to read_committed consumers only
    when the checkpoint that closes their transaction completes and commits, so a record can wait a full interval
    plus the checkpoint's duration. The interval is therefore half the latency objective (the static rule only
    requires it not to exceed the objective, which leaves no time for the checkpoint itself)."""
    c = {"execution.checkpointing.dir": CKPT_DIR}
    if eos:
        c["execution.checkpointing.interval"] = dur(interval_ms if interval_ms is not None else slo_ms // 2)
        c["execution.checkpointing.mode"] = "EXACTLY_ONCE"
    else:
        c["execution.checkpointing.interval"] = alo_interval
        c["execution.checkpointing.mode"] = "AT_LEAST_ONCE"
    return c


# ------------------------------------------------------------------------------------------------------ grid (ID)
def grid(sc: dict) -> str:
    s, track = sc["spec"], sc["track"]
    key = re.search(r"keyed by `(\w+)`", sc["prompt"]).group(1)
    eos, high = s["delivery"] == "exactly-once", s["durability"] != "standard"
    p, n = par(s["throughput_eps"]), parts(s["throughput_eps"], s["event_bytes"])
    n = max(n, p)
    cfg = {"retention.ms": s["retention_days"] * DAY}
    if high:
        cfg = {"min.insync.replicas": 2, **cfg}
    topics = [Topic(s["src"], n, 3, **{k.replace(".", "_"): v for k, v in cfg.items()}),
              Topic(s["sink"], n, 3, **{k.replace(".", "_"): v for k, v in cfg.items()})]
    cols = [(key, "STRING"), ("payload", "STRING"), ("event_time", "TIMESTAMP(3)")]
    tables = [Table("src", cols, src_table("src", s["src"], track, cols, f"{s['sink']}-job")),
              Table("snk", cols + [("enriched_at", "TIMESTAMP(3)")],
                    sink_opts(s["sink"], track, eos, f"{s['sink']}-tx"))]
    flink = ckpt(eos, s["latency_slo_ms"])
    why = (f"{n} partition{"s" if n > 1 else ""}: {s['throughput_eps']:,} events/s x {s['event_bytes']} B = "
           f"{s['throughput_eps'] * s['event_bytes'] / 1e6:.2f} MB/s at 5 MB/s per partition (and at least the "
           f"parallelism); parallelism {p} at 20,000 events/s per source subtask; replication factor 3"
           + (" with min.insync.replicas=2, so acknowledged writes survive the loss of one broker" if high else "")
           + f"; retention {s['retention_days']} days. "
           + ("Exactly-once: checkpointing in EXACTLY_ONCE mode with an interval of half the latency objective "
              "(read_committed consumers see results only when the checkpoint commits the Kafka transaction, so the "
              "checkpoint itself must fit in the objective), a "
              "transactional sink with a transactional-id prefix and a transaction timeout above the checkpoint "
              "interval and below the broker maximum of 15 minutes." if eos else
              "At-least-once: checkpointing enabled so the sink flushes on every checkpoint."))
    return render(track, topics, flink, tables, p, why, job_name=s["sink"])


# -------------------------------------------------------------------------------------------- out-of-distribution
def f_cdc(track):
    t = [Topic("pg.public.customers", parts(5000, 1024), 3, min_insync_replicas=2, retention_ms=3 * DAY),
         Topic("customers-latest", 2, 3, min_insync_replicas=2, cleanup_policy="compact")]
    cols = [("customer_id", "BIGINT"), ("name", "STRING"), ("email", "STRING"), ("updated_at", "TIMESTAMP(3)")]
    src = src_table("src", "pg.public.customers", track, cols, "customers-latest-job", fmt="debezium-json")
    snk = {"connector": "upsert-kafka", "topic": "customers-latest", "properties.bootstrap.servers": boot(track),
           "key.format": "json", "value.format": "json"}
    tables = [Table("customers_cdc", cols, src, pk="customer_id"), Table("customers_latest", cols, snk, pk="customer_id")]
    return t, ckpt(False, None, "5 s"), tables, 1, (
        "The source is read with the debezium-json format as a changelog; the result is an upsert-kafka table keyed by "
        "customer_id on a compacted topic, which keeps the latest value of every key indefinitely. Replication factor 3 "
        "with min.insync.replicas=2 for durability; a 5 s checkpoint interval bounds the upsert sink's flush delay.")


def f_join(track):
    n = parts(20000, 2048)
    t = [Topic(x, n, 3, min_insync_replicas=2, retention_ms=7 * DAY) for x in ("orders", "payments", "orders-paid")]
    oc = [("order_id", "STRING"), ("amount", "DOUBLE"), ("event_ts", "TIMESTAMP(3)")]
    pc = [("order_id", "STRING"), ("payment_id", "STRING"), ("event_ts", "TIMESTAMP(3)")]
    tables = [Table("orders", oc, src_table("orders", "orders", track, oc, "orders-paid-job"), watermark="event_ts"),
              Table("payments", pc, src_table("payments", "payments", track, pc, "orders-paid-job"), watermark="event_ts"),
              Table("orders_paid", [("order_id", "STRING"), ("amount", "DOUBLE"), ("payment_id", "STRING"),
                                    ("paid_ts", "TIMESTAMP(3)")], sink_opts("orders-paid", track, True, "orders-paid-tx"))]
    return t, ckpt(True, 2000), tables, 2, (
        "9 partitions per topic (20,000 x 2,048 B = 40.96 MB/s); event-time watermarks on both inputs for the interval "
        "join; exactly-once with a 1 s checkpoint interval, half the latency objective.")


def f_dedup(track):
    t = [Topic("clicks", parts(30000, 512), 3, retention_ms=DAY), Topic("clicks-unique", parts(30000, 512), 3, retention_ms=DAY)]
    cols = [("event_id", "STRING"), ("user_id", "STRING"), ("ts", "TIMESTAMP(3)")]
    f = ckpt(True, 60000)
    f.update({"state.backend.type": "rocksdb", "execution.checkpointing.incremental": "true",
              "table.exec.state.ttl": "25 h", "taskmanager.numberOfTaskSlots": 1})
    tables = [Table("clicks", cols, src_table("clicks", "clicks", track, cols, "clicks-dedup-job")),
              Table("clicks_unique", cols, sink_opts("clicks-unique", track, True, "clicks-unique-tx"))]
    return t, f, tables, 3, (
        "About 200 GB of deduplication state: RocksDB with incremental checkpoints, one slot per TaskManager and "
        "parallelism 3 so that the state is spread over all three TaskManagers; state TTL of 25 h so that keys expire "
        "after the 24 h horizon and state stays bounded; exactly-once with a 30 s checkpoint interval.")


def f_backpressure(track):
    t = [Topic("market-ticks", parts(50000, 512), 3, retention_ms=DAY), Topic("market-bars", 3, 3, retention_ms=DAY)]
    cols = [("symbol", "STRING"), ("price", "DOUBLE"), ("ts", "TIMESTAMP(3)")]
    f = ckpt(True, 30000, interval_ms=10000)
    f.update({"execution.checkpointing.unaligned.enabled": "true", "execution.checkpointing.max-concurrent-checkpoints": 1,
              "execution.checkpointing.timeout": "60 s"})
    tables = [Table("ticks", cols, src_table("ticks", "market-ticks", track, cols, "market-bars-job"), watermark="ts"),
              Table("bars", [("symbol", "STRING"), ("window_start", "TIMESTAMP(3)"), ("avg_price", "DOUBLE")],
                    sink_opts("market-bars", track, True, "market-bars-tx"))]
    return t, f, tables, 3, (
        "Unaligned checkpoints (one concurrent checkpoint) let barriers overtake buffered data under sustained "
        "backpressure; exactly-once with a 10 s checkpoint interval, below the 30 s latency objective.")


def f_idle(track):
    t = [Topic("meter-readings", 64, 3, retention_ms=7 * DAY), Topic("meter-aggregates", 2, 3, retention_ms=7 * DAY)]
    cols = [("device_id", "STRING"), ("kwh", "DOUBLE"), ("ts", "TIMESTAMP(3)")]
    f = ckpt(False, None)
    f["table.exec.source.idle-timeout"] = "1 min"
    tables = [Table("readings", cols, src_table("readings", "meter-readings", track, cols, "meter-agg-job"), watermark="ts"),
              Table("aggregates", [("device_id", "STRING"), ("window_start", "TIMESTAMP(3)"), ("kwh", "DOUBLE")],
                    sink_opts("meter-aggregates", track, False, ""))]
    return t, f, tables, 1, (
        "64 partitions as required; an idle-source timeout of 1 minute excludes idle partitions from the watermark "
        "so that the windows of active partitions still close.")


def f_large(track):
    mx = 5 * 1024 * 1024
    t = [Topic("documents", 42, 3, max_message_bytes=mx, retention_ms=2 * DAY),
         Topic("documents-indexed", 42, 3, max_message_bytes=mx, retention_ms=2 * DAY)]
    cols = [("doc_id", "STRING"), ("body", "STRING")]
    tables = [Table("docs", cols, src_table("docs", "documents", track, cols, "documents-index-job",
                                            **{"properties.max.partition.fetch.bytes": str(mx)})),
              Table("docs_indexed", cols + [("metadata", "STRING")],
                    sink_opts("documents-indexed", track, False, "", **{"properties.max.request.size": str(mx)}))]
    return t, ckpt(False, None), tables, 3, (
        "Events up to 4 MiB: max.message.bytes of 5 MiB on both topics and a producer max.request.size of 5 MiB; "
        "200 events/s x 1 MB = 200 MB/s needs at least 40 partitions at 5 MB/s each.")


def f_compress(track):
    t = [Topic("logs-raw", parts(100000, 512), 3, compression_type="zstd", retention_bytes=50 * 1024 ** 3,
               retention_ms=3 * DAY),
         Topic("logs-alerts", 3, 3, retention_ms=7 * DAY)]
    cols = [("level", "STRING"), ("message", "STRING"), ("ts", "TIMESTAMP(3)")]
    tables = [Table("logs", cols, src_table("logs", "logs-raw", track, cols, "logs-alerts-job")),
              Table("alerts", cols, sink_opts("logs-alerts", track, False, ""))]
    return t, ckpt(False, None), tables, 5, (
        "compression.type=zstd makes the broker store logs-raw in zstd whatever the producers use; retention.bytes is "
        "per partition (50 GiB) and applies together with retention.ms (3 days).")


def f_tiered(track):
    t = [Topic("trades", parts(20000, 1024), 3, min_insync_replicas=2, remote_storage_enable="true",
               local_retention_ms=DAY, retention_ms=90 * DAY),
         Topic("trades-enriched", parts(20000, 1024), 3, min_insync_replicas=2, retention_ms=7 * DAY)]
    cols = [("trade_id", "STRING"), ("symbol", "STRING"), ("qty", "BIGINT"), ("ts", "TIMESTAMP(3)")]
    tables = [Table("trades", cols, src_table("trades", "trades", track, cols, "trades-enrich-job")),
              Table("trades_enriched", cols + [("venue", "STRING")], sink_opts("trades-enriched", track, True, "trades-tx"))]
    return t, ckpt(True, 1000), tables, 1, (
        "Tiered storage on trades (remote.storage.enable=true, local.retention.ms of 1 day, retention.ms of 90 days); "
        "this requires remote.log.storage.system.enable=true and a remote storage manager on the brokers, which the "
        "default broker settings do not provide. Exactly-once with a 500 ms checkpoint interval, half the latency objective.")


def f_consumers(track):
    n = parts(30000, 1024)
    t = [Topic("card-payments", n, 3, retention_ms=7 * DAY), Topic("fraud-scores", n, 3, retention_ms=7 * DAY),
         Topic("merchant-stats", 2, 3, retention_ms=7 * DAY)]
    cols = [("payment_id", "STRING"), ("merchant_id", "STRING"), ("amount", "DOUBLE"), ("ts", "TIMESTAMP(3)")]
    tables = [Table("payments_fraud", cols, src_table("p1", "card-payments", track, cols, "fraud-scoring")),
              Table("payments_stats", cols, src_table("p2", "card-payments", track, cols, "merchant-stats")),
              Table("fraud_scores", [("payment_id", "STRING"), ("score", "DOUBLE")], sink_opts("fraud-scores", track, False, "")),
              Table("merchant_stats", [("merchant_id", "STRING"), ("total", "DOUBLE")], sink_opts("merchant-stats", track, False, ""))]
    return t, ckpt(False, None), tables, 2, (
        "Each pipeline reads card-payments through its own source table with a distinct properties.group.id, so both "
        "consume every partition and commit their own offsets.")


def f_tight(track):
    t = [Topic(x, 1, 3, min_insync_replicas=2, retention_ms=7 * DAY) for x in ("payment-instructions", "payment-instructions-accepted")]
    cols = [("instruction_id", "STRING"), ("amount", "DOUBLE"), ("ts", "TIMESTAMP(3)")]
    f = ckpt(True, 50)
    f["execution.checkpointing.min-pause"] = "0 ms"
    tables = [Table("instr", cols, src_table("instr", "payment-instructions", track, cols, "payments-validate-job")),
              Table("accepted", cols, sink_opts("payment-instructions-accepted", track, True, "payments-tx"))]
    return t, f, tables, 1, (
        "read_committed consumers see results only when a checkpoint commits, so the checkpoint interval is 25 ms with "
        "no minimum pause; whether checkpoints this frequent keep up is a runtime question.")


def f_restart(track):
    t = [Topic("shipments", parts(10000, 1024), 3, retention_ms=7 * DAY), Topic("shipments-enriched", 3, 3, retention_ms=7 * DAY)]
    cols = [("shipment_id", "STRING"), ("status", "STRING"), ("ts", "TIMESTAMP(3)")]
    f = ckpt(False, None)
    f.update({"restart-strategy.type": "failure-rate", "restart-strategy.failure-rate.max-failures-per-interval": 10,
              "restart-strategy.failure-rate.failure-rate-interval": "10 min",
              "restart-strategy.failure-rate.delay": "10 s"})
    if track == "k8s":
        f.update({"high-availability.type": "kubernetes", "high-availability.storageDir": "s3://flink-state/ha"})
    tables = [Table("shipments", cols, src_table("s", "shipments", track, cols, "shipments-enrich-job")),
              Table("shipments_enriched", cols + [("eta", "TIMESTAMP(3)")], sink_opts("shipments-enriched", track, False, ""))]
    return t, f, tables, 1, (
        "Failure-rate restart strategy: at most 10 failures per 10-minute interval, then the job fails permanently"
        + ("; Kubernetes high availability with its storage directory on S3." if track == "k8s" else "."))


def f_rocksdb(track):
    t = [Topic("user-activity", parts(15000, 512), 3, retention_ms=7 * DAY),
         Topic("user-activity-features", parts(15000, 512), 3, retention_ms=7 * DAY)]
    cols = [("user_id", "STRING"), ("action", "STRING"), ("ts", "TIMESTAMP(3)")]
    f = ckpt(False, None)
    f.update({"state.backend.type": "rocksdb", "state.backend.rocksdb.memory.managed": "true",
              "taskmanager.memory.managed.size": "4g", "execution.checkpointing.incremental": "true",
              "taskmanager.numberOfTaskSlots": 1})
    tables = [Table("activity", cols, src_table("a", "user-activity", track, cols, "activity-features-job"), watermark="ts"),
              Table("features", [("user_id", "STRING"), ("actions_7d", "BIGINT"), ("ts", "TIMESTAMP(3)")],
                    sink_opts("user-activity-features", track, False, ""))]
    return t, f, tables, 3, (
        "RocksDB state backend using Flink managed memory, with an explicit 4 GB managed-memory size per TaskManager "
        "(a fraction would be taken from total Flink memory, which is smaller than the 8 GB process size); one slot "
        "per TaskManager and parallelism 3, because managed memory is divided among a TaskManager's slots.")


def f_mixed(track):
    t = [Topic("platform-events", parts(40000, 512), 3, retention_ms=7 * DAY),
         Topic("alerts", 2, 1, retention_ms=DAY),
         Topic("audit-log", parts(40000, 512), 3, min_insync_replicas=2, retention_ms=365 * DAY)]
    cols = [("event_id", "STRING"), ("category", "STRING"), ("body", "STRING"), ("ts", "TIMESTAMP(3)")]
    tables = [Table("events", cols, src_table("e", "platform-events", track, cols, "platform-router-job")),
              Table("alerts", cols, sink_opts("alerts", track, False, "")),
              Table("audit", cols, sink_opts("audit-log", track, False, ""))]
    return t, ckpt(False, None), tables, 2, (
        "Per-topic retention and durability: alerts with one replica for one day, audit-log with three replicas and "
        "min.insync.replicas=2 for 365 days.")


def f_compact_delete(track):
    t = [Topic("session-activity", parts(15000, 512), 3, retention_ms=3 * DAY),
         Topic("session-state", parts(15000, 512), 3, cleanup_policy="compact,delete", retention_ms=30 * DAY,
               delete_retention_ms=DAY)]
    cols = [("session_id", "STRING"), ("last_page", "STRING"), ("updated_at", "TIMESTAMP(3)")]
    snk = {"connector": "upsert-kafka", "topic": "session-state", "properties.bootstrap.servers": boot(track),
           "key.format": "json", "value.format": "json"}
    tables = [Table("activity", cols, src_table("a", "session-activity", track, cols, "session-state-job")),
              Table("session_state", cols, snk, pk="session_id")]
    return t, ckpt(False, None), tables, 1, (
        "cleanup.policy=compact,delete keeps the latest value per session and deletes segments older than 30 days; "
        "delete.retention.ms keeps tombstones for 1 day; the sink is an upsert-kafka table keyed by session_id.")


def f_ckpt_retention(track):
    t = [Topic(x, parts(10000, 512), 3, min_insync_replicas=2, retention_ms=7 * DAY) for x in ("ledger-entries", "balances")]
    cols = [("account_id", "STRING"), ("amount", "DECIMAL(18, 2)"), ("ts", "TIMESTAMP(3)")]
    f = ckpt(True, 10000)
    f.update({"execution.checkpointing.num-retained": 5,
              "execution.checkpointing.externalized-checkpoint-retention": "RETAIN_ON_CANCELLATION"})
    tables = [Table("ledger", cols, src_table("l", "ledger-entries", track, cols, "balances-job")),
              Table("balances", cols, sink_opts("balances", track, True, "balances-tx"))]
    return t, f, tables, 1, (
        "Five retained checkpoints, retained on cancellation; exactly-once with a 5 s checkpoint interval.")


def f_elastic(track):
    n = parts(120000, 512)
    t = [Topic("ride-requests", n, 3, retention_ms=3 * DAY), Topic("ride-requests-enriched", n, 3, retention_ms=3 * DAY)]
    cols = [("request_id", "STRING"), ("rider_id", "STRING"), ("ts", "TIMESTAMP(3)")]
    f = ckpt(False, None)
    if track == "native":
        f["scheduler-mode"] = "REACTIVE"
    else:
        f.update({"job.autoscaler.enabled": "true", "job.autoscaler.stabilization.interval": "1m",
                  "job.autoscaler.metrics.window": "5m"})
    tables = [Table("requests", cols, src_table("r", "ride-requests", track, cols, "rides-enrich-job")),
              Table("enriched", cols + [("zone", "STRING")], sink_opts("ride-requests-enriched", track, False, ""))]
    return t, f, tables, par(120000), (
        ("Reactive mode uses every available TaskManager and rescales when TaskManagers join or leave"
         if track == "native" else "The operator's autoscaler adjusts the job's parallelism to the load")
        + f"; {n} partitions for the 120,000 events/s peak.")


FAMILIES = {"cdc-upsert": f_cdc, "interval-join": f_join, "dedup-large-state": f_dedup,
            "backpressure-checkpoints": f_backpressure, "idle-partitions": f_idle, "large-messages": f_large,
            "compression-size-retention": f_compress, "tiered-storage": f_tiered, "independent-consumers": f_consumers,
            "eos-tight-latency": f_tight, "restart-ha": f_restart, "rocksdb-memory": f_rocksdb,
            "mixed-retention": f_mixed, "compact-delete": f_compact_delete, "checkpoint-retention": f_ckpt_retention,
            "elasticity": f_elastic}


def reference(sc: dict) -> str:
    if sc["split"] == "ID":
        return grid(sc)
    if sc["split"] == "CASE":
        return (ROOT.parent / "runs/e7/gold" / f"gold_{sc['track']}.txt").read_text()
    topics, flink, tables, p, why = FAMILIES[sc["family"]](sc["track"])
    return render(sc["track"], topics, flink, tables, p, why, job_name=sc["family"])


def main():
    bench = ROOT / "kfbench-v1.jsonl"
    out = ROOT / "kfbench-v1-reference.jsonl"
    with out.open("w") as f:
        for ln in bench.open():
            sc = json.loads(ln)
            f.write(json.dumps({"id": sc["id"], "text": reference(sc)}) + "\n")
    print(f"wrote {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
