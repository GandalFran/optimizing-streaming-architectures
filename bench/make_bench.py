"""Builds the frozen Kafka/Flink configuration benchmark (KFConfBench v1).

ID  : stratified grid over the requirement factors of the original case study.
OOD : 16 hand-written scenarios (each in both deployment tracks) that exercise configuration knowledge absent
      from the grid (CDC/upsert, joins, large state, backpressure, idle sources, large messages, compression,
      tiered storage, consumer isolation, tight exactly-once latency, restart/HA, RocksDB memory, mixed
      retention, compaction, checkpoint retention, elasticity).
Written and frozen before any model output was produced. Usage: python make_bench.py out.jsonl
"""
from __future__ import annotations

import hashlib
import itertools
import json
import math
import random
import sys

DAY = 86_400_000
GiB = 2 ** 30
PART_BPS = 5_000_000      # one partition sustains at most 5 MB/s of produce traffic (stated in the prompt)
SUBTASK_EPS = 20_000      # one source subtask processes at most 20,000 events/s (stated in the prompt)

BASIS = {
    "topic": "Kafka 4.1 kafka-topics.sh / Strimzi KafkaTopic: a topic referenced by the job must be created",
    "rf_max": "Kafka: replication factor cannot exceed the number of brokers (3)",
    "durability": "Kafka docs (Durability): RF=3 with min.insync.replicas=2 tolerates one broker loss without losing acked data",
    "retention": "Kafka topic config retention.ms",
    "partitions": "Prompt sizing assumption: 5 MB/s per partition and 20k events/s per source subtask",
    "ckpt": "Flink Kafka connector: at-least-once / exactly-once sinks flush or commit on checkpoints; checkpointing is disabled unless execution.checkpointing.interval is set",
    "eos_mode": "Flink checkpointing docs: exactly-once requires CheckpointingMode.EXACTLY_ONCE",
    "eos_sink": "Flink Kafka connector: sink.delivery-guarantee=exactly-once requires sink.transactional-id-prefix",
    "txn": "KafkaSinkBuilder default transaction.timeout.ms = 1 h (javap of connector 3.3.0-1.20) > broker transaction.max.timeout.ms = 15 min (Kafka 4.1 default); must also exceed the checkpoint interval",
    "latency": "Flink Kafka connector: with exactly-once, records become visible to read_committed consumers only when the checkpoint completes",
    "parallelism": "Prompt sizing assumption: 20k events/s per source subtask",
    "slots": "Flink: parallelism must fit into TaskManagers x taskmanager.numberOfTaskSlots (default 1)",
    "memory": "Prompt environment: 8 GB per TaskManager",
    "cpu": "Prompt environment: 4 CPU per TaskManager pod",
    "label": "Strimzi: KafkaTopic must carry strimzi.io/cluster=<cluster> to be reconciled",
    "ckpt_dir": "Flink: without a checkpoint directory, checkpoints are kept in JobManager memory",
    "slot_core": "Common practice: one slot per CPU core",
}


def E(id, type, kind, category, basis, **kw):
    return dict(id=id, type=type, kind=kind, category=category, basis=BASIS.get(basis, basis), **kw)


def req_partitions(eps, bytes_):
    return max(math.ceil(eps * bytes_ / PART_BPS), math.ceil(eps / SUBTASK_EPS), 1)


def pipeline_expect(track, srcs, sinks, delivery, slo_ms, retention, durability, src_bytes):
    """srcs: {topic: eps}; retention: {topic: ms or None}; durability: {topic: 'high'|'standard'}."""
    ex = []
    for t in list(srcs) + list(sinks):
        ex.append(E(f"topic:{t}", "topic_exists", "correctness", "consistency", "topic", topic=t))
        ex.append(E(f"rf_max:{t}", "topic_rf", "resource", "replication", "rf_max", topic=t, max=3))
        if durability.get(t) == "high":
            ex.append(E(f"rf_min:{t}", "topic_rf", "correctness", "durability", "durability", topic=t, min=3))
            ex.append(E(f"minisr:{t}", "topic_config", "correctness", "durability", "durability", topic=t,
                        key="min.insync.replicas", op=">=", value=2))
        if retention.get(t) is not None:
            ex.append(E(f"retention:{t}", "topic_config", "correctness", "retention", "retention", topic=t,
                        key="retention.ms", op="==", value=retention[t]))
        if track == "k8s":
            ex.append(E(f"label:{t}", "topic_label", "correctness", "consistency", "label", topic=t, value="my-cluster"))
    for t, eps in srcs.items():
        ex.append(E(f"partitions:{t}", "topic_partitions_min", "resource", "under-provisioning", "partitions",
                    topic=t, value=req_partitions(eps, src_bytes.get(t, 1024))))
        ex.append(E(f"src_table:{t}", "table_connector", "correctness", "consistency", "topic", topic=t,
                    role="source", value=["kafka", "upsert-kafka"]))
    for t in sinks:
        ex.append(E(f"sink_table:{t}", "table_connector", "correctness", "consistency", "topic", topic=t,
                    role="sink", value=["kafka", "upsert-kafka"]))
    ex.append(E("tables_topics", "tables_reference_topics", "correctness", "consistency", "topic"))
    if delivery in ("exactly-once", "at-least-once"):
        ex.append(E("ckpt_enabled", "flink", "correctness", "checkpointing", "ckpt",
                    key="execution.checkpointing.interval", op="set"))
        ex.append(E("ckpt_dir", "flink_prefix", "policy", "checkpointing", "ckpt_dir",
                    key="execution.checkpointing.dir", prefixes=["s3://", "s3a://", "s3p://"]))
    if delivery == "exactly-once":
        ex.append(E("eos_mode", "flink", "correctness", "exactly-once", "eos_mode",
                    key="execution.checkpointing.mode", op="==", value="EXACTLY_ONCE"))
        for t in sinks:
            ex.append(E(f"eos_sink:{t}", "eos_sink", "correctness", "exactly-once", "eos_sink", topic=t, role="sink"))
            ex.append(E(f"txn_timeout:{t}", "txn_timeout", "correctness", "exactly-once", "txn", topic=t, role="sink"))
        if slo_ms is not None:
            ex.append(E("eos_latency", "flink", "correctness", "latency", "latency",
                        key="execution.checkpointing.interval", op="<=", value=f"{slo_ms} ms"))
    elif delivery == "at-least-once":
        for t in sinks:
            ex.append(E(f"alo_sink:{t}", "table", "correctness", "delivery", "eos_sink", topic=t, role="sink",
                        key="sink.delivery-guarantee", op="in", value=["at-least-once", "exactly-once"],
                        default="at-least-once"))
    ex.append(E("parallelism", "parallelism_min", "resource", "under-provisioning", "parallelism",
                value=max(math.ceil(eps / SUBTASK_EPS) for eps in srcs.values())))
    ex.append(E("slots", "slots_capacity", "resource", "resources", "slots", tms=3))
    ex.append(E("tm_memory", "tm_memory_max", "resource", "resources", "memory", bytes=8 * GiB))
    if track == "k8s":
        ex.append(E("tm_cpu", "tm_cpu_max", "resource", "resources", "cpu", cores=4))
    ex.append(E("slot_core", "slots_per_core", "policy", "resources", "slot_core", cores=4))
    return ex


# ------------------------------------------------------------------------------------------------ prompts
SYSTEM = "You are an expert Software Architect specializing in streaming data platforms (Kafka, Flink)."


def env_text(track, extra=""):
    if track == "native":
        return ("Environment:\n"
                f"- Apache Kafka 4.1 in KRaft mode: 3 brokers with default broker settings{extra}; bootstrap server `kafka:9092`.\n"
                "- Apache Flink 1.20 standalone session cluster: 3 TaskManagers, each with 4 CPU cores and 8 GB of memory; "
                "durable checkpoint storage is available at `s3://flink-state/checkpoints`.")
    return ("Environment:\n"
            f"- Kubernetes with the Strimzi operator 0.48 managing the Kafka cluster `my-cluster` (Kafka 4.1, KRaft, 3 brokers "
            f"with default broker settings{extra}); bootstrap service `my-cluster-kafka-bootstrap:9092`.\n"
            "- Flink Kubernetes Operator 1.13 running Flink 1.20; each TaskManager pod may use at most 4 CPU and 8 GiB of "
            "memory, and at most 3 TaskManager pods may run; durable checkpoint storage is available at "
            "`s3://flink-state/checkpoints`; the SQL job is packaged as `local:///opt/flink/usrlib/sql-runner.jar`.")


SIZING = ("Sizing assumptions: one Kafka partition sustains at most 5 MB/s of produce traffic; one Flink source "
          "subtask processes at most 20,000 events/s.")


def format_text(track):
    if track == "native":
        return ("Answer with exactly three fenced code blocks, in this order:\n"
                "1. ```bash``` with the `kafka-topics.sh` commands that create all topics.\n"
                "2. ```yaml``` with the Flink configuration (`config.yaml` keys).\n"
                "3. ```sql``` with the Flink SQL `CREATE TABLE` statements for every Kafka source and sink table.\n"
                "After the code blocks, briefly justify the main values.")
    return ("Answer with exactly three fenced code blocks, in this order:\n"
            "1. ```yaml``` with the Strimzi `KafkaTopic` resources for all topics.\n"
            "2. ```yaml``` with the `FlinkDeployment` resource (put the Flink configuration in `spec.flinkConfiguration`).\n"
            "3. ```sql``` with the Flink SQL `CREATE TABLE` statements for every Kafka source and sink table.\n"
            "After the code blocks, briefly justify the main values.")


def build_prompt(track, pipeline, requirements, env_extra=""):
    req = "\n".join(f"- {r}" for r in requirements)
    return (f"Design the Apache Kafka topics and the Apache Flink configuration for the following streaming pipeline.\n\n"
            f"Pipeline: {pipeline}\n\nRequirements:\n{req}\n\n{env_text(track, env_extra)}\n\n{SIZING}\n\n{format_text(track)}")


def fmt_ms(ms):
    return f"{ms // 1000} s" if ms >= 1000 and ms % 1000 == 0 else f"{ms} ms"


DELIVERY_TXT = {"exactly-once": "exactly-once end-to-end (no duplicates and no loss, also after failures)",
                "at-least-once": "at-least-once (no loss after failures; duplicates are acceptable)"}
DURABILITY_TXT = {"high": "both topics must survive the loss of any single Kafka broker without losing acknowledged data",
                  "standard": "default durability is sufficient"}

DOMAINS = [("payment", "payments", "payments-validated", "account_id"),
           ("card transaction", "card-transactions", "card-transactions-enriched", "card_id"),
           ("clickstream", "clicks", "clicks-enriched", "session_id"),
           ("IoT telemetry", "iot-telemetry", "iot-telemetry-clean", "device_id"),
           ("order", "orders", "orders-validated", "order_id"),
           ("inventory update", "inventory-updates", "inventory-updates-enriched", "sku"),
           ("ad impression", "ad-impressions", "ad-impressions-enriched", "campaign_id"),
           ("vehicle GPS", "vehicle-positions", "vehicle-positions-clean", "vehicle_id"),
           ("trade", "trades", "trades-enriched", "instrument_id"),
           ("application log", "app-logs", "app-logs-parsed", "service"),
           ("user session", "session-events", "session-events-enriched", "user_id"),
           ("sensor reading", "sensor-readings", "sensor-readings-clean", "sensor_id")]


def grid_scenarios(seed=20261110):
    rng = random.Random(seed)
    tps, deliveries, slos, durs, tracks = [5000, 20000, 50000, 150000], ["at-least-once", "exactly-once"], \
        [100, 1000, 10000], ["standard", "high"], ["native", "k8s"]
    out = []
    n = 0
    for rep in range(2):
        for tp, dl, slo, du, tr in itertools.product(tps, deliveries, slos, durs, tracks):
            n += 1
            dom, src, sink, key = rng.choice(DOMAINS)
            size = rng.choice([512, 2048])
            ret_days = rng.choice([1, 7, 30])
            pipeline = (f"a Flink SQL job reads {dom} events (JSON, keyed by `{key}`) from the Kafka topic `{src}`, "
                        f"filters out malformed events, enriches each event with derived fields, and writes every valid "
                        f"event to the Kafka topic `{sink}` (a per-event transformation without windows or joins).")
            reqs = [f"Input rate: {tp:,} events/s sustained; average event size {size} bytes.",
                    f"End-to-end latency (event produced to result visible to downstream consumers): below {fmt_ms(slo)} at p99.",
                    f"Delivery guarantee: {DELIVERY_TXT[dl]}.",
                    f"Retention: keep data in both topics for {ret_days} days.",
                    f"Durability: {DURABILITY_TXT[du]}."]
            spec = dict(throughput_eps=tp, event_bytes=size, latency_slo_ms=slo, delivery=dl, retention_days=ret_days,
                        durability=du, src=src, sink=sink)
            ex = pipeline_expect(tr, {src: tp}, [sink], dl, slo, {src: ret_days * DAY, sink: ret_days * DAY},
                                 {src: du, sink: du}, {src: size})
            out.append(dict(id=f"ID-{n:03d}", split="ID", track=tr, family="grid", spec=spec, system=SYSTEM,
                            prompt=build_prompt(tr, pipeline, reqs), expect=ex))
    return out


def case_study():
    """The banking case study of the original manuscript, made explicit (both tracks)."""
    out = []
    for tr in ("native", "k8s"):
        pipeline = ("a Flink SQL job reads banking transactions (JSON, keyed by `account_id`) from the Kafka topic "
                    "`banking-transactions`, validates and enriches each transaction, and writes it to the Kafka topic "
                    "`banking-transactions-validated` (a per-event transformation without windows or joins).")
        reqs = ["Input rate: 50,000 events/s sustained; average event size 1024 bytes.",
                "End-to-end latency (event produced to result visible to downstream consumers): below 100 ms at p99.",
                f"Delivery guarantee: {DELIVERY_TXT['exactly-once']}.",
                "Retention: keep data in both topics for 7 days.",
                f"Durability: {DURABILITY_TXT['high']}."]
        src, sink = "banking-transactions", "banking-transactions-validated"
        ex = pipeline_expect(tr, {src: 50000}, [sink], "exactly-once", 100, {src: 7 * DAY, sink: 7 * DAY},
                             {src: "high", sink: "high"}, {src: 1024})
        out.append(dict(id=f"CASE-banking-{tr}", split="CASE", track=tr, family="case-study",
                        spec=dict(throughput_eps=50000, event_bytes=1024, latency_slo_ms=100, delivery="exactly-once",
                                  retention_days=7, durability="high", src=src, sink=sink),
                        system=SYSTEM, prompt=build_prompt(tr, pipeline, reqs), expect=ex))
    return out


# ------------------------------------------------------------------------------------------------ OOD
def F(id, key, op, value=None, kind="correctness", category="ood", basis=""):
    return E(id, "flink", kind, category, basis, key=key, op=op, value=value)


def T(id, topic, key, op, value, kind="correctness", category="ood", basis=""):
    return E(id, "topic_config", kind, category, basis, topic=topic, key=key, op=op, value=value)


def ood_scenarios():
    S = []

    def add(name, pipeline, reqs, srcs, sinks, delivery, slo, retention, durability, src_bytes, extra,
            env_extra="", track_extra=None):
        for tr in ("native", "k8s"):
            ex = pipeline_expect(tr, srcs, sinks, delivery, slo, retention, durability, src_bytes)
            ex = [e for e in ex if e["id"] not in {x["id"] for x in extra}] + list(extra)
            if track_extra and track_extra.get(tr):
                ex += track_extra[tr]["expect"]
                reqs_tr = reqs + track_extra[tr].get("reqs", [])
            else:
                reqs_tr = reqs
            S.append(dict(id=f"OOD-{len(S) // 2 + 1:02d}-{name}-{tr}", split="OOD", track=tr, family=name, spec={},
                          system=SYSTEM, prompt=build_prompt(tr, pipeline, reqs_tr, env_extra), expect=ex))

    add("cdc-upsert",
        "a Flink SQL job reads Debezium change events (JSON) of the database table `customers` from the Kafka topic "
        "`pg.public.customers` and maintains the latest profile per `customer_id` in the Kafka topic `customers-latest`, "
        "which downstream services read as a table where only the latest value per key matters.",
        ["Input rate: 5,000 change events/s; average event size 1024 bytes.",
         "Results must be visible downstream within 5 s.",
         f"Delivery guarantee: {DELIVERY_TXT['at-least-once']}.",
         "Retention: keep change events in `pg.public.customers` for 3 days; `customers-latest` must keep the latest value "
         "of every key indefinitely.",
         "Durability: both topics must survive the loss of any single Kafka broker without losing acknowledged data."],
        {"pg.public.customers": 5000}, ["customers-latest"], "at-least-once", None,
        {"pg.public.customers": 3 * DAY}, {"pg.public.customers": "high", "customers-latest": "high"},
        {"pg.public.customers": 1024},
        [T("compacted:customers-latest", "customers-latest", "cleanup.policy", "==", "compact", category="topic-semantics",
           basis="Kafka log compaction keeps the latest value per key"),
         E("src_format", "table_any", "correctness", "consistency", "Flink debezium-json format", topic="pg.public.customers",
           role="source", keys=["value.format", "format"], value="debezium-json"),
         E("upsert_sink", "table_connector", "correctness", "topic-semantics", "Flink upsert-kafka connector",
           topic="customers-latest", role="sink", value=["upsert-kafka"]),
         E("upsert_pk", "table_pk", "correctness", "topic-semantics", "upsert-kafka requires a PRIMARY KEY",
           topic="customers-latest", role="sink")])

    add("interval-join",
        "a Flink SQL job joins the Kafka topics `orders` and `payments` on `order_id`, emitting an order when its payment "
        "arrives within 15 minutes of the order according to the event-time column `event_ts` (an interval join), and "
        "writes paid orders to the Kafka topic `orders-paid`.",
        ["Input rate: 20,000 events/s on each of `orders` and `payments`; average event size 2048 bytes.",
         "End-to-end latency (event produced to result visible to downstream consumers): below 2 s at p99.",
         f"Delivery guarantee: {DELIVERY_TXT['exactly-once']}.",
         "Retention: keep data in all topics for 7 days.",
         "Durability: all topics must survive the loss of any single Kafka broker without losing acknowledged data."],
        {"orders": 20000, "payments": 20000}, ["orders-paid"], "exactly-once", 2000,
        {t: 7 * DAY for t in ("orders", "payments", "orders-paid")}, {t: "high" for t in ("orders", "payments", "orders-paid")},
        {"orders": 2048, "payments": 2048},
        [E("wm:orders", "table_watermark", "correctness", "event-time", "Flink SQL: event-time interval joins need time attributes (WATERMARK)", topic="orders"),
         E("wm:payments", "table_watermark", "correctness", "event-time", "Flink SQL: event-time interval joins need time attributes (WATERMARK)", topic="payments")])

    add("dedup-large-state",
        "a Flink SQL job deduplicates the Kafka topic `clicks` by `event_id` over a 24-hour horizon and writes each "
        "unique click once to the Kafka topic `clicks-unique`. The deduplication state is expected to reach about 200 GB.",
        ["Input rate: 30,000 events/s; average event size 512 bytes.",
         "End-to-end latency (event produced to result visible to downstream consumers): below 60 s at p99.",
         f"Delivery guarantee: {DELIVERY_TXT['exactly-once']}.",
         "Duplicates of the same `event_id` arriving more than 24 hours apart may be emitted again; state must not grow without bound.",
         "Retention: keep data in both topics for 1 day.",
         "Durability: default durability is sufficient."],
        {"clicks": 30000}, ["clicks-unique"], "exactly-once", 60000, {"clicks": DAY, "clicks-unique": DAY},
        {}, {"clicks": 512},
        [F("rocksdb", "state.backend.type", "==", "rocksdb", kind="resource", category="state",
           basis="200 GB of state cannot be held on the JVM heap of 3 x 8 GB TaskManagers"),
         F("state_ttl", "table.exec.state.ttl", ">=", "24 h", kind="resource", category="state",
           basis="Flink SQL: table.exec.state.ttl bounds state; 0 means never expire"),
         F("incremental", "execution.checkpointing.incremental", "==", "true", kind="policy", category="state",
           basis="Flink docs: incremental checkpoints for large RocksDB state")])

    add("backpressure-checkpoints",
        "a Flink SQL job aggregates the Kafka topic `market-ticks` per `symbol` in 10-second tumbling windows and writes "
        "the aggregates to the Kafka topic `market-bars`. A few symbols dominate the traffic, so sustained backpressure "
        "is expected.",
        ["Input rate: 50,000 events/s; average event size 512 bytes.",
         "Checkpoints must keep completing in well under 60 s even under sustained backpressure.",
         "End-to-end latency (event produced to result visible to downstream consumers): below 30 s at p99.",
         f"Delivery guarantee: {DELIVERY_TXT['exactly-once']}.",
         "Retention: keep data in both topics for 1 day.", "Durability: default durability is sufficient."],
        {"market-ticks": 50000}, ["market-bars"], "exactly-once", 30000, {"market-ticks": DAY, "market-bars": DAY}, {},
        {"market-ticks": 512},
        [E("bp_mechanism", "any_of", "correctness", "checkpointing", "Flink docs: unaligned checkpoints or buffer debloating keep checkpoints fast under backpressure",
           options=[dict(type="flink", key="execution.checkpointing.unaligned.enabled", op="==", value="true"),
                    dict(type="flink", key="taskmanager.network.memory.buffer-debloat.enabled", op="==", value="true")]),
         E("unaligned_concurrency", "any_of", "correctness", "checkpointing", "Flink docs: unaligned checkpoints only work with exactly-once and one concurrent checkpoint",
           options=[dict(type="flink", key="execution.checkpointing.unaligned.enabled", op="==", value="false"),
                    dict(type="flink", key="execution.checkpointing.max-concurrent-checkpoints", op="==", value=1)])])

    add("idle-partitions",
        "a Flink SQL job computes 5-minute event-time tumbling windows per `device_id` over the Kafka topic "
        "`meter-readings` (event-time column `ts`; the topic must have 64 partitions) and writes the aggregates to the "
        "Kafka topic `meter-aggregates`. At night many partitions receive no data for hours, but windows of the active "
        "partitions must still close on time.",
        ["Input rate: 10,000 events/s during the day; average event size 512 bytes.",
         f"Delivery guarantee: {DELIVERY_TXT['at-least-once']}.",
         "Retention: keep data in both topics for 7 days.", "Durability: default durability is sufficient."],
        {"meter-readings": 10000}, ["meter-aggregates"], "at-least-once", None,
        {"meter-readings": 7 * DAY, "meter-aggregates": 7 * DAY}, {}, {"meter-readings": 512},
        [E("partitions:meter-readings", "topic_partitions_min", "correctness", "consistency", "Stated in the pipeline", topic="meter-readings", value=64),
         E("wm:meter-readings", "table_watermark", "correctness", "event-time", "Event-time windows need a WATERMARK", topic="meter-readings"),
         E("idleness", "any_of", "correctness", "event-time", "Flink: idle sources hold back watermarks unless an idle timeout is set",
           options=[dict(type="flink", key="table.exec.source.idle-timeout", op=">", value="0 ms"),
                    dict(type="table", topic="meter-readings", key="scan.watermark.idle-timeout", op="set")])])

    add("large-messages",
        "a Flink SQL job reads document events from the Kafka topic `documents`, extracts metadata, and writes the "
        "documents with their metadata to the Kafka topic `documents-indexed`. Individual events can be as large as 4 MB.",
        ["Input rate: 200 events/s; average event size 1 MB, maximum 4 MB.",
         f"Delivery guarantee: {DELIVERY_TXT['at-least-once']}.",
         "Retention: keep data in both topics for 2 days.", "Durability: default durability is sufficient."],
        {"documents": 200}, ["documents-indexed"], "at-least-once", None,
        {"documents": 2 * DAY, "documents-indexed": 2 * DAY}, {}, {"documents": 1_000_000},
        [T("msgsize:documents", "documents", "max.message.bytes", ">=", 4_194_304, category="message-size",
           basis="Kafka: records larger than max.message.bytes (default ~1 MB) are rejected"),
         T("msgsize:documents-indexed", "documents-indexed", "max.message.bytes", ">=", 4_194_304, category="message-size",
           basis="Kafka: records larger than max.message.bytes (default ~1 MB) are rejected"),
         E("producer_size", "table", "correctness", "message-size", "Kafka producer max.request.size (default 1 MB) limits record size",
           topic="documents-indexed", role="sink", key="properties.max.request.size", op=">=", value=4_194_304, default=1_048_576)])

    add("compression-size-retention",
        "a Flink SQL job reads application logs from the Kafka topic `logs-raw`, keeps ERROR and WARN entries, and "
        "writes them to the Kafka topic `logs-alerts`.",
        ["Input rate: 100,000 events/s; average event size 512 bytes.",
         "`logs-raw` must be stored compressed with zstd regardless of the compression used by the producers.",
         "`logs-raw` must retain at most 50 GiB per partition or 3 days, whichever limit is reached first; "
         "`logs-alerts` keeps data for 7 days.",
         f"Delivery guarantee: {DELIVERY_TXT['at-least-once']}.", "Durability: default durability is sufficient."],
        {"logs-raw": 100000}, ["logs-alerts"], "at-least-once", None, {"logs-raw": 3 * DAY, "logs-alerts": 7 * DAY}, {},
        {"logs-raw": 512},
        [T("zstd:logs-raw", "logs-raw", "compression.type", "==", "zstd", category="topic-semantics",
           basis="Kafka topic-level compression.type overrides the producer codec"),
         T("retbytes:logs-raw", "logs-raw", "retention.bytes", "==", 50 * GiB, category="retention",
           basis="Kafka retention.bytes is per partition")])

    add("tiered-storage",
        "a Flink SQL job enriches trades from the Kafka topic `trades` and writes them to the Kafka topic `trades-enriched`.",
        ["Input rate: 20,000 events/s; average event size 1024 bytes.",
         "End-to-end latency (event produced to result visible to downstream consumers): below 1 s at p99.",
         f"Delivery guarantee: {DELIVERY_TXT['exactly-once']}.",
         "`trades` must be kept for 90 days, but only the most recent 24 hours may stay on the brokers' local disks; "
         "`trades-enriched` keeps data for 7 days.",
         "Durability: both topics must survive the loss of any single Kafka broker without losing acknowledged data."],
        {"trades": 20000}, ["trades-enriched"], "exactly-once", 1000, {"trades": 90 * DAY, "trades-enriched": 7 * DAY},
        {"trades": "high", "trades-enriched": "high"}, {"trades": 1024},
        [T("remote:trades", "trades", "remote.storage.enable", "==", "true", category="tiered-storage",
           basis="Kafka tiered storage (KIP-405): topic-level remote.storage.enable"),
         T("local_ret:trades", "trades", "local.retention.ms", "==", DAY, category="tiered-storage",
           basis="Kafka tiered storage: local.retention.ms bounds local data")],
        env_extra=", plus tiered storage enabled on the brokers (`remote.log.storage.system.enable=true` with an S3 remote storage manager)")

    add("independent-consumers",
        "two independent Flink SQL pipelines read the same Kafka topic `card-payments`: the first scores fraud and writes "
        "to the Kafka topic `fraud-scores`; the second computes per-merchant statistics and writes to the Kafka topic "
        "`merchant-stats`. Each pipeline must consume `card-payments` completely and independently, with its own "
        "committed offsets.",
        ["Input rate: 30,000 events/s; average event size 1024 bytes.",
         f"Delivery guarantee: {DELIVERY_TXT['at-least-once']}.",
         "Retention: keep data in all topics for 7 days.", "Durability: default durability is sufficient."],
        {"card-payments": 30000}, ["fraud-scores", "merchant-stats"], "at-least-once", None,
        {t: 7 * DAY for t in ("card-payments", "fraud-scores", "merchant-stats")}, {}, {"card-payments": 1024},
        [E("group_ids", "distinct_option", "correctness", "consistency", "Kafka: consumers sharing a group.id split the partitions instead of each reading all data",
           topic="card-payments", key="properties.group.id", min_tables=2)])

    add("eos-tight-latency",
        "a Flink SQL job validates payment instructions from the Kafka topic `payment-instructions` and writes accepted "
        "instructions to the Kafka topic `payment-instructions-accepted`, which is read by `read_committed` consumers.",
        ["Input rate: 5,000 events/s; average event size 512 bytes.",
         "End-to-end latency (event produced to result visible to downstream consumers): below 50 ms at p99.",
         f"Delivery guarantee: {DELIVERY_TXT['exactly-once']}.",
         "Retention: keep data in both topics for 7 days.",
         "Durability: both topics must survive the loss of any single Kafka broker without losing acknowledged data."],
        {"payment-instructions": 5000}, ["payment-instructions-accepted"], "exactly-once", 50,
        {t: 7 * DAY for t in ("payment-instructions", "payment-instructions-accepted")},
        {t: "high" for t in ("payment-instructions", "payment-instructions-accepted")}, {"payment-instructions": 512},
        [F("min_pause", "execution.checkpointing.min-pause", "<=", "50 ms", category="latency",
           basis="Flink: min-pause between checkpoints lengthens the effective checkpoint interval")])

    restart = [F("restart_type", "restart-strategy.type", "in", ["failure-rate", "failurerate"], category="fault-tolerance",
                 basis="Flink restart strategies"),
               F("restart_max", "restart-strategy.failure-rate.max-failures-per-interval", "==", 10, category="fault-tolerance",
                 basis="Flink failure-rate restart strategy"),
               F("restart_interval", "restart-strategy.failure-rate.failure-rate-interval", "==", "10 min",
                 category="fault-tolerance", basis="Flink failure-rate restart strategy")]
    add("restart-ha",
        "a Flink SQL job enriches shipment events from the Kafka topic `shipments` and writes them to the Kafka topic "
        "`shipments-enriched`.",
        ["Input rate: 10,000 events/s; average event size 1024 bytes.",
         f"Delivery guarantee: {DELIVERY_TXT['at-least-once']}.",
         "After a failure the job must restart automatically, but it must give up (fail permanently) if more than 10 "
         "failures happen within 10 minutes.",
         "Retention: keep data in both topics for 7 days.", "Durability: default durability is sufficient."],
        {"shipments": 10000}, ["shipments-enriched"], "at-least-once", None,
        {"shipments": 7 * DAY, "shipments-enriched": 7 * DAY}, {}, {"shipments": 1024}, restart,
        track_extra={"k8s": {"reqs": ["A JobManager failure must not lose the job: use Kubernetes-based high availability."],
                             "expect": [F("ha_type", "high-availability.type", "in",
                                          ["kubernetes", "org.apache.flink.kubernetes.highavailability.kuberneteshaservicesfactory"],
                                          category="fault-tolerance", basis="Flink Kubernetes HA services"),
                                        F("ha_dir", "high-availability.storageDir", "set", category="fault-tolerance",
                                          basis="Flink HA requires high-availability.storageDir")]}})

    add("rocksdb-memory",
        "a Flink SQL job maintains per-`user_id` rolling 7-day activity counters (an OVER aggregation) from the Kafka "
        "topic `user-activity` and writes them to the Kafka topic `user-activity-features`. The keyed state is about "
        "60 GB in total.",
        ["Input rate: 15,000 events/s; average event size 512 bytes.",
         "Keyed state must live in RocksDB, and RocksDB must be able to use at least 4 GB of each TaskManager's 8 GB through Flink managed memory.",
         f"Delivery guarantee: {DELIVERY_TXT['at-least-once']}.",
         "Retention: keep data in both topics for 7 days.", "Durability: default durability is sufficient."],
        {"user-activity": 15000}, ["user-activity-features"], "at-least-once", None,
        {"user-activity": 7 * DAY, "user-activity-features": 7 * DAY}, {}, {"user-activity": 512},
        [F("rocksdb", "state.backend.type", "==", "rocksdb", category="state", basis="Requirement"),
         F("rocksdb_managed", "state.backend.rocksdb.memory.managed", "==", "true", category="memory",
           basis="Flink: RocksDB uses managed memory only when state.backend.rocksdb.memory.managed=true"),
         E("managed_mem", "any_of", "correctness", "memory", "Flink memory model: managed memory = fraction x total Flink memory, or an explicit size",
           options=[dict(type="flink", key="taskmanager.memory.managed.fraction", op=">=", value=0.5),
                    dict(type="flink", key="taskmanager.memory.managed.size", op=">=", value="4 gb")])])

    add("mixed-retention",
        "a Flink SQL job routes events from the Kafka topic `platform-events` into two Kafka topics: security alerts go to "
        "`alerts` and every event is copied to `audit-log`.",
        ["Input rate: 40,000 events/s; average event size 512 bytes.",
         f"Delivery guarantee: {DELIVERY_TXT['at-least-once']}.",
         "`platform-events` keeps data for 7 days; `alerts` keeps data for 1 day and a single replica is acceptable "
         "because alerts can be regenerated; `audit-log` keeps data for 365 days and must survive the loss of any single "
         "Kafka broker without losing acknowledged data."],
        {"platform-events": 40000}, ["alerts", "audit-log"], "at-least-once", None,
        {"platform-events": 7 * DAY, "alerts": DAY, "audit-log": 365 * DAY}, {"audit-log": "high"},
        {"platform-events": 512}, [])

    add("compact-delete",
        "a Flink SQL job reads session events from the Kafka topic `session-activity` and maintains the latest state of "
        "every session, keyed by `session_id`, in the Kafka topic `session-state` (an upsert table).",
        ["Input rate: 15,000 events/s; average event size 512 bytes.",
         f"Delivery guarantee: {DELIVERY_TXT['at-least-once']}.",
         "`session-state` must keep the latest value per session, but sessions inactive for more than 30 days must be "
         "removed entirely; deletion markers (tombstones) must be kept for 1 day.",
         "`session-activity` keeps data for 3 days.", "Durability: default durability is sufficient."],
        {"session-activity": 15000}, ["session-state"], "at-least-once", None, {"session-activity": 3 * DAY}, {},
        {"session-activity": 512},
        [E("compact_delete", "any_of", "correctness", "topic-semantics", "Kafka cleanup.policy=compact,delete",
           options=[dict(type="topic_config", topic="session-state", key="cleanup.policy", op="==", value="compact,delete"),
                    dict(type="topic_config", topic="session-state", key="cleanup.policy", op="==", value="delete,compact")]),
         T("ret:session-state", "session-state", "retention.ms", "==", 30 * DAY, category="retention", basis="Kafka retention.ms"),
         T("tombstone:session-state", "session-state", "delete.retention.ms", "==", DAY, category="topic-semantics",
           basis="Kafka delete.retention.ms keeps tombstones"),
         E("upsert_sink", "table_connector", "correctness", "topic-semantics", "upsert-kafka", topic="session-state",
           role="sink", value=["upsert-kafka"]),
         E("upsert_pk", "table_pk", "correctness", "topic-semantics", "upsert-kafka requires a PRIMARY KEY",
           topic="session-state", role="sink")])

    add("checkpoint-retention",
        "a Flink SQL job computes account balances from the Kafka topic `ledger-entries` and writes balance updates to "
        "the Kafka topic `balances`.",
        ["Input rate: 10,000 events/s; average event size 512 bytes.",
         "End-to-end latency (event produced to result visible to downstream consumers): below 10 s at p99.",
         f"Delivery guarantee: {DELIVERY_TXT['exactly-once']}.",
         "Operators must be able to restore the job from any of the last 5 completed checkpoints, and checkpoints must "
         "be kept when the job is cancelled.",
         "Retention: keep data in both topics for 7 days.",
         "Durability: both topics must survive the loss of any single Kafka broker without losing acknowledged data."],
        {"ledger-entries": 10000}, ["balances"], "exactly-once", 10000,
        {"ledger-entries": 7 * DAY, "balances": 7 * DAY}, {"ledger-entries": "high", "balances": "high"},
        {"ledger-entries": 512},
        [F("num_retained", "execution.checkpointing.num-retained", "==", 5, category="checkpointing",
           basis="Flink execution.checkpointing.num-retained (alias state.checkpoints.num-retained)"),
         F("externalized", "execution.checkpointing.externalized-checkpoint-retention", "==", "RETAIN_ON_CANCELLATION",
           category="checkpointing", basis="Flink externalized checkpoint retention"),
         E("ckpt_dir", "flink_prefix", "correctness", "checkpointing", "Retained checkpoints need durable storage",
           key="execution.checkpointing.dir", prefixes=["s3://", "s3a://", "s3p://"])])

    add("elasticity",
        "a Flink SQL job enriches ride requests from the Kafka topic `ride-requests` and writes them to the Kafka topic "
        "`ride-requests-enriched`.",
        ["Input rate: varies during the day between 5,000 and 120,000 events/s; average event size 512 bytes.",
         f"Delivery guarantee: {DELIVERY_TXT['at-least-once']}.",
         "Retention: keep data in both topics for 3 days.", "Durability: default durability is sufficient."],
        {"ride-requests": 120000}, ["ride-requests-enriched"], "at-least-once", None,
        {"ride-requests": 3 * DAY, "ride-requests-enriched": 3 * DAY}, {}, {"ride-requests": 512}, [],
        track_extra={"native": {"reqs": ["The job must automatically use all TaskManagers that are available and rescale "
                                         "itself when TaskManagers are added or removed (standalone deployment)."],
                                "expect": [F("reactive", "scheduler-mode", "==", "REACTIVE", category="elasticity",
                                             basis="Flink reactive mode for standalone application clusters")]},
                     "k8s": {"reqs": ["The Flink Kubernetes Operator must automatically scale the job's parallelism with the load."],
                             "expect": [F("autoscaler", "job.autoscaler.enabled", "==", "true", category="elasticity",
                                          basis="Flink Kubernetes Operator autoscaler")]}})
    return S


def main(out):
    scen = grid_scenarios() + case_study() + ood_scenarios()
    ids = [s["id"] for s in scen]
    assert len(ids) == len(set(ids)), "duplicate ids"
    with open(out, "w") as f:
        for s in scen:
            f.write(json.dumps(s, sort_keys=True) + "\n")
    h = hashlib.sha256(open(out, "rb").read()).hexdigest()
    by = {}
    for s in scen:
        by[(s["split"], s["track"])] = by.get((s["split"], s["track"]), 0) + 1
    print(f"{len(scen)} scenarios {by} sha256={h}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "kfbench-v1.jsonl")
