"""Dataset v2, step 2: instruction/response pairs derived deterministically from the official catalogues.

Same documentation as the RAG baseline (Kafka 4.1.2 generated configuration docs, Flink 1.20.5 ConfigOptions and
connector/format options, Flink Kubernetes Operator 1.13 options). No LLM is involved; every response is a template
filled with catalogue fields, so nothing in this component can be hallucinated.
"""
import html
import json
import pathlib
import random
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from kfbench.catalog import Catalog  # noqa: E402

rng = random.Random(20261110)


def clean(s, n=600):
    s = re.sub(r"\s+", " ", html.unescape(str(s or ""))).strip()
    return s[:n]


def pair(q, a, src):
    return {"messages": [{"role": "user", "content": q}, {"role": "assistant", "content": a}], "source": src}


def flink_desc():
    """Descriptions of Flink options from the 1.20 configuration HTML (the reflection dump has no descriptions)."""
    d = {}
    for f in ("flink-config.html", "flink-table-config.html", "flink-kafka-connector.html", "flink-upsert-kafka.html",
              "flink-format-json.html"):
        p = ROOT / "rag" / "raw" / f
        if not p.exists():
            continue
        for row in re.findall(r"<tr>(.*?)</tr>", p.read_text(), re.S):
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.S)
            if len(cells) >= 3 and "<h5>" in cells[0]:
                key = clean(re.sub("<[^>]+>", "", cells[0]))
                d.setdefault(key, clean(re.sub("<[^>]+>", " ", cells[-1])))
    return d


def short_type(t):
    return (t or "").rsplit(".", 1)[-1].replace("$", ".")


def main(out):
    cat = Catalog()
    desc = flink_desc()
    P = []
    # --- Flink cluster / job options
    for key, o in sorted(cat.flink_cluster.items()):
        if o.get("deprecated_field"):
            continue
        t, dflt = short_type(o.get("type")), o.get("default")
        enum = f" Allowed values: {', '.join(o['enum'])}." if o.get("enum") else ""
        d = desc.get(key, "")
        ex = dflt if dflt not in (None, "null") else ("<value>" if not o.get("enum") else o["enum"][0])
        a = (f"`{key}` is an Apache Flink 1.20 configuration option of type {t}"
             f"{'' if dflt in (None, 'null') else f' with default `{dflt}`'}.{enum} {d}".strip() +
             f"\n\nIn `config.yaml`:\n```yaml\n{key}: {ex}\n```")
        P.append(pair(rng.choice([f"What does the Flink 1.20 option `{key}` configure, and how do I set it?",
                                  f"Explain the Apache Flink 1.20 configuration key `{key}`.",
                                  f"How is `{key}` used in a Flink 1.20 cluster configuration?"]), a, "flink-option"))
    # deprecated aliases
    for old, new in sorted(cat.flink_deprecated.items()):
        P.append(pair(f"Is `{old}` still the right Flink configuration key in Flink 1.20?",
                      f"`{old}` is deprecated in Flink 1.20; use `{new}` instead. The deprecated key is still read as a "
                      f"fallback, but new configurations should use `{new}`.", "flink-deprecated"))
    # --- Kafka SQL connector and formats
    for key, o in sorted(cat.table_options.items()):
        if str(o.get("class")).rsplit(".", 1)[-1] not in ("KafkaConnectorOptions", "FactoryUtil") or o.get("deprecated_alias_of"):
            continue
        t, dflt = short_type(o.get("type")), o.get("default")
        enum = f" Allowed values: {', '.join(e.lower().replace('_', '-') for e in o['enum'])}." if o.get("enum") else ""
        a = (f"`{key}` is an option of the Flink SQL Kafka connector (flink-connector-kafka 3.3 for Flink 1.20), type {t}"
             f"{'' if dflt in (None, 'null') else f', default `{dflt}`'}.{enum} {desc.get(key, '')}".strip() +
             f"\n\nIt goes in the `WITH (...)` clause of `CREATE TABLE`, e.g. `'{key}' = '...'`.")
        P.append(pair(f"In a Flink 1.20 SQL `CREATE TABLE ... WITH ('connector' = 'kafka', ...)`, what is the option `{key}`?",
                      a, "flink-kafka-connector"))
    P.append(pair("Which Flink SQL connector options do I need for an exactly-once Kafka sink in Flink 1.20?",
                  "Set `'sink.delivery-guarantee' = 'exactly-once'` and a unique `'sink.transactional-id-prefix'`, and "
                  "enable checkpointing (`execution.checkpointing.interval`): transactions are committed when a checkpoint "
                  "completes, so records become visible to `read_committed` consumers only then. The Kafka sink uses a "
                  "default producer `transaction.timeout.ms` of 1 hour, which exceeds the broker default "
                  "`transaction.max.timeout.ms` of 15 minutes; set `'properties.transaction.timeout.ms'` below the broker "
                  "maximum and above the checkpoint interval plus the expected checkpoint duration.", "flink-kafka-connector"))
    for fmt, opts in sorted(cat.format_options.items()):
        for key, o in sorted(opts.items()):
            P.append(pair(f"What does the `{fmt}` format option `{key}` do in Flink 1.20 SQL?",
                          f"`{fmt}.{key}` (type {short_type(o.get('type'))}, default `{o.get('default')}`) is an option of "
                          f"the `{fmt}` format; in a table that uses `'format' = '{fmt}'` it is written as "
                          f"`'{fmt}.{key}' = '...'` (with `value.`/`key.` prefixes when using `value.format`/`key.format`). "
                          f"{desc.get(key, desc.get(f'{fmt}.{key}', ''))}".strip(), "flink-format"))
    # --- Kafka topic / producer / consumer configs
    for scope, name, ex in (("kafka_topic", "topic-level", "topic"), ("kafka_producer", "producer", "client"),
                            ("kafka_consumer", "consumer", "client")):
        for key, r in sorted(getattr(cat, scope).items()):
            a = (f"`{key}` is an Apache Kafka 4.1 {name} configuration. {clean(r.get('description'))} "
                 f"Type: {r.get('type')}. Default: {r.get('default')}."
                 + (f" Valid values: {r.get('valid_values')}." if r.get('valid_values') else ""))
            if ex == "topic":
                a += (f"\n\nSet it at topic creation with `kafka-topics.sh --bootstrap-server <host:port> --create --topic "
                      f"<name> --config {key}=<value>`, or in a Strimzi `KafkaTopic` under `spec.config`.")
            else:
                a += f"\n\nIn a Flink SQL Kafka table it is passed as `'properties.{key}' = '<value>'`."
            P.append(pair(f"What is the Kafka 4.1 {name} setting `{key}`?", a, f"kafka-{name}"))
    P.append(pair("Can I still create Kafka 4.1 topics with `kafka-topics.sh --zookeeper`?",
                  "No. The `--zookeeper` option was removed from `kafka-topics.sh` in Kafka 3.0 (KIP-604), and Kafka 4.x "
                  "runs only in KRaft mode. Use `--bootstrap-server <host:port>`.", "kafka-cli"))
    for k in ("replication.factor", "partitions", "acks", "num.partitions"):
        if k not in cat.kafka_topic:
            P.append(pair(f"Can I pass `--config {k}=...` to `kafka-topics.sh --create`?",
                          f"No. `{k}` is not a topic-level configuration in Kafka 4.1. "
                          + ("Use `--replication-factor` and `--partitions` on `kafka-topics.sh` (or `spec.replicas` and "
                             "`spec.partitions` in a Strimzi `KafkaTopic`)." if k != "acks" else
                             "`acks` is a producer setting."), "kafka-cli"))
    # --- Flink Kubernetes Operator options
    for key in sorted(cat.operator_keys):
        P.append(pair(f"Where do I set the Flink Kubernetes Operator option `{key}`?",
                      f"`{key}` is a Flink Kubernetes Operator 1.13 configuration option. It can be set in the operator's "
                      f"configuration or per deployment in `FlinkDeployment.spec.flinkConfiguration`.", "flink-operator"))
    rng.shuffle(P)
    with open(out, "w") as f:
        for p in P:
            f.write(json.dumps(p) + "\n")
    from collections import Counter
    print(len(P), Counter(p["source"] for p in P))


if __name__ == "__main__":
    main(sys.argv[1])
