"""Versioned configuration catalogues (Kafka 4.1.2, Flink 1.20.5, Strimzi 0.48, Flink Kubernetes Operator 1.13).

Every catalogue is extracted programmatically from the official distribution or documentation by
`catalog/build_catalog.sh`; nothing here is hand-typed except the small, documented lists marked MANUAL.
"""
from __future__ import annotations

import html
import json
import os
import pathlib
import re
from functools import lru_cache

import yaml

CATALOG_DIR = pathlib.Path(os.environ.get("KFBENCH_CATALOG", pathlib.Path(__file__).resolve().parents[2] / "catalog"))
RAG_RAW = pathlib.Path(os.environ.get("KFBENCH_RAGRAW", CATALOG_DIR.parent / "rag" / "raw"))

# Classes whose options are connector/format options (valid only inside CREATE TABLE ... WITH (...)).
_TABLE_OPTION_CLASSES = {
    "KafkaConnectorOptions", "KafkaConnectorOptionsUtil", "KafkaSourceOptions", "DynamicKafkaSourceOptions",
    "FileSystemConnectorOptions", "DataGenConnectorOptions", "PrintConnectorOptions", "LookupOptions",
    "AsyncSinkConnectorOptions", "PythonDynamicTableOptions", "LookupJoinHintOptions",
}
_FORMAT_CLASSES = {
    "JsonFormatOptions": "json", "CsvFormatOptions": "csv", "RawFormatOptions": "raw",
    "DebeziumJsonFormatOptions": "debezium-json", "CanalJsonFormatOptions": "canal-json",
    "MaxwellJsonFormatOptions": "maxwell-json", "OggJsonFormatOptions": "ogg-json",
}
# MANUAL: generic table options defined in org.apache.flink.table.factories.FactoryUtil (Flink 1.20), which the
# reflection pass does not pick up because the class is not named *Options.
FACTORY_UTIL_OPTIONS = {
    "connector", "format", "property-version", "sink.parallelism", "scan.parallelism",
    "scan.watermark.idle-timeout", "scan.watermark.emit.strategy", "scan.watermark.emit.on-event.gap",
    "scan.watermark.alignment.group", "scan.watermark.alignment.max-drift", "scan.watermark.alignment.update-interval",
    "sink.keyed-shuffle",
}
# MANUAL: Flink SQL connector identifiers shipped by Apache Flink 1.20 / flink-connector-kafka 3.3.
CONNECTORS_AVAILABLE = {"kafka", "upsert-kafka", "filesystem", "datagen", "print", "blackhole"}
# Official formats documented for Flink 1.20 (avro/protobuf/parquet/orc exist but their jars are not in this
# distribution, so their format-specific options cannot be checked: they are accepted and flagged unverifiable).
FORMATS_KNOWN = {"json", "csv", "raw", "debezium-json", "canal-json", "maxwell-json", "ogg-json",
                 "avro", "avro-confluent", "debezium-avro-confluent", "protobuf", "parquet", "orc"}
# Dynamic key prefixes that Flink forwards to plug-ins and therefore cannot be enumerated.
FLINK_DYNAMIC_PREFIXES = ("metrics.reporter.", "s3.", "fs.s3a.", "s3a.", "fs.", "env.java.opts", "containerized.",
                          "kubernetes.jobmanager.annotations", "kubernetes.taskmanager.annotations", "external-resource.",
                          "security.kerberos.", "high-availability.zookeeper.", "pipeline.global-job-parameters.",
                          "state.backend.rocksdb.compaction.style")


class Catalog:
    def __init__(self, d: pathlib.Path = CATALOG_DIR):
        d = pathlib.Path(d)
        k = json.loads((d / "kafka-4.1.2-configs.json").read_text())
        self.kafka_topic = k["topic_config"]
        self.kafka_broker = k["kafka_config"]
        self.kafka_producer = k["producer_config"]
        self.kafka_consumer = k["consumer_config"]
        self.kafka_admin = k.get("admin_client_config", {})
        self.kafka_client_keys = set(self.kafka_producer) | set(self.kafka_consumer) | set(self.kafka_admin)

        self.flink_cluster: dict[str, dict] = {}      # cluster/job configuration (config.yaml)
        self.flink_deprecated: dict[str, str] = {}    # deprecated fallback key -> current key
        self.table_options: dict[str, dict] = {}      # connector options
        self.format_options: dict[str, dict[str, dict]] = {}
        for line in (d / "flink-1.20.5-options.jsonl").read_text().splitlines():
            r = json.loads(line)
            cls = r["class"].rsplit(".", 1)[-1]
            if cls in _FORMAT_CLASSES:
                self.format_options.setdefault(_FORMAT_CLASSES[cls], {})[r["key"]] = r
                continue
            target = self.table_options if cls in _TABLE_OPTION_CLASSES else self.flink_cluster
            prev = target.get(r["key"])
            if prev is None or (prev.get("deprecated_field") and not r.get("deprecated_field")):
                target[r["key"]] = r
            for fb in r.get("fallback", []):
                if fb.get("deprecated"):
                    if cls in _TABLE_OPTION_CLASSES:
                        self.table_options.setdefault(fb["key"], dict(r, deprecated_alias_of=r["key"]))
                    else:
                        self.flink_deprecated[fb["key"]] = r["key"]
        for key in FACTORY_UTIL_OPTIONS:
            self.table_options.setdefault(key, {"key": key, "class": "FactoryUtil", "type": "java.lang.String",
                                                "enum": [], "default": None})
        # Flink Kubernetes Operator 1.13: options documented in the operator's configuration page, plus (v1.3) every
        # ConfigOption extracted by reflection from the 1.13.0 jars with its fallback keys; deprecated declarations
        # are kept, as for Flink, so that they produce a warning instead of an unknown-key failure.
        self.operator_keys = self._operator_keys()
        self.operator_deprecated: set[str] = set()
        self.operator_fallbacks: dict[str, list[str]] = {}
        p = d / "flink-kubernetes-operator-1.13.0-options.jsonl"
        for line in (p.read_text().splitlines() if p.exists() else []):
            r = json.loads(line)
            self.operator_keys.add(r["key"])
            if r.get("deprecated_field"):
                self.operator_deprecated.add(r["key"])
            self.operator_fallbacks[r["key"]] = [fb["key"] for fb in r.get("fallback", [])]
            for fb in r.get("fallback", []):
                self.operator_keys.add(fb["key"])
                if fb.get("deprecated"):
                    self.operator_deprecated.add(fb["key"])
        self.kafka_topics_cli = self._cli_options(d / "cli-kafka-topics-4.1.2.txt")
        self.crds = self._crds(d / "crds")

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _operator_keys() -> set[str]:
        p = RAG_RAW / "operator-config.html"
        if not p.exists():
            return set()
        t = p.read_text()
        return {html.unescape(k).strip() for k in re.findall(r"<td><h5>([^<]+)</h5></td>", t)}

    @staticmethod
    def _cli_options(p: pathlib.Path) -> set[str]:
        return set(re.findall(r"^(--[a-z][a-z0-9-]+)", p.read_text(), re.M)) if p.exists() else set()

    @staticmethod
    def _crds(d: pathlib.Path) -> dict[tuple[str, str], dict]:
        out = {}
        for f in sorted(d.glob("*.y*ml")):
            for doc in yaml.safe_load_all(f.read_text()):
                if not doc or doc.get("kind") != "CustomResourceDefinition":
                    continue
                spec = doc["spec"]
                for v in spec["versions"]:
                    if v.get("served") is False:  # v1.3: a version the API server does not serve is not usable
                        continue
                    schema = (v.get("schema") or {}).get("openAPIV3Schema")
                    if schema:
                        out[(f"{spec['group']}/{v['name']}", spec["names"]["kind"])] = schema
        return out

    # ------------------------------------------------------------------ queries
    def flink_key_status(self, key: str) -> str:
        """'ok' | 'deprecated' | 'operator' | 'dynamic' | 'table-option' | 'unknown'."""
        if key in self.flink_cluster:  # the non-deprecated declaration is preferred when both exist
            return "deprecated" if self.flink_cluster[key].get("deprecated_field") else "ok"
        if key in self.flink_deprecated:
            return "deprecated"
        if key in self.operator_keys or key.startswith(("kubernetes.operator.", "job.autoscaler.")):
            return "operator" if key in self.operator_keys else "unknown"
        if key.startswith(FLINK_DYNAMIC_PREFIXES):
            return "dynamic"
        if key in self.table_options:
            return "table-option"
        return "unknown"

    def flink_option(self, key: str) -> dict | None:
        key = self.flink_deprecated.get(key, key)
        return self.flink_cluster.get(key)

    def flink_default(self, key: str):
        o = self.flink_option(key)
        return None if o is None else o.get("default")

    def topic_default(self, key: str):
        """Effective topic-level default (Kafka docs), resolving broker-level defaults where documented."""
        if key == "partitions":
            return int(self.kafka_broker["num.partitions"]["default"])
        if key == "replication-factor":
            return int(self.kafka_broker["default.replication.factor"]["default"])
        rec = self.kafka_topic.get(key)
        if rec is None or rec.get("default") is None:
            return None
        d = str(rec["default"]).strip()
        m = re.match(r"(-?\d+)\s*(\(|$)", d)  # e.g. "604800000 (7 days)"
        return m.group(1) if m else d


@lru_cache(maxsize=1)
def get_catalog() -> Catalog:
    return Catalog()
