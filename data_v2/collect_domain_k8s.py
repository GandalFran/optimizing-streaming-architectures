"""Dataset v2, step 1: stream the full substratusai/the-stack-yaml-k8s and keep Kafka/Flink manifests.

A file is kept if (a) 50 < len < 10000 characters (as in v1), and (b) it mentions kafka/flink/strimzi or carries a signal:
an apiVersion of Strimzi (kafka.strimzi.io) or of the Apache Flink Kubernetes Operator (flink.apache.org), a
Kafka or Flink container image, or Kafka/Flink configuration keys. Provenance (repo, path, licence, hexsha) is kept
for deduplication and the repository-level split.
"""
import hashlib, json, os, re, sys
from pathlib import Path
from datasets import load_dataset

SIG = {
    "strimzi_api": re.compile(r"apiVersion:\s*kafka\.strimzi\.io/", re.I),
    "flink_operator_api": re.compile(r"apiVersion:\s*flink\.apache\.org/", re.I),
    "kafka_image": re.compile(r"image:\s*['\"]?[\w./-]*(cp-kafka|bitnami/kafka|apache/kafka|wurstmeister/kafka|strimzi/kafka|confluentinc/cp-server)", re.I),
    "flink_image": re.compile(r"image:\s*['\"]?[\w./-]*(?<![a-z])flink[:@'\"\s]", re.I),
    "kafka_keys": re.compile(r"(bootstrap\.servers|num\.partitions|log\.retention\.(ms|hours)|min\.insync\.replicas|KAFKA_(ADVERTISED_LISTENERS|BROKER_ID|ZOOKEEPER_CONNECT|NUM_PARTITIONS|LOG_RETENTION))"),
    "flink_keys": re.compile(r"(taskmanager\.numberOfTaskSlots|jobmanager\.memory|taskmanager\.memory|execution\.checkpointing\.|state\.backend|state\.checkpoints\.dir|flink-conf\.yaml)"),
}
DATA_V2 = Path(__file__).resolve().parent
out = open(DATA_V2 / "domain_k8s_raw.jsonl", "w")
n = kept = 0
for row in load_dataset("substratusai/the-stack-yaml-k8s", split="train", streaming=True):
    n += 1
    c = row.get("content", "")
    if not (50 < len(c) < 10000):
        continue
    sigs = [k for k, rx in SIG.items() if rx.search(c)]
    mention = bool(re.search(r"kafka|flink|strimzi", c, re.I))
    if not sigs and not mention:
        continue
    kept += 1
    out.write(json.dumps({"sha256": hashlib.sha256(c.strip().encode()).hexdigest(), "hexsha": row["hexsha"],
                          "repo": row["max_stars_repo_name"], "path": row["max_stars_repo_path"],
                          "licenses": row["max_stars_repo_licenses"], "signals": sigs, "mention": mention, "content": c}) + "\n")
    if n % 50000 == 0:
        print(n, kept, flush=True)
print("DONE scanned", n, "kept", kept, flush=True)
