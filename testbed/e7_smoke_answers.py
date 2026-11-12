"""Write the E7 smoke-test answers (GOLD answers of tests/test_validator.py and targeted variants)."""
import importlib.util
import sys
from pathlib import Path

REV = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("tv", REV / "tests" / "test_validator.py")
tv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tv)
G = tv.GOLD_NATIVE
out = Path(sys.argv[1] if len(sys.argv) > 1 else REV / "runs" / "e7" / "smoke_answers")
out.mkdir(parents=True, exist_ok=True)
variants = {
    "a_gold_native": G,
    "b_no_txn_timeout": G.replace(",\n  'properties.transaction.timeout.ms' = '600000'", ""),
    "c_no_ckpt_interval": G.replace("    interval: 100 ms\n", ""),
    "d_zookeeper": G.replace("kafka-topics.sh --bootstrap-server kafka:9092", "kafka-topics.sh --zookeeper localhost:2181"),
    "e_gold_k8s": tv.GOLD_K8S,
    # harness-path check only (NOT a validator gold answer): GOLD_K8S with the source options of GOLD_NATIVE
    "e2_gold_k8s_with_group_id": tv.GOLD_K8S.replace(
        "'properties.bootstrap.servers'='my-cluster-kafka-bootstrap:9092','format'='json');",
        "'properties.bootstrap.servers'='my-cluster-kafka-bootstrap:9092','format'='json',\n"
        " 'properties.group.id'='validator','properties.auto.offset.reset'='earliest');", 1),
}
for k, v in variants.items():
    if k not in ("a_gold_native", "e_gold_k8s"):
        assert v not in (G, tv.GOLD_K8S), k
    (out / f"{k}.txt").write_text(v)
    print(out / f"{k}.txt")
