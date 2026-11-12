"""Dataset v2, step 4: relevance filter and assembly (run after build_v2.py, whose back-translations it reuses).

Relevance filter, decided by inspecting a random sample of back-translated manifests and before any v2 model was
trained or evaluated: many files that mention kafka/flink/strimzi only once are unrelated cluster dumps (RBAC
bindings, EndpointSlices) in which the word appears incidentally. A manifest is kept if it carries a Kafka/Flink
signal (see collect_domain_k8s.py) or mentions kafka|flink|strimzi at least three times.
"""
import collections, json, os, pathlib, random, re

CODE_ROOT = pathlib.Path(__file__).resolve().parents[1]
D = pathlib.Path(os.environ.get("KFCONF_DATA_V2", CODE_ROOT / "data_v2"))
V1 = pathlib.Path(os.environ.get("KFCONF_ORIGINAL_DATA", CODE_ROOT / "data"))
rows = [json.loads(l) for l in open(D / "domain_k8s_bt.jsonl")]
n_mention = lambda r: len(re.findall(r"kafka|flink|strimzi", r["content"], re.I))
kept = [r for r in rows if r["signals"] or n_mention(r) >= 3]
stats = json.load(open(D / "v2_stats.json"))
stats.update(relevance_filtered=len(kept), relevance_dropped=len(rows) - len(kept),
             kept_repos_train=len({r["repo"] for r in kept if r["split"] == "train"}),
             kept_repos_val=len({r["repo"] for r in kept if r["split"] == "val"}),
             kept_files_val=sum(r["split"] == "val" for r in kept),
             kept_with_signal=sum(bool(r["signals"]) for r in kept))
msg = lambda q, a, s: {"messages": [{"role": "user", "content": q}, {"role": "assistant", "content": a}], "source": s}
rng = random.Random(20261110)
is_k8s = lambda r: r["messages"][0]["content"].startswith("Generate a Kubernetes configuration file for")
ca_tr = [r for r in map(json.loads, open(V1 / "train.jsonl")) if not is_k8s(r)]
ca_va = [r for r in map(json.loads, open(V1 / "test.jsonl")) if not is_k8s(r)]
train = [msg(r["instruction"], f"```yaml\n{r['content'].strip()}\n```", "domain-k8s") for r in kept if r["split"] == "train"]
val = [msg(r["instruction"], f"```yaml\n{r['content'].strip()}\n```", "domain-k8s") for r in kept if r["split"] == "val"]
train += [json.loads(l) for l in open(D / "doc_pairs.jsonl")]
train += [dict(r, source="codealpaca") for r in rng.sample(ca_tr, 5000)]
val += [dict(r, source="codealpaca") for r in rng.sample(ca_va, 250)]
rng.shuffle(train)
for name, data in (("train", train), ("val", val)):
    with open(D / f"v2_{name}.jsonl", "w") as f:
        for r in data:
            f.write(json.dumps(r) + "\n")
stats["v2_train"] = dict(collections.Counter(r["source"] for r in train))
stats["v2_val"] = dict(collections.Counter(r["source"] for r in val))
stats["v2_train_total"], stats["v2_val_total"] = len(train), len(val)
json.dump(stats, open(D / "v2_stats.json", "w"), indent=1)
print(json.dumps({k: v for k, v in stats.items() if k.startswith(("relevance", "kept", "v2_"))}, indent=1))
