"""Dataset v2, step 3: filter, deduplicate, split by repository, back-translate instructions, assemble.

1. Filter domain_k8s_raw.jsonl: drop manifests of a different product's Flink operator API (flinkoperator.k8s.io),
   whose resources are not those of the Apache Flink Kubernetes Operator used in the evaluation.
2. Deduplicate: exact (sha256 of stripped content) and near duplicates (MinHash/LSH, verified Jaccard >= 0.8 on word
   5-shingles; one file kept per connected component).
3. Split by repository (max_stars_repo_name): 5 % of repositories (hash-based, seed-free) form the validation set.
4. Instruction back-translation (Li et al., 2024) with Qwen2.5-14B-Instruct (temperature 0.2): an informative request
   for each manifest replaces v1's constant prompt; the target is the manifest itself in a ```yaml fence.
5. Assemble with the documentation pairs (doc_pairs.jsonl) and a seeded subsample of 5,000 CodeAlpaca pairs from
   v1's training portion (validation: 250 CodeAlpaca pairs from v1's validation portion).
"""
import argparse
import collections
import hashlib
import json
import os
import random
import re
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "analysis"))
from leakage import near_dup_pairs  # noqa: E402

BT_SYSTEM = "You write realistic requests that platform engineers send to an expert colleague."
CODE_ROOT = Path(__file__).resolve().parents[1]
DATA_V2 = CODE_ROOT / "data_v2"
BT_USER = ("Below is a Kubernetes manifest from a real repository. Write the request (2 to 6 sentences) that an engineer "
           "would have written to obtain exactly this manifest from an expert: state what should be deployed or "
           "configured and every concrete requirement that the values in the manifest satisfy (names, images and "
           "versions, replicas, partitions, replication, retention, resources, ports, checkpointing, and so on). Write it "
           "as a request; do not quote or describe the YAML and do not mention that a manifest already exists. Output only "
           "the request.\n\n```yaml\n{content}\n```")


def repo_in_val(repo):
    return int(hashlib.sha256(repo.encode()).hexdigest(), 16) % 100 < 5


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default=DATA_V2 / "domain_k8s_raw.jsonl", type=Path)
    ap.add_argument("--docs", default=DATA_V2 / "doc_pairs.jsonl", type=Path)
    ap.add_argument("--v1", default=CODE_ROOT / "data", type=Path,
                    help="directory containing the original train.jsonl and test.jsonl")
    ap.add_argument("--out", default=DATA_V2, type=Path)
    a = ap.parse_args()
    rows = [json.loads(l) for l in open(a.raw)]
    stats = {"raw": len(rows)}
    rows = [r for r in rows if not re.search(r"apiVersion:\s*flinkoperator\.k8s\.io", r["content"])]
    stats["after_api_filter"] = len(rows)
    seen, uniq = set(), []
    for r in rows:
        if r["sha256"] not in seen:
            seen.add(r["sha256"])
            uniq.append(r)
    stats["after_exact_dedup"] = len(uniq)
    pairs = near_dup_pairs([r["content"] for r in uniq])
    parent = list(range(len(uniq)))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    for i, j in pairs:
        parent[find(i)] = find(j)
    keep = {}
    for i in range(len(uniq)):
        keep.setdefault(find(i), i)
    uniq = [uniq[i] for i in sorted(keep.values())]
    stats["near_dup_pairs"] = len(pairs)
    stats["after_near_dedup"] = len(uniq)
    for r in uniq:
        r["split"] = "val" if repo_in_val(r["repo"]) else "train"
    stats["repos_train"] = len({r["repo"] for r in uniq if r["split"] == "train"})
    stats["repos_val"] = len({r["repo"] for r in uniq if r["split"] == "val"})
    stats["files_val"] = sum(r["split"] == "val" for r in uniq)
    stats["signals"] = collections.Counter(s for r in uniq for s in r["signals"])
    stats["mention_only"] = sum(not r["signals"] for r in uniq)

    # ---- back-translation
    from vllm import LLM, SamplingParams
    from huggingface_hub import snapshot_download
    llm = LLM(model=snapshot_download("Qwen/Qwen2.5-14B-Instruct", local_files_only=True), dtype="bfloat16",
              max_model_len=8192, gpu_memory_utilization=0.85, seed=0)
    tok = llm.get_tokenizer()
    prompts = [tok.apply_chat_template([{"role": "system", "content": BT_SYSTEM},
                                        {"role": "user", "content": BT_USER.format(content=r["content"])}],
                                       tokenize=False, add_generation_prompt=True) for r in uniq]
    outs = llm.generate(prompts, SamplingParams(temperature=0.2, top_p=0.9, max_tokens=400, seed=0), use_tqdm=False)
    for r, o in zip(uniq, outs):
        r["instruction"] = o.outputs[0].text.strip()
        r["bt_finish"] = o.outputs[0].finish_reason
    stats["bt_truncated"] = sum(r["bt_finish"] == "length" for r in uniq)
    a.out.mkdir(parents=True, exist_ok=True)
    with open(a.out / "domain_k8s_bt.jsonl", "w") as f:
        for r in uniq:
            f.write(json.dumps(r) + "\n")

    def msg(q, ans, src):
        return {"messages": [{"role": "user", "content": q}, {"role": "assistant", "content": ans}], "source": src}

    rng = random.Random(20261110)
    ca_tr = [json.loads(l) for l in open(a.v1 / "train.jsonl")]
    ca_tr = [r for r in ca_tr if not r["messages"][0]["content"].startswith("Generate a Kubernetes configuration file for")]
    ca_va = [json.loads(l) for l in open(a.v1 / "test.jsonl")]
    ca_va = [r for r in ca_va if not r["messages"][0]["content"].startswith("Generate a Kubernetes configuration file for")]
    train = [msg(r["instruction"], f"```yaml\n{r['content'].strip()}\n```", "domain-k8s") for r in uniq if r["split"] == "train"]
    val = [msg(r["instruction"], f"```yaml\n{r['content'].strip()}\n```", "domain-k8s") for r in uniq if r["split"] == "val"]
    train += [dict(json.loads(l)) for l in open(a.docs)]
    train += [dict(r, source="codealpaca") for r in rng.sample(ca_tr, 5000)]
    val += [dict(r, source="codealpaca") for r in rng.sample(ca_va, 250)]
    rng.shuffle(train)
    for name, data in (("train", train), ("val", val)):
        with open(a.out / f"v2_{name}.jsonl", "w") as f:
            for r in data:
                f.write(json.dumps(r) + "\n")
    stats["v2_train"] = collections.Counter(r["source"] for r in train)
    stats["v2_val"] = collections.Counter(r["source"] for r in val)
    json.dump(stats, open(a.out / "v2_stats.json", "w"), indent=1, default=dict)
    print(json.dumps(stats, indent=1, default=dict))


if __name__ == "__main__":
    main()
