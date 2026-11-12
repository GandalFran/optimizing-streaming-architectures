"""E5: duplicate, near-duplicate and repository-overlap analysis of the original training data (train/val split),
and memorisation analysis of model answers against the training corpus.

Near duplicates: MinHash (128 permutations) over word 5-shingles with LSH (32 bands x 4 rows) to propose candidate
pairs, every candidate verified with the exact Jaccard similarity of the shingle sets (threshold 0.8).
"""
import argparse
import collections
import hashlib
import json
import os
import re
from pathlib import Path

import numpy as np

P = (1 << 61) - 1
ROOT = Path(__file__).resolve().parents[1]
RNG = np.random.default_rng(12345)
A = RNG.integers(1, P, size=128, dtype=np.uint64)
B = RNG.integers(0, P, size=128, dtype=np.uint64)


def shingles(text, k=5):
    w = re.findall(r"\S+", text)
    if len(w) < k:
        return {" ".join(w)} if w else set()
    return {" ".join(w[i:i + k]) for i in range(len(w) - k + 1)}


def h64(s):
    return int.from_bytes(hashlib.blake2b(s.encode(), digest_size=8).digest(), "little") % P


def minhash(sh):
    if not sh:
        return np.full(128, P, dtype=np.uint64)
    x = np.array([h64(s) for s in sh], dtype=np.uint64)
    # (a*x + b) mod p computed in float-safe way via Python ints would be slow; use object fallback for exactness
    v = (np.outer(A.astype(object), x.astype(object)) + B.astype(object)[:, None]) % P
    return np.array(v.min(axis=1), dtype=np.uint64)


def near_dup_pairs(docs, thr=0.8, bands=32, rows=4):
    sh = [shingles(d) for d in docs]
    sig = [minhash(s) for s in sh]
    buckets = collections.defaultdict(list)
    for i, s in enumerate(sig):
        for b in range(bands):
            buckets[(b, s[b * rows:(b + 1) * rows].tobytes())].append(i)
    cand = set()
    for ids in buckets.values():
        if 1 < len(ids) < 500:
            for x in range(len(ids)):
                for y in range(x + 1, len(ids)):
                    cand.add((ids[x], ids[y]))
    pairs = []
    for i, j in cand:
        u = len(sh[i] | sh[j])
        if u and len(sh[i] & sh[j]) / u >= thr:
            pairs.append((i, j))
    return pairs


def load(path):
    rows = []
    for line in open(path):
        m = json.loads(line)["messages"]
        rows.append({"k8s": m[0]["content"].startswith("Generate a Kubernetes configuration file for"),
                     "prompt": m[0]["content"], "resp": m[1]["content"]})
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=ROOT / "data", type=Path,
                    help="directory containing the original train.jsonl and test.jsonl")
    ap.add_argument("--prov", default=ROOT / "results" / "k8s_provenance.jsonl", type=Path,
                    help="Kubernetes provenance JSONL stored with this repository")
    ap.add_argument("--out", default=ROOT / "results" / "leakage.json", type=Path,
                    help="output JSON path")
    a = ap.parse_args()
    tr, va = load(a.data / "train.jsonl"), load(a.data / "test.jsonl")
    res = {"n_train": len(tr), "n_val": len(va),
           "n_train_k8s": sum(r["k8s"] for r in tr), "n_val_k8s": sum(r["k8s"] for r in va),
           "distinct_k8s_prompts": len({r["prompt"] for r in tr + va if r["k8s"]})}
    H = lambda s: hashlib.sha256(s.strip().encode()).hexdigest()
    trh = collections.Counter(H(r["prompt"] + "\x00" + r["resp"]) for r in tr)
    res["exact_dup_pairs_within_train"] = sum(v - 1 for v in trh.values() if v > 1)
    tr_resp = {H(r["resp"]) for r in tr}
    for name, sel in (("all", lambda r: True), ("k8s", lambda r: r["k8s"]), ("codealpaca", lambda r: not r["k8s"])):
        vv = [r for r in va if sel(r)]
        res[f"val_resp_exact_in_train_{name}"] = sum(H(r["resp"]) in tr_resp for r in vv)
        res[f"val_n_{name}"] = len(vv)
    # near duplicates among K8s files (train+val), and across the split
    k8s = [(s, r["resp"]) for s, rows in (("train", tr), ("val", va)) for r in rows if r["k8s"]]
    pairs = near_dup_pairs([d for _, d in k8s])
    res["k8s_near_dup_pairs"] = len(pairs)
    res["k8s_near_dup_pairs_cross_split"] = sum(k8s[i][0] != k8s[j][0] for i, j in pairs)
    val_with_nd = {i if k8s[i][0] == "val" else j for i, j in pairs if k8s[i][0] != k8s[j][0]}
    res["k8s_val_files_with_near_dup_in_train"] = len(val_with_nd)
    # repository overlap
    prov = [json.loads(l) for l in open(a.prov)]
    repos = collections.defaultdict(set)
    for p in prov:
        for s in p["split"]:
            repos[s].add(p["repo"])
    res["k8s_repos_train"] = len(repos["train"])
    res["k8s_repos_val"] = len(repos["test"])
    res["k8s_val_repos_also_in_train"] = len(repos["test"] & repos["train"])
    val_files_shared_repo = sum(1 for p in prov if "test" in p["split"] and p["repo"] in repos["train"])
    res["k8s_val_files_from_repos_in_train"] = val_files_shared_repo
    res["k8s_files_mentioning"] = {kw: sum(bool(re.search(kw, d, re.I)) for _, d in k8s)
                                   for kw in ("kafka", "flink", "zookeeper", "strimzi", r"kind:\s*KafkaTopic",
                                              r"kind:\s*FlinkDeployment", "stream")}
    res["k8s_any_stream_kw"] = sum(bool(re.search(r"kafka|flink|zookeeper|strimzi|stream", d, re.I)) for _, d in k8s)
    res["k8s_kinds_top"] = collections.Counter(
        (re.search(r"^kind:\s*(\S+)", d, re.M) or [None, "none"])[1] for _, d in k8s).most_common(12)
    json.dump(res, open(a.out, "w"), indent=1)
    print(json.dumps(res, indent=1))


if __name__ == "__main__":
    main()
