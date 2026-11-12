"""Memorisation check (E5): do fine-tuned models reproduce training manifests?

For every greedy answer, each fenced code block (or, if the answer has none, the whole answer) is compared with the Kubernetes manifests of the corresponding training
set (v1: the 5,000 manifests of the original data; v2: the relevant domain manifests) by Jaccard similarity of word
5-shingles; candidates come from MinHash/LSH and every reported similarity is exact. Base models are compared with the
same corpora as a reference for chance overlap.
"""
import collections
import glob
import json
import os
import re
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from leakage import minhash, shingles  # noqa: E402

BANDS, ROWS = 32, 4


def index(docs):
    sh = [shingles(d) for d in docs]
    buckets = collections.defaultdict(list)
    for i, s in enumerate(sh):
        sig = minhash(s)
        for b in range(BANDS):
            buckets[(b, sig[b * ROWS:(b + 1) * ROWS].tobytes())].append(i)
    return sh, buckets


def best_match(text, sh, buckets):
    s = shingles(text)
    if len(s) < 5:
        return 0.0
    sig = minhash(s)
    cand = set()
    for b in range(BANDS):
        cand.update(buckets.get((b, sig[b * ROWS:(b + 1) * ROWS].tobytes()), []))
    best = 0.0
    for i in cand:
        u = len(s | sh[i])
        if u:
            best = max(best, len(s & sh[i]) / u)
    return best


def main():
    home = os.path.expanduser("~")
    v1 = [json.loads(l)["messages"][1]["content"] for f in ("train", "test") for l in open(f"{home}/phd/data/{f}.jsonl")
          if json.loads(l)["messages"][0]["content"].startswith("Generate a Kubernetes configuration file for")]
    v2 = [json.loads(l)["content"] for l in open(f"{home}/phd-rev/data_v2/domain_k8s_bt.jsonl")]
    corpora = {"v1": index(v1), "v2": index(v2)}
    out = {}
    for f in sorted(glob.glob(f"{home}/phd-rev/runs/main/*.greedy.jsonl")):
        model = os.path.basename(f).replace(".greedy.jsonl", "")
        if model.endswith("-rp"):
            continue
        corpus = "v2" if model.startswith(("ft-v2", "abl-v2")) else "v1"
        rows = [json.loads(l) for l in open(f)]
        res = {}
        for cname in ("v1", "v2"):
            sh, bk = corpora[cname]
            best = []
            for r in rows:
                blocks = re.findall(r"```[^\n]*\n(.*?)(?:```|\Z)", r["text"], re.S) or [r["text"]]  # unfenced: whole answer
                best.append(max([best_match(b, sh, bk) for b in blocks] or [0.0]))
            best = np.array(best)
            res[cname] = {"share_ge_0.8": float((best >= 0.8).mean()), "share_ge_0.5": float((best >= 0.5).mean()),
                          "median": float(np.median(best))}
        out[model] = {"own_corpus": corpus, **res}
        print(model, corpus, {k: {kk: round(vv, 3) for kk, vv in v.items()} for k, v in res.items()}, flush=True)
    json.dump(out, open(f"{home}/phd-rev/results/memorisation.json", "w"), indent=1)


if __name__ == "__main__":
    main()
