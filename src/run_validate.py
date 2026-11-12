"""Validates generated answers and writes one result row per (scenario, model, seed)."""
import argparse, collections, json, os, sys
sys.path.insert(0, os.path.dirname(__file__))
from kfbench.validate import validate

ap = argparse.ArgumentParser()
ap.add_argument("--bench", required=True)
ap.add_argument("--gens", nargs="+", required=True)
ap.add_argument("--out", required=True)
a = ap.parse_args()
bench = {json.loads(l)["id"]: json.loads(l) for l in open(a.bench)}
agg = collections.defaultdict(lambda: collections.Counter())
with open(a.out, "w") as f:
    for g in a.gens:
        for line in open(g):
            r = json.loads(line)
            s = bench[r["id"]]
            v = validate(r["text"], s, r.get("finish_reason"))
            row = {k: r[k] for k in ("id", "model", "mode", "seed", "finish_reason", "n_output_tokens", "rag")}
            row.update(split=s["split"], track=s["track"], family=s["family"], **v)
            f.write(json.dumps(row) + "\n")
            c = agg[(r["model"], r["mode"])]
            c["n"] += 1
            for k in ("L1", "L2", "L3", "L4", "requirements_ok", "policy_ok", "truncated"):
                c[k] += bool(v[k])
            c["unknown_keys"] += v["n_unknown_keys"]
for (m, mode), c in sorted(agg.items()):
    n = c["n"]
    print(f"{m:28s} {mode:6s} n={n:4d} " + " ".join(f"{k}={100*c[k]/n:5.1f}" for k in ("L1", "L2", "L3", "L4", "requirements_ok", "truncated"))
          + f" unk/ans={c['unknown_keys']/n:.2f}")
