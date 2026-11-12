"""Compares the verdicts of two validator versions on every stored answer of the main comparison.

  python analysis/revalidation_diff.py runs/main_v12 runs/main > results/revalidation_v1.3.json

Both directories hold <model>.validated.jsonl files produced by src/run_validate.py from the same generations.
"""
import collections
import glob
import json
import os
import sys

old_dir, new_dir = sys.argv[1], sys.argv[2]
changes, n, versions = collections.Counter(), 0, set()
examples = []
for f in sorted(glob.glob(os.path.join(old_dir, "*.validated.jsonl"))):
    g = os.path.join(new_dir, os.path.basename(f))
    old = {(r["id"], r["mode"], r["seed"]): r for r in map(json.loads, open(f))}
    new = {(r["id"], r["mode"], r["seed"]): r for r in map(json.loads, open(g))}
    assert old.keys() == new.keys(), f
    for k, o in old.items():
        w = new[k]
        n += 1
        versions.add((o["validator_version"], w["validator_version"]))
        for field in ("L1", "L2", "L3", "L4", "valid", "requirements_ok", "policy_ok", "n_unknown_keys", "n_deprecated"):
            if o.get(field) != w.get(field):
                changes[field] += 1
                if len(examples) < 20:
                    examples.append({"file": os.path.basename(f), "key": list(k), "field": field,
                                     "old": o.get(field), "new": w.get(field)})
        oe = {e: v["ok"] for e, v in o["expectations"].items()}
        we = {e: v["ok"] for e, v in w["expectations"].items()}
        changes["expectations"] += oe != we
print(json.dumps({"answers": n, "versions": sorted(versions), "changes": dict(changes), "examples": examples}, indent=1))
