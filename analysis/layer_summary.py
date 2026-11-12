"""E6 summary: outcome of the validation layer per model and feedback mode, and its cost.

The final answer of every attempt is re-validated with the current validator (VALIDATOR_VERSION), so that the reported
final validity and requirement satisfaction use the same validator as the main comparison; the script asserts that
re-validation does not change any accept/reject verdict (the layer took its decisions with the validator of its run).
"""
import collections
import sys
import glob
import json
import pathlib
import re

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from kfbench.validate import VALIDATOR_VERSION, validate  # noqa: E402

BENCH = {json.loads(l)["id"]: json.loads(l) for l in open(ROOT / "bench" / "kfbench-v1.jsonl")}
OUT = ROOT / "results" / "layer_summary.json"
STATUSES = ["accepted-unchanged", "accepted-repaired", "accepted-regenerated-1", "accepted-regenerated-2", "rejected"]


def revalidate(r):
    v = validate(r["final_text"], BENCH[r["id"]], r.get("finish"))
    req = [e for e in v["expectations"].values() if e["kind"] in ("correctness", "resource")]
    return v["valid"], v["L3"], sum(e["ok"] for e in req) / len(req)


def main():
    res = {}
    changes = collections.Counter()
    for f in sorted(glob.glob(str(ROOT / "runs" / "layer" / "*.jsonl"))):
        R = [json.loads(l) for l in open(f)]
        if not R:
            continue
        name, mode = pathlib.Path(f).stem.rsplit(".", 1)
        m = re.match(r"^(.*)-s(\d+)$", name)
        base = m.group(1) if m else name
        key = f"{base}|{mode}"
        d = res.setdefault(key, {"n": 0, "status": collections.Counter(), "validate_ms": [], "regen_s": [],
                                 "req_sat_0": [], "req_sat_final": [], "final_valid": [], "final_L3": [],
                                 "rounds_used": [], "repair_actions": collections.Counter()})
        for r in R:
            d["n"] += 1
            d["status"][r["status"]] += 1
            h = r["history"]
            d["validate_ms"] += [x["validate_ms"] for x in h if "validate_ms" in x]
            d["regen_s"] += [x["gen_s_amortised"] for x in h if "gen_s_amortised" in x]
            d["req_sat_0"].append(h[0]["req_sat"])
            valid, l3, rs = revalidate(r)
            assert valid == r["final_valid"] and l3 == r["final_L3"], (f, r["id"])
            changes["final_answers"] += 1
            changes["req_sat_changed"] += abs(rs - r["final_req_sat"]) > 1e-9
            d["req_sat_final"].append(rs)
            d["final_valid"].append(valid)
            d["final_L3"].append(l3)
            d["rounds_used"].append(sum(isinstance(x["round"], int) and x["round"] > 0 for x in h))
            for x in h:
                for act in x.get("actions", []):
                    d["repair_actions"][act.split(" ")[0] + " " + act.split(" ")[1]] += 1
    out = {}
    for key, d in res.items():
        n = d["n"]
        out[key] = {
            "n": n,
            "share": {s: d["status"][s] / n for s in STATUSES},
            "accepted": sum(d["status"][s] for s in STATUSES if s != "rejected") / n,
            "final_valid": float(np.mean(d["final_valid"])), "final_L3": float(np.mean(d["final_L3"])),
            "req_sat_round0": float(np.mean(d["req_sat_0"])), "req_sat_final": float(np.mean(d["req_sat_final"])),
            "validate_ms_median": float(np.median(d["validate_ms"])), "validate_ms_p95": float(np.percentile(d["validate_ms"], 95)),
            "regen_s_mean": float(np.mean(d["regen_s"])) if d["regen_s"] else 0.0,
            "mean_rounds": float(np.mean(d["rounds_used"])),
            "repair_actions": dict(d["repair_actions"]),
        }
    out["_revalidation"] = {"validator_version": VALIDATOR_VERSION, **changes}
    json.dump(out, open(OUT, "w"), indent=1)
    for k, v in sorted(out.items()):
        if k.startswith("_"):
            continue
        print(f"{k:28s} accepted={100*v['accepted']:5.1f}% valid={100*v['final_valid']:4.1f}% L3={100*v['final_L3']:4.1f}% "
              f"validate={v['validate_ms_median']:.0f}ms(p95 {v['validate_ms_p95']:.0f}) regen={v['regen_s_mean']:.1f}s/round")


if __name__ == "__main__":
    main()
