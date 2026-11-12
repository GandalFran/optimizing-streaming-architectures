"""Aggregates validated runs into the numbers reported in the manuscript (E3/E4/E8 tables, figure data).

Unit of analysis: one answer = (scenario, model, decoding seed). Greedy: one answer per scenario. Sampled: five seeds
per scenario; per-scenario values are averaged over seeds before any between-model statistic, and confidence
intervals come from a percentile bootstrap over scenarios (2,000 resamples, fixed seed), so seeds are never treated
as independent scenarios. Paired comparisons: Wilcoxon signed-rank on per-scenario requirement satisfaction and
McNemar's exact test on per-scenario L1 pass (greedy), Holm-corrected within each family.
"""
import argparse
import collections
import re
import glob
import json
import os

import numpy as np
from scipy import stats

ORDER = ["phi-3-mini-4k", "mistral-7b-v0.3", "llama-3.1-8b", "qwen2.5-7b", "qwen2.5-7b-rag", "qwen2.5-14b",
         "llama-3.1-70b", "ft-v1", "ft-v1fix", "ft-v2", "ft-v2-rag"]
FINDING_GROUPS = {
    "truncated": {"truncated"}, "missing artefact": {"missing-artefact"},
    "syntax": {"syntax", "sql-syntax", "cli-syntax", "malformed-structure"},
    "unknown key/field": {"unknown-key", "unknown-field", "unknown-cli-option", "broker-key-as-topic-config",
                          "unknown-connector", "unknown-format"},
    "misplaced key": {"misplaced-key"}, "invalid value": {"invalid-value", "crd-type", "crd-enum", "crd-range"},
    "missing required option": {"missing-required-option", "crd-required"},
    "removed / unsupported API": {"removed-option", "unsupported-api-version", "version-mismatch"},
    "duplicate keys (repetition)": {"duplicate-key"}, "deprecated key": {"deprecated-key"},
}


def req_sat(r):
    e = [x for x in r["expectations"].values() if x["kind"] in ("correctness", "resource")]
    return sum(x["ok"] for x in e) / len(e) if e else 1.0


def boot_ci(per_scen: np.ndarray, B=2000, seed=0):
    rng = np.random.default_rng(seed)
    n = len(per_scen)
    idx = rng.integers(0, n, size=(B, n))
    m = per_scen[idx].mean(axis=1)
    return float(np.percentile(m, 2.5)), float(np.percentile(m, 97.5))


def holm(p):
    order = np.argsort(p)
    adj = np.empty(len(p))
    run = 0.0
    for rank, i in enumerate(order):
        run = max(run, min(1.0, (len(p) - rank) * p[i]))
        adj[i] = run
    return adj


def load(paths):
    rows = []
    for f in paths:
        for line in open(f):
            r = json.loads(line)
            r["req_sat"] = req_sat(r)
            m = re.match(r"^(.*)-s(\d+)(-rp)?$", r["model"])  # fine-tuned runs: one model, several training seeds
            r["train_seed"] = int(m.group(2)) if m else None
            if m:
                r["model"] = m.group(1) + (m.group(3) or "")
            rows.append(r)
    return rows


def per_scenario(rows, metric):
    d = collections.defaultdict(list)
    for r in rows:
        d[r["id"]].append(float(metric(r)))
    return {k: float(np.mean(v)) for k, v in d.items()}


METRICS = {"L1": lambda r: r["L1"], "L2": lambda r: r["L2"], "L3": lambda r: r["L3"], "valid": lambda r: r["valid"],
           "req_sat": lambda r: r["req_sat"], "unknown_keys": lambda r: r["n_unknown_keys"],
           "truncated": lambda r: r["truncated"], "repetition": lambda r: r["n_duplicate_keys"] > 0}


def summarise(rows, split=None):
    out = {}
    models = [m for m in ORDER if any(r["model"] == m for r in rows)] + \
             sorted({r["model"] for r in rows} - set(ORDER))
    for m in models:
        for mode in ("greedy", "sample"):
            R = [r for r in rows if r["model"] == m and r["mode"] == mode and (split is None or split(r))]
            if not R:
                continue
            s = {"n_answers": len(R), "n_scenarios": len({r["id"] for r in R})}
            for k, f in METRICS.items():
                ps = per_scenario(R, f)
                arr = np.array(list(ps.values()))
                lo, hi = boot_ci(arr)
                s[k] = {"mean": float(arr.mean()), "ci95": [lo, hi]}
            if mode == "sample":  # between-seed spread of the per-seed means (variance from sampling)
                for k in ("L1", "req_sat"):
                    seeds = collections.defaultdict(list)
                    for r in R:
                        seeds[r["seed"]].append(float(METRICS[k](r)))
                    sm = [np.mean(v) for v in seeds.values()]
                    s[k]["seed_sd"] = float(np.std(sm, ddof=1)) if len(sm) > 1 else 0.0
            ts = sorted({r["train_seed"] for r in R if r["train_seed"] is not None})
            if len(ts) > 1:  # spread across training seeds of the per-seed mean requirement satisfaction / L1
                for k in ("L1", "req_sat", "valid", "truncated", "unknown_keys"):
                    per = [np.mean([float(METRICS[k](r)) for r in R if r["train_seed"] == t]) for t in ts]
                    s[k]["train_seed_sd"] = float(np.std(per, ddof=1))
                    s[k]["train_seed_means"] = [float(x) for x in per]
                s["train_seeds"] = ts
            out[(m, mode)] = s
    return out


def findings_matrix(rows, mode="greedy"):
    mat = {}
    for m in sorted({r["model"] for r in rows}):
        R = [r for r in rows if r["model"] == m and r["mode"] == mode]
        if not R:
            continue
        row = {}
        for g, cats in FINDING_GROUPS.items():
            row[g] = float(np.mean([any(f["category"] in cats for f in r["findings"]) for r in R]))
        req = collections.defaultdict(list)
        for r in R:
            for e in r["expectations"].values():
                if e["kind"] in ("correctness", "resource"):
                    req[e["category"]].append(e["ok"])
        for c, v in req.items():
            row[f"req:{c}"] = 1.0 - float(np.mean(v))  # violation rate of that requirement category
        mat[m] = row
    return mat


def paired_tests(rows, ref, mode="greedy", others=None, pairs=None):
    """Paired tests of each model against `ref` (or, with `pairs`, of each (model, reference) pair). Holm correction
    is applied within the family passed in, so the main comparison, the ablation and the repetition-penalty analysis
    are separate families."""
    res = {}
    if pairs is None:
        others = [m for m in (others or sorted({r["model"] for r in rows})) if m != ref]
        pairs = [(m, ref) for m in others]
    pw, pm = [], []
    for m, rf in pairs:
        ref_rs = per_scenario([r for r in rows if r["model"] == rf and r["mode"] == mode], METRICS["req_sat"])
        ref_l1 = per_scenario([r for r in rows if r["model"] == rf and r["mode"] == mode], METRICS["L1"])
        rs = per_scenario([r for r in rows if r["model"] == m and r["mode"] == mode], METRICS["req_sat"])
        l1 = per_scenario([r for r in rows if r["model"] == m and r["mode"] == mode], METRICS["L1"])
        ids = sorted(set(rs) & set(ref_rs))
        if not ids:
            continue
        d = np.array([ref_rs[i] - rs[i] for i in ids])
        w = stats.wilcoxon(d) if np.any(d != 0) else None
        b = sum(ref_l1[i] > 0.5 and l1[i] < 0.5 for i in ids)
        c = sum(ref_l1[i] < 0.5 and l1[i] > 0.5 for i in ids)
        mc = stats.binomtest(b, b + c, 0.5).pvalue if b + c else 1.0
        res[m] = {"ref": rf, "n": len(ids), "mean_diff_req_sat": float(d.mean()), "wilcoxon_p": float(w.pvalue) if w else 1.0,
                  "mcnemar_L1_b": b, "mcnemar_L1_c": c, "mcnemar_p": float(mc)}
        pw.append(res[m]["wilcoxon_p"])
        pm.append(res[m]["mcnemar_p"])
    for m, a, b in zip(list(res), holm(np.array(pw)) if pw else [], holm(np.array(pm)) if pm else []):
        res[m]["wilcoxon_p_holm"], res[m]["mcnemar_p_holm"] = float(a), float(b)
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", nargs="+", default=sorted(glob.glob("runs/main/*.validated.jsonl")))
    ap.add_argument("--ref", default=None)
    ap.add_argument("--out", default="results/main_summary.json")
    a = ap.parse_args()
    rows = load([p for p in a.runs if "peek" not in p])
    splits = {"all": None, "ID": lambda r: r["split"] == "ID", "OOD": lambda r: r["split"] == "OOD",
              "native": lambda r: r["track"] == "native", "k8s": lambda r: r["track"] == "k8s",
              "CASE": lambda r: r["split"] == "CASE"}
    out = {name: {f"{m}|{mode}": v for (m, mode), v in summarise(rows, f).items()} for name, f in splits.items()}
    out["findings_greedy"] = findings_matrix(rows, "greedy")
    out["findings_sample"] = findings_matrix(rows, "sample")
    present = {r["model"] for r in rows}
    if a.ref:  # main family: the models of the main comparison only
        main_models = [m for m in ORDER if m in present]
        out["paired_vs_" + a.ref] = paired_tests(rows, a.ref, "greedy", main_models)
        out["paired_sample_vs_" + a.ref] = paired_tests(rows, a.ref, "sample", main_models)
    abl = sorted(m for m in present if m.startswith("abl-"))
    if abl and "ft-v2" in present:  # ablation family: each variant against the full FT-v2 configuration
        out["paired_ablation_vs_ft-v2"] = paired_tests(rows, "ft-v2", "greedy", abl)
        out["paired_sample_ablation_vs_ft-v2"] = paired_tests(rows, "ft-v2", "sample", abl)
    scale = [(b, s) for b, s in (("qwen2.5-14b", "qwen2.5-7b"), ("llama-3.1-70b", "llama-3.1-8b"))
             if b in present and s in present]
    if scale:  # scale family: larger model against the smaller model of the same family
        out["paired_scale"] = paired_tests(rows, None, "greedy", pairs=scale)
        out["paired_sample_scale"] = paired_tests(rows, None, "sample", pairs=scale)
    rp = [(m, m[:-3]) for m in sorted(present) if m.endswith("-rp") and m[:-3] in present]
    if rp:  # repetition-penalty family: each model with penalty against itself without it
        out["paired_rp"] = paired_tests(rows, None, "greedy", pairs=rp)
        out["paired_sample_rp"] = paired_tests(rows, None, "sample", pairs=rp)
    perf = {}
    for p in glob.glob("runs/main/*.perf.json"):
        d = json.load(open(p))
        perf[d["model"]] = d
    out["perf"] = perf
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    json.dump(out, open(a.out, "w"), indent=1)
    for key, v in out["all"].items():
        print(f"{key:28s} L1={100*v['L1']['mean']:5.1f} L2={100*v['L2']['mean']:5.1f} L3={100*v['L3']['mean']:5.1f} "
              f"valid={100*v['valid']['mean']:4.1f} req_sat={100*v['req_sat']['mean']:5.1f} "
              f"[{100*v['req_sat']['ci95'][0]:.1f},{100*v['req_sat']['ci95'][1]:.1f}] unk={v['unknown_keys']['mean']:.2f}")


if __name__ == "__main__":
    main()
