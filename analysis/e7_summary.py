"""E7 summary: runtime criteria per model, static-vs-runtime agreement, and causes of runtime failure.

Inputs: runs/e7/*.json (campaign results; smoke tests excluded) and runs/main/*.validated.jsonl (static verdict of the
same answer: greedy, seed 0). Output: results/e7_summary.json.
"""
import collections
import glob
import json
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[1]
CRIT = ["C1_deploy", "C2_checkpoints", "C3_throughput", "C4_latency", "C5_delivery"]


def cause(r):
    """Root cause of a runtime failure, from the driver's recorded reason, command results and Flink/SQL errors."""
    if r.get("untestable_reason"):
        return "untestable"
    why = (r.get("criteria_reasons") or {}).get("C1_deploy") or ""
    sc = r.get("sql_client") or {}
    sqlerr = " ".join(str(sc.get(k) or "") for k in ("error", "stderr_tail", "output_tail")) if isinstance(sc, dict) else str(sc)
    txt = " ".join([why, r.get("root_exception") or "", sqlerr] + [c.get("stderr_tail") or "" for c in r.get("topic_commands") or []])
    t = txt.lower()
    if r["criteria"].get("C1_deploy") is False:
        if "no flinkdeployment" in t or "no create table" in t:
            return "artefact missing"
        if "is not served" in t or "strimzi.io/cluster" in t:
            return "Kubernetes resource not accepted"
        if "not valid yaml" in t:
            return "invalid YAML"
        if "zookeeper is not a recognized option" in t:
            return "topic: removed CLI option"
        if "topic not created" in t or "topics missing" in t:
            return "topics not created"
        if "transaction timeout is larger" in t:
            return "transaction timeout > broker maximum"
        if "group.id is required" in t:
            return "source without group.id"
        if "unsupported options" in t or "required options are missing" in t:
            return "SQL: unsupported or missing connector option"
        if "unknown identifier" in t or "doesn't support defining primary key" in t or "classnotfound" in t:
            return "SQL: invalid type, constraint or class"
        if "encountered" in t or "parse" in t or "not defined" in t or "cannot be resolved" in t:
            return "SQL: syntax or reference error"
        if "slot" in t or "noresourceavailable" in t:
            return "insufficient slots"
        return "job did not start (other)"
    if r["criteria"].get("C5_delivery") is False:
        return "data lost or duplicated after failure"
    if r["criteria"].get("C2_checkpoints") is False:
        return "no completed checkpoints"
    if r["criteria"].get("C4_latency") is False:
        return "latency objective missed"
    if r["criteria"].get("C3_throughput") is False:
        return "throughput below offered load"
    return "pass" if r.get("runtime_pass") else "indeterminate"


def main():
    static, static_s = {}, {}
    for f in glob.glob(str(ROOT / "runs" / "main" / "*.validated.jsonl")):
        for line in open(f):
            v = json.loads(line)
            if v["mode"] == "greedy":
                static[(v["id"], v["model"])] = v
            else:
                static_s[(v["id"], v["model"], int(v["seed"]))] = v
    rows = []
    for f in sorted(glob.glob(str(ROOT / "runs" / "e7" / "*.json"))):
        r = json.load(open(f))
        if "criteria" not in r:
            continue
        s = static.get((r["id"], r["model"]))
        if s is None and int(r.get("seed", 0)) > 0:
            s = static_s.get((r["id"], r["model"], int(r["seed"])))
        r["_static"] = None if s is None else {k: s[k] for k in ("L1", "L2", "L3", "valid", "requirements_ok")}
        r["_cause"] = cause(r)
        rows.append(r)
    out = {"n": len(rows), "per_model": {}, "agreement": {}, "causes": {}}
    by = collections.defaultdict(list)
    for r in rows:
        dec = "greedy" if int(r.get("seed", 0)) == 0 else "sampled"
        by[f'{re.sub(r"-s\d+$", "", r["model"])}|{dec}'].append(r)
    for m, R in by.items():
        testable = [r for r in R if not r.get("untestable_reason")]
        d = {"n": len(R), "untestable": len(R) - len(testable)}
        for c in CRIT:
            vals = [r["criteria"][c] for r in testable if r["criteria"].get(c) is not None]
            d[c] = {"pass": sum(vals), "n": len(vals)}
        rp = [r["runtime_pass"] for r in testable]
        d["runtime_pass"] = {"pass": sum(x is True for x in rp), "n": len(rp), "indeterminate": sum(x is None for x in rp)}
        out["per_model"][m] = d
        out["causes"][m] = dict(collections.Counter(r["_cause"] for r in R))
    # static vs runtime agreement (all models pooled, testable answers with a static verdict)
    pairs = [r for r in rows if r["_static"] is not None and not r.get("untestable_reason")]
    for sk, rk in (("L3", "C1_deploy"), ("valid", "runtime_pass"), ("L3", "runtime_pass")):
        cm = collections.Counter()
        for r in pairs:
            rv = r["criteria"].get(rk) if rk != "runtime_pass" else r.get("runtime_pass")
            if rv is None:
                continue
            cm[(bool(r["_static"][sk]), bool(rv))] += 1
        out["agreement"][f"{sk}->{rk}"] = {f"static_{a}_runtime_{b}": n for (a, b), n in cm.items()}
    out["rows"] = [{k: r.get(k) for k in ("id", "model", "seed", "track", "criteria", "runtime_pass", "untestable_reason",
                                          "latency_ms", "offered_eps", "sink_eps", "lost", "duplicates",
                                          "checkpoints_completed", "_static", "_cause", "root_exception")} for r in rows]
    json.dump(out, open(ROOT / "results" / "e7_summary.json", "w"), indent=1, default=str)
    for m, d in out["per_model"].items():
        print(f"{m:16s} n={d['n']:3d} untestable={d['untestable']} " +
              " ".join(f"{c.split('_')[0]}={d[c]['pass']}/{d[c]['n']}" for c in CRIT) +
              f" pass={d['runtime_pass']['pass']}/{d['runtime_pass']['n']}  causes={out['causes'][m]}")
    print(json.dumps(out["agreement"], indent=1))


if __name__ == "__main__":
    main()
