"""Writes numbers.tex: every number quoted in the manuscript as \\R{key}, computed from result files.

The manuscript defines \\newcommand{\\R}[1]{\\csname r@#1\\endcsname}; a missing key renders as nothing and is caught by
the final check (grep for '??' and the log of undefined control sequences), so no number can be typed by hand.
"""
import collections
import glob
import numpy as np
import json
import os
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
N = {}


def put(k, v, fmt=None):
    if isinstance(v, float) and fmt is None:
        fmt = "{:.1f}"
    N[k] = fmt.format(v) if fmt else (f"{v:,}" if isinstance(v, int) else str(v))


def pct(x):
    return 100.0 * x


def main(out):
    bench = [json.loads(l) for l in open(ROOT / "bench" / "kfbench-v1.jsonl")]
    global EOS_IDS
    EOS_IDS = {b["id"] for b in bench if any(e["type"] == "eos_sink" for e in b["expect"])}
    put("bench.total", len(bench))
    for s in ("ID", "OOD", "CASE"):
        put(f"bench.{s}", sum(b["split"] == s for b in bench))
    cnt = [sum(e["kind"] in ("correctness", "resource") for e in b["expect"]) for b in bench]
    put("bench.checks_per_scenario", sum(cnt) / len(cnt))
    fam = collections.Counter()
    for b in bench:
        for e in b["expect"]:
            c = e["category"]
            if e["kind"] == "policy":
                fam["policy"] += 1
            elif b["split"] == "OOD" and c not in ("consistency", "durability", "retention", "checkpointing",
                                                   "exactly-once", "latency", "under-provisioning", "resources",
                                                   "replication", "delivery"):
                fam["ood"] += 1
            elif c in ("under-provisioning", "resources", "replication"):
                fam["capacity"] += 1
            elif c == "delivery":
                fam["checkpointing"] += 1
            else:
                fam[c] += 1
    for k, v in fam.items():
        put(f"chk.{k}", v)

    from kfbench.catalog import get_catalog
    cat = get_catalog()
    put("cat.flink_keys", len(cat.flink_cluster))
    put("cat.flink_deprecated", len(cat.flink_deprecated))
    put("cat.table_options", len(cat.table_options))
    put("cat.operator_keys", len(cat.operator_keys))
    tests = subprocess.run([sys.executable, str(ROOT / "tests" / "test_validator.py")], capture_output=True, text=True)
    put("val.n_tests", tests.stdout.count("PASS"))
    assert "FAIL" not in tests.stdout, tests.stdout

    res = ROOT / "results"
    if (res / "validator_eval.json").exists():  # evaluation of the validator itself (analysis/validator_eval.py)
        V = json.load(open(res / "validator_eval.json"))
        from kfbench.validate import VALIDATOR_VERSION
        assert V["validator_version"] == VALIDATOR_VERSION, "re-run analysis/validator_eval.py"
        put("val.version", VALIDATOR_VERSION)
        put("veval.ref.n", V["reference"]["all"]["n"]); put("veval.ref.valid", V["reference"]["all"]["valid"])
        for sp in ("ID", "OOD", "CASE"):
            put(f"veval.ref.{sp}", V["reference"][sp]["n"])
        put("veval.neutral.classes", sum(v["n"] > 0 for v in V["neutral"].values()))
        put("veval.neutral.n", sum(v["n"] for v in V["neutral"].values()))
        put("veval.neutral.valid", sum(v["still_valid"] for v in V["neutral"].values()))
        put("veval.fault.classes", len(V["faults"]))
        put("veval.fault.n", sum(v["n"] for v in V["faults"].values()))
        put("veval.fault.detected", sum(v["detected"] for v in V["faults"].values()))
        for g, v in V["fault_groups"].items():
            put(f"veval.fault.{g}.classes", v["classes"]); put(f"veval.fault.{g}.n", v["n"])
            put(f"veval.fault.{g}.detected", v["detected"])
        put("veval.fault.default_equal", len(V["faults"]["family-specific setting removed (OOD)"]["not_a_defect_equals_default"]))
        it = V["official"]
        src = lambda p: [x for x in it if x["source"].startswith(p)]
        put("veval.off.n", len(it)); put("veval.off.rejected", sum(x["rejected"] for x in it))
        put("veval.off.false", sum(x["false_rejection"] for x in it))
        assert not any(x["missed_expected_rejection"] for x in it)
        put("veval.off.flink_keys", len({x["item"].split(":")[0] for x in src("config.yaml")}))
        put("veval.off.flink_active", len({x["item"].split(":")[0] for x in src("config.yaml (active)")}))
        put("veval.off.flink_commented", len({x["item"].split(":")[0] for x in src("config.yaml (commented")}))
        put("veval.off.topics", len(src("Kafka 4.1.2"))); put("veval.off.strimzi", len(src("Strimzi")))
        put("veval.off.operator", len(src("Flink Kubernetes Operator"))); put("veval.off.sql", len(src("Flink Kafka connector")))
        put("veval.off.accepted", sum(not x["rejected"] for x in it))
        if (res / "revalidation_v1.3.json").exists():  # verdicts of v1.2 vs v1.3 on every stored answer
            RV = json.load(open(res / "revalidation_v1.3.json"))
            put("reval.answers", RV["answers"])
            for fld in ("L1", "L2", "L3", "L4", "valid", "requirements_ok"):
                put(f"reval.{fld}_changed", RV["changes"].get(fld, 0))
            put("reval.verdicts_changed", sum(RV["changes"].get(f, 0) for f in ("L1", "L2", "L3", "L4", "valid")))
            assert RV["changes"].get("valid", 0) == 0, "Section 4.2 states that no validity verdict changed"
            put("reval.expectations_changed", RV["changes"].get("expectations", 0))
            put("reval.unknown_changed", RV["changes"].get("n_unknown_keys", 0))
        if (res / "validator_eval_v1.2.json").exists():
            V2 = json.load(open(res / "validator_eval_v1.2.json"))
            fp = [x for x in V2["official"] if x["false_rejection"]]
            put("veval.v12.false", len(fp))
            put("veval.v12.false_classes", len({f.split(":")[0] for x in fp for f in x["findings"]}))
            put("veval.v12.missed", sum(v["n"] - v["detected"] for v in V2["faults"].values()))

    res = ROOT / "results"
    if (res / "leakage.json").exists():
        L = json.load(open(res / "leakage.json"))
        scanned = int(re.search(r"DONE scanned (\d+)", open(res / "collect_v2.log").read()).group(1))
        put("leak.k8s_pool", scanned)
        put("leak.k8s_stream_kw", L["k8s_any_stream_kw"])
        put("leak.k8s_stream_kw_pct", 100 * L["k8s_any_stream_kw"] / (L["n_train_k8s"] + L["n_val_k8s"]), "{:.1f}")
        put("leak.ca_exact", L["val_resp_exact_in_train_codealpaca"])
        put("leak.val_ca", L["val_n_codealpaca"])
        put("leak.val_k8s", L["val_n_k8s"])
        put("leak.k8s_val_neardup", L["k8s_val_files_with_near_dup_in_train"])
        put("leak.k8s_val_shared_repo", L["k8s_val_files_from_repos_in_train"])
        put("leak.k8s_val_shared_repo_pct", pct(L["k8s_val_files_from_repos_in_train"] / L["val_n_k8s"]))
        put("leak.k8s_val_repos_shared", L["k8s_val_repos_also_in_train"])
    if (res / "k8s_provenance.jsonl").exists():
        P = [json.loads(l) for l in open(res / "k8s_provenance.jsonl")]
        put("leak.prov_matched", f"{sum(p['split'] != ['NOT-FOUND'] for p in P):,}")
    if (res / "v2_stats.json").exists():
        V = json.load(open(res / "v2_stats.json"))
        put("v2.scanned", int(re.search(r"DONE scanned (\d+)", open(res / "collect_v2.log").read()).group(1)))
        put("v2.raw", V["raw"])
        put("v2.api_removed", V["raw"] - V["after_api_filter"])
        put("v2.after_near_dedup", V["after_near_dedup"])
        put("v2.after_exact_dedup", V["after_exact_dedup"])
        put("v2.repos_total", V["repos_train"] + V["repos_val"])
        put("v2.relevant", V["relevance_filtered"])
        put("v2.relevant_signal", V["kept_with_signal"])
        put("v2.files_val", V["kept_files_val"])
        put("v2.repos_val", V["kept_repos_val"])
        put("v2.repos_train_kept", V["kept_repos_train"])
        put("v2.train_domain", V["v2_train"]["domain-k8s"])
        put("v2.docs", sum(v for k, v in V["v2_train"].items() if k not in ("domain-k8s", "codealpaca")))
        put("v2.train_total", V["v2_train_total"])
        put("v2.val_total", V["v2_val_total"])
    meta = ROOT / "rag" / "retrieved-k8.jsonl.meta.json"
    if meta.exists():
        put("rag.n_chunks", json.load(open(meta))["n_chunks"])
    if (res / "main_summary.json").exists():
        S = json.load(open(res / "main_summary.json"))
        for split, d in S.items():
            if split in ("findings_greedy", "findings_sample", "perf") or split.startswith("paired"):
                continue
            for key, v in d.items():
                m, mode = key.split("|")
                for met in ("L1", "L2", "L3", "valid", "req_sat", "truncated", "repetition"):
                    put(f"{split}.{m}.{mode}.{met}", pct(v[met]["mean"]))
                    put(f"{split}.{m}.{mode}.{met}.lo", pct(v[met]["ci95"][0]))
                    put(f"{split}.{m}.{mode}.{met}.hi", pct(v[met]["ci95"][1]))
                put(f"{split}.{m}.{mode}.unk", v["unknown_keys"]["mean"], "{:.2f}")
                for met in ("L1", "req_sat", "valid"):  # spread across training seeds (fine-tuned models only)
                    if "train_seed_sd" in v[met]:
                        put(f"{split}.{m}.{mode}.{met}.tsd", pct(v[met]["train_seed_sd"]))

        for fk, mode in (("findings_greedy", "greedy"), ("findings_sample", "sample")):  # error map (Fig. E)
            for m, row in S.get(fk, {}).items():
                for g, v in row.items():
                    g = g.replace(" / ", "-").replace("/", "-").replace(" ", "_").replace("(", "").replace(")", "")
                    put(f"find.{mode}.{m}.{g}", pct(v))
        fams = (("paired_vs_qwen2.5-7b", "paired", "greedy"), ("paired_sample_vs_qwen2.5-7b", "paired", "sample"),
                ("paired_ablation_vs_ft-v2", "pabl", "greedy"), ("paired_sample_ablation_vs_ft-v2", "pabl", "sample"),
                ("paired_scale", "pscale", "greedy"), ("paired_sample_scale", "pscale", "sample"),
                ("paired_rp", "prp", "greedy"), ("paired_sample_rp", "prp", "sample"))
        for key, pre, mode in fams:  # one Holm family per key; diff = model minus its reference, in points
            for m, v in S.get(key, {}).items():
                put(f"{pre}.{mode}.{m}.diff", -100 * v["mean_diff_req_sat"], "{:+.1f}")
                pw = v["wilcoxon_p_holm"]
                put(f"{pre}.{mode}.{m}.p", "<0.001" if pw < 0.001 else f"{pw:.3f}")
                put(f"{pre}.{mode}.{m}.mcn_b", v["mcnemar_L1_b"])
                put(f"{pre}.{mode}.{m}.mcn_c", v["mcnemar_L1_c"])
                pm = v["mcnemar_p_holm"]
                put(f"{pre}.{mode}.{m}.mcn_p", "<0.001" if pm < 0.001 else f"{pm:.3f}")
        for m, p in S.get("perf", {}).items():
            put(f"perf.{m}.lat", p["latency_s_median"], "{:.1f}")
            put(f"perf.{m}.lat95", p["latency_s_p95"], "{:.1f}")
            put(f"perf.{m}.tps", p["tokens_per_s_median"], "{:.0f}")

    if (res / "layer_summary.json").exists():
        Ls = json.load(open(res / "layer_summary.json"))
        RV = Ls.pop("_revalidation", None)
        if RV:
            put("val.layer_final", RV["final_answers"]); put("val.layer_reqsat_changed", RV["req_sat_changed"])
        for key, v in Ls.items():
            m, mode = key.split("|")
            for k2, v2 in v["share"].items():
                put(f"layer.{m}.{mode}.{k2}", pct(v2))
            put(f"layer.{m}.{mode}.accepted", pct(v["accepted"]))
            put(f"layer.{m}.{mode}.valid", pct(v["final_valid"]))
            put(f"layer.{m}.{mode}.L3", pct(v["final_L3"]))
            put(f"layer.{m}.{mode}.rs0", pct(v["req_sat_round0"]))
            put(f"layer.{m}.{mode}.rsF", pct(v["req_sat_final"]))
        put("layer.validate_ms_median_max", max(v["validate_ms_median"] for v in Ls.values()), "{:.0f}")
        put("layer.validate_ms_p95_max", max(v["validate_ms_p95"] for v in Ls.values()), "{:.1f}")
        put("layer.max_accepted", max(pct(v["accepted"]) for v in Ls.values()), "{:.1f}")
        large = ("qwen2.5-14b", "llama-3.1-70b")  # the two larger references; every other model has at most 8B
        for mode in ("syntax", "full"):
            put(f"layer.max_accepted_{mode}", max(pct(v["accepted"]) for k, v in Ls.items() if k.endswith("|" + mode)))
            put(f"layer.max_accepted_small_{mode}", max(pct(v["accepted"]) for k, v in Ls.items()
                                                        if k.endswith("|" + mode) and k.split("|")[0] not in large))
        put("layer.unchanged_max", max(pct(v["share"]["accepted-unchanged"]) for v in Ls.values()))
        put("layer.repaired_max", max(pct(v["share"]["accepted-repaired"]) for v in Ls.values()))
        put("layer.n_models", len({k.split("|")[0] for k in Ls}))
    if (res / "e7_summary.json").exists():
        E = json.load(open(res / "e7_summary.json"))
        rows = [r for r in E["rows"] if r["model"] not in ("gold", "reference")]
        ref = [r for r in E["rows"] if r["model"] == "reference"]  # positive control: reference answers, ID scenarios
        if ref:
            put("e7.ref.n", len(ref))
            put("e7.ref.untestable", sum(bool(r.get("untestable_reason")) for r in ref))
            put("e7.ref.deployed", sum(r["criteria"].get("C1_deploy") is True for r in ref))
            put("e7.ref.pass", sum(r.get("runtime_pass") is True for r in ref))
            for c in ("C1_deploy", "C2_checkpoints", "C3_throughput", "C4_latency", "C5_delivery"):
                vals = [r["criteria"].get(c) for r in ref if r["criteria"].get(c) is not None]
                put(f"e7.ref.{c.split('_')[0]}.pass", sum(v is True for v in vals)); put(f"e7.ref.{c.split('_')[0]}.n", len(vals))
            put("e7.ref.lost", sum(r.get("lost") or 0 for r in ref)); put("e7.ref.dup", sum(r.get("duplicates") or 0 for r in ref))
            eos = [r for r in ref if r["id"] in EOS_IDS]
            put("e7.ref.eos_n", len(eos)); put("e7.ref.eos_pass", sum(r.get("runtime_pass") is True for r in eos))
            put("e7.ref.alo_n", len(ref) - len(eos))
            put("e7.ref.alo_pass", sum(r.get("runtime_pass") is True for r in ref if r["id"] not in EOS_IDS))
            for r in ref:
                if r.get("latency_ms"):
                    put(f"e7.ref.{r['id']}.p99", r["latency_ms"].get("p99") or 0, "{:,.0f}")
                put(f"e7.ref.{r['id']}.sink", (r.get("sink_eps") or 0) / 1000, "{:.1f}")
                put(f"e7.ref.{r['id']}.offered", (r.get("offered_eps") or 0) / 1000, "{:.0f}")
            # first version of the reference answers: checkpoint interval equal to the latency objective
            for f in sorted(glob.glob(str(ROOT / "runs" / "e7" / "reference-v0" / "ID-*__reference__s0.json"))):
                r0 = json.load(open(f))
                if r0.get("latency_ms"):
                    put(f"e7.ref0.{r0['id']}.p99", r0["latency_ms"].get("p99") or 0, "{:,.0f}")
                    put(f"e7.ref0.{r0['id']}.pass", "yes" if r0.get("runtime_pass") else "no")
        for dec, sel in (("greedy", lambda r: int(r.get("seed") or 0) == 0), ("sampled", lambda r: int(r.get("seed") or 0) > 0)):
            R = [r for r in rows if sel(r)]
            put(f"e7.{dec}.n", len(R))
            put(f"e7.{dec}.untestable", sum(bool(r.get("untestable_reason")) for r in R))
            put(f"e7.{dec}.deployed", sum(r["criteria"].get("C1_deploy") is True for r in R))
            put(f"e7.{dec}.pass", sum(r.get("runtime_pass") is True for r in R))
        put("e7.n", len(rows))
        put("e7.models", len({r["model"] for r in rows}))
        put("e7.scenarios", len({r["id"] for r in rows}))
        for r in E["rows"]:
            if r["model"] == "gold":
                tag = "native" if r["id"].endswith("native") else "k8s"
                for q in ("p50", "p95", "p99"):
                    put(f"e7.gold.{tag}.{q}", r["latency_ms"].get(q) or 0, "{:.0f}")
                put(f"e7.gold.{tag}.ckpts", r.get("checkpoints_completed") or 0)
                put(f"e7.gold.{tag}.lost", r.get("lost") or 0)
                put(f"e7.gold.{tag}.dup", r.get("duplicates") or 0)
                put(f"e7.gold.{tag}.offered", (r.get("offered_eps") or 0) / 1000, "{:.1f}")
                put(f"e7.gold.{tag}.sink", (r.get("sink_eps") or 0) / 1000, "{:.1f}")
        stages = {}
        for r in rows:
            stages[r["_cause"]] = stages.get(r["_cause"], 0) + 1
        early = sum(n for c, n in stages.items() if c in ("artefact missing", "invalid YAML"))
        kafka_k8s = sum(n for c, n in stages.items() if c in ("Kubernetes resource not accepted", "topics not created", "topic: removed CLI option"))
        sqlrej = sum(n for c, n in stages.items() if c.startswith("SQL") or c == "source without group.id")
        put("e7.stage.incomplete", early); put("e7.stage.kafka_k8s", kafka_k8s); put("e7.stage.sql", sqlrej)
        put("e7.stage.incomplete_pct", pct(early / len(rows))); put("e7.stage.kafka_k8s_pct", pct(kafka_k8s / len(rows)))
        put("e7.stage.sql_pct", pct(sqlrej / len(rows)))
    if (res / "memorisation.json").exists():
        M = json.load(open(res / "memorisation.json"))
        put("mem.max_share05", max(pct(max(v["v1"]["share_ge_0.5"], v["v2"]["share_ge_0.5"])) for v in M.values()))
        put("mem.max_share08", max(pct(max(v["v1"]["share_ge_0.8"], v["v2"]["share_ge_0.8"])) for v in M.values()))
    manuscript_meta = json.load(open(res / "manuscript_metadata.json"))
    put("fig.count", manuscript_meta["figure_count"])

    if (res / "training_summary.json").exists():
        T = json.load(open(res / "training_summary.json"))
        o = T[0]
        put("train.orig.first_eval", o["first_eval_loss"], "{:.3f}"); put("train.orig.first_eval_step", o["first_eval_step"])
        put("train.orig.min_eval", o["min_eval_loss"], "{:.3f}"); put("train.orig.min_step", f"{o['min_eval_step']:,}")
        put("train.orig.final_eval", o["final_eval_loss"], "{:.3f}"); put("train.orig.final_eval_step", f"{o['final_eval_step']:,}")
        put("train.orig.first_train", o["first_train_loss"], "{:.3f}")
        put("train.orig.final_train", o["final_train_loss_mean_last10"], "{:.3f}")
        put("train.orig.runtime", o["runtime_min"], "{:.1f}")
        fams = {"v1fix-r64-nf4": "v1fix", "v2-r64-nf4": "v2", "v2-r8-nf4": "abl-r8", "v2-r16-nf4": "abl-r16",
                "v2-r32-nf4": "abl-r32", "v2-r64-bf16": "abl-bf16", "v2-nodocs": "abl-nodocs", "v2-noca": "abl-noca",
                "v2-nodomain": "abl-nodomain"}
        for fam, k in fams.items():
            R = [r for r in T if re.match(re.escape(fam) + r"-s\d+$", r["run"]) and r["min_eval_loss"] is not None]
            if not R:
                continue
            put(f"train.{k}.samples", f"{R[0].get('train_samples') or 0:,}")
            put(f"train.{k}.min_eval", float(np.mean([r["min_eval_loss"] for r in R])), "{:.3f}")
            put(f"train.{k}.min_eval_sd", float(np.std([r["min_eval_loss"] for r in R], ddof=1)) if len(R) > 1 else 0.0, "{:.3f}")
            put(f"train.{k}.final_eval", float(np.mean([r["final_eval_loss"] for r in R])), "{:.3f}")
            steps = sorted({r["selected_step"] for r in R})
            put(f"train.{k}.selected", " or ".join(f"{x:,}" for x in steps))
            put(f"train.{k}.steps", f"{R[0]['steps']:,}")
            put(f"train.{k}.runtime", float(np.mean([r["runtime_min"] for r in R if r["runtime_min"]])), "{:.0f}")
            put(f"train.{k}.n", len(R))
    if (res / "main_summary.json").exists():  # ablation summaries: range of rank effects and of training-seed spread
        S = json.load(open(res / "main_summary.json"))
        ranks = ("abl-v2-r8", "abl-v2-r16", "abl-v2-r32")
        diffs = [-100 * S[k][m]["mean_diff_req_sat"] for k in ("paired_ablation_vs_ft-v2", "paired_sample_ablation_vs_ft-v2")
                 for m in ranks if m in S.get(k, {})]
        if diffs:
            put("pabl.rank.min", min(diffs), "{:+.1f}")
            put("pabl.rank.max", max(diffs), "{:+.1f}")
            sds = [100 * S["all"][f"{m}|{md}"]["req_sat"].get("train_seed_sd", 0) for m in ranks + ("ft-v2",)
                   for md in ("greedy", "sample") if f"{m}|{md}" in S["all"]]
            put("pabl.rank.tsd_max", max(sds))
            tr = [100 * S["all"][f"{m}|{md}"]["truncated"]["mean"] for m in ranks for md in ("greedy", "sample")
                  if f"{m}|{md}" in S["all"]]
            put("pabl.rank.trunc_min", min(tr))
        allabl = [m for m in {k.split("|")[0] for k in S["all"]} if m == "ft-v2" or m.startswith("abl-v2-")]
        if allabl:  # every FT-v2 configuration of the ablation
            put("pabl.all.n_configs", len(allabl))
            put("pabl.all.greedy_trunc_min", min(100 * S["all"][f"{m}|greedy"]["truncated"]["mean"] for m in allabl))
            put("pabl.all.rs_max", max(100 * S["all"][f"{m}|{md}"]["req_sat"]["mean"] for m in allabl
                                       for md in ("greedy", "sample")))
            put("pabl.all.valid_max", max(100 * S["all"][f"{m}|{md}"]["valid"]["mean"] for m in allabl
                                          for md in ("greedy", "sample")))
    if (res / "main_summary.json").exists():  # benchmark subsets (main models, sampled decoding unless stated)
        S = json.load(open(res / "main_summary.json"))
        main = [m for m in ("phi-3-mini-4k", "mistral-7b-v0.3", "llama-3.1-8b", "qwen2.5-7b", "qwen2.5-14b", "llama-3.1-70b",
                            "qwen2.5-7b-rag", "ft-v1", "ft-v1fix", "ft-v2") if f"{m}|sample" in S["all"]]
        put("models.n", len(main))
        small = ("phi-3-mini-4k", "mistral-7b-v0.3", "llama-3.1-8b", "qwen2.5-7b")  # base models of at most 8B
        l1 = [100 * S["all"][f"{m}|sample"]["L1"]["mean"] for m in small]
        put("range.l1_small_base.sample.min", min(l1)); put("range.l1_small_base.sample.max", max(l1))
        unk = [S["all"][f"{m}|sample"]["unknown_keys"]["mean"] for m in main]
        put("range.unk.sample.min", min(unk), "{:.2f}"); put("range.unk.sample.max", max(unk), "{:.2f}")
        pairs = [(m, md) for m in main for md in ("greedy", "sample")]
        ov = [S["ID"][f"{m}|{md}"]["req_sat"]["ci95"][0] <= S["OOD"][f"{m}|{md}"]["req_sat"]["ci95"][1] and
              S["OOD"][f"{m}|{md}"]["req_sat"]["ci95"][0] <= S["ID"][f"{m}|{md}"]["req_sat"]["ci95"][1] for m, md in pairs]
        put("split.idood_pairs", len(pairs))
        put("split.idood_overlap", sum(ov))
        put("split.ood_valid_max", max(100 * S["OOD"][f"{m}|{md}"]["valid"]["mean"] for m, md in pairs))
        put("split.k8s_lower_sample", sum(S["native"][f"{m}|sample"]["req_sat"]["mean"] > S["k8s"][f"{m}|sample"]["req_sat"]["mean"]
                                          for m in main))
        put("split.n_ood", S["OOD"]["qwen2.5-7b|sample"]["n_scenarios"])
    if (res / "generation_diag.json").exists():  # loops and fences in the raw generations (generation_diag.py)
        G = json.load(open(res / "generation_diag.json"))
        for key, v in G["by_model"].items():
            m, mode = key.split("|")
            m = m.replace("@", ".")
            put(f"gen.{m}.{mode}.loop", pct(v["loop"]))
            put(f"gen.{m}.{mode}.fenced", pct(v["fenced"]))
            if "one_block" in v:
                put(f"gen.{m}.{mode}.one_block", pct(v["one_block"]))
                put(f"gen.{m}.{mode}.tokens", round(v["mean_tokens"]))
            if v["loop_of_truncated"] is not None:
                put(f"gen.{m}.{mode}.loop_of_trunc", pct(v["loop_of_truncated"]))
        main = [v for k, v in G["by_model"].items() if "@" not in k and "-rp" not in k and not k.startswith("abl-")
                and k.endswith("|greedy") and v["truncated"] >= 0.2]  # greedy runs with at least 20% truncations
        put("gen.greedy.loop_of_trunc_min", min(pct(v["loop_of_truncated"]) for v in main))
        put("gen.greedy.loop_of_trunc_max", max(pct(v["loop_of_truncated"]) for v in main))
    extra = res / "extra_numbers.json"
    if extra.exists():
        for k, v in json.load(open(extra)).items():
            N[k] = str(v)
    with open(out, "w") as f:
        f.write("% generated by code/analysis/export_numbers.py -- do not edit\n")
        for k in sorted(N):
            f.write(f"\\expandafter\\def\\csname r@{k}\\endcsname{{{N[k]}}}\n")
    print(len(N), "numbers written to", out)


if __name__ == "__main__":
    main(sys.argv[1])
