"""Generate Markdown counterparts of the manuscript's data-derived tables.

The tables are intentionally read from the canonical result JSON and run
records in ``code/``.  ``paper/figures/generate_tables.py`` is a thin entry
point that writes the review-friendly ``paper/figures/tables.md`` file.
"""
import argparse
import json
import pathlib


HERE = pathlib.Path(__file__).resolve().parent


def locate_code_root() -> pathlib.Path:
    for parent in HERE.parents:
        if (parent / "results" / "main_summary.json").is_file() and (parent / "analysis").is_dir():
            return parent
        if (parent / "code" / "results" / "main_summary.json").is_file() and (parent / "code" / "analysis").is_dir():
            return parent / "code"
    raise FileNotFoundError("Could not find code/results/main_summary.json from this script")


ROOT = locate_code_root()
RES = ROOT / "results"
NAMES = {
    "phi-3-mini-4k": "Phi-3-mini-4k", "mistral-7b-v0.3": "Mistral-7B-v0.3",
    "llama-3.1-8b": "Llama-3.1-8B", "qwen2.5-7b": "Qwen2.5-7B",
    "qwen2.5-14b": "Qwen2.5-14B", "llama-3.1-70b": "Llama-3.1-70B",
    "qwen2.5-7b-rag": "Qwen2.5-7B + RAG", "ft-v1": "FT-v1 (original)",
    "ft-v1fix": "FT-v1-fix", "ft-v2": "FT-v2",
}
ABL = [
    ("ft-v2", "FT-v2: r=64, NF4, all data"), ("abl-v2-r32", "r=32"),
    ("abl-v2-r16", "r=16"), ("abl-v2-r8", "r=8"),
    ("abl-v2-bf16", "no quantisation (bf16)"),
    ("abl-v2-nodocs", "without documentation pairs"),
    ("abl-v2-nodomain", "without domain manifests"),
    ("abl-v2-noca", "without CodeAlpaca"),
]


def pct(value: float, digits: int = 1) -> str:
    return f"{100 * value:.{digits}f}"


def md_table(headers: list[str], rows: list[list[str]]) -> str:
    line = "| " + " | ".join(headers) + " |"
    divider = "| " + " | ".join("---" for _ in headers) + " |"
    return "\n".join([line, divider] + ["| " + " | ".join(row) + " |" for row in rows])


def main_comparison(summary: dict) -> str:
    rows = []
    for model, name in NAMES.items():
        for mode, short in (("greedy", "G"), ("sample", "S")):
            value = summary["all"].get(f"{model}|{mode}")
            if not value:
                continue
            req = value["req_sat"]
            ci = f"{pct(req['mean'])} [{pct(req['ci95'][0])}, {pct(req['ci95'][1])}]"
            if req.get("train_seed_sd") is not None:
                ci += f" ±{pct(req['train_seed_sd'])}"
            rows.append([
                name if mode == "greedy" else "", short, pct(value["L1"]["mean"]),
                pct(value["L2"]["mean"]), pct(value["L3"]["mean"]),
                pct(value["valid"]["mean"]), ci, f"{value['unknown_keys']['mean']:.1f}",
                pct(value["truncated"]["mean"]),
            ])
    return md_table(["Model", "Dec.", "L1", "L2", "L3", "Valid", "Req. sat. [95% CI]", "Unk.", "Trunc."], rows)


def performance(summary: dict) -> str:
    rows = []
    for model, name in NAMES.items():
        value = summary["perf"].get(model) or summary["perf"].get(f"{model}-s1")
        if value:
            rows.append([name, f"{value['latency_s_median']:.1f}", f"{value['latency_s_p95']:.1f}",
                         f"{value['tokens_per_s_median']:.0f}"])
    return md_table(["Model", "Median latency (s)", "p95 latency (s)", "Tokens/s"], rows)


def subset_results(summary: dict) -> str:
    rows = []
    for model, name in NAMES.items():
        if f"{model}|sample" not in summary["all"]:
            continue
        cells = []
        for split in ("ID", "OOD", "native", "k8s"):
            value = summary[split][f"{model}|sample"]["req_sat"]
            cells.append(f"{pct(value['mean'])} [{pct(value['ci95'][0])}, {pct(value['ci95'][1])}]")
        rows.append([name] + cells)
    return md_table(["Model", "ID", "OOD", "Direct", "Kubernetes"], rows)


def layer_results(layer: dict) -> str:
    rows = []
    for model, name in NAMES.items():
        for mode in ("syntax", "full"):
            value = layer.get(f"{model}|{mode}")
            if not value:
                continue
            share = value["share"]
            rows.append([name if mode == "syntax" else "", mode, pct(share["accepted-unchanged"]),
                         pct(share["accepted-repaired"]), pct(share["accepted-regenerated-1"]),
                         pct(share["accepted-regenerated-2"]), pct(share["rejected"]),
                         pct(value["final_valid"])])
    return md_table(["Model", "Mode", "Gen.", "Repair", "R1", "R2", "Rejected", "Valid"], rows)


def ablation(summary: dict) -> str:
    rows = []
    for model, name in ABL:
        for mode, short in (("greedy", "G"), ("sample", "S")):
            value = summary["all"].get(f"{model}|{mode}")
            if not value:
                continue
            req = value["req_sat"]
            req_text = pct(req["mean"])
            if req.get("train_seed_sd") is not None:
                req_text += f" ±{pct(req['train_seed_sd'])}"
            test = summary.get("paired_ablation_vs_ft-v2" if mode == "greedy" else "paired_sample_ablation_vs_ft-v2", {}).get(model)
            delta = "--" if not test else f"{-100 * test['mean_diff_req_sat']:+.1f} (p={test['wilcoxon_p_holm']:.3f})"
            findings = summary.get(f"findings_{mode}", {}).get(model, {})
            rows.append([name if mode == "greedy" else "", short, pct(value["L1"]["mean"]), req_text,
                         delta, f"{value['unknown_keys']['mean']:.1f}", pct(value["truncated"]["mean"]),
                         pct(findings.get("missing artefact", 0))])
    return md_table(["Configuration", "Dec.", "L1", "Req. sat.", "Delta vs FT-v2", "Unk.", "Trunc.", "Miss."], rows)


def training(runs: list[dict]) -> str:
    rows = []
    for run in runs:
        name = run["run"]
        if not (name.startswith("FT-v1") or name.startswith("v1fix-r64-nf4") or name.startswith("v2-r64-nf4")):
            continue
        first_train = run["train"][0]
        label = name.replace("v1fix-r64-nf4-s", "FT-v1-fix s").replace("v2-r64-nf4-s", "FT-v2 s")
        rows.append([label, f"{run['steps']:,}", f"{first_train[1]:.3f} ({first_train[0]:,})",
                     f"{run['final_train_loss_mean_last10']:.3f}",
                     f"{run['min_eval_loss']:.3f} ({run['min_eval_step']:,})",
                     f"{run['selected_step']:,}", f"{run['runtime_min']:.0f}"])
    return md_table(["Run", "Last step", "Train first", "Train last 10", "Validation minimum", "Selected", "Minutes"], rows)


def validator_eval(value: dict) -> str:
    neutral = sum(item["n"] for item in value["neutral"].values())
    neutral_valid = sum(item["still_valid"] for item in value["neutral"].values())
    rows = [
        ["Reference answers", str(value["reference"]["all"]["n"]), "valid", str(value["reference"]["all"]["valid"]) + " valid"],
        ["Semantics-preserving rewrites", str(neutral), "valid", str(neutral_valid) + " valid"],
    ]
    for group, label in (("format", "format and syntax"), ("schema", "schema"), ("version", "version"), ("requirements", "requirements")):
        item = value["fault_groups"][group]
        rows.append([f"Injected defects: {label} ({item['classes']} classes)", str(item["n"]), "rejected", str(item["detected"]) + " rejected"])
    official = value["official"]
    rejected = sum(item["rejected"] for item in official)
    rows.append(["Official artefacts", str(len(official)), "pass L1-L3", f"{len(official) - rejected} accepted, {rejected} rejected"])
    return md_table(["Test", "n", "Expected", "Outcome"], rows)


def runtime_summary(testbed: dict) -> str:
    groups: dict[tuple[str, str], list[dict]] = {}
    for row in testbed["rows"]:
        model = row["model"].rsplit("-s", 1)[0]
        dec = "G" if int(row.get("seed") or 0) == 0 else "S"
        groups.setdefault((model, dec), []).append(row)
    rows = []
    for (model, dec), entries in groups.items():
        label = "Reference answers" if model in {"gold", "reference"} else NAMES.get(model, model)
        passed = sum(row.get("runtime_pass") is True for row in entries)
        deployed = sum(row.get("_cause") not in {"artefact missing", "invalid YAML"} for row in entries)
        rows.append([label, dec, str(len(entries)), str(deployed), str(passed)])
    return md_table(["Model", "Dec.", "Deployments", "Past parsing", "Passed all runtime criteria"], rows)


def build_markdown() -> str:
    main = json.loads((RES / "main_summary.json").read_text())
    layer = json.loads((RES / "layer_summary.json").read_text())
    testbed = json.loads((RES / "e7_summary.json").read_text())
    training_runs = json.loads((RES / "training_summary.json").read_text())
    validator = json.loads((RES / "validator_eval.json").read_text())
    parts = [
        "# Data-derived tables for manuscript comparison",
        "",
        "Generated by `paper/figures/generate_tables.py` from the public `code/` results and run records. "
        "Values below are the Markdown counterparts of empirical manuscript tables; static design tables remain editorial LaTeX content.",
        "",
        "## Main comparison (`tab_main`)", main_comparison(main),
        "", "## Inference performance (`tab_perf`)", performance(main),
        "", "## Benchmark subsets (`tab_splits`)", subset_results(main),
        "", "## Validation layer (`tab_layer`)", layer_results(layer),
        "", "## Executable validation (`tab_e7` summary)", runtime_summary(testbed),
        "", "## Fine-tuning ablation (`tab_ablation`)", ablation(main),
        "", "## Training runs (`tab_training`)", training(training_runs),
        "", "## Validator evaluation (`tab_valeval`)", validator_eval(validator),
        "",
        "## Notes",
        "",
        "- `G` denotes greedy decoding and `S` sampled decoding.",
        "- Percentages are calculated from the canonical JSON summaries. Confidence intervals are 95% bootstrap intervals where shown.",
        "- The manuscript's hyperparameter, requirement-family and defect-class tables describe methods and rules rather than separately computed result matrices.",
    ]
    return "\n".join(parts) + "\n"


def main(output: pathlib.Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(build_markdown(), encoding="utf-8")
    print("tables written to", output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Markdown comparison tables from code results.")
    parser.add_argument("--output", type=pathlib.Path, default=HERE / "tables.md")
    main(parser.parse_args().output)
