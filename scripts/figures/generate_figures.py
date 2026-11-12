"""Data figures of the revised manuscript (vector PDF). Every value is read from results/*.json produced by the
analysis scripts; nothing is typed by hand.

Design: models sit on an axis (position carries identity); colour encodes only the approach type (base model, retrieval,
fine-tuned) with a marker shape as secondary encoding.  The palette and publication settings are shared verbatim with
the PHD-8 Elsevier-style figures: blue, orange and aqua plus the common ink, muted, grid and neutral tones.
"""
import argparse
import json
import pathlib

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402
import numpy as np  # noqa: E402

HERE = pathlib.Path(__file__).resolve().parent


def locate_code_root() -> pathlib.Path:
    """Find the public ``code`` directory from either supported script location.

    The script is kept both under ``code/scripts/figures`` and alongside the
    manuscript in ``paper/figures``.  It deliberately reads the canonical JSON
    summaries from ``code/results`` in both cases.
    """
    for parent in HERE.parents:
        if (parent / "results" / "main_summary.json").is_file() and (parent / "analysis").is_dir():
            return parent
        if (parent / "code" / "results" / "main_summary.json").is_file() and (parent / "code" / "analysis").is_dir():
            return parent / "code"
    raise FileNotFoundError("Could not find code/results/main_summary.json from this script")


ROOT = locate_code_root()
OUT = HERE

BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
INK, INK2, MUTED, GRID, AXIS, NEUTRAL = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7", "#a9a7a0"
TYPE_COLOR = {"base": BLUE, "rag": ORANGE, "ft": AQUA}
TYPE_MARK = {"base": "o", "rag": "s", "ft": "D"}
LEVELS = [("L1", "L1 format & syntax", AQUA), ("L2", "L2 schema", BLUE), ("L3", "L3 version", ORANGE)]
NAMES = {"phi-3-mini-4k": ("Phi-3-mini-4k (3.8B)", "base"), "mistral-7b-v0.3": ("Mistral-7B-v0.3", "base"),
         "llama-3.1-8b": ("Llama-3.1-8B", "base"), "qwen2.5-7b": ("Qwen2.5-7B", "base"),
         "qwen2.5-14b": ("Qwen2.5-14B", "base"), "llama-3.1-70b": ("Llama-3.1-70B", "base"),
         "qwen2.5-7b-rag": ("Qwen2.5-7B + RAG", "rag"),
         "ft-v1": ("FT-v1 (original)", "ft"), "ft-v1fix": ("FT-v1-fix", "ft"), "ft-v2": ("FT-v2", "ft"),
         "ft-v2-rag": ("FT-v2 + RAG", "ft")}
plt.rcParams.update({
    "font.family": "serif", "font.size": 7.5, "axes.titlesize": 8,
    "axes.labelsize": 7.5, "xtick.labelsize": 6.5, "ytick.labelsize": 6.5,
    "legend.fontsize": 6.3, "axes.edgecolor": AXIS, "axes.labelcolor": INK2,
    "xtick.color": MUTED, "ytick.color": MUTED, "text.color": INK, "axes.grid": True,
    "axes.axisbelow": True, "axes.facecolor": "white", "figure.facecolor": "white", "axes.spines.top": False,
    "axes.spines.right": False, "axes.linewidth": 0.55, "xtick.major.width": 0.55,
    "ytick.major.width": 0, "grid.color": GRID, "grid.linewidth": 0.5, "pdf.fonttype": 42,
    "ps.fonttype": 42, "savefig.dpi": 300, "savefig.bbox": "tight",
})


def models_in(S, split="all"):
    present = {k.split("|")[0] for k in S[split]}
    return [m for m in NAMES if m in present]


def fig_main(S):
    """Figure C: level pass rates (sampled decoding) and requirement satisfaction (greedy and sampled) per model."""
    ms = models_in(S)
    y = np.arange(len(ms))[::-1]
    fig, (a, b) = plt.subplots(1, 2, figsize=(6.6, 0.32 * len(ms) + 1.4), sharey=True,
                               gridspec_kw={"width_ratios": [1.15, 1], "wspace": 0.12})
    h = 0.26
    for i, (lv, label, col) in enumerate(LEVELS):
        vals = [100 * S["all"][f"{m}|sample"][lv]["mean"] for m in ms]
        a.barh(y + (1 - i) * h, vals, height=h - 0.03, color=col, label=label, edgecolor="white", linewidth=0.6)
        for yy, v in zip(y + (1 - i) * h, vals):
            a.text(v + 1, yy, f"{v:.0f}", va="center", ha="left", fontsize=6.5, color=INK2)
    a.set_xlim(0, 105)
    a.set_xlabel("answers passing the level (%)\nsampled decoding")
    a.set_yticks(y)
    a.set_yticklabels([NAMES[m][0] for m in ms])
    a.xaxis.grid(True, color=GRID, linewidth=0.5)
    a.set_axisbelow(True)
    a.legend(loc="lower right", frameon=False, fontsize=6.5)
    for m, yy in zip(ms, y):
        t = NAMES[m][1]
        for mode, off, fill in (("greedy", 0.14, False), ("sample", -0.14, True)):
            v = S["all"][f"{m}|{mode}"]["req_sat"]
            mu, lo, hi = (100 * v["mean"], 100 * v["ci95"][0], 100 * v["ci95"][1])
            b.plot([lo, hi], [yy + off] * 2, color=TYPE_COLOR[t], linewidth=1.6, solid_capstyle="round")
            b.plot(mu, yy + off, marker=TYPE_MARK[t], markersize=5, color=TYPE_COLOR[t],
                   markerfacecolor=TYPE_COLOR[t] if fill else "white", markeredgewidth=1.2)
    b.set_xlabel("requirement satisfaction (%)\nmean and 95% CI over scenarios")
    b.xaxis.grid(True, color=GRID, linewidth=0.5)
    b.set_axisbelow(True)
    lo = min(100 * S["all"][f"{m}|{md}"]["req_sat"]["ci95"][0] for m in ms for md in ("greedy", "sample"))
    b.set_xlim(max(0, lo - 5), 100)
    from matplotlib.lines import Line2D
    handles = [Line2D([], [], marker="o", color=INK2, markerfacecolor="white", linestyle="", label="greedy"),
               Line2D([], [], marker="o", color=INK2, linestyle="", label="sampled (5 seeds)")]
    handles += [Line2D([], [], marker=TYPE_MARK[t], color=TYPE_COLOR[t], linestyle="", label=lab)
                for t, lab in (("base", "base model"), ("rag", "retrieval"), ("ft", "fine-tuned"))
                if any(NAMES[m][1] == t for m in ms)]
    b.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.5, -0.24), ncol=2, frameon=False, fontsize=6.5)
    fig.savefig(OUT / "fig_4_main_comparison.pdf", bbox_inches="tight")
    plt.close(fig)


def fig_errors(S, mode="sample"):
    """Figure E: share of answers with at least one finding of each kind, and violation rate per requirement family."""
    F = S[f"findings_{mode}"]
    ms = [m for m in NAMES if m in F]
    cols = ["truncated", "syntax", "missing artefact", "unknown key/field", "misplaced key", "invalid value",
            "missing required option", "removed / unsupported API", "duplicate keys (repetition)", "req:consistency",
            "req:checkpointing", "req:exactly-once", "req:latency", "req:durability", "req:retention",
            "req:under-provisioning", "req:resources"]
    cols = [c for c in cols if any(F[m].get(c, 0) > 0 for m in ms)]
    M = np.array([[100 * F[m].get(c, np.nan) for c in cols] for m in ms])
    fig, ax = plt.subplots(figsize=(6.6, 0.3 * len(ms) + 1.9))
    elsevier_blue = LinearSegmentedColormap.from_list("elsevier_blue", ["#ffffff", BLUE])
    im = ax.imshow(M, cmap=elsevier_blue, vmin=0, vmax=100, aspect="auto")
    for i in range(M.shape[0]):
        for j in range(M.shape[1]):
            v = M[i, j]
            if not np.isnan(v):
                ax.text(j, i, f"{v:.0f}", ha="center", va="center", fontsize=6,
                        color="white" if v > 60 else INK)
    ax.set_yticks(range(len(ms)))
    ax.set_yticklabels([NAMES[m][0] for m in ms])
    ax.set_xticks(range(len(cols)))
    ax.set_xticklabels([c.replace("req:", "") for c in cols], rotation=50, ha="right", fontsize=6.5)
    ax.axvline(cols.index("req:consistency") - 0.5 if "req:consistency" in cols else len(cols) - 0.5,
               color=INK, linewidth=1)
    ax.tick_params(length=0)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.set_title("left: answers with $\\geq$1 finding of the kind (%) — right: violated requirement checks of the family (%)",
                 fontsize=7, color=INK2, loc="left")
    fig.colorbar(im, ax=ax, fraction=0.025, pad=0.01).ax.tick_params(labelsize=6)
    fig.savefig(OUT / "fig_8_error_map.pdf", bbox_inches="tight")
    plt.close(fig)


def fig_layer(L):
    """Figure F: outcome of the validation layer per model (two feedback modes)."""
    order = [m for m in NAMES if any(k.startswith(m + "|") for k in L)]
    st = [("accepted-unchanged", "accepted as generated", BLUE), ("accepted-repaired", "accepted after repair", AQUA),
          ("accepted-regenerated-1", "accepted after 1 regeneration", ORANGE),
          ("accepted-regenerated-2", "accepted after 2 regenerations", MUTED), ("rejected", "rejected", NEUTRAL)]
    fig, axes = plt.subplots(1, 2, figsize=(6.6, 0.3 * len(order) + 1.3), sharey=True, gridspec_kw={"wspace": 0.06})
    for ax, mode, title in zip(axes, ("syntax", "full"), ("feedback: format, schema, version",
                                                          "feedback: + requirement checks")):
        y = np.arange(len(order))[::-1]
        left = np.zeros(len(order))
        for key, lab, col in st:
            v = np.array([100 * L.get(f"{m}|{mode}", {}).get("share", {}).get(key, 0) for m in order])
            ax.barh(y, v, left=left, color=col, height=0.62, label=lab, edgecolor="white", linewidth=0.6)
            left += v
        for yy, m in zip(y, order):
            acc = 100 * L.get(f"{m}|{mode}", {}).get("accepted", 0)
            ax.text(101, yy, f"{acc:.0f}%", va="center", fontsize=6.5, color=INK2)
        ax.set_xlim(0, 112)
        ax.set_title(title, fontsize=7, color=INK2, loc="left")
        ax.set_xlabel("answers (%)")
        ax.set_yticks(y)
        ax.set_yticklabels([NAMES[m][0] for m in order])
    axes[0].legend(loc="upper center", bbox_to_anchor=(1.0, -0.2), ncol=3, frameon=False, fontsize=6.5)
    fig.savefig(OUT / "fig_5_validation_layer.pdf", bbox_inches="tight")
    plt.close(fig)


def fig_testbed(E):
    """Figure G: runtime outcome of every deployed answer, by root cause."""
    rows = E["rows"]
    order = [m for m in list(NAMES) + ["gold"] if any(r["model"].rsplit("-s", 1)[0] == m or r["model"] == m for r in rows)]
    STAGES = [("incomplete or unparsable answer", {"artefact missing", "invalid YAML"}, NEUTRAL),
              ("rejected by Kafka / Kubernetes", {"Kubernetes resource not accepted", "topics not created",
                                                  "topic: removed CLI option"}, MUTED),
              ("rejected by Flink SQL", {"SQL: syntax or reference error", "SQL: invalid type, constraint or class",
                                         "SQL: unsupported or missing connector option", "source without group.id"}, INK2),
              ("job failed to start", {"transaction timeout > broker maximum", "insufficient slots",
                                       "job did not start (other)"}, INK),
              ("ran; guarantee violated", {"no completed checkpoints", "data lost or duplicated after failure",
                                           "throughput below offered load"}, ORANGE),
              ("ran; latency objective missed", {"latency objective missed"}, ORANGE),
              ("passed all criteria", {"pass"}, AQUA),
              ("not testable on the testbed", {"untestable"}, "#ffffff")]
    stage_of = {c: (lab, col) for lab, cs, col in STAGES for c in cs}
    fig, ax = plt.subplots(figsize=(6.6, 0.3 * len(order) + 1.6))
    y = np.arange(len(order))[::-1]
    for yi, m in zip(y, order):
        R = [r for r in rows if r["model"] == m or r["model"].rsplit("-s", 1)[0] == m
             or (m == "gold" and r["model"] == "reference")]  # reference answers: case study (gold) and grid
        left = 0
        for lab, cs, col in STAGES:
            n = sum(r["_cause"] in cs for r in R)
            if n:
                hatch = "////" if col == "#ffffff" else None  # untestable: outlined and hatched, not a colour
                ax.barh(yi, 100 * n / len(R), left=left, color=col, height=0.62, linewidth=0.6,
                        edgecolor=NEUTRAL if hatch else "white", hatch=hatch,
                        label=lab if lab not in ax.get_legend_handles_labels()[1] else None)
                left += 100 * n / len(R)
        ax.text(101, yi, f"n={len(R)}", va="center", fontsize=6.5, color=INK2)
    ax.set_yticks(y)
    ax.set_yticklabels([NAMES.get(m, ("reference answers", ""))[0] for m in order])
    ax.set_xlim(0, 112)
    ax.set_xlabel("deployed answers (%), by first failing stage")
    h, l = ax.get_legend_handles_labels()
    order_l = [lab for lab, _, _ in STAGES if lab in l]
    ax.legend([h[l.index(x)] for x in order_l], order_l, loc="upper center", bbox_to_anchor=(0.5, -0.18), ncol=3,
              frameon=False, fontsize=6.5)
    fig.savefig(OUT / "fig_6_testbed.pdf", bbox_inches="tight")
    plt.close(fig)


def fig_training(T):
    """Figure H: training and validation loss; left: original run vs. v1-fix seeds (same data); right: v2 seeds."""
    fig, axes = plt.subplots(1, 2, figsize=(6.6, 2.4), gridspec_kw={"wspace": 0.25})
    groups = [(axes[0], lambda n: n.startswith("FT-v1") or n.startswith("v1fix"), "v1 data (original split)"),
              (axes[1], lambda n: n.startswith("v2-r64-nf4"), "v2 data")]
    for ax, sel, title in groups:
        for r in [r for r in T if sel(r["run"])]:
            orig = r["run"].startswith("FT-v1")
            col = ORANGE if orig else AQUA
            tr = np.array(r["train"]); ev = np.array(r["eval"])
            if len(tr):
                k = 25
                sm = np.convolve(tr[:, 1], np.ones(k) / k, mode="valid")
                ax.plot(tr[k - 1:, 0], sm, color=col, linewidth=0.8, alpha=0.45)
            if len(ev):
                ax.plot(ev[:, 0], ev[:, 1], color=col, linewidth=1.6, marker="o" if not orig else None, markersize=2.5)
            if r.get("selected_step"):
                sel_ev = [e for s_, e in r["eval"] if s_ <= r["selected_step"]]
                if sel_ev:
                    ax.plot(r["selected_step"], sel_ev[-1], marker="v", color=INK, markersize=5)
        ax.set_title(title, fontsize=7, color=INK2, loc="left")
        ax.set_xlabel("optimizer step")
        ax.set_ylabel("loss")
        ax.yaxis.grid(True, color=GRID, linewidth=0.5)
        ax.set_ylim(0.2, 1.2)
    from matplotlib.lines import Line2D
    h = [Line2D([], [], color=ORANGE, lw=1.6, label="original run (FT-v1)"),
         Line2D([], [], color=AQUA, lw=1.6, marker="o", ms=2.5, label="revised runs (3 seeds)"),
         Line2D([], [], color=INK2, lw=0.8, alpha=0.6, label="training loss (smoothed)"),
         Line2D([], [], color=INK2, lw=1.6, label="validation loss"),
         Line2D([], [], color=INK, marker="v", ls="", label="selected checkpoint")]
    axes[1].legend(handles=h, loc="upper center", bbox_to_anchor=(-0.15, -0.3), ncol=3, frameon=False, fontsize=6.5)
    fig.savefig(OUT / "fig_3_training.pdf", bbox_inches="tight")
    plt.close(fig)


ABL = [("ft-v2", "FT-v2: r = 64, NF4, all data"), ("abl-v2-r32", "r = 32"), ("abl-v2-r16", "r = 16"),
       ("abl-v2-r8", "r = 8"), ("abl-v2-bf16", "no quantisation (bf16)"),
       ("abl-v2-nodocs", "without documentation pairs"), ("abl-v2-nodomain", "without domain manifests"),
       ("abl-v2-noca", "without CodeAlpaca")]


def fig_ablation(S):
    """Figure D: ablation of FT-v2 (three training seeds each): requirement satisfaction and truncation, with the
    per-seed means as small points and the base model Qwen2.5-7B as a reference line."""
    rows = [(m, lab) for m, lab in ABL if f"{m}|greedy" in S["all"]]
    if len(rows) < 2:
        return
    y = np.arange(len(rows))[::-1]
    col = TYPE_COLOR["ft"]
    fig, axes = plt.subplots(1, 2, figsize=(6.6, 0.3 * len(rows) + 1.3), sharey=True, gridspec_kw={"wspace": 0.1})
    for ax, met, xlab in ((axes[0], "req_sat", "requirement satisfaction (%)"), (axes[1], "truncated", "answers truncated (%)")):
        for mode, off, fill, ls in (("greedy", 0.15, False, (0, (3, 2))), ("sample", -0.15, True, "-")):
            ref = S["all"].get(f"qwen2.5-7b|{mode}")
            if ref:
                ax.axvline(100 * ref[met]["mean"], color=TYPE_COLOR["base"], linewidth=0.9, linestyle=ls, zorder=1)
            for (m, _), yy in zip(rows, y):
                v = S["all"][f"{m}|{mode}"][met]
                for x in v.get("train_seed_means", []):
                    ax.plot(100 * x, yy + off, marker="o", markersize=2.6, color=col, alpha=0.55, linestyle="", zorder=2)
                ax.plot(100 * v["mean"], yy + off, marker="D", markersize=5, color=col, zorder=3,
                        markerfacecolor=col if fill else "white", markeredgewidth=1.2, linestyle="")
        ax.set_xlabel(xlab)
        ax.xaxis.grid(True, color=GRID, linewidth=0.5)
        ax.set_axisbelow(True)
    axes[0].set_yticks(y)
    axes[0].set_yticklabels([lab for _, lab in rows])
    for ax in axes:
        ax.axhline(y[0] - 0.5, color=GRID, linewidth=0.8)
    axes[1].set_xlim(-3, 103)
    from matplotlib.lines import Line2D
    handles = [Line2D([], [], marker="D", color=col, markerfacecolor="white", linestyle="", label="greedy (mean of 3 training seeds)"),
               Line2D([], [], marker="D", color=col, linestyle="", label="sampled (mean of 3 training seeds)"),
               Line2D([], [], marker="o", markersize=2.6, color=col, alpha=0.55, linestyle="", label="one training seed"),
               Line2D([], [], color=TYPE_COLOR["base"], linestyle=(0, (3, 2)), label="Qwen2.5-7B, greedy"),
               Line2D([], [], color=TYPE_COLOR["base"], label="Qwen2.5-7B, sampled")]
    axes[0].legend(handles=handles, loc="upper center", bbox_to_anchor=(1.0, -0.2), ncol=3, frameon=False, fontsize=6.5)
    fig.savefig(OUT / "fig_7_ablation.pdf", bbox_inches="tight")
    plt.close(fig)


def main(output: pathlib.Path) -> None:
    global OUT
    OUT = output.resolve()
    OUT.mkdir(parents=True, exist_ok=True)
    S = json.load(open(ROOT / "results" / "main_summary.json"))
    fig_main(S)
    fig_errors(S)
    fig_ablation(S)
    if (ROOT / "results" / "layer_summary.json").exists():
        fig_layer({k: v for k, v in json.load(open(ROOT / "results" / "layer_summary.json")).items() if not k.startswith("_")})
    if (ROOT / "results" / "training_summary.json").exists():
        fig_training(json.load(open(ROOT / "results" / "training_summary.json")))
    if (ROOT / "results" / "e7_summary.json").exists():
        fig_testbed(json.load(open(ROOT / "results" / "e7_summary.json")))
    print("figures written to", OUT)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate the manuscript data figures from code/results JSON summaries."
    )
    parser.add_argument(
        "--output", type=pathlib.Path, default=HERE,
        help="directory for the six vector PDFs (default: this script's directory)",
    )
    args = parser.parse_args()
    main(args.output)
