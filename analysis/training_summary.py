"""Training summary (Section 5.1, Figure H): every run's losses, selected checkpoint and duration, read from the trainer
logs (original run: training_metrics.json of job 1284; revised runs: models/<run>/run.json)."""
import glob
import json
import pathlib
import re
import sys

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUT = ROOT / "results"
RES_RT = OUT / "orig_train_runtime.txt"


def summarise(name, hist, best_step=None):
    tr = [(h["step"], h["loss"]) for h in hist if "loss" in h]
    ev = [(h["step"], h["eval_loss"]) for h in hist if "eval_loss" in h]
    rt = [h["train_runtime"] for h in hist if "train_runtime" in h]
    b = min(ev, key=lambda x: x[1]) if ev else (None, None)
    last10 = [l for s, l in tr[-10:]]
    return {"run": name, "steps": tr[-1][0] if tr else None, "first_train_loss": tr[0][1] if tr else None,
            "final_train_loss_mean_last10": float(np.mean(last10)) if last10 else None,
            "first_eval_loss": ev[0][1] if ev else None, "first_eval_step": ev[0][0] if ev else None,
            "final_eval_loss": ev[-1][1] if ev else None, "final_eval_step": ev[-1][0] if ev else None,
            "min_eval_loss": b[1], "min_eval_step": b[0], "selected_step": best_step,
            "runtime_min": rt[0] / 60 if rt else None, "train": tr, "eval": ev}


def main():
    runs = []
    orig = json.load(open(ROOT / "data" / "original-run" / "training_metrics.json"))
    o = summarise("FT-v1 (original, job 1284)", orig["log_history"], best_step=8916)
    rt = RES_RT.read_text() if RES_RT.exists() else ""
    m = re.search(r"train_runtime.: ([0-9.]+)", rt)
    o["runtime_min"] = float(m.group(1)) / 60 if m else None  # from logs/train_1284.out
    runs.append(o)
    for f in sorted(glob.glob(str(ROOT / "results" / "runs" / "*" / "run.json"))):
        d = json.load(open(f))
        name = pathlib.Path(f).parent.name
        bc = d.get("best_checkpoint") or ""
        m = re.search(r"checkpoint-(\d+)", bc)
        s = summarise(name, d["log_history"], int(m.group(1)) if m else None)
        s["train_samples"] = d.get("train_samples")
        s["effective_batch"] = d.get("effective_batch")
        runs.append(s)
    json.dump(runs, open(OUT / "training_summary.json", "w"), indent=1)
    for r in runs:
        f3 = lambda x: "n/a" if x is None else f"{x:.3f}"  # noqa: E731  (runs without any evaluation)
        print(f"{r['run']:28s} steps={r['steps']} min_eval={f3(r['min_eval_loss'])}@{r['min_eval_step']} "
              f"final_eval={f3(r['final_eval_loss'])} selected={r['selected_step']} runtime={r['runtime_min'] or 0:.0f} min")


if __name__ == "__main__":
    main()
