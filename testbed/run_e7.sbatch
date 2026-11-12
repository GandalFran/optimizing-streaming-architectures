#!/bin/bash
#SBATCH --job-name=e7-deploy
#SBATCH --partition=short
#SBATCH --cpus-per-task=44
#SBATCH --mem=128G
#SBATCH --time=04:00:00
#SBATCH --output=slurm-%x-%j.out
# E7 executable validation: deploy a list of answers one after another, each on a fresh
# 3-broker Kafka + Flink session cluster (CPU only; no GPU is requested).
#
#   LIST=runs/e7/sample.jsonl sbatch testbed/run_e7.sbatch
#   LIST=... DURATION=60 OUTDIR=runs/e7/smoke sbatch testbed/run_e7.sbatch
#
# LIST: JSONL, one deployment per line:
#   {"id": "ID-001", "model": "qwen2.5-14b", "seed": 0}                      answers from runs/main/<model>.greedy.jsonl (seed 0)
#                                                                              or runs/main/<model>.sample.jsonl (seed >= 1)
#   {"id": ..., "model": ..., "seed": ..., "answers": "path.jsonl"}          explicit generation file
#   {"id": ..., "model": "gold", "answer_file": "path.txt", "tag": "..."}    a raw answer text
#   optional "args": ["--duration", "60"]                                     per-deployment driver options
# A deployment whose result file already exists is skipped (the job can be resubmitted after a timeout).
# 44 CPUs = 22 physical cores: 3 brokers x 2, 3 TaskManagers x 4, JobManager 1, load generation 2,
# measurement reader 1 (see README.md for why the brokers need 2 cores).
# ~6 min per deployment at the default 180 s load: about 35 deployments fit in the 4 h short partition
# (scheduling note: a shorter --time, e.g. sbatch --time=01:00:00, backfills much sooner on cn001).
set -uo pipefail
REPO_DIR="${REPO_DIR:-${SLURM_SUBMIT_DIR:-$(cd "$(dirname "$0")/.." && pwd)}}"
cd "$REPO_DIR"
PY="${PYTHON_BIN:-python}"
OUT=${OUTDIR:-runs/e7}
ROOT=/tmp/${USER}-e7/${SLURM_JOB_ID:-local}
EXTRA="--duration ${DURATION:-180} --run-root $ROOT ${E7_ARGS:-}"
mkdir -p "$OUT" logs
echo "E7 job ${SLURM_JOB_ID:-local} on $(hostname): $(nproc) CPUs, list ${LIST}"
$PY - "$LIST" "$OUT" <<'PYEOF' > "$OUT/.cmds.${SLURM_JOB_ID:-local}"
import json, shlex, sys
lst, out = sys.argv[1], sys.argv[2]
for ln in open(lst):
    if not ln.strip():
        continue
    r = json.loads(ln)
    seed = int(r.get("seed", 0))
    tag = r.get("tag") or f"{r['id']}__{r['model']}__s{seed}"
    args = ["--id", r["id"], "--out", f"{out}/{tag}.json", "--model", r["model"], "--seed", str(seed)]
    if r.get("answer_file"):
        args += ["--answer-file", r["answer_file"]]
    else:
        ans = r.get("answers") or f"runs/main/{r['model']}.{'greedy' if seed == 0 else 'sample'}.jsonl"
        args += ["--answers", ans]
    args += [str(x) for x in r.get("args", [])]          # per-deployment overrides, e.g. ["--duration", "60"]
    print(tag + "\t" + shlex.join(args))
PYEOF
while IFS=$'\t' read -r TAG ARGS; do
  if [ -f "$OUT/$TAG.json" ]; then echo "skip $TAG (result exists)"; continue; fi
  echo "=== $(date +%T) $TAG"
  eval "$PY testbed/deploy_check.py $EXTRA $ARGS" || echo "driver exit $? for $TAG"
  # belt and braces: nothing of this deployment may survive into the next one
  pkill -9 -u "$USER" -f "$ROOT/" 2>/dev/null || true
  rm -rf "$ROOT"
done < "$OUT/.cmds.${SLURM_JOB_ID:-local}"
echo "E7 job done $(date +%T)"
