#!/bin/bash
# Re-validate stored generations with the current validator (VALIDATOR_VERSION in src/kfbench/validate.py)
set -euo pipefail
REPO_DIR="${REPO_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
CONDA_BASE="${CONDA_BASE:-$HOME/miniconda3}"
source "$CONDA_BASE/etc/profile.d/conda.sh" && conda activate vllm
cd "$REPO_DIR"
for m in $MODELS; do
  python src/run_validate.py --bench bench/kfbench-v1.jsonl --gens runs/main/$m.greedy.jsonl runs/main/$m.sample.jsonl --out runs/main/$m.validated.jsonl | tail -2
done
