#!/bin/bash
#SBATCH --job-name=kf-layer
#SBATCH --partition=short
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=96G
#SBATCH --time=03:00:00
#SBATCH --output=slurm-%x-%j.out
# E6. Vars: MODEL NAME MODE [LORA] [RAG] [MAXLEN]
set -euo pipefail
REPO_DIR="${REPO_DIR:-${SLURM_SUBMIT_DIR:-$(cd "$(dirname "$0")/.." && pwd)}}"
CONDA_BASE="${CONDA_BASE:-$HOME/miniconda3}"
source "$CONDA_BASE/etc/profile.d/conda.sh" && conda activate vllm
export HF_HUB_OFFLINE=1 VLLM_LOGGING_LEVEL=WARNING
cd "$REPO_DIR" && mkdir -p runs/layer
EXTRA="--tp ${TP:-1}"; [ -n "${LORA:-}" ] && EXTRA="$EXTRA --lora $LORA"; [ -n "${RAG:-}" ] && EXTRA="$EXTRA --rag $RAG"
python src/validation_layer.py --model $MODEL --name $NAME $EXTRA --bench bench/kfbench-v1.jsonl \
  --greedy runs/main/$NAME.greedy.jsonl --mode $MODE --rounds 2 --max-model-len ${MAXLEN:-16384} \
  --out runs/layer/$NAME.$MODE.jsonl
