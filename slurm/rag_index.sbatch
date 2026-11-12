#!/bin/bash
#SBATCH --job-name=kf-rag
#SBATCH --partition=short
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=48G
#SBATCH --time=01:00:00
#SBATCH --output=slurm-%x-%j.out
set -euo pipefail
REPO_DIR="${REPO_DIR:-${SLURM_SUBMIT_DIR:-$(cd "$(dirname "$0")/.." && pwd)}}"
CONDA_BASE="${CONDA_BASE:-$HOME/miniconda3}"
source "$CONDA_BASE/etc/profile.d/conda.sh" && conda activate vllm
export HF_HUB_OFFLINE=1
cd "$REPO_DIR" && python src/rag.py --bench bench/kfbench-v1.jsonl --out rag/retrieved-k8.jsonl --k 8
