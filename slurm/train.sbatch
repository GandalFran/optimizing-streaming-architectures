#!/bin/bash
#SBATCH --job-name=kf-train
#SBATCH --partition=medium
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=96G
#SBATCH --time=23:00:00
#SBATCH --output=slurm-%x-%j.out
# Vars: RUN (output name), ARGS (extra args for train_lora.py)
set -euo pipefail
REPO_DIR="${REPO_DIR:-${SLURM_SUBMIT_DIR:-$(cd "$(dirname "$0")/.." && pwd)}}"
CONDA_BASE="${CONDA_BASE:-$HOME/miniconda3}"
source "$CONDA_BASE/etc/profile.d/conda.sh" && conda activate phd_env
export HF_HUB_OFFLINE=1 TOKENIZERS_PARALLELISM=false
# deepspeed (imported by accelerate) probes $CUDA_HOME/bin/nvcc at import time; the system toolkit is gone, so point it
# at the CUDA toolkit shipped in the vllm-latest conda env (only used for the version probe; no kernels are compiled)
export CUDA_HOME="${CUDA_HOME:-$CONDA_BASE/envs/vllm-latest}" DS_BUILD_OPS=0
cd "$REPO_DIR" && mkdir -p models
python src/train_lora.py --out models/$RUN $ARGS
