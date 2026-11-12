#!/bin/bash
#SBATCH --job-name=kf-pilot
#SBATCH --partition=short
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --output=slurm-%x-%j.out
# E0 pilot: base vs the original adapter (two checkpoints) on 40 scenarios, greedy decoding.
set -euo pipefail
REPO_DIR="${REPO_DIR:-${SLURM_SUBMIT_DIR:-$(cd "$(dirname "$0")/.." && pwd)}}"
CONDA_BASE="${CONDA_BASE:-$HOME/miniconda3}"
source "$CONDA_BASE/etc/profile.d/conda.sh" && conda activate vllm
export HF_HUB_OFFLINE=1 VLLM_LOGGING_LEVEL=WARNING
cd "$REPO_DIR"
B=bench/kfbench-v1.jsonl; IDS=bench/pilot-ids.txt; O=runs/pilot
Q=Qwen/Qwen2.5-7B-Instruct
LORA_DIR="${LORA_DIR:?Set LORA_DIR to the directory containing the original adapter checkpoints}"
python src/generate.py --model $Q --name qwen2.5-7b --bench $B --ids $IDS --out $O/qwen2.5-7b.jsonl
python src/generate.py --model $Q --name ft-v1-ckpt8916 --lora "$LORA_DIR/checkpoint-8916" --bench $B --ids $IDS --out $O/ft-v1-ckpt8916.jsonl
python src/generate.py --model $Q --name ft-v1-ckpt5944 --lora "$LORA_DIR/checkpoint-5944" --bench $B --ids $IDS --out $O/ft-v1-ckpt5944.jsonl
python src/run_validate.py --bench $B --gens $O/*.jsonl --out $O/validated.jsonl
