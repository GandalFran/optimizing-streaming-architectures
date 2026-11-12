#!/bin/bash
#SBATCH --job-name=kf-eval
#SBATCH --partition=short
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=96G
#SBATCH --time=04:00:00
#SBATCH --output=slurm-%x-%j.out
# E3/E4: full benchmark, greedy (+30 batch-1 latency requests) and 5 sampled seeds. Vars: MODEL NAME [LORA] [RAG] [MAXLEN]
set -euo pipefail
REPO_DIR="${REPO_DIR:-${SLURM_SUBMIT_DIR:-$(cd "$(dirname "$0")/.." && pwd)}}"
CONDA_BASE="${CONDA_BASE:-$HOME/miniconda3}"
source "$CONDA_BASE/etc/profile.d/conda.sh" && conda activate vllm
export HF_HUB_OFFLINE=1 VLLM_LOGGING_LEVEL=WARNING
cd "$REPO_DIR"
B=bench/kfbench-v1.jsonl; O=runs/main; mkdir -p $O
EXTRA="--tp ${TP:-1}"; [ -n "${LORA:-}" ] && EXTRA="$EXTRA --lora $LORA"; [ -n "${RAG:-}" ] && EXTRA="$EXTRA --rag $RAG"
[ -n "${REP:-}" ] && EXTRA="$EXTRA --repetition-penalty $REP"
ML=${MAXLEN:-8192}
python src/generate.py --model $MODEL --name $NAME $EXTRA --bench $B --out $O/$NAME.greedy.jsonl --mode greedy --latency-sample 30 --max-model-len $ML
python src/generate.py --model $MODEL --name $NAME $EXTRA --bench $B --out $O/$NAME.sample.jsonl --mode sample --seeds 1,2,3,4,5 --max-model-len $ML
python src/run_validate.py --bench $B --gens $O/$NAME.greedy.jsonl $O/$NAME.sample.jsonl --out $O/$NAME.validated.jsonl
