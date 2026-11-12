# Domain-Specific Instruction Tuning of Large Language Models for Kafka and Flink Configuration Generation: An Evaluation with Layered Static and Executable Validation

This is the public-facing final revision package. It contains only the reproducibility code, frozen inputs and
results used by the revised manuscript; superseded collaborator material and local development caches are excluded.

Companion repository of *Domain-Specific Instruction Tuning of Large Language Models for Kafka and Flink
Configuration Generation: An Evaluation with Layered Static and Executable Validation* (revision of COMNET-D-26-03896). Every number, table and figure in the manuscript is produced by the scripts below from the runs
stored here.  The flattened manuscript contains its rendered numeric macros directly; `analysis/export_numbers.py`
remains available to regenerate the macro source used by the response letter.

## Layout

| path | content |
|---|---|
| `catalog/` | version-specific catalogues and the scripts that extract them: Kafka 4.1.2 generated configuration docs and CLI help, Flink 1.20.5 and Flink Kubernetes Operator 1.13.0 `ConfigOption`s by reflection (`DumpFlinkOptions.java`), Strimzi 0.48 CRDs; `SHA256SUMS`; `official-examples/` (configuration examples of the target versions used to evaluate the validator) |
| `bench/` | `kfbench-v1.jsonl` (226 scenarios with requirement specifications; frozen), its generator `make_bench.py`, the testbed sample `e7-scenarios.json`, and one reference answer per scenario (`kfbench-v1-reference.jsonl`, written by `make_reference.py`) |
| `src/kfbench/` | validator (levels L1 format/syntax, L2 schema, L3 version, L4 requirements; `VALIDATOR_VERSION = "1.3"`) |
| `tests/` | validator unit tests (`pytest tests/`) |
| `src/generate.py` | generation with vLLM (greedy, five sampled seeds, optional LoRA adapter, retrieval, repetition penalty, tensor parallelism) |
| `src/run_validate.py` | validates stored generations |
| `src/rag.py`, `rag/` | retrieval baseline (BGE-M3 index over a versioned documentation snapshot) and the retrieved chunks per scenario |
| `src/validation_layer.py` | validate → repair → regenerate with feedback (K = 2) → reject; modes `syntax` and `full` |
| `src/train_lora.py` | QLoRA fine-tuning (all hyperparameters as in the manuscript's fine-tuning table) |
| `data/` | compact immutable inputs retained from the original run (training log used for the historical-loss comparison) |
| `data_v2/` | domain dataset v2: collection, deduplication and repository split, instruction back-translation, documentation pairs, assembly; `redact_for_release.py` |
| `testbed/` | executable validation (Kafka 4.1.2 + Flink 1.20.5, failure injection); see `testbed/README.md` |
| `slurm/` | the SLURM jobs used for every run |
| `runs/` | stored generations and validation results (`main/`), validation-layer runs (`layer/`), testbed results (`e7/`) |
| `results/` | analysis outputs (JSON) that the manuscript's numbers, tables and figures are generated from |
| `analysis/` | analysis scripts (below) |
| `scripts/figures/generate_figures.py` | standardized generator for the six data-derived manuscript PDFs; reads only `results/*.json` |
| `scripts/figures/generate_tables.py` | Markdown counterparts of the data-derived manuscript tables; reads canonical result JSON and run records |

## Software

- Training: Python 3.11, PyTorch 2.4, transformers 4.46, PEFT 0.13, TRL 0.12, bitsandbytes 0.44 (H100 GPUs).
- Generation and validation layer: vLLM 0.13 with `tokenizer_mode="hf"` for every model.
- Analysis: NumPy, SciPy, Matplotlib (`requirements` in each script's imports).

## Configuration for HPC runs

The SLURM launchers are portable: they default to the directory from which the
job is submitted (or `REPO_DIR`) and use `CONDA_BASE` when it is not the
standard `$HOME/miniconda3`.  Set `LORA_DIR` before running `slurm/pilot.sbatch`
and `PYTHON_BIN` before `testbed/run_e7.sbatch` when the intended interpreter is
not on `PATH`.  `catalog/build_catalog.sh` requires `OPT`, the directory holding
the local JDK, Kafka and Flink installations.  Generated scheduler logs are
ignored by Git.

## Reproducing the results

1. Catalogues: `catalog/build_catalog.sh` (downloads the exact software versions and extracts the catalogues).
2. Validator tests and evaluation: `pytest tests/`; `python bench/make_reference.py` (reference answers);
   `python analysis/validator_eval.py` (reference answers, semantics-preserving rewrites, injected defects and official
   examples → `results/validator_eval.json`; with `--src <older kfbench package> --out ...` the same evaluation of an
   earlier validator version, e.g. `results/validator_eval_v1.2.json`).
3. Main comparison: `slurm/eval_model.sbatch` per model (variables `MODEL NAME [LORA] [RAG] [REP] [TP]`), then
   `slurm/revalidate.sh` to re-validate every stored generation with the current validator.
4. Retrieval baseline: `slurm/rag_index.sbatch`, then `eval_model.sbatch` with `RAG`.
5. Fine-tuning: `slurm/train.sbatch` with `RUN` and `ARGS` (e.g. `--seed 1 --rank 16`; data ablations use
   `--sources domain-k8s+codealpaca`; use `+`, not `,`, inside `sbatch --export`).
6. Validation layer: `slurm/layer.sbatch` per model and mode.
7. Testbed: `testbed/run_e7.sbatch` with a deployment list from `runs/e7/list-*.jsonl` (`list-reference.jsonl`: the
   reference answers of the in-distribution testbed scenarios, a positive control).
8. Analysis, in this order:
   ```
   python analysis/aggregate.py --ref qwen2.5-7b      # results/main_summary.json (CIs, paired tests, Holm families)
   python analysis/revalidation_diff.py runs/main_v12 runs/main > results/revalidation_v1.3.json   # v1.2 vs v1.3 verdicts
   python analysis/layer_summary.py                   # results/layer_summary.json (final answers re-validated)
   python analysis/e7_summary.py                      # results/e7_summary.json
   python analysis/training_summary.py                # results/training_summary.json
   python analysis/generation_diag.py runs/main/*.greedy.jsonl runs/main/*.sample.jsonl > results/generation_diag.json
   python analysis/memorisation.py                    # results/memorisation.json (needs the training data)
   python analysis/leakage.py                         # results/leakage.json (original split)
   python analysis/export_numbers.py <paper>/numbers.tex
   python scripts/figures/generate_figures.py --output <paper>/figures
   python scripts/figures/generate_tables.py --output <paper>/figures/tables.md
   ```

## Data and credentials

Training data derived from The Stack is redistributed under its terms of use with per-file provenance. Public
manifests can contain credentials (private keys or cloud access keys); the released JSONL files are redacted with
`data_v2/redact_for_release.py`, which replaces recognised values with typed placeholders before publication.

