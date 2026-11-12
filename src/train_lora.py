"""QLoRA/LoRA fine-tuning of Qwen2.5-7B-Instruct, parameterised for seeds and ablations (E8/E9).

Defaults reproduce the original run (job 1284): r=64, alpha=16, dropout 0.1, all linear projections, NF4 with double
quantisation and bf16 compute, lr 2e-4 with linear decay, 3 epochs, max length 4096, effective batch size 8.
Differences from job 1284, fixed before any rerun: explicit seed for every source of randomness; evaluation every
500 steps and selection of the checkpoint with the lowest validation loss (the same rule for every run).
Stack: transformers 4.46.0, peft 0.13.2, trl 0.12.0, bitsandbytes 0.44.1 (conda env phd_env).
"""
import argparse
import collections
import json
import os

import torch
from datasets import load_dataset
from peft import LoraConfig
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig, set_seed
from trl import SFTConfig, SFTTrainer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--train", default=os.path.expanduser("~/phd/data/train.jsonl"))
    ap.add_argument("--val", default=os.path.expanduser("~/phd/data/test.jsonl"))
    ap.add_argument("--subset", choices=["hybrid", "codealpaca", "k8s"], default="hybrid", help="v1 data composition")
    ap.add_argument("--sources", help="v2 data composition: values (or prefixes) of the 'source' field to keep, separated "
                                      "by '+' or ',' ('+' is safe inside sbatch --export, where ',' separates variables)")
    ap.add_argument("--rank", type=int, default=64)
    ap.add_argument("--alpha", type=int, default=16)
    ap.add_argument("--quant", choices=["nf4", "int8", "bf16"], default="nf4")
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--epochs", type=float, default=3)
    ap.add_argument("--per-device-batch", type=int, default=2)
    ap.add_argument("--effective-batch", type=int, default=8)
    ap.add_argument("--out", required=True)
    ap.add_argument("--legacy-pad", action="store_true",
                    help="reproduce job 1284: pad_token = eos_token, which masks <|im_end|> in the labels")
    ap.add_argument("--max-steps", type=int, default=-1, help="smoke tests only")
    a = ap.parse_args()
    set_seed(a.seed)

    model_name = "Qwen/Qwen2.5-7B-Instruct"
    tok = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
    # Job 1284 set pad_token = eos_token (<|im_end|>). The LM collator masks pad tokens in the labels, so the model
    # never learned to end its turn (verified: every <|im_end|> label = -100). Fixed: keep Qwen's own pad token.
    if a.legacy_pad:
        tok.pad_token = tok.eos_token
    else:
        tok.pad_token = "<|endoftext|>"
    assert a.legacy_pad or tok.pad_token_id != tok.eos_token_id

    sources = {x for x in a.sources.replace("+", ",").split(",") if x} if a.sources else None

    def keep(ex):
        if sources is not None:
            src = ex.get("source") or ""
            return any(src == s or src.startswith(s) for s in sources)
        is_k8s = ex["messages"][0]["content"].startswith("Generate a Kubernetes configuration file for")
        return {"hybrid": True, "codealpaca": not is_k8s, "k8s": is_k8s}[a.subset]

    train = load_dataset("json", data_files=a.train, split="train").filter(keep)
    source_counts = dict(collections.Counter(train["source"])) if "source" in train.column_names else None
    print("training pairs per source:", source_counts, flush=True)
    if "source" in train.column_names:
        train = train.remove_columns(["source"])
    val = load_dataset("json", data_files=a.val, split="train")  # same mixed validation set for every run
    if "source" in val.column_names:
        val = val.remove_columns(["source"])

    q = None
    if a.quant == "nf4":
        q = BitsAndBytesConfig(load_in_4bit=True, bnb_4bit_quant_type="nf4", bnb_4bit_compute_dtype=torch.bfloat16,
                               bnb_4bit_use_double_quant=True)
    elif a.quant == "int8":
        q = BitsAndBytesConfig(load_in_8bit=True)
    model = AutoModelForCausalLM.from_pretrained(model_name, quantization_config=q, torch_dtype=torch.bfloat16,
                                                 device_map={"": int(os.environ.get("LOCAL_RANK", 0))},
                                                 trust_remote_code=True)
    world = int(os.environ.get("WORLD_SIZE", 1))
    accum = max(1, a.effective_batch // (a.per_device_batch * world))
    peft_config = LoraConfig(r=a.rank, lora_alpha=a.alpha, lora_dropout=0.1, bias="none", task_type="CAUSAL_LM",
                             target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"])
    args = SFTConfig(output_dir=a.out, num_train_epochs=a.epochs, per_device_train_batch_size=a.per_device_batch,
                     per_device_eval_batch_size=4, gradient_accumulation_steps=accum, learning_rate=2e-4,
                     lr_scheduler_type="linear", bf16=True, tf32=True, logging_steps=10, eval_strategy="steps",
                     eval_steps=500, save_strategy="steps", save_steps=500, save_total_limit=2,
                     load_best_model_at_end=True, metric_for_best_model="eval_loss", greater_is_better=False,
                     max_seq_length=4096, packing=False, report_to="none", seed=a.seed, data_seed=a.seed, max_steps=a.max_steps,
                     gradient_checkpointing=True, gradient_checkpointing_kwargs={"use_reentrant": False})
    trainer = SFTTrainer(model=model, train_dataset=train, eval_dataset=val, peft_config=peft_config,
                         processing_class=tok, args=args)
    import glob
    ckpts = sorted(glob.glob(os.path.join(a.out, "checkpoint-*")), key=lambda x: int(x.rsplit("-", 1)[-1]))
    trainer.train(resume_from_checkpoint=ckpts[-1] if ckpts else None)  # resume after a time-limit kill
    trainer.save_model(os.path.join(a.out, "best"))
    state = trainer.state
    json.dump({"args": vars(a), "effective_batch": a.per_device_batch * world * accum, "train_samples": len(train),
               "train_sources": source_counts,
               "best_checkpoint": state.best_model_checkpoint, "best_eval_loss": state.best_metric,
               "log_history": state.log_history}, open(os.path.join(a.out, "run.json"), "w"), indent=1)


if __name__ == "__main__":
    main()
