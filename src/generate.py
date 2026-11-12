"""Generates answers for KFConfBench with vLLM under one identical protocol for every model.

Same system prompt, user prompt and output-format instruction; each model's own chat template; no repair, no
filtering, no re-sampling. Greedy decoding (temperature 0) is the primary setting; sampled runs (T=0.7, top-p 0.95)
with fixed seeds measure variance. Optional retrieval-augmented context (--rag) is prepended to the user prompt.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time

from vllm import LLM, SamplingParams
from vllm.lora.request import LoRARequest

sys.path.insert(0, os.path.dirname(__file__))


def build_messages(tok, system, user):
    msgs = [{"role": "system", "content": system}, {"role": "user", "content": user}]
    try:
        return tok.apply_chat_template(msgs, tokenize=False, add_generation_prompt=True), "system+user"
    except Exception:  # templates without a system role (e.g. Mistral-7B-Instruct-v0.3): fold it into the user turn
        msgs = [{"role": "user", "content": f"{system}\n\n{user}"}]
        return tok.apply_chat_template(msgs, tokenize=False, add_generation_prompt=True), "folded-system"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True)
    ap.add_argument("--name", required=True)
    ap.add_argument("--lora")
    ap.add_argument("--bench", required=True)
    ap.add_argument("--ids", help="file with one scenario id per line (subset)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--mode", choices=["greedy", "sample"], default="greedy")
    ap.add_argument("--seeds", default="1,2,3,4,5")
    ap.add_argument("--rag", help="JSONL with retrieved context per scenario id (from rag.py)")
    ap.add_argument("--max-new-tokens", type=int, default=2048)
    ap.add_argument("--max-model-len", type=int, default=8192)
    ap.add_argument("--latency-sample", type=int, default=0, help="also time N requests one at a time")
    ap.add_argument("--gpu-mem", type=float, default=0.85)
    ap.add_argument("--tp", type=int, default=1, help="tensor-parallel GPUs (large models)")
    ap.add_argument("--repetition-penalty", type=float, default=1.0,
                    help="secondary analysis only (post hoc): identical for every model; 1.0 = disabled")
    args = ap.parse_args()

    scen = [json.loads(l) for l in open(args.bench)]
    if args.ids:
        keep = {l.strip() for l in open(args.ids) if l.strip()}
        scen = [s for s in scen if s["id"] in keep]
    rag = {}
    if args.rag:
        rag = {r["id"]: r["context"] for r in map(json.loads, open(args.rag))}

    # tokenizer_mode="hf" for every model: with vLLM 0.13 the default for Mistral repos is mistral_common, whose
    # apply_chat_template(tokenize=False) renders newlines as the literal text "<0x0A>" (verified), corrupting prompts.
    kw = dict(model=args.model, dtype="bfloat16", max_model_len=args.max_model_len, gpu_memory_utilization=args.gpu_mem,
              enforce_eager=False, trust_remote_code=True, seed=0, tokenizer_mode="hf",
              tensor_parallel_size=args.tp)
    if args.lora:
        kw.update(enable_lora=True, max_lora_rank=64, max_loras=1)
    llm = LLM(**kw)
    tok = llm.get_tokenizer()
    lora = LoRARequest(args.name, 1, args.lora) if args.lora else None

    prompts, meta = [], []
    for s in scen:
        user = s["prompt"]
        if s["id"] in rag:
            user = f"{rag[s['id']]}\n\n{user}"
        text, template = build_messages(tok, s["system"], user)
        assert "<0x0A>" not in text, "prompt rendered with literal byte tokens"
        prompts.append(text)
        meta.append((s["id"], template, len(tok(text).input_ids)))

    if args.mode == "greedy":
        runs = [(0, SamplingParams(temperature=0.0, max_tokens=args.max_new_tokens, seed=0,
                                   repetition_penalty=args.repetition_penalty))]
    else:
        runs = [(int(sd), SamplingParams(temperature=0.7, top_p=0.95, max_tokens=args.max_new_tokens, seed=int(sd),
                                         repetition_penalty=args.repetition_penalty))
                for sd in args.seeds.split(",")]

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as f:
        for seed, sp in runs:
            t0 = time.time()
            outs = llm.generate(prompts, sp, lora_request=lora, use_tqdm=False)
            wall = time.time() - t0
            for (sid, template, ntok), o in zip(meta, outs):
                c = o.outputs[0]
                f.write(json.dumps({"id": sid, "model": args.name, "mode": args.mode, "seed": seed,
                                    "temperature": sp.temperature, "top_p": sp.top_p, "text": c.text,
                                    "finish_reason": c.finish_reason, "n_prompt_tokens": ntok,
                                    "n_output_tokens": len(c.token_ids), "template": template,
                                    "rag": sid in rag, "repetition_penalty": args.repetition_penalty}) + "\n")
            print(f"[{args.name}] seed={seed} {len(prompts)} prompts in {wall:.1f}s", flush=True)

    if args.latency_sample:
        sp = SamplingParams(temperature=0.0, max_tokens=args.max_new_tokens, seed=0)
        lat, tps = [], []
        llm.generate(prompts[:1], sp, lora_request=lora, use_tqdm=False)  # warm-up
        for p in prompts[:args.latency_sample]:
            t0 = time.perf_counter()
            o = llm.generate([p], sp, lora_request=lora, use_tqdm=False)[0].outputs[0]
            dt = time.perf_counter() - t0
            lat.append(dt)
            tps.append(len(o.token_ids) / dt)
        lat.sort()
        perf = {"model": args.name, "n": len(lat), "latency_s_median": statistics.median(lat),
                "latency_s_p95": lat[min(len(lat) - 1, int(round(0.95 * (len(lat) - 1))))],
                "tokens_per_s_median": statistics.median(tps), "batch_size": 1, "lora": bool(lora)}
        with open(args.out.replace(".jsonl", ".perf.json"), "w") as f:
            json.dump(perf, f, indent=1)
        print(perf, flush=True)


if __name__ == "__main__":
    main()
