"""E6: deterministic validation layer between generation and deployment.

Generate -> validate -> (deterministic repair) -> validate -> (regenerate with violation feedback, at most K rounds)
-> accept or reject. Round 0 reuses the greedy answers already produced by generate.py, so the layer is evaluated on
exactly the answers reported in the main comparison.

Repair is restricted to semantics-preserving rewrites: (1) duplicate YAML keys are removed keeping the last
occurrence (what common YAML loaders already do); (2) deprecated Flink keys are renamed to their Flink 1.20
replacement (one-to-one fallback keys declared by Flink itself). Nothing else is changed by the layer.

Feedback modes: 'syntax' feeds back only L1-L3 findings (no requirement specification needed); 'full' also feeds back
the failed correctness/resource expectations of the scenario (requires a machine-readable requirement spec).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from kfbench.catalog import get_catalog  # noqa: E402
from kfbench.parse import code_blocks, load_yaml_docs  # noqa: E402
from kfbench.validate import validate  # noqa: E402

import yaml  # noqa: E402


def repair(text: str):
    """Returns (new_text, list_of_actions). Only semantics-preserving rewrites."""
    cat = get_catalog()
    actions, out, pos = [], [], 0
    for m in re.finditer(r"```[ \t]*([A-Za-z0-9_+-]*)[^\n]*\n(.*?)```", text, re.S):
        body = m.group(2)
        new = body
        if m.group(1).lower() in ("yaml", "yml"):
            docs, dups, err = load_yaml_docs(body)
            if not err and dups:
                new = "\n---\n".join(yaml.safe_dump(d, sort_keys=False) for d in docs)
                actions.append(f"removed duplicate YAML keys {sorted(set(dups))[:5]}")
        for old, cur in cat.flink_deprecated.items():
            rx = re.compile(rf"(^|\s|['\"]){re.escape(old)}(\s*[:=]|['\"])", re.M)
            if rx.search(new):
                new = rx.sub(lambda mm: f"{mm.group(1)}{cur}{mm.group(2)}", new)
                actions.append(f"renamed deprecated {old} -> {cur}")
        out.append(text[pos:m.start(2)] + new)
        pos = m.end(2)
    out.append(text[pos:])
    return "".join(out), actions


def feedback(v: dict, mode: str) -> str:
    lines = [f"- [{f['level']}] {f['artefact']}: {f['message']}" for f in v["findings"] if f["level"] in ("L1", "L2", "L3")]
    if mode == "full":
        lines += [f"- [requirement] {k}: {e['msg']}" for k, e in v["expectations"].items()
                  if not e["ok"] and e["kind"] in ("correctness", "resource")]
    lines = lines[:40]
    return ("Your answer failed automatic validation against Apache Kafka 4.1, Apache Flink 1.20, Strimzi 0.48 and the "
            "Flink Kubernetes Operator 1.13:\n" + "\n".join(lines) +
            "\n\nReturn the complete corrected answer in the same format (the same three fenced code blocks).")


def is_ok(v, mode):
    return v["L3"] if mode == "syntax" else v["valid"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True)
    ap.add_argument("--name", required=True)
    ap.add_argument("--lora")
    ap.add_argument("--bench", required=True)
    ap.add_argument("--greedy", required=True, help="round-0 answers (generate.py, greedy)")
    ap.add_argument("--rag")
    ap.add_argument("--mode", choices=["syntax", "full"], required=True)
    ap.add_argument("--rounds", type=int, default=2)
    ap.add_argument("--out", required=True)
    ap.add_argument("--max-model-len", type=int, default=16384)
    ap.add_argument("--tp", type=int, default=1)
    a = ap.parse_args()

    from vllm import LLM, SamplingParams
    from vllm.lora.request import LoRARequest
    from generate import build_messages

    bench = {json.loads(l)["id"]: json.loads(l) for l in open(a.bench)}
    rag = {r["id"]: r["context"] for r in map(json.loads, open(a.rag))} if a.rag else {}
    r0 = {r["id"]: r for r in map(json.loads, open(a.greedy))}
    state = {}
    for sid, r in r0.items():
        t0 = time.perf_counter()
        v = validate(r["text"], bench[sid], r["finish_reason"])
        state[sid] = {"id": sid, "model": a.name, "mode": a.mode, "history": [{"round": 0, "valid": v["valid"], "L3": v["L3"],
                      "req_sat": req_sat(v), "validate_ms": 1000 * (time.perf_counter() - t0)}],
                      "text": r["text"], "finish": r["finish_reason"], "v": v, "status": None, "user": None}
        s = state[sid]
        if is_ok(v, a.mode):
            s["status"] = "accepted-unchanged"
            continue
        t0 = time.perf_counter()
        fixed, acts = repair(r["text"])
        if acts:
            v2 = validate(fixed, bench[sid], r["finish_reason"])
            s["history"].append({"round": "repair", "actions": acts, "valid": v2["valid"], "L3": v2["L3"],
                                 "req_sat": req_sat(v2), "validate_ms": 1000 * (time.perf_counter() - t0)})
            if is_ok(v2, a.mode):
                s.update(status="accepted-repaired", text=fixed, v=v2)
                continue
            s.update(text=fixed, v=v2)

    kw = dict(model=a.model, dtype="bfloat16", max_model_len=a.max_model_len, gpu_memory_utilization=0.85,
              trust_remote_code=True, seed=0, tokenizer_mode="hf",  # see generate.py
              tensor_parallel_size=a.tp)
    if a.lora:
        kw.update(enable_lora=True, max_lora_rank=64, max_loras=1)
    llm = LLM(**kw)
    tok = llm.get_tokenizer()
    lora = LoRARequest(a.name, 1, a.lora) if a.lora else None
    sp = SamplingParams(temperature=0.0, max_tokens=2048, seed=0)
    for rnd in range(1, a.rounds + 1):
        todo = [s for s in state.values() if s["status"] is None]
        if not todo:
            break
        prompts = []
        for s in todo:
            sc = bench[s["id"]]
            user = (f"{rag[s['id']]}\n\n" if s["id"] in rag else "") + sc["prompt"]
            msgs = [{"role": "system", "content": sc["system"]}, {"role": "user", "content": user},
                    {"role": "assistant", "content": s["text"]}, {"role": "user", "content": feedback(s["v"], a.mode)}]
            try:
                p = tok.apply_chat_template(msgs, tokenize=False, add_generation_prompt=True)
            except Exception:
                msgs = [{"role": "user", "content": sc["system"] + "\n\n" + user}] + msgs[2:]
                p = tok.apply_chat_template(msgs, tokenize=False, add_generation_prompt=True)
            assert "<0x0A>" not in p, "prompt rendered with literal byte tokens"
            ids = tok(p).input_ids
            if len(ids) > a.max_model_len - 2048:  # keep the request inside the context window
                p = tok.decode(ids[-(a.max_model_len - 2048):])
            prompts.append(p)
        t0 = time.perf_counter()
        outs = llm.generate(prompts, sp, lora_request=lora, use_tqdm=False)
        gen_s = (time.perf_counter() - t0) / max(1, len(todo))
        for s, o in zip(todo, outs):
            c = o.outputs[0]
            t1 = time.perf_counter()
            v = validate(c.text, bench[s["id"]], c.finish_reason)
            s["history"].append({"round": rnd, "valid": v["valid"], "L3": v["L3"], "req_sat": req_sat(v),
                                 "gen_s_amortised": gen_s, "validate_ms": 1000 * (time.perf_counter() - t1),
                                 "finish": c.finish_reason})
            s.update(text=c.text, v=v, finish=c.finish_reason)
            if is_ok(v, a.mode):
                s["status"] = f"accepted-regenerated-{rnd}"
    for s in state.values():
        if s["status"] is None:
            s["status"] = "rejected"
    with open(a.out, "w") as f:
        for s in state.values():
            v = s.pop("v")
            s.pop("user", None)
            s["final_valid"], s["final_L3"], s["final_req_sat"] = v["valid"], v["L3"], req_sat(v)
            s["final_text"] = s.pop("text")
            f.write(json.dumps(s) + "\n")
    from collections import Counter
    print(a.name, a.mode, Counter(s["status"] for s in state.values()))


def req_sat(v):
    e = [x for x in v["expectations"].values() if x["kind"] in ("correctness", "resource")]
    return sum(x["ok"] for x in e) / len(e) if e else 1.0


if __name__ == "__main__":
    main()
