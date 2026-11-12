"""Diagnostics of the raw generations behind the truncation and repetition analysis (Sections 5.2 and 5.6).

For every answer: whether generation hit the token limit, whether the truncated text ends in a repetition loop,
whether the answer contains fenced code blocks at all and whether exactly one, and its length in tokens. A loop is
detected on the last 2,000 characters by their zlib compression ratio: repeated text compresses to below 0.15 of its
size, whereas configuration text that is merely long stays around 0.3-0.5 (the two modes are well separated on every
model; see the printed quantiles).
Usage: python generation_diag.py runs/main/*.greedy.jsonl runs/main/*.sample.jsonl > results/generation_diag.json
"""
import collections
import json
import re
import statistics
import sys
import zlib

LOOP_RATIO = 0.15


def ratio(text):
    t = text[-2000:].encode()
    return len(zlib.compress(t, 9)) / max(1, len(t))


def main(paths):
    acc = collections.defaultdict(lambda: collections.Counter())
    ratios = collections.defaultdict(lambda: {"length": [], "stop": []})
    for p in paths:
        for line in open(p):
            r = json.loads(line)
            m = re.match(r"^(.*)-s(\d+)(-rp)?$", r["model"])
            model = m.group(1) + (m.group(3) or "") if m else r["model"]
            seed = f"s{m.group(2)}" if m else None
            z = ratio(r["text"])
            trunc = r["finish_reason"] == "length"
            for key in [(model, r["mode"])] + ([(f"{model}@{seed}", r["mode"])] if seed else []):
                c = acc[key]
                c["n"] += 1
                c["truncated"] += trunc
                c["loop"] += trunc and z < LOOP_RATIO
                c["fenced"] += "```" in r["text"]
                c["one_block"] += r["text"].count("```") in (1, 2)  # a single fenced block (possibly unclosed)
                c["tokens"] += r["n_output_tokens"]
            if len(r["text"]) >= 2000:
                ratios[model]["length" if trunc else "stop"].append(z)
    out = {}
    for (model, mode), c in sorted(acc.items()):
        out[f"{model}|{mode}"] = {"n": c["n"], "truncated": c["truncated"] / c["n"], "loop": c["loop"] / c["n"],
                                  "loop_of_truncated": c["loop"] / c["truncated"] if c["truncated"] else None,
                                  "fenced": c["fenced"] / c["n"], "one_block": c["one_block"] / c["n"],
                                  "mean_tokens": c["tokens"] / c["n"]}
    for model, d in sorted(ratios.items()):
        q = {k: [round(x, 3) for x in statistics.quantiles(v, n=10)] if len(v) > 2 else v for k, v in d.items()}
        print(f"{model:24s} length-deciles={q['length'][:1] + q['length'][-1:]} stop-deciles={q['stop'][:1]}",
              file=sys.stderr)
    json.dump({"loop_ratio_threshold": LOOP_RATIO, "by_model": out}, sys.stdout, indent=1)


if __name__ == "__main__":
    main(sys.argv[1:])
