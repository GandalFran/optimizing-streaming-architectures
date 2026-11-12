"""Retrieval-augmented baseline: index official documentation, retrieve top-k chunks per scenario.

Corpus (versioned snapshot, downloaded 2026-11-10): Kafka 4.1.2 generated configuration docs (topic, broker,
producer, consumer), `kafka-topics.sh --help` of 4.1.2, Flink 1.20 documentation pages (configuration, table
configuration, Kafka and Upsert Kafka SQL connectors, JSON format, checkpointing, checkpoints, state backends,
TaskManager memory, Kafka DataStream connector), Flink Kubernetes Operator 1.13 configuration and CRD reference, and
field descriptions of the Strimzi 0.48 KafkaTopic CRD. Dense retrieval with BAAI/bge-m3 (cosine similarity).
"""
from __future__ import annotations

import argparse
import html
import json
import pathlib
import re

import numpy as np
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _txt(s):
    s = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", s, flags=re.S)
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", s))).strip()


def chunks():
    out = []
    k = json.load(open(ROOT / "catalog" / "kafka-4.1.2-configs.json"))
    for scope, name in (("topic_config", "topic-level"), ("kafka_config", "broker"), ("producer_config", "producer"),
                        ("consumer_config", "consumer")):
        for key, r in k[scope].items():
            out.append(("kafka-docs", f"Kafka 4.1 {name} configuration `{key}`: {r.get('description', '')} "
                        f"Type: {r.get('type')}. Default: {r.get('default')}. Valid values: {r.get('valid_values')}."))
    cli = (ROOT / "catalog" / "cli-kafka-topics-4.1.2.txt").read_text()
    for blk in re.split(r"\n(?=--)", cli):
        if blk.strip().startswith("--"):
            out.append(("kafka-topics.sh 4.1 --help", "kafka-topics.sh option " + re.sub(r"\s+", " ", blk.strip())))
    raw = ROOT / "rag" / "raw"
    for f in sorted(raw.glob("*.html")):
        t = f.read_text()
        body = re.search(r"<main.*?</main>|<article.*?</article>", t, re.S)
        t = body.group(0) if body else t
        for row in re.findall(r"<tr>(.*?)</tr>", t, re.S):  # option tables
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.S)
            if len(cells) >= 3 and re.search(r"<h5>", cells[0]):
                cells = [_txt(c) for c in cells]
                out.append((f.stem, f"Option `{cells[0]}` (default: {cells[1]}; type: {cells[2] if len(cells) > 3 else ''}): {cells[-1]}"))
        t = re.sub(r"<table.*?</table>", " ", t, flags=re.S)
        for sec in re.split(r"<h[23][^>]*>", t)[1:]:
            title, _, rest = sec.partition("</h")
            words = _txt(rest.split(">", 1)[-1]).split()
            for i in range(0, len(words), 220):
                piece = " ".join(words[i:i + 260])
                if len(piece) > 80:
                    out.append((f.stem, f"[{_txt(title)}] {piece}"))
    for crd in yaml.safe_load_all(open(ROOT / "catalog" / "crds" / "strimzi-crds-0.48.0.yaml")):
        if crd and crd.get("spec", {}).get("names", {}).get("kind") == "KafkaTopic":
            v = next(x for x in crd["spec"]["versions"] if x["name"] == "v1beta2")
            spec = v["schema"]["openAPIV3Schema"]["properties"]["spec"]
            for fld, sch in spec["properties"].items():
                out.append(("strimzi-crd", f"Strimzi KafkaTopic (kafka.strimzi.io/v1beta2) spec.{fld} ({sch.get('type')}): "
                            f"{sch.get('description', '')}"))
            out.append(("strimzi-crd", "A Strimzi KafkaTopic must carry the label strimzi.io/cluster set to the name of "
                        "the Kafka cluster; metadata.name is the topic name unless spec.topicName is set."))
    return out


def query_text(prompt: str) -> str:
    """Pipeline and requirement parts only (the environment and format boilerplate is identical across scenarios)."""
    p = prompt.split("Environment:")[0]
    return p.replace("Design the Apache Kafka topics and the Apache Flink configuration for the following streaming pipeline.", "").strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bench", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--k", type=int, default=8)
    a = ap.parse_args()
    import torch
    from huggingface_hub import snapshot_download
    from transformers import AutoModel, AutoTokenizer
    path = snapshot_download("BAAI/bge-m3", local_files_only=True)  # local snapshot: no Hub calls on the compute node
    tok = AutoTokenizer.from_pretrained(path)
    enc = AutoModel.from_pretrained(path, torch_dtype=torch.float16).cuda().eval()

    @torch.no_grad()
    def embed(texts, bs=64):  # bge-m3 dense embedding = L2-normalised [CLS] of the last hidden state
        out = []
        for i in range(0, len(texts), bs):
            b = tok(texts[i:i + bs], padding=True, truncation=True, max_length=512, return_tensors="pt").to("cuda")
            h = enc(**b).last_hidden_state[:, 0]
            out.append(torch.nn.functional.normalize(h.float(), dim=-1).cpu().numpy())
        return np.concatenate(out)

    docs = chunks()
    E = embed([d[1] for d in docs])
    scen = [json.loads(l) for l in open(a.bench)]
    Q = embed([query_text(s["prompt"]) for s in scen])
    sims = Q @ E.T
    with open(a.out, "w") as f:
        for s, row in zip(scen, sims):
            top = np.argsort(-row)[:a.k]
            ctx = ("Reference documentation retrieved from the official Apache Kafka 4.1, Apache Flink 1.20, Flink "
                   "Kubernetes Operator 1.13 and Strimzi 0.48 documentation (use it where relevant):\n\n" +
                   "\n\n".join(f"[{i + 1}] ({docs[j][0]}) {docs[j][1][:1200]}" for i, j in enumerate(top)))
            f.write(json.dumps({"id": s["id"], "context": ctx, "hits": [docs[j][0] for j in top],
                                "scores": [float(row[j]) for j in top]}) + "\n")
    json.dump({"n_chunks": len(docs), "k": a.k, "model": "BAAI/bge-m3"}, open(a.out + ".meta.json", "w"))
    print(len(docs), "chunks indexed")


if __name__ == "__main__":
    main()
