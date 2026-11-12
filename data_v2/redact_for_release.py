"""Redacts credentials found in public manifests before redistributing training data.

The Stack contains files with real credentials (e.g. AWS access keys, PEM private keys). Training used the data as
collected; the released copies replace every match below with a typed placeholder, and the number of replacements is
reported. Usage: python redact_for_release.py <in.jsonl> <out.jsonl>
"""
import collections
import json
import re
import sys

PATTERNS = [
    ("PEM_PRIVATE_KEY", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.S)),
    ("AWS_ACCESS_KEY_ID", re.compile(r"\b(AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("AWS_SECRET_ACCESS_KEY", re.compile(r"(?i)(aws_secret_access_key|secretAccessKey|aws-secret-key)(\W{1,4})([A-Za-z0-9/+=]{40})")),
    ("HF_TOKEN", re.compile(r"\bhf_[A-Za-z0-9]{30,}\b")),
    ("GITHUB_TOKEN", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{36,}\b")),
    ("SLACK_TOKEN", re.compile(r"\bxox[abpors]-[A-Za-z0-9-]{10,}\b")),
    ("GOOGLE_API_KEY", re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b")),
]


def redact(text, counts):
    for name, rx in PATTERNS:
        def sub(m, name=name):
            counts[name] += 1
            if name == "AWS_SECRET_ACCESS_KEY":
                return f"{m.group(1)}{m.group(2)}<REDACTED:{name}>"
            return f"<REDACTED:{name}>"
        text = rx.sub(sub, text)
    return text


def walk(obj, counts):
    if isinstance(obj, str):
        return redact(obj, counts)
    if isinstance(obj, list):
        return [walk(x, counts) for x in obj]
    if isinstance(obj, dict):
        return {k: walk(v, counts) for k, v in obj.items()}
    return obj


if __name__ == "__main__":
    counts = collections.Counter()
    with open(sys.argv[2], "w") as out:
        for line in open(sys.argv[1]):
            out.write(json.dumps(walk(json.loads(line), counts)) + "\n")
    print(sys.argv[1], dict(counts))
