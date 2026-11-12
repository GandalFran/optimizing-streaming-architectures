"""
E7 sink reader: a read_committed consumer on the sink topic.

"Visible to downstream consumers" is defined as: returned to a consumer with
isolation.level=read_committed. For every record the reader keeps __seq,
__sent_ms and its own receive wall clock (taken when the poll batch returns;
batches return after at most 2 ms). Latency = receive - __sent_ms; generator,
pipeline and reader share one host clock.

The reader subscribes before the job starts and never creates the topic
(allow.auto.create.topics=false), so a sink topic the answer did not create
is not created by the measurement.
"""
from __future__ import annotations
import argparse, json, os, signal, sys, threading, time
from array import array

import numpy as np
from confluent_kafka import Consumer

_stop = threading.Event()
SEQ, SENT = b'"__seq":', b'"__sent_ms":'


def _num(v: bytes, key: bytes) -> int | None:
    i = v.find(key)
    if i < 0:
        return None
    i += len(key)
    j = i
    L = len(v)
    while j < L and v[j] not in (44, 125, 32):      # ',' '}' ' '
        j += 1
    try:
        return int(v[i:j])
    except ValueError:
        try:
            return int(float(v[i:j]))
        except ValueError:
            return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--group", required=True)
    ap.add_argument("--out", required=True, help="npz with seq, sent_ms, recv_ms")
    ap.add_argument("--progress", required=True)
    a = ap.parse_args()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: _stop.set())

    assigned = {"n": 0, "t": None}

    def on_assign(c, parts):
        assigned["n"] = len(parts)
        assigned["t"] = time.time()

    c = Consumer({
        "bootstrap.servers": a.bootstrap, "group.id": a.group,
        "isolation.level": "read_committed", "auto.offset.reset": "earliest",
        "enable.auto.commit": False, "allow.auto.create.topics": False,
        "fetch.wait.max.ms": 5, "topic.metadata.refresh.interval.ms": 2000,
    })
    c.subscribe([a.topic], on_assign=on_assign)
    seqs, sents, recvs = array("q"), array("q"), array("d")
    seen = bytearray(1 << 22)
    distinct = unparsed = tombstones = errors = 0
    last_err = None
    t_prog = 0.0
    while not _stop.is_set():
        msgs = c.consume(num_messages=5000, timeout=0.002)
        now = time.time() * 1000.0
        for m in msgs:
            if m.error():
                errors += 1
                last_err = str(m.error())
                continue
            v = m.value()
            if v is None:
                tombstones += 1
                continue
            s, t = _num(v, SEQ), _num(v, SENT)
            if s is None or t is None or s < 0 or s > (1 << 31):
                unparsed += 1
                continue
            seqs.append(s)
            sents.append(t)
            recvs.append(now)
            if s >= len(seen):
                seen.extend(bytearray(max(s + 1 - len(seen), len(seen))))
            if seen[s] == 0:
                distinct += 1
            if seen[s] < 255:
                seen[s] += 1
        if now - t_prog >= 1000:
            t_prog = now
            with open(a.progress + ".tmp", "w") as f:
                json.dump({"t": now / 1000, "n": len(seqs), "distinct": distinct, "assigned": assigned["n"],
                           "t_assigned": assigned["t"], "unparsed": unparsed, "tombstones": tombstones,
                           "errors": errors, "last_error": last_err}, f)
            os.replace(a.progress + ".tmp", a.progress)
    c.close()
    np.savez(a.out, seq=np.frombuffer(seqs, dtype=np.int64), sent_ms=np.frombuffer(sents, dtype=np.int64),
             recv_ms=np.frombuffer(recvs, dtype=np.float64))
    with open(a.progress, "w") as f:
        json.dump({"t": time.time(), "n": len(seqs), "distinct": distinct, "assigned": assigned["n"],
                   "t_assigned": assigned["t"], "unparsed": unparsed, "tombstones": tombstones,
                   "errors": errors, "last_error": last_err, "final": True}, f)
    return 0


if __name__ == "__main__":
    sys.exit(main())
