"""
E7 load generator: one process of a group that together offers a constant rate.

The local generator pre-serialises every record once:
pre-serialised once (a JSON prefix produced from the source table's columns),
so sending costs one integer format and one produce() call, and several
processes share the target rate from one shared start time t0.

Each record ends with  "__sent_ms":<wall clock ms just before produce()>,
"__seq":<n * proc_count + proc_index>}. The producer is idempotent with
acks=all; delivery reports are requested for failures only, and every failed
sequence number is written to the result file so that loss is computed over
records the source topic actually acknowledged.
"""
from __future__ import annotations
import argparse, json, os, signal, sys, threading, time
from collections import Counter

from confluent_kafka import Producer

_stop = threading.Event()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--templates", required=True, help="JSON lines {k: key text|null, v: value prefix}")
    ap.add_argument("--rate", type=float, required=True, help="total rate of the group (records/s)")
    ap.add_argument("--duration", type=float, required=True)
    ap.add_argument("--t0", type=float, required=True)
    ap.add_argument("--proc-index", type=int, default=0)
    ap.add_argument("--proc-count", type=int, default=1)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    tpl = []
    with open(a.templates) as f:
        for line in f:
            d = json.loads(line)
            tpl.append((None if d["k"] is None else str(d["k"]).encode(), d["v"].encode()))
    n_tpl = len(tpl)

    failed: list[int] = []
    errs: Counter = Counter()

    def on_delivery(err, msg):
        if err is None:
            return
        v = msg.value() or b""
        i = v.rfind(b'"__seq":')
        try:
            failed.append(int(v[i + 8:v.index(b"}", i)]))
        except ValueError:
            failed.append(-1)
        errs[err.name()] += 1

    producer = Producer({
        "bootstrap.servers": a.bootstrap,
        "acks": "all", "enable.idempotence": True,
        "delivery.report.only.error": True, "on_delivery": on_delivery,
        "queue.buffering.max.messages": 1_000_000, "queue.buffering.max.kbytes": 2_097_152,
    })
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: _stop.set())

    while time.time() < a.t0 and not _stop.is_set():
        time.sleep(0.005)

    rate = a.rate / a.proc_count
    P, k = a.proc_count, a.proc_index
    n = 0                     # records handed to the producer by this process
    buffer_full = 0
    per_sec: Counter = Counter()
    credit, last = 0.0, time.time()
    produce, topic = producer.produce, a.topic
    t_first = None
    q_sec, qlen = 0, {}
    i = (k * n_tpl) // P          # processes start at different templates
    while not _stop.is_set():
        now = time.time()
        if now - a.t0 >= a.duration:
            break
        credit = min(credit + (now - last) * rate, max(1.0, rate * 0.05))
        last = now
        burst = int(credit)
        if burst < 1:
            time.sleep(0.0005)
            continue
        credit -= burst
        for _ in range(burst):
            kb, pre = tpl[i]
            ms = int(time.time() * 1000)
            payload = pre + b'"__sent_ms":%d,"__seq":%d}' % (ms, n * P + k)
            while True:
                try:
                    produce(topic, value=payload, key=kb)
                    break
                except BufferError:
                    buffer_full += 1
                    producer.poll(0.005)
            if t_first is None:
                t_first = ms
            per_sec[ms // 1000] += 1
            n += 1
            i += 1
            if i == n_tpl:
                i = 0
        producer.poll(0)
        sec = int(now)
        if sec != q_sec:                     # once per second: records waiting inside the client
            q_sec = sec
            qlen[str(sec)] = len(producer)
    t_last = time.time()
    left = producer.flush(120)
    res = {"proc_index": k, "proc_count": P, "sent": n, "failed": failed, "error_counts": dict(errs),
           "unflushed": left, "buffer_full_retries": buffer_full, "t_first_ms": t_first,
           "t_stop": t_last, "rate_target": rate, "per_sec": {str(s): c for s, c in sorted(per_sec.items())},
           "client_queue_per_sec": qlen, "cpu_s": round(sum(os.times()[:2]), 2)}
    with open(a.out, "w") as f:
        json.dump(res, f)
    print(f"gen {k}/{P}: sent={n} failed={len(failed)} unflushed={left}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
