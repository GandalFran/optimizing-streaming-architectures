"""
Measurement-chain calibration for E7 (no Flink): generator -> topic -> read_committed reader.

Same node layout, brokers (default settings), generator and reader as deploy_check.py,
but the reader consumes the generator's own topic. The latency measured here is the
floor the harness itself adds (produce path + broker replication + consumer fetch +
reader polling); runtime latencies in E7 results include it.

    deploy_check-style:  python e7_harness_check.py --out runs/e7/harness_check.json [--rate 50000 --bytes 1024]
"""
from __future__ import annotations
import argparse, json, math, os, signal, subprocess, sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
import deploy_check as D  # noqa: E402  (sets the local vendor import path)
from ports import acquire_port_block  # noqa: E402
from sysmon import SysMon  # noqa: E402
from topology import allocate  # noqa: E402
import numpy as np  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--rate", type=float, default=50000)
    ap.add_argument("--bytes", type=int, default=1024)
    ap.add_argument("--duration", type=float, default=60)
    ap.add_argument("--warmup", type=float, default=10)
    ap.add_argument("--run-root", default=f"/tmp/{os.environ.get('USER', 'e7')}-e7")
    ap.add_argument("--kafka-cores", type=int, default=2)
    a = ap.parse_args()
    out = Path(a.out)
    art = out.with_suffix("")
    art.mkdir(parents=True, exist_ok=True)
    base, sentinel = acquire_port_block(0)
    inst = f"e7h-{os.getpid()}"
    topo = allocate(inst, port_base=base, root=a.run_root,
                    roles=[("kafka", 3), ("flink", 3), ("jm", 1), ("control", 1), ("meas", 1)],
                    cores={"kafka": a.kafka_cores, "flink": 4, "jm": 1, "control": 2, "meas": 1})
    run_dir = Path(a.run_root) / inst
    mon = SysMon(art / "sysmon.jsonl", interval=2.0)
    mon.start()
    k = D.DefaultKafka(nodes=topo.role("kafka"), broker_ports=[base + i for i in range(3)],
                       controller_ports=[base + 10 + i for i in range(3)], run_dir=run_dir, config={})
    procs = []
    R = {"rate": a.rate, "bytes": a.bytes, "duration": a.duration}
    try:
        k.start()
        for i, pid in enumerate(k.pids().values()):
            mon.track(f"kafka{i}", pid, "kafka")
        r = k._run([k._bin("kafka-topics.sh"), "--bootstrap-server", k.bootstrap, "--create", "--topic", "harness",
                    "--partitions", "12", "--replication-factor", "3", "--config", "min.insync.replicas=2"])
        R["create_rc"] = r.returncode
        pad = "x" * max(0, a.bytes - len('{"id":"k00000000","p":"",') - len('"__sent_ms":1731234567890,"__seq":12345678}'))
        tp = art / "templates.jsonl"
        with open(tp, "w") as f:
            for i in range(1024):
                f.write(json.dumps({"k": None, "v": '{"id":"k%08d","p":"%s",' % (i, pad)}) + "\n")

        def spawn(key, argv, node=None):
            p = subprocess.Popen((node or topo.control).wrap(argv), stdout=open(art / f"{key}.log", "wb"),
                                 stderr=subprocess.STDOUT, env=k.env(), start_new_session=True)
            mon.track(key, p.pid, "harness")
            procs.append(p)
            return p
        reader = spawn("reader", [D.PY, str(HERE / "e7_sink_reader.py"), "--bootstrap", k.bootstrap, "--topic",
                                  "harness", "--group", "h", "--out", str(art / "records.npz"),
                                  "--progress", str(art / "progress.json")], node=topo.role("meas")[0])
        time.sleep(5)
        n = max(1, math.ceil(a.rate / 30000))
        t0 = time.time() + 4
        gens = [spawn(f"gen{i}", [D.PY, str(HERE / "e7_gen.py"), "--bootstrap", k.bootstrap, "--topic", "harness",
                                  "--templates", str(tp), "--rate", str(a.rate), "--duration", str(a.duration),
                                  "--t0", f"{t0:.3f}", "--proc-index", str(i), "--proc-count", str(n),
                                  "--out", str(art / f"gen{i}.json")]) for i in range(n)]
        for g in gens:
            g.wait(timeout=a.duration + 200)
        time.sleep(5)
        os.killpg(os.getpgid(reader.pid), signal.SIGTERM)
        reader.wait(timeout=90)
        z = np.load(art / "records.npz")
        lat = z["recv_ms"] - z["sent_ms"]
        m = z["sent_ms"] >= (t0 + a.warmup) * 1000
        p = np.percentile(lat[m], [50, 95, 99, 99.9])
        gres = [json.loads((art / f"gen{i}.json").read_text()) for i in range(n)]
        sent = sum(g["sent"] for g in gres)
        R.update(latency_ms={"p50": round(float(p[0]), 1), "p95": round(float(p[1]), 1), "p99": round(float(p[2]), 1),
                             "p999": round(float(p[3]), 1), "n": int(m.sum())},
                 sent=sent, received=int(len(lat)), offered_eps=round(sent / a.duration, 1))
        acc = {}
        for ln in open(art / "sysmon.jsonl"):
            row = json.loads(ln)
            if t0 + a.warmup <= row.get("t", 0) <= t0 + a.duration:
                for name, v in (row.get("c") or {}).items():
                    if v.get("cores") is not None:
                        acc.setdefault(name, []).append(v["cores"])
        R["cpu_cores"] = {kk: round(sum(v) / len(v), 2) for kk, v in sorted(acc.items())}
    finally:
        for p in procs:
            if p.poll() is None:
                try:
                    os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
        k.stop()
        mon.stop()
        import shutil
        shutil.rmtree(run_dir, ignore_errors=True)
        sentinel.close()
        out.write_text(json.dumps(R, indent=2))
        print(json.dumps(R), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
