"""
E7 executable validation: deploy ONE generated answer on a fresh local
Kafka 4.1.2 + Flink 1.20.5 testbed and measure objective runtime criteria.

    deploy_check.py --id CASE-banking-native --answer-file a.txt --model gold --out runs/e7/x.json
    deploy_check.py --answers runs/main/qwen2.5-14b.greedy.jsonl --id ID-001 --model qwen2.5-14b --seed 0 --out ...

Runs inside a SLURM CPU allocation. It uses the repository-local testbed
primitives in ``testbed/vendor`` (topology, KafkaCluster, FlinkCluster and
SysMon) and the local port allocator. What is reproduced, every substitution
and the known limits are documented in README.md next to this file.

Honesty rules: no measurement is invented. A quantity that could not be
measured is null and `null_reasons` says why. A deployment that cannot be judged
on this testbed (missing format jar, memory beyond the testbed budget, scenario
shape not supported) gets `untestable_reason` and no criteria.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
CODE_ROOT = Path(os.environ.get("STREAMING_ARCH_CODE_ROOT", HERE.parent))
VENDOR = HERE / "vendor"
sys.path.insert(0, str(VENDOR))
sys.path.insert(0, str(HERE))

import numpy as np  # noqa: E402
import yaml  # noqa: E402

import e7_artefacts as A  # noqa: E402
from flink_cluster import FLINK, FlinkCluster, rest, wait_port  # noqa: E402
from ports import acquire_port_block  # noqa: E402
from kafka_cluster import KAFKA, KafkaCluster  # noqa: E402
from sysmon import SysMon  # noqa: E402
from topology import allocate, quality  # noqa: E402

PY = sys.executable
BENCH = CODE_ROOT / "bench" / "kfbench-v1.jsonl"
DELIVERY_FAILURE = ("exactly-once", "at-least-once")


# =============================================================================== clusters
class DefaultKafka(KafkaCluster):
    """Local KafkaCluster with Kafka's *default* broker settings.

    Only what a loopback KRaft testbed needs is set: node identity, the
    controller quorum, listeners and the log directory (the heap is set by
    KafkaCluster.start). Everything else -- partitions, replication factor,
    min.insync.replicas, transaction.max.timeout.ms, retention, topic
    auto-creation, compression -- keeps Kafka 4.1.2's default.
    """

    def _props(self, i: int) -> Path:
        node, n = self.nodes[i], len(self.nodes)
        quorum = ",".join(f"{j + 1}@127.0.0.1:{self.controller_ports[j]}" for j in range(n))
        data = Path(node.state_dir) / "kafka-data"
        data.mkdir(parents=True, exist_ok=True)
        props = "\n".join([
            "process.roles=broker,controller",
            f"node.id={i + 1}",
            f"controller.quorum.voters={quorum}",
            f"listeners=PLAINTEXT://127.0.0.1:{self.broker_ports[i]},CONTROLLER://127.0.0.1:{self.controller_ports[i]}",
            f"advertised.listeners=PLAINTEXT://127.0.0.1:{self.broker_ports[i]}",
            "controller.listener.names=CONTROLLER",
            "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
            f"log.dirs={data}",
        ]) + "\n"
        path = self.run_dir / "conf" / f"broker{i}.properties"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(props)
        return path


@dataclass
class AnswerFlink(FlinkCluster):
    """Local FlinkCluster process handling with the answer's configuration."""
    conf_flat: dict = field(default_factory=dict)

    def render(self) -> None:
        self.conf_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        (self.run_dir / "pids" / self.name).mkdir(parents=True, exist_ok=True)
        for src in (FLINK / "conf").iterdir():
            if src.is_file() and src.name != "config.yaml":
                shutil.copy(src, self.conf_dir / src.name)
        (self.conf_dir / "config.yaml").write_text(
            yaml.safe_dump(self.conf_flat, sort_keys=False, default_flow_style=False, width=1 << 20))

    def spawn_jm(self):
        return self._spawn("jobmanager", self.jm_node, [str(FLINK / "bin" / "jobmanager.sh"), "start-foreground"])

    def spawn_tm(self, key: str, node, resource_id: str):
        return self._spawn(key, node, [str(FLINK / "bin" / "taskmanager.sh"), "start-foreground",
                                       f"-Dtaskmanager.resource-id={resource_id}"])


# =============================================================================== helpers
def tail(path: Path, n: int = 4000) -> str:
    try:
        b = path.read_bytes()
        return b[-n:].decode(errors="replace")
    except OSError:
        return ""


def exc_chain(stack: str | None, limit: int = 6) -> str | None:
    """First line of a Java stack trace plus its 'Caused by' lines."""
    if not stack:
        return None
    lines = stack.strip().splitlines()
    out = [lines[0].strip()]
    for ln in lines[1:]:
        s = ln.strip()
        if s.startswith("Caused by:") and s not in out:
            out.append(s)
    if len(out) > limit:
        out = out[:2] + ["..."] + out[-(limit - 3):]
    return "\n".join(x[:400] for x in out)


def describe_topics(kafka: KafkaCluster) -> dict:
    r = kafka._run([kafka._bin("kafka-topics.sh"), "--bootstrap-server", kafka.bootstrap, "--describe",
                    "--exclude-internal"], timeout=120)
    out = {}
    for ln in r.stdout.splitlines():
        m = re.match(r"Topic:\s*(\S+)\s+TopicId:\s*\S+\s+PartitionCount:\s*(\d+)\s+ReplicationFactor:\s*(\d+)\s+Configs:\s*(.*)$",
                     ln.strip())
        if m:
            cfg = dict(kv.split("=", 1) for kv in m.group(4).split(",") if "=" in kv)
            out[m.group(1)] = {"partitions": int(m.group(2)), "replication_factor": int(m.group(3)), "configs": cfg}
    if r.returncode != 0:
        out["_error"] = r.stderr[-500:]
    return out


def sql_client_error(text: str) -> str | None:
    text = re.sub(r"\x1b\[[0-9;]*m", "", text)            # ANSI colours of the SQL client
    i = text.find("[ERROR]")
    if i < 0:
        m = re.search(r"(?m)^(\S*Exception[^\n]*)", text)
        return m.group(1)[:1500] if m else None
    chunk = text[i:i + 6000]
    lines = [ln for ln in chunk.splitlines() if ln.strip()]
    keep = [lines[0]] + [ln for ln in lines[1:] if not ln.startswith("\tat ") and not ln.startswith("        at ")]
    return "\n".join(keep[:12])[:2500]


def parse_slots(conf: dict) -> int:
    from kfbench.parse import parse_int
    return parse_int(conf.get("taskmanager.numberOfTaskSlots")) or 1


# =============================================================================== the deployment
class Deployment:
    def __init__(self, args, scenario: dict, text: str, finish_reason: str | None, model: str, seed: int):
        self.a, self.sc, self.text, self.fr = args, scenario, text, finish_reason
        spec = scenario.get("spec") or {}
        self.spec = spec
        self.out = Path(args.out)
        self.art = self.out.with_suffix("")          # artefact directory next to the result file
        self.art.mkdir(parents=True, exist_ok=True)
        self.R: dict = {
            "id": scenario["id"], "model": model, "seed": seed, "track": scenario["track"],
            "family": scenario.get("family"), "spec": spec, "finish_reason": finish_reason,
            "host": socket.gethostname(), "slurm_job": os.environ.get("SLURM_JOB_ID"),
            "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "substitutions": [], "translations": [], "augmentations": [], "topic_commands": [],
            "job_submitted": False, "job_id": None, "job_state_final": None, "root_exception": None,
            "checkpoints_completed": None, "checkpoints_failed": None,
            "offered_eps": None, "sink_eps": None, "latency_ms": {"p50": None, "p95": None, "p99": None},
            "slo_ms": spec.get("latency_slo_ms"), "lost": None, "duplicates": None,
            "untestable_reason": None,
            "criteria": {"C1_deploy": None, "C2_checkpoints": None, "C3_throughput": None, "C4_latency": None,
                         "C5_delivery": None},
            "runtime_pass": None, "null_reasons": {}, "criteria_reasons": {}, "events": [], "notes": [],
        }
        self.kafka = self.flink = self.sysmon = None
        self.sentinel = None
        self.gens: list[subprocess.Popen] = []
        self.reader: subprocess.Popen | None = None
        self.sqlc: subprocess.Popen | None = None
        self.timeline: list = []
        self.tm_rid: dict[str, str] = {}
        self.t0 = None
        self.jid = None

    # ------------------------------------------------------------------ plumbing
    def ev(self, event: str, **kw):
        e = {"t": round(time.time(), 3), "rel": None if self.t0 is None else round(time.time() - self.t0, 2),
             "event": event, **kw}
        self.R["events"].append(e)
        print(f"[e7 {time.strftime('%H:%M:%S')}] {event} {json.dumps(kw, default=str)[:300]}", flush=True)

    def write(self):
        self.R["finished"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        tmp = self.out.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.R, indent=2, default=str))
        os.replace(tmp, self.out)

    def ctl(self, argv: list[str]) -> list[str]:
        return self.topo.control_wrap(argv)

    def aux(self, key: str, argv: list[str], node=None) -> subprocess.Popen:
        log = open(self.art / f"{key}.log", "ab")
        wrapped = node.wrap(argv) if node is not None else self.ctl(argv)
        p = subprocess.Popen(wrapped, stdout=log, stderr=subprocess.STDOUT, env=self.kafka.env(),
                             start_new_session=True)
        if self.sysmon:
            self.sysmon.track(key, p.pid, "harness")
        return p

    # ------------------------------------------------------------------ main
    def run(self) -> dict:
        a, R = self.a, self.R
        plan = A.plan_answer(self.text, self.sc, self.fr)
        self.plan = plan
        R["translations"] = plan.translations
        R["notes"] += plan.notes
        R["ignored"] = {"commands": plan.ignored_commands, "sql_statements": plan.ignored_sql}
        R["static_validator"] = plan.validator
        (self.art / "answer.txt").write_text(self.text)
        if plan.untestable:
            R["untestable_reason"] = plan.untestable
            if not a.deploy_untestable:
                R["notes"].append("not deployed: untestable on this testbed")
                self.write()
                return R
        try:
            self._deploy()
        except BaseException as exc:            # includes SystemExit from SIGTERM
            R["driver_error"] = f"{type(exc).__name__}: {exc}"
            R["driver_traceback"] = traceback.format_exc()[-4000:]
            print(R["driver_traceback"], flush=True)
        finally:
            try:
                self._teardown()
            except Exception as exc:
                R["teardown_error"] = f"{type(exc).__name__}: {exc}"[:800]
            self._criteria()
            self.write()
        return R

    def _deploy(self):
        a, R, plan, spec = self.a, self.R, self.plan, self.spec
        # ---- node layout, ports
        base, self.sentinel = acquire_port_block(0)
        inst = f"e7-{os.getpid()}"
        # The measurement reader gets its own node ("meas") so that load generation cannot starve it.
        roles = [("kafka", 3), ("flink", 3), ("jm", 1), ("control", 1)] + ([("meas", 1)] if a.meas_cores else [])
        cores = {"kafka": a.kafka_cores, "flink": a.tm_cores, "jm": a.jm_cores, "control": a.ctl_cores,
                 "meas": a.meas_cores}
        self.topo = allocate(inst, port_base=base, root=a.run_root, roles=roles, cores=cores)
        self.topo.save(str(self.art / "topology.json"))
        R["topology"] = {"port_base": base, "cores_per_role": cores, "quality": quality(self.topo),
                         "nodes": {n.name: n.cpu_spec for n in self.topo.nodes}}
        # The driver keeps the job's full CPU mask (numactl refuses CPUs outside the caller's mask);
        # every process it starts is pinned explicitly to its node.
        self.run_dir = Path(a.run_root) / inst
        self.sysmon = SysMon(self.art / "sysmon.jsonl", interval=2.0)
        self.sysmon.start()

        # ---- Kafka with default broker settings
        t = time.time()
        self.kafka = DefaultKafka(nodes=self.topo.role("kafka"), broker_ports=[base + i for i in range(3)],
                                  controller_ports=[base + 10 + i for i in range(3)], run_dir=self.run_dir, config={})
        self.kafka.start()
        for i, pid in enumerate(self.kafka.pids().values()):
            self.sysmon.track(f"kafka{i}", pid, "kafka")
        R["kafka_broker_properties"] = (self.run_dir / "conf" / "broker0.properties").read_text().splitlines()
        self.ev("kafka_up", seconds=round(time.time() - t, 1), bootstrap=self.kafka.bootstrap)
        boot = self.kafka.bootstrap

        # ---- topics: the answer's commands (native) or translated KafkaTopics (k8s)
        env = self.kafka.env()
        jobs = []
        if plan.track == "native":
            for c in plan.topic_cmds:
                argv, subs = A.bind_topic_command(c, boot)
                jobs.append((c["answer_cmd"], argv, subs, c.get("error")))
        else:
            for t in plan.topic_translations:
                argv = (t["tokens"] + ["--bootstrap-server", boot]) if t["tokens"] else None
                jobs.append((t["resource"], argv, [{"kind": "bootstrap-server", "to": boot}] if argv else [],
                             t["skipped"]))
        for answer_cmd, argv, subs, err in jobs:
            e = {"answer_cmd": answer_cmd, "cmd": shlex.join(argv) if argv else None, "substitutions": subs,
                 "rc": None, "stdout_tail": None, "stderr_head": None, "stderr_tail": err}
            if argv:
                try:
                    r = subprocess.run(argv, capture_output=True, text=True, env=env, timeout=120)
                    e.update(rc=r.returncode, stdout_tail=r.stdout[-600:], stderr_head=r.stderr[:600],
                             stderr_tail=r.stderr[-1200:])
                except subprocess.TimeoutExpired:
                    e.update(rc=None, stderr_tail="timed out after 120 s")
            R["topic_commands"].append(e)
            for s in subs:
                R["substitutions"].append({"artefact": "topics", "cmd": answer_cmd[:200], **s})
        topics = describe_topics(self.kafka)
        R["topics_after_commands"] = topics
        topics_ok = spec["src"] in topics and spec["sink"] in topics
        R["topics_created"] = {"source": spec["src"] in topics, "sink": spec["sink"] in topics}
        self.ev("topics", created=sorted(k for k in topics if not k.startswith("_")), ok=topics_ok)

        # ---- no job can be built from the answer: a model failure, not a testbed one
        if plan.flink_conf_error or plan.job_error:
            R["root_exception"] = plan.flink_conf_error or plan.job_error
            R["failure_stage"] = "answer"
            R["criteria_reasons"]["C1_deploy"] = R["root_exception"]
            self.c1 = False
            return

        # ---- Flink session cluster: JM + n TMs, answer's configuration
        rpc_port, rest_port = base + 30, base + 31
        conf, subs = A.build_flink_conf(plan.flink_answer_conf, plan.k8s_over, plan.track, str(self.run_dir),
                                        rpc_port, rest_port)
        R["substitutions"] += subs
        R["flink_config_deployed"] = {k: v for k, v in conf.items() if k != "env.java.opts.all"}
        sql, ssubs, aug = A.build_job_sql(plan.sql_text, plan.tables, plan.src, plan.snk, boot)
        R["substitutions"] += ssubs
        R["augmentations"] += aug
        jm = self.topo.role("jm")[0]
        self.flink = AnswerFlink(name="e7", nodes=self.topo.role("flink"), jm_node=jm, rpc_port=rpc_port,
                                 rest_port=rest_port, prom_lo=0, prom_hi=0, run_dir=self.run_dir, config={},
                                 conf_flat=conf)
        self.flink.render()
        (self.art / "config.yaml").write_text((self.flink.conf_dir / "config.yaml").read_text())
        (self.art / "job.sql").write_text(sql)
        t = time.time()
        self.flink.spawn_jm()
        try:
            wait_port(rest_port, 120, "jobmanager REST")
        except TimeoutError:
            R["root_exception"] = "JobManager did not start: " + (sql_client_error(
                tail(self.flink.log_dir / "jobmanager.out", 20000)) or tail(self.flink.log_dir / "jobmanager.out", 1500))
            R["failure_stage"] = "jobmanager-start"
            self.c1 = False
            return
        self.sysmon.track("flink-jm", self.flink.procs["jobmanager"].pid, "flink")
        n_tm = plan.n_taskmanagers
        for i in range(n_tm):
            key = f"taskmanager{i}"
            self.tm_rid[key] = f"tm{i}"
            self.flink.spawn_tm(key, self.flink.nodes[i], f"tm{i}")
            self.sysmon.track(f"flink-tm{i}", self.flink.procs[key].pid, "flink")
        reg = self._wait_tms(n_tm, 90)
        R["taskmanagers"] = {"started": n_tm, "registered": reg, "slots_per_tm": parse_slots(conf)}
        if reg < n_tm:
            R["startup_errors"] = {k: sql_client_error(tail(self.flink.log_dir / f"{k}.out", 20000))
                                   for k in self.flink.procs if k.startswith("taskmanager")
                                   and self.flink.procs[k].poll() is not None}
        self.ev("flink_up", seconds=round(time.time() - t, 1), taskmanagers=reg)

        # ---- measurement reader subscribes before the job exists
        self.reader = self.aux("sink_reader", [PY, str(HERE / "e7_sink_reader.py"), "--bootstrap", boot,
                                               "--topic", spec["sink"], "--group", f"e7-measure-{os.getpid()}",
                                               "--out", str(self.art / "sink_records.npz"),
                                               "--progress", str(self.art / "sink_progress.json")],
                               node=(self.topo.role("meas") or [None])[0])

        # ---- submit the SQL job
        job_path = self.flink.conf_dir / "job.sql"
        job_path.write_text(sql)
        t_submit = time.time()
        log = self.art / "sql-client.log"
        with open(log, "wb") as lf:
            try:
                r = subprocess.run(self.ctl([str(FLINK / "bin" / "sql-client.sh"), "-f", str(job_path)]),
                                   stdout=lf, stderr=subprocess.STDOUT, env=self.flink.env(), timeout=300)
                rc = r.returncode
            except subprocess.TimeoutExpired:
                rc = None
        out = tail(log, 200000)
        m = re.search(r"Job ID:\s*([0-9a-f]{32})", out)
        R["sql_client"] = {"rc": rc, "seconds": round(time.time() - t_submit, 1), "error": sql_client_error(out)}
        if not m:
            R["root_exception"] = R["sql_client"]["error"] or f"sql-client rc={rc}, no job id"
            R["failure_stage"] = "sql-submit"
            self.c1 = False
            self.ev("submit_failed", error=(R["root_exception"] or "")[:300])
            return
        self.jid = m.group(1)
        R["job_submitted"], R["job_id"] = True, self.jid
        self.ev("submitted", job=self.jid, seconds=R["sql_client"]["seconds"])

        # ---- C1: RUNNING (every vertex) within the deadline
        t_run = self._wait_running(t_submit, a.running_timeout)
        R["seconds_to_running"] = None if t_run is None else round(t_run - t_submit, 1)
        self.c1 = topics_ok and t_run is not None and (t_run - t_submit) <= a.c1_deadline
        if not topics_ok:
            R["criteria_reasons"]["C1_deploy"] = "source and/or sink topic not created by the answer's commands"
        elif t_run is None:
            R["criteria_reasons"]["C1_deploy"] = f"job not RUNNING within {a.running_timeout:.0f} s"
        elif not self.c1:
            R["criteria_reasons"]["C1_deploy"] = f"job RUNNING only after {t_run - t_submit:.0f} s"
        if t_run is None:
            self._final_job_state()
            return

        # ---- load + failure injection + measurement
        time.sleep(a.settle)
        self._load_and_measure()

    # ------------------------------------------------------------------ waits
    def _wait_tms(self, n: int, timeout: float) -> int:
        deadline, got = time.time() + timeout, 0
        while time.time() < deadline:
            try:
                got = len(rest(f"{self.flink.base}/taskmanagers").get("taskmanagers", []))
                if got >= n:
                    return got
            except Exception:
                pass
            if all(self.flink.procs[k].poll() is not None for k in self.flink.procs if k.startswith("taskmanager")):
                break
            time.sleep(1)
        return got

    def _job(self) -> dict:
        return rest(f"{self.flink.base}/jobs/{self.jid}")

    def _wait_running(self, t_submit: float, timeout: float) -> float | None:
        last = None
        while time.time() < t_submit + timeout:
            try:
                d = self._job()
                st = d.get("state") or d.get("status")
                vs = Counter(v["status"] for v in d.get("vertices", []))
                cur = (st, tuple(sorted(vs.items())))
                if cur != last:
                    self.ev("job_state", state=st, vertices=dict(vs))
                    last = cur
                if st == "RUNNING" and vs and set(vs) == {"RUNNING"}:
                    return time.time()
                if st in ("FAILED", "CANCELED", "FINISHED"):
                    return None
            except Exception:
                pass
            time.sleep(1)
        return None

    # ------------------------------------------------------------------ load & measure
    def _snapshot(self) -> dict:
        s = {"t": round(time.time(), 2), "rel": None if self.t0 is None else round(time.time() - self.t0, 1)}
        try:
            d = self._job()
            s["state"] = d.get("state") or d.get("status")
            s["vertices"] = dict(Counter(v["status"] for v in d.get("vertices", [])))
        except Exception as exc:
            s["error"] = str(exc)[:200]
        try:
            c = rest(f"{self.flink.base}/jobs/{self.jid}/checkpoints").get("counts", {})
            s["ckpt"] = {k: c.get(k) for k in ("completed", "failed", "in_progress", "restored")}
        except Exception:
            pass
        try:
            p = json.loads((self.art / "sink_progress.json").read_text())
            s["sink_n"], s["sink_distinct"] = p.get("n"), p.get("distinct")
        except Exception:
            pass
        return s

    def _kill_tm(self):
        """SIGKILL the TaskManager that holds most busy slots; restart it after `restart_after` s."""
        try:
            tms = rest(f"{self.flink.base}/taskmanagers").get("taskmanagers", [])
            busy = {t["id"]: t.get("slotsNumber", 0) - t.get("freeSlots", 0) for t in tms}
        except Exception:
            busy = {}
        alive = [k for k in self.tm_rid if self.flink.procs.get(k) and self.flink.procs[k].poll() is None]
        if not alive:
            self.ev("kill_skipped", reason="no live TaskManager")
            return
        victim = max(alive, key=lambda k: (busy.get(self.tm_rid[k], 0), k))
        p = self.flink.procs[victim]
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        t_kill = time.time()
        self.R["failure_injection"] = {"victim": victim, "resource_id": self.tm_rid[victim],
                                       "busy_slots": busy.get(self.tm_rid[victim]), "t_kill": t_kill,
                                       "rel_kill": round(t_kill - self.t0, 2)}
        self.ev("taskmanager_killed", victim=victim, busy_slots=busy.get(self.tm_rid[victim]))
        time.sleep(self.a.restart_after)
        idx = int(victim[len("taskmanager"):])
        rid = self.tm_rid[victim] + "-r"
        self.tm_rid[victim] = rid
        self.flink.spawn_tm(victim, self.flink.nodes[idx], rid)
        self.sysmon.track(f"flink-tm{idx}", self.flink.procs[victim].pid, "flink")
        self.R["failure_injection"]["t_restart"] = time.time()
        self.R["failure_injection"]["down_s"] = round(time.time() - t_kill, 2)
        self.ev("taskmanager_restarted", victim=victim, resource_id=rid)

    def _load_and_measure(self):
        a, R, spec, plan = self.a, self.R, self.spec, self.plan
        rate_req = float(spec.get("throughput_eps") or 0)
        rate = min(rate_req, a.rate_cap)
        if rate < rate_req:
            R["notes"].append(f"offered rate capped at {a.rate_cap:.0f} ev/s (scenario asks {rate_req:.0f})")
        R["load"] = {"rate_requested": rate_req, "rate_target": rate, "rate_capped": rate < rate_req,
                     "duration_s": a.duration}
        key_hint = None
        m = re.search(r"keyed by `([^`]+)`", self.sc.get("prompt", ""))
        if m:
            key_hint = m.group(1)
        tpls, info = A.make_templates(plan.src, spec.get("event_bytes"), a.templates, a.seed_load, key_hint)
        R["load"]["records"] = info
        tpath = self.art / "templates.jsonl"
        with open(tpath, "w") as f:
            for x in tpls:
                f.write(json.dumps(x) + "\n")
        n_proc = max(1, math.ceil(rate / 30000))
        self.t0 = time.time() + 4.0
        R["t0"] = self.t0
        for k in range(n_proc):
            self.gens.append(self.aux(f"generator{k}", [
                PY, str(HERE / "e7_gen.py"), "--bootstrap", self.kafka.bootstrap, "--topic", spec["src"],
                "--templates", str(tpath), "--rate", str(rate), "--duration", str(a.duration),
                "--t0", f"{self.t0:.3f}", "--proc-index", str(k), "--proc-count", str(n_proc),
                "--out", str(self.art / f"generator{k}.json")]))
        inject = spec.get("delivery") in DELIVERY_FAILURE
        R["failure_injection_planned"] = inject
        timer = None
        if inject:
            timer = threading.Timer(max(0.0, self.t0 + a.kill_at - time.time()), self._kill_tm)
            timer.daemon = True
            timer.start()
        self.ev("load_started", rate=rate, processes=n_proc, kill_at=a.kill_at if inject else None)
        while time.time() < self.t0 + a.duration:
            self.timeline.append(self._snapshot())
            time.sleep(2)
        for g in self.gens:
            try:
                g.wait(timeout=180)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(g.pid), signal.SIGKILL)
        if timer:
            timer.join(timeout=a.restart_after + 30)
        gres = []
        for k in range(n_proc):
            try:
                gres.append(json.loads((self.art / f"generator{k}.json").read_text()))
            except Exception as exc:
                R["null_reasons"]["generator"] = f"generator {k} wrote no result: {exc}"
        self.gres = gres
        acked = sum(g["sent"] - len(g["failed"]) for g in gres)
        t_load_end = max([g.get("t_stop", time.time()) for g in gres] or [time.time()])
        self.ev("load_stopped", acked=acked, sent=sum(g["sent"] for g in gres),
                failed=sum(len(g["failed"]) for g in gres))
        # ---- drain: up to `drain` s after the load stopped, until every acked record is visible
        while time.time() < t_load_end + a.drain:
            snap = self._snapshot()
            self.timeline.append(snap)
            if gres and snap.get("sink_distinct") is not None and snap["sink_distinct"] >= acked:
                break
            time.sleep(2)
        R["drain_s"] = round(time.time() - t_load_end, 1)
        self._final_job_state()
        self._stop_reader()
        self._metrics(gres, t_load_end)

    def _final_job_state(self):
        R = self.R
        try:
            d = self._job()
            R["job_state_final"] = d.get("state") or d.get("status")
            R["vertex_states_final"] = dict(Counter(v["status"] for v in d.get("vertices", [])))
        except Exception as exc:
            R["null_reasons"]["job_state_final"] = f"REST: {exc}"[:300]
        try:
            c = rest(f"{self.flink.base}/jobs/{self.jid}/checkpoints")
            R["checkpoints_completed"] = c.get("counts", {}).get("completed")
            R["checkpoints_failed"] = c.get("counts", {}).get("failed")
            R["checkpoint_summary"] = {k: (c.get("summary", {}).get(k) or {}) for k in ("end_to_end_duration",
                                                                                    "state_size")}
            ck = rest(f"{self.flink.base}/jobs/{self.jid}/checkpoints/config")
            R["checkpoint_config_effective"] = ck
        except Exception as exc:
            R["null_reasons"]["checkpoints"] = f"REST: {exc}"[:300]
        try:
            mt = rest(f"{self.flink.base}/jobs/{self.jid}/metrics?get=numRestarts,fullRestarts")
            vals = {m["id"]: m.get("value") for m in mt or []}
            v = vals.get("numRestarts", vals.get("fullRestarts"))
            R["num_restarts"] = int(float(v)) if v is not None else None
            if v is None:
                R["null_reasons"]["num_restarts"] = "metric not exposed by the JobManager REST API at the end"
        except Exception:
            pass
        try:
            e = rest(f"{self.flink.base}/jobs/{self.jid}/exceptions?maxExceptions=20")
            hist = (e.get("exceptionHistory") or {}).get("entries") or []
            R["exception_history"] = [{"t": x.get("timestamp"), "task": x.get("taskName"),
                                       "chain": exc_chain(x.get("stacktrace"))} for x in hist[:10]]
            root = e.get("root-exception") or (hist[0].get("stacktrace") if hist else None)
            R["root_exception"] = exc_chain(root)
            if root:
                (self.art / "root_exception.txt").write_text(root)
        except Exception as exc:
            R["null_reasons"]["root_exception"] = f"REST: {exc}"[:300]

    def _stop_reader(self):
        if self.reader and self.reader.poll() is None:
            try:
                os.killpg(os.getpgid(self.reader.pid), signal.SIGTERM)
                self.reader.wait(timeout=90)
            except (subprocess.TimeoutExpired, ProcessLookupError):
                pass

    def _metrics(self, gres: list, t_load_end: float):
        a, R = self.a, self.R
        if not gres:
            R["null_reasons"]["offered_eps"] = "no generator result"
            return
        P = gres[0]["proc_count"]
        max_seq = max(g["sent"] * P for g in gres) + P
        produced = np.zeros(max_seq + 1, dtype=bool)
        for g in gres:
            produced[np.arange(g["sent"], dtype=np.int64) * P + g["proc_index"]] = True
        failed = [s for g in gres for s in g["failed"] if s >= 0]
        if failed:
            produced[np.array(failed, dtype=np.int64)] = False
        acked = int(produced.sum())
        unflushed = sum(g.get("unflushed", 0) for g in gres)
        R["generator_client_queue_max"] = max((max(g.get("client_queue_per_sec", {}).values() or [0]) for g in gres),
                                              default=None)
        R["produced"] = {"sent": sum(g["sent"] for g in gres), "acked": acked, "delivery_failed": len(failed),
                         "error_counts": dict(sum((Counter(g["error_counts"]) for g in gres), Counter())),
                         "unflushed": unflushed}
        t0 = self.t0
        dur = max(1e-9, t_load_end - t0)
        R["offered_eps_whole_load"] = round(acked / dur, 1)
        # steady-state window W: after warm-up, before the failure (minus a guard), else until load end
        ws = t0 + a.warmup
        we = (t0 + a.kill_at - a.guard) if R.get("failure_injection_planned") else t0 + a.duration
        ws_s, we_s = math.ceil(ws), math.floor(we)
        R["steady_window"] = {"start_rel_s": ws_s - t0, "end_rel_s": we_s - t0, "length_s": we_s - ws_s}
        per_sec = Counter()
        for g in gres:
            for s, c in g["per_sec"].items():
                per_sec[int(s)] += c
        L = max(1, we_s - ws_s)
        offered = sum(per_sec.get(s, 0) for s in range(ws_s, we_s)) / L
        R["offered_eps"] = round(offered, 1)
        R["offered_per_second"] = {str(s - int(t0)): per_sec[s] for s in sorted(per_sec)}

        try:
            z = np.load(self.art / "sink_records.npz")
            seq, sent, recv = z["seq"], z["sent_ms"], z["recv_ms"]
        except Exception as exc:
            for k in ("sink_eps", "latency_ms", "lost", "duplicates"):
                R["null_reasons"][k] = f"sink reader produced no data file: {exc}"
            return
        try:
            R["sink_reader"] = json.loads((self.art / "sink_progress.json").read_text())
        except Exception:
            pass
        n = len(seq)
        R["sink_records_received"] = int(n)
        inrange = seq < len(produced)
        counts = np.bincount(seq[inrange], minlength=len(produced))
        R["lost"] = int((produced & (counts == 0)).sum())
        R["duplicates"] = int((produced & (counts > 1)).sum())
        R["duplicate_extra_copies"] = int(np.clip(counts - 1, 0, None)[produced].sum())
        R["received_not_acked"] = int(((~produced) & (counts > 0)).sum()) + int((~inrange).sum())
        if unflushed:
            R["notes"].append(f"{unflushed} records were still unflushed at generator exit; "
                              "they count as produced only if they reached the sink")

        recv_sec = np.floor(recv / 1000.0).astype(np.int64)
        if n:
            idx = np.arange(ws_s, we_s)
            cnt = np.bincount(np.clip(recv_sec[(recv_sec >= ws_s) & (recv_sec < we_s)] - ws_s, 0, L - 1),
                              minlength=L)[:L]
            cum = np.cumsum(cnt)
            slope = float(np.polyfit(idx - ws_s, cum, 1)[0]) if L >= 3 else float(cnt.sum() / L)
            R["sink_eps"] = round(slope, 1)
            R["sink_eps_count"] = round(float(cnt.sum()) / L, 1)
        else:
            R["sink_eps"], R["sink_eps_count"] = 0.0, 0.0
        allsec = Counter((recv_sec - int(t0)).tolist()) if n else Counter()
        R["sink_per_second"] = {str(k): v for k, v in sorted(allsec.items())}

        if n:
            order = np.argsort(recv, kind="stable")
            _, first = np.unique(seq[order], return_index=True)
            fi = order[first]
            lat = recv[fi] - sent[fi]
            sf = sent[fi]
            mask = (sf >= ws_s * 1000) & (sf < we_s * 1000)
            if mask.any():
                p = np.percentile(lat[mask], [50, 95, 99])
                R["latency_ms"] = {"p50": round(float(p[0]), 1), "p95": round(float(p[1]), 1),
                                   "p99": round(float(p[2]), 1), "max": round(float(lat[mask].max()), 1),
                                   "n": int(mask.sum())}
            else:
                R["null_reasons"]["latency_ms"] = ("no record sent in the steady-state window became visible to "
                                                   "a read_committed consumer")
            p = np.percentile(lat, [50, 95, 99])
            R["latency_ms_all_records"] = {"p50": round(float(p[0]), 1), "p95": round(float(p[1]), 1),
                                           "p99": round(float(p[2]), 1), "max": round(float(lat.max()), 1),
                                           "n": int(len(lat))}
        else:
            R["null_reasons"]["latency_ms"] = "no record became visible to a read_committed consumer"

        # CPU use per component in the steady window (to spot testbed saturation)
        try:
            acc: dict = {}
            for ln in open(self.art / "sysmon.jsonl"):
                row = json.loads(ln)
                if ws <= row.get("t", 0) <= we:
                    for name, v in (row.get("c") or {}).items():
                        if v.get("cores") is not None:
                            acc.setdefault(name, []).append(v["cores"])
            R["cpu_cores_steady"] = {k: round(sum(v) / len(v), 2) for k, v in sorted(acc.items())}
            R["cpu_capacity_logical"] = {r: 2 * c for r, c in R["topology"]["cores_per_role"].items()}
        except Exception:
            pass

    # ------------------------------------------------------------------ criteria
    def _criteria(self):
        R, spec = self.R, self.spec
        if R.get("untestable_reason"):
            return
        C, why = R["criteria"], R["criteria_reasons"]
        c1 = getattr(self, "c1", None)
        C["C1_deploy"] = c1
        if c1 is None:
            why.setdefault("C1_deploy", "deployment aborted before the job could be judged (see driver_error)")
        delivery = spec.get("delivery")
        ran = R.get("offered_eps") is not None
        if delivery in DELIVERY_FAILURE:
            if R["checkpoints_completed"] is not None:
                C["C2_checkpoints"] = R["checkpoints_completed"] >= 3
            elif not R["job_submitted"] or R.get("seconds_to_running") is None:
                C["C2_checkpoints"] = False
                why["C2_checkpoints"] = "job not running"
        # C3 / C4 / C5 need a running job and a load phase
        if not ran:
            if R.get("driver_error"):
                why.setdefault("C3_throughput", "not measured: driver error")
            elif c1 is False or R.get("seconds_to_running") is None:
                for k in ("C3_throughput", "C4_latency"):
                    C[k] = False
                    why[k] = "job not running: no load phase"
                if delivery in DELIVERY_FAILURE:
                    C["C5_delivery"] = False
                    why["C5_delivery"] = "job not running: no load phase"
        else:
            if R.get("sink_eps") is not None and R["offered_eps"]:
                C["C3_throughput"] = R["sink_eps"] >= 0.95 * R["offered_eps"]
            p99 = R["latency_ms"].get("p99")
            if p99 is not None and R["slo_ms"] is not None:
                C["C4_latency"] = p99 <= R["slo_ms"]
            elif R.get("sink_records_received") is not None and R["slo_ms"] is not None:
                C["C4_latency"] = False
                why["C4_latency"] = R["null_reasons"].get("latency_ms", "no visible records")
            if delivery == "exactly-once" and R["lost"] is not None:
                C["C5_delivery"] = R["lost"] == 0 and R["duplicates"] == 0
            elif delivery == "at-least-once" and R["lost"] is not None:
                C["C5_delivery"] = R["lost"] == 0
        applicable = ["C1_deploy", "C3_throughput", "C4_latency"]
        if delivery in DELIVERY_FAILURE:
            applicable += ["C2_checkpoints", "C5_delivery"]
        for k in C:
            if k not in applicable:
                C[k] = None
                why[k] = "not applicable (delivery guarantee does not require it)"
        vals = [C[k] for k in applicable]
        if any(v is False for v in vals):
            R["runtime_pass"] = False
        elif all(v is True for v in vals):
            R["runtime_pass"] = True
        else:
            R["runtime_pass"] = None
        R["criteria_applicable"] = applicable

    # ------------------------------------------------------------------ teardown
    def _teardown(self):
        R = self.R
        for g in self.gens:
            if g.poll() is None:
                try:
                    os.killpg(os.getpgid(g.pid), signal.SIGTERM)
                    g.wait(timeout=30)
                except (subprocess.TimeoutExpired, ProcessLookupError):
                    try:
                        os.killpg(os.getpgid(g.pid), signal.SIGKILL)
                    except ProcessLookupError:
                        pass
        self._stop_reader()
        if self.reader and self.reader.poll() is None:
            try:
                os.killpg(os.getpgid(self.reader.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
        if self.kafka is not None:
            try:
                R["topics_after_run"] = describe_topics(self.kafka)
            except Exception:
                pass
        if self.flink is not None:
            if self.jid:
                try:
                    rest(f"{self.flink.base}/jobs/{self.jid}", method="PATCH")
                except Exception:
                    pass
            try:
                self.flink.stop()
            except Exception as exc:
                R.setdefault("notes", []).append(f"flink stop: {exc}")
        if self.kafka is not None:
            self.kafka.stop()
        if self.sysmon is not None:
            self.sysmon.stop()
        R["timeline"] = self.timeline
        if hasattr(self, "run_dir"):
            self._preserve_logs()
            if not self.a.keep_state:
                shutil.rmtree(self.run_dir, ignore_errors=True)
        if self.sentinel is not None:
            try:
                self.sentinel.close()
            except Exception:
                pass
        # every process started for this deployment must be gone
        left = []
        for name, procs in (("flink", self.flink.procs if self.flink else {}),
                            ("kafka", self.kafka.procs if self.kafka else {})):
            for k, p in procs.items():
                if p.poll() is None:
                    left.append(f"{name}:{k}")
        R["teardown_leftovers"] = left

    def _preserve_logs(self, tail_bytes: int = 300_000):
        src = self.run_dir / "logs"
        if not src.exists():
            return
        dst = self.art / "node_logs"
        for f in src.rglob("*"):
            if f.is_file():
                try:
                    out = dst / f.relative_to(src)
                    out.parent.mkdir(parents=True, exist_ok=True)
                    out.write_bytes(f.read_bytes()[-tail_bytes:])
                except OSError:
                    pass


# =============================================================================== CLI
def load_answer(a) -> tuple[str, str | None, str, int]:
    if a.answer_file:
        return Path(a.answer_file).read_text(), a.finish_reason, a.model or Path(a.answer_file).stem, a.seed
    with open(a.answers) as f:
        for ln in f:
            r = json.loads(ln)
            if r["id"] != a.id:
                continue
            if a.model and r.get("model") != a.model:
                continue
            if a.seed is not None and int(r.get("seed", 0)) != a.seed:
                continue
            return r["text"], r.get("finish_reason"), r.get("model", a.model), int(r.get("seed", 0))
    raise SystemExit(f"no answer for id={a.id} model={a.model} seed={a.seed} in {a.answers}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--id", required=True, help="scenario id in the benchmark")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--answer-file", help="answer text")
    src.add_argument("--answers", help="generation JSONL (fields id, model, seed, text, finish_reason)")
    ap.add_argument("--model", default=None)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--finish-reason", default="stop", help="for --answer-file")
    ap.add_argument("--bench", default=str(BENCH))
    ap.add_argument("--out", required=True, help="result JSON; artefacts go to the directory of the same stem")
    ap.add_argument("--duration", type=float, default=180.0, help="load duration after the job is RUNNING")
    ap.add_argument("--warmup", type=float, default=None, help="default duration/6")
    ap.add_argument("--kill-at", type=float, default=None, help="default duration/2")
    ap.add_argument("--guard", type=float, default=None, help="steady window ends this long before the kill; "
                                                               "default duration/18")
    ap.add_argument("--restart-after", type=float, default=15.0)
    ap.add_argument("--drain", type=float, default=60.0)
    ap.add_argument("--rate-cap", type=float, default=50000.0)
    ap.add_argument("--settle", type=float, default=5.0, help="pause between RUNNING and the load start")
    ap.add_argument("--c1-deadline", type=float, default=60.0)
    ap.add_argument("--running-timeout", type=float, default=120.0)
    ap.add_argument("--templates", type=int, default=4096)
    ap.add_argument("--seed-load", type=int, default=1)
    # 2 physical cores per broker: with 1 core the brokers throttled the EOS pipeline (GOLD answer, 50k ev/s:
    # p50 3.6 s vs 0.5 s steady latency, 575 vs 1320 checkpoints in 180 s), i.e. the testbed, not the answer.
    ap.add_argument("--kafka-cores", type=int, default=2)
    ap.add_argument("--tm-cores", type=int, default=4)
    ap.add_argument("--jm-cores", type=int, default=1)
    ap.add_argument("--ctl-cores", type=int, default=2)
    ap.add_argument("--meas-cores", type=int, default=1, help="0 = reader shares the control node")
    ap.add_argument("--run-root", default=f"/tmp/{os.environ.get('USER', 'e7')}-e7")
    ap.add_argument("--keep-state", action="store_true")
    ap.add_argument("--deploy-untestable", action="store_true", help="deploy even if judged untestable (diagnostic)")
    a = ap.parse_args()
    a.warmup = a.duration / 6 if a.warmup is None else a.warmup
    a.kill_at = a.duration / 2 if a.kill_at is None else a.kill_at
    a.guard = a.duration / 18 if a.guard is None else a.guard

    bench = {json.loads(ln)["id"]: json.loads(ln) for ln in open(a.bench)}
    if a.id not in bench:
        raise SystemExit(f"unknown scenario {a.id}")
    text, fr, model, seed = load_answer(a)
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)

    def on_term(signum, _frame):
        raise SystemExit(f"signal {signum}")
    signal.signal(signal.SIGTERM, on_term)

    d = Deployment(a, bench[a.id], text, fr, model, seed)
    d.R["driver_args"] = {k: v for k, v in vars(a).items()}
    R = d.run()
    print(json.dumps({k: R.get(k) for k in ("id", "model", "job_state_final", "criteria", "runtime_pass",
                                            "untestable_reason")}, default=str), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
