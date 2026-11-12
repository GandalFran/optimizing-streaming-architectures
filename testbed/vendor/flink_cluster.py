"""
One Flink session cluster (JobManager + TaskManagers) pinned to a set of nodes.

An experiment instance runs several: the production cluster on the flink
nodes, and the two arms of the paired micro-canary on the canary node. Each has
its own configuration directory and port block, and all attach to the same
Kafka brokers.

After every submission the cluster reads back, through the REST API, what
Flink actually applied -- parallelism, checkpointing, state backend, slots,
TaskManager memory -- and compares it with what was intended. A mismatch
raises, so a run cannot report one configuration while running another. (The
earlier implementation hardcoded the checkpoint interval inside the job, which
silently overrode every tuned value.)
"""
from __future__ import annotations
import json, os, shutil, signal, string, subprocess, time
import urllib.error, urllib.request
from dataclasses import dataclass, field
from pathlib import Path

import yaml

OPT = Path(os.environ.get("TUNING_OPT", Path(__file__).resolve().parents[1] / "runtime"))
JAVA_HOME = OPT / "jdk17"
FLINK = OPT / "flink"
CODE = Path(__file__).resolve().parent.parent

JAVA_OPTS = " ".join([
    "--add-exports=java.base/sun.net.util=ALL-UNNAMED",
    "--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED",
    *[f"--add-opens=java.base/{p}=ALL-UNNAMED" for p in (
        "java.lang", "java.net", "java.io", "java.nio", "sun.nio.ch",
        "java.lang.reflect", "java.util", "java.util.concurrent",
        "java.util.concurrent.atomic", "java.util.concurrent.locks",
        "java.text", "java.time")],
])

# Fields that live in the TaskManager process: changing any of them requires
# restarting TaskManagers, not just resubmitting the job.
TM_LEVEL = ("tm_process_memory_mb", "managed_memory_fraction",
            "network_memory_fraction", "slots_per_tm")


def _native(o):
    """Recursively convert numpy scalars to Python types for serialisation."""
    if isinstance(o, dict):
        return {k: _native(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_native(v) for v in o]
    # np.str_ subclasses str, so test the type's module rather than isinstance
    return o.item() if type(o).__module__ == "numpy" else o


class ConfigMismatch(RuntimeError):
    """Flink applied something other than what was requested."""


def rest(url: str, data: bytes | None = None, method: str = "GET",
         timeout: float = 20.0):
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
        return json.loads(body) if body else {}


def wait_port(port: int, timeout: float, what: str) -> None:
    import socket
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as s:
            s.settimeout(0.5)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.5)
    raise TimeoutError(f"{what} did not open port {port} within {timeout}s")


@dataclass
class FlinkCluster:
    name: str
    nodes: list                      # topology.Node objects hosting TMs
    jm_node: object                  # topology.Node hosting the JobManager
    rpc_port: int
    rest_port: int
    prom_lo: int
    prom_hi: int
    run_dir: Path
    config: dict                     # the "flink" section
    job_env: dict = field(default_factory=dict)   # template substitutions
    jm_memory: str = "2048m"
    procs: dict = field(default_factory=dict)      # name -> Popen
    job_id: str = ""
    history: list = field(default_factory=list)

    # ---------------------------------------------------------------- paths
    @property
    def conf_dir(self) -> Path:
        return self.run_dir / "conf" / f"flink-{self.name}"

    @property
    def log_dir(self) -> Path:
        return self.run_dir / "logs" / f"flink-{self.name}"

    @property
    def state_root(self) -> Path:
        return Path(self.jm_node.state_dir) / f"flink-{self.name}"

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.rest_port}"

    def env(self) -> dict:
        e = dict(os.environ)
        e["JAVA_HOME"] = str(JAVA_HOME)
        e["PATH"] = f"{JAVA_HOME}/bin:{e.get('PATH', '')}"
        e["FLINK_CONF_DIR"] = str(self.conf_dir)
        e["FLINK_LOG_DIR"] = str(self.log_dir)
        e["FLINK_PID_DIR"] = str(self.run_dir / "pids" / self.name)
        return e

    # --------------------------------------------------------------- config
    def render(self) -> None:
        f = self.config
        self.conf_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        (self.run_dir / "pids" / self.name).mkdir(parents=True, exist_ok=True)
        for extra in ("log4j.properties", "log4j-console.properties",
                      "logback.xml", "logback-console.xml"):
            src = FLINK / "conf" / extra
            if src.exists():
                shutil.copy(src, self.conf_dir / extra)
        ckpt, savep = self.state_root / "checkpoints", self.state_root / "savepoints"
        rocks, iotmp = self.state_root / "rocksdb", self.state_root / "io"
        for d in (ckpt, savep, rocks, iotmp):
            d.mkdir(parents=True, exist_ok=True)

        cfg = {
            "env": {"java": {"opts": {"all": JAVA_OPTS}}},
            "jobmanager": {
                "bind-host": "127.0.0.1",
                "rpc": {"address": "127.0.0.1", "port": self.rpc_port},
                "memory": {"process": {"size": self.jm_memory}},
                "execution": {"failover-strategy": "region"},
            },
            "taskmanager": {
                "bind-host": "127.0.0.1", "host": "127.0.0.1",
                "numberOfTaskSlots": int(f["slots_per_tm"]),
                "memory": {
                    "process": {"size": f"{int(f['tm_process_memory_mb'])}m"},
                    "managed": {"fraction": float(f["managed_memory_fraction"])},
                    "network": {"fraction": float(f["network_memory_fraction"])},
                },
            },
            # Spread subtasks over TaskManagers. Flink's default packs slots
            # onto the first TaskManager, so raising parallelism would load one
            # node while the others idle.
            "cluster": {"evenly-spread-out-slots": True},
            # A job that cannot obtain its slots fails after this long rather
            # than waiting Flink's default five minutes.
            "slot": {"request": {"timeout": "60000ms"}},
            "parallelism": {"default": int(f["parallelism"])},
            "rest": {"address": "127.0.0.1", "bind-address": "127.0.0.1",
                     "port": self.rest_port},
            "execution": {"checkpointing": {
                "interval": f"{int(f['checkpoint_interval_ms'])}ms",
                "timeout": f"{int(f['checkpoint_timeout_ms'])}ms",
                "min-pause": f"{int(f['min_pause_between_checkpoints_ms'])}ms",
                "max-concurrent-checkpoints": int(f["max_concurrent_checkpoints"]),
                "unaligned": {"enabled": bool(f["unaligned_checkpoints"])},
                "mode": "EXACTLY_ONCE",
                "externalized-checkpoint-retention": "RETAIN_ON_CANCELLATION",
                "dir": f"file://{ckpt}",
                "savepoint-dir": f"file://{savep}",
            }},
            "state": {"backend": {"type": f["state_backend"],
                                  "incremental": bool(f.get("incremental_checkpoints", False)),
                                  "rocksdb": {"localdir": str(rocks)}}},
            "restart-strategy": {"type": "fixed-delay", "fixed-delay": {
                "attempts": 2147483647, "delay": f"{int(f['restart_delay_ms'])}ms"}},
            "metrics": {
                "reporter": {"prom": {
                    "factory.class": "org.apache.flink.metrics.prometheus."
                                     "PrometheusReporterFactory",
                    "port": f"{self.prom_lo}-{self.prom_hi}"}},
                "latency": {"interval": "0"},
            },
            "io": {"tmp": {"dirs": str(iotmp)}},
        }
        (self.conf_dir / "config.yaml").write_text(yaml.safe_dump(_native(cfg), sort_keys=False))

    # ------------------------------------------------------------ processes
    def _spawn(self, key: str, node, argv: list[str], cpus: str | None = None):
        wrapped = (["numactl", f"--physcpubind={cpus}", f"--membind={node.numa}", "--"]
                   + argv) if cpus else node.wrap(argv)
        log = open(self.log_dir / f"{key}.out", "ab")
        log.write(f"\n=== spawn {time.time():.0f}: {' '.join(wrapped)}\n".encode())
        log.flush()
        p = subprocess.Popen(wrapped, stdout=log, stderr=subprocess.STDOUT,
                             env=self.env(), start_new_session=True)
        self.procs[key] = p
        return p

    def start(self, cpu_override: dict | None = None) -> None:
        """Start JobManager and TaskManagers. `cpu_override` maps a process key
        to an explicit CPU list, used to split one node between canary arms."""
        cpu_override = cpu_override or {}
        self.render()
        self._spawn("jobmanager", self.jm_node,
                    [str(FLINK / "bin" / "jobmanager.sh"), "start-foreground"],
                    cpus=cpu_override.get("jobmanager"))
        wait_port(self.rest_port, 120, f"{self.name} jobmanager")
        self.start_taskmanagers(int(self.config["taskmanagers"]), cpu_override)

    def start_taskmanagers(self, n: int, cpu_override: dict | None = None) -> None:
        cpu_override = cpu_override or {}
        for i in range(n):
            key = f"taskmanager{i}"
            node = self.nodes[i % len(self.nodes)]
            self._spawn(key, node,
                        [str(FLINK / "bin" / "taskmanager.sh"), "start-foreground"],
                        cpus=cpu_override.get(key))
        self.wait_taskmanagers(n)

    def scale_taskmanagers(self, n: int) -> None:
        """Start or stop TaskManagers so exactly `n` run (highest index first)."""
        cur = sorted(int(k[len("taskmanager"):]) for k, p in self.procs.items()
                     if k.startswith("taskmanager") and p.poll() is None)
        if n > len(cur):
            for i in range(len(cur), n):
                node = self.nodes[i % len(self.nodes)]
                self._spawn(f"taskmanager{i}", node,
                            [str(FLINK / "bin" / "taskmanager.sh"), "start-foreground"])
        elif n < len(cur):
            for i in cur[n:]:
                self._kill(f"taskmanager{i}")
        self.wait_taskmanager_count(n)

    def wait_taskmanager_count(self, n: int, timeout: float = 120.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if len(rest(f"{self.base}/taskmanagers").get("taskmanagers", [])) == n:
                    return
            except Exception:
                pass
            time.sleep(1)
        raise TimeoutError(f"{self.name}: TaskManager count did not reach {n}")

    def wait_taskmanagers(self, n: int, timeout: float = 120.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                tms = rest(f"{self.base}/taskmanagers").get("taskmanagers", [])
                if len(tms) >= n:
                    return
            except (urllib.error.URLError, ConnectionError):
                pass
            time.sleep(1)
        raise TimeoutError(f"{self.name}: {n} TaskManagers did not register")

    def stop_taskmanagers(self) -> None:
        keys = [k for k in self.procs if k.startswith("taskmanager")]
        for k in keys:
            self._kill(k)
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                if not rest(f"{self.base}/taskmanagers").get("taskmanagers"):
                    return
            except Exception:
                return
            time.sleep(1)

    def _kill(self, key: str, grace: float = 20.0) -> None:
        p = self.procs.pop(key, None)
        if not p or p.poll() is not None:
            return
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGTERM)
            p.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
            p.wait(timeout=10)
        except ProcessLookupError:
            pass

    def stop(self) -> None:
        for k in [k for k in self.procs if k.startswith("taskmanager")]:
            self._kill(k)
        self._kill("jobmanager")

    def alive(self) -> dict[str, bool]:
        return {k: p.poll() is None for k, p in self.procs.items()}

    def pids(self) -> dict[str, int]:
        return {k: p.pid for k, p in self.procs.items() if p.poll() is None}

    # ----------------------------------------------------------------- jobs
    def submit(self, savepoint: str | None = None, timeout: float = 240.0) -> str:
        tmpl = string.Template((CODE / "testbed" / "job.sql.tmpl").read_text())
        sql = tmpl.substitute({**self.job_env,
                               "PARALLELISM": int(self.config["parallelism"]),
                               # Flink's Prometheus reporter rewrites '-' to
                               # '_' in label values; use '_' so the job_name
                               # label equals the configured name.
                               "JOB_NAME": f"pipeline_{self.name}"})
        if savepoint:
            # Restore keyed state. Unclaimed state is NOT ignored: if the new
            # plan cannot take over the old state the submission fails, rather
            # than a "stateful" rollout silently starting from empty state.
            sql = (f"SET 'execution.savepoint.path' = '{savepoint}';\n"
                   f"SET 'execution.savepoint.ignore-unclaimed-state' = 'false';\n"
                   + sql)
        path = self.conf_dir / "job.sql"
        path.write_text(sql)
        log = self.log_dir / "sql-client.out"
        with open(log, "ab") as lf:
            r = subprocess.run(
                self.jm_node.wrap([str(FLINK / "bin" / "sql-client.sh"), "-f", str(path)]),
                stdout=lf, stderr=subprocess.STDOUT, env=self.env(), timeout=300)
        if r.returncode != 0:
            raise RuntimeError(f"{self.name}: sql-client rc={r.returncode}\n"
                               + log.read_text()[-2500:])
        self.job_id = self.wait_running(timeout)
        return self.job_id

    def wait_running(self, timeout: float) -> str:
        """Wait until a job exists and *all* of its vertices are RUNNING."""
        deadline, last = time.time() + timeout, "no job"
        while time.time() < deadline:
            try:
                for j in rest(f"{self.base}/jobs").get("jobs", []):
                    if j["status"] in ("FAILED",):
                        exc = rest(f"{self.base}/jobs/{j['id']}/exceptions")
                        raise RuntimeError(
                            f"{self.name}: job failed: "
                            f"{str(exc.get('root-exception'))[:1500]}")
                    if j["status"] != "RUNNING":
                        last = f"{j['id']} {j['status']}"
                        continue
                    d = rest(f"{self.base}/jobs/{j['id']}")
                    states = {v["status"] for v in d.get("vertices", [])}
                    if states == {"RUNNING"}:
                        return j["id"]
                    last = f"{j['id']} vertices {sorted(states)}"
            except urllib.error.URLError:
                pass
            time.sleep(1)
        raise TimeoutError(f"{self.name}: job not fully RUNNING in {timeout}s ({last})")

    def running_job(self) -> str | None:
        try:
            for j in rest(f"{self.base}/jobs").get("jobs", []):
                if j["status"] == "RUNNING":
                    return j["id"]
        except Exception:
            pass
        return None

    def stop_with_savepoint(self, timeout: float = 300.0) -> str:
        job = self.running_job()
        if not job:
            raise RuntimeError(f"{self.name}: no running job")
        target = f"file://{self.state_root / 'savepoints'}"
        r = rest(f"{self.base}/jobs/{job}/stop", method="POST",
                 data=json.dumps({"drain": False, "targetDirectory": target}).encode())
        trig = r["request-id"]
        deadline = time.time() + timeout
        while time.time() < deadline:
            s = rest(f"{self.base}/jobs/{job}/savepoints/{trig}")
            if s.get("status", {}).get("id") == "COMPLETED":
                op = s.get("operation", {})
                if op.get("failure-cause"):
                    raise RuntimeError(f"savepoint failed: {op['failure-cause']}")
                self.job_id = ""
                return op["location"]
            time.sleep(1)
        raise TimeoutError(f"{self.name}: savepoint timed out")

    def cancel(self) -> None:
        job = self.running_job()
        if job:
            try:
                rest(f"{self.base}/jobs/{job}", method="PATCH")
            except Exception:
                pass

    # ---------------------------------------------------------- verification
    def effective(self) -> dict:
        """What Flink reports it is actually running."""
        job = self.job_id or self.running_job()
        d = rest(f"{self.base}/jobs/{job}")
        ck = rest(f"{self.base}/jobs/{job}/checkpoints/config")
        tms = rest(f"{self.base}/taskmanagers").get("taskmanagers", [])
        ck_stats = rest(f"{self.base}/jobs/{job}/checkpoints")
        restored = (ck_stats.get("latest") or {}).get("restored")
        return {
            "job_id": job,
            "vertex_parallelism": {v["name"][:60]: v["parallelism"]
                                   for v in d.get("vertices", [])},
            "checkpoint_config": ck,
            "taskmanagers": [{"id": t["id"][-12:], "slots": t["slotsNumber"],
                              "free_slots": t["freeSlots"],
                              "memory": t.get("memoryConfiguration", {})}
                             for t in tms],
            "restored_from": restored,
        }

    def verify(self, expect_restore: bool = False) -> dict:
        """Compare effective with intended configuration; raise on mismatch."""
        f, eff = self.config, self.effective()
        ck = eff["checkpoint_config"]
        problems = []

        pars = set(eff["vertex_parallelism"].values())
        want_p = int(f["parallelism"])
        # Sinks/sources may be chained differently, but every vertex must run
        # at the configured parallelism for the comparison to be meaningful.
        if pars != {want_p}:
            problems.append(f"parallelism {sorted(pars)} != {want_p}")
        if int(ck.get("interval", -1)) != int(f["checkpoint_interval_ms"]):
            problems.append(f"checkpoint interval {ck.get('interval')} != "
                            f"{f['checkpoint_interval_ms']}")
        if int(ck.get("timeout", -1)) != int(f["checkpoint_timeout_ms"]):
            problems.append(f"checkpoint timeout {ck.get('timeout')} != "
                            f"{f['checkpoint_timeout_ms']}")
        if int(ck.get("min_pause", -1)) != int(f["min_pause_between_checkpoints_ms"]):
            problems.append(f"min pause {ck.get('min_pause')} != "
                            f"{f['min_pause_between_checkpoints_ms']}")
        if int(ck.get("max_concurrent", -1)) != int(f["max_concurrent_checkpoints"]):
            problems.append(f"max concurrent {ck.get('max_concurrent')} != "
                            f"{f['max_concurrent_checkpoints']}")
        if bool(ck.get("unaligned_checkpoints")) != bool(f["unaligned_checkpoints"]):
            problems.append(f"unaligned {ck.get('unaligned_checkpoints')} != "
                            f"{f['unaligned_checkpoints']}")
        backend = str(ck.get("state_backend", ""))
        want_b = "RocksDB" if f["state_backend"] == "rocksdb" else "HashMap"
        if want_b.lower() not in backend.lower():
            problems.append(f"state backend {backend} != {f['state_backend']}")
        tms = eff["taskmanagers"]
        if len(tms) != int(f["taskmanagers"]):
            problems.append(f"{len(tms)} TaskManagers != {f['taskmanagers']}")
        if any(t["slots"] != int(f["slots_per_tm"]) for t in tms):
            problems.append(f"slots {[t['slots'] for t in tms]} != {f['slots_per_tm']}")
        if expect_restore and not eff["restored_from"]:
            problems.append("job did not restore from the savepoint")

        eff["verified"] = not problems
        eff["problems"] = problems
        self.history.append({"t": time.time(), "intended": dict(f), "effective": eff})
        if problems:
            raise ConfigMismatch(f"{self.name}: " + "; ".join(problems))
        return eff
