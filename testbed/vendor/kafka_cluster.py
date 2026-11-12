"""
Three-broker Kafka (KRaft) cluster, one broker per kafka node.

Topics under measurement are created with `message.timestamp.type =
LogAppendTime`: the partition leader stamps every record with its own clock at
append. End-to-end latency is then computed as

    sink-topic append time  -  generator send time (`sent_ms`)

which is exactly "event generation to sink acknowledgment", and -- unlike a
consumer-side timestamp -- does not depend on how fast the measuring consumer
keeps up. Generator, brokers and collector share one host clock, so the
subtraction involves no cross-machine skew.

Broker thread pools and topic-level settings are altered dynamically through
the admin API, and every change is read back before it is trusted.
"""
from __future__ import annotations
import os, re, signal, subprocess, time
from dataclasses import dataclass, field
from pathlib import Path

OPT = Path(os.environ.get("TUNING_OPT", Path(__file__).resolve().parents[1] / "runtime"))
JAVA_HOME = OPT / "jdk17"
KAFKA = OPT / "kafka"


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
class KafkaCluster:
    nodes: list                  # topology.Node, role == "kafka"
    broker_ports: list[int]
    controller_ports: list[int]
    run_dir: Path
    config: dict                 # the "kafka" section
    procs: dict = field(default_factory=dict)
    cluster_id: str = ""

    @property
    def bootstrap(self) -> str:
        return ",".join(f"127.0.0.1:{p}" for p in self.broker_ports)

    def env(self) -> dict:
        e = dict(os.environ)
        e["JAVA_HOME"] = str(JAVA_HOME)
        e["PATH"] = f"{JAVA_HOME}/bin:{e.get('PATH', '')}"
        return e

    def _bin(self, tool: str) -> str:
        return str(KAFKA / "bin" / tool)

    def _run(self, argv: list[str], timeout: float = 120.0) -> subprocess.CompletedProcess:
        return subprocess.run(argv, capture_output=True, text=True,
                              env=self.env(), timeout=timeout)

    # -------------------------------------------------------------- brokers
    def _props(self, i: int) -> Path:
        k, node = self.config, self.nodes[i]
        n = len(self.nodes)
        quorum = ",".join(f"{j + 1}@127.0.0.1:{self.controller_ports[j]}" for j in range(n))
        data = Path(node.state_dir) / "kafka-data"
        data.mkdir(parents=True, exist_ok=True)
        props = "\n".join([
            "process.roles=broker,controller",
            f"node.id={i + 1}",
            f"controller.quorum.voters={quorum}",
            f"listeners=PLAINTEXT://127.0.0.1:{self.broker_ports[i]},"
            f"CONTROLLER://127.0.0.1:{self.controller_ports[i]}",
            f"advertised.listeners=PLAINTEXT://127.0.0.1:{self.broker_ports[i]}",
            "inter.broker.listener.name=PLAINTEXT",
            "controller.listener.names=CONTROLLER",
            "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
            f"log.dirs={data}",
            f"num.partitions={k['partitions']}",
            f"default.replication.factor={k['replication_factor']}",
            f"min.insync.replicas={k['min_insync_replicas']}",
            f"offsets.topic.replication.factor={min(3, n)}",
            f"transaction.state.log.replication.factor={min(3, n)}",
            f"transaction.state.log.min.isr={min(2, n)}",
            f"log.retention.ms={k['retention_ms']}",
            f"num.io.threads={k['num_io_threads']}",
            f"num.network.threads={k['num_network_threads']}",
            # Brokers keep whatever compression the producer used; the tunable
            # compression setting acts producer-side, in the Flink sink.
            "compression.type=producer",
            "auto.create.topics.enable=false",
            "group.initial.rebalance.delay.ms=0",
            "log.segment.bytes=268435456",
        ]) + "\n"
        path = self.run_dir / "conf" / f"broker{i}.properties"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(props)
        return path

    def start(self) -> None:
        if not self.cluster_id:
            self.cluster_id = self._run([self._bin("kafka-storage.sh"), "random-uuid"]).stdout.strip()
        heap = 4096
        logs = self.run_dir / "logs"
        logs.mkdir(parents=True, exist_ok=True)
        for i, node in enumerate(self.nodes):
            props = self._props(i)
            r = self._run([self._bin("kafka-storage.sh"), "format", "--cluster-id",
                           self.cluster_id, "--config", str(props), "--ignore-formatted"])
            if r.returncode != 0:
                raise RuntimeError(f"format broker{i}: {r.stderr[-800:]}")
            env = self.env()
            env["KAFKA_HEAP_OPTS"] = f"-Xmx{heap}m -Xms{heap}m"
            env["LOG_DIR"] = str(logs / f"kafka{i}")
            out = open(logs / f"kafka{i}.out", "ab")
            self.procs[f"kafka{i}"] = subprocess.Popen(
                node.wrap([self._bin("kafka-server-start.sh"), str(props)]),
                stdout=out, stderr=subprocess.STDOUT, env=env, start_new_session=True)
        for i, p in enumerate(self.broker_ports):
            try:
                wait_port(p, 120, f"kafka broker {i}")
            except TimeoutError as exc:
                proc = self.procs.get(f"kafka{i}")
                state = "exited rc=%s" % proc.poll() if proc and proc.poll() is not None else "still running"
                tail = ""
                logf = logs / f"kafka{i}.out"
                if logf.exists():
                    tail = logf.read_text(errors="replace")[-2500:]
                raise TimeoutError(f"{exc}; broker process {state}; log tail:\n{tail}") from None
        self._wait_quorum()

    def _wait_quorum(self, timeout: float = 90.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            r = self._run([self._bin("kafka-broker-api-versions.sh"),
                           "--bootstrap-server", self.bootstrap], timeout=30)
            if r.returncode == 0 and r.stdout.count("(id:") >= len(self.nodes):
                return
            time.sleep(2)
        raise TimeoutError("kafka brokers did not all register")

    def stop(self) -> None:
        for k, p in list(self.procs.items()):
            if p.poll() is None:
                try:
                    os.killpg(os.getpgid(p.pid), signal.SIGTERM)
                except ProcessLookupError:
                    pass
        for k, p in list(self.procs.items()):
            try:
                p.wait(timeout=30)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
        self.procs.clear()

    def pids(self) -> dict[str, int]:
        return {k: p.pid for k, p in self.procs.items() if p.poll() is None}

    # --------------------------------------------------------------- topics
    def create_topic(self, name: str, partitions: int, rf: int,
                     configs: dict | None = None, log_append_time: bool = True) -> None:
        cfg = dict(configs or {})
        if log_append_time:
            cfg["message.timestamp.type"] = "LogAppendTime"
        argv = [self._bin("kafka-topics.sh"), "--create", "--if-not-exists",
                "--bootstrap-server", self.bootstrap, "--topic", name,
                "--partitions", str(partitions), "--replication-factor", str(rf)]
        for k, v in cfg.items():
            argv += ["--config", f"{k}={v}"]
        r = self._run(argv)
        if r.returncode != 0 and "already exists" not in r.stderr:
            raise RuntimeError(f"create {name}: {r.stderr[-800:]}")

    def describe_topic(self, name: str) -> dict:
        r = self._run([self._bin("kafka-topics.sh"), "--describe",
                       "--bootstrap-server", self.bootstrap, "--topic", name])
        if r.returncode != 0:
            raise RuntimeError(f"describe {name}: {r.stderr[-500:]}")
        m = re.search(r"PartitionCount:\s*(\d+)\s+ReplicationFactor:\s*(\d+)", r.stdout)
        cfgs = dict(re.findall(r"([a-z.]+)=([^,\s]+)", r.stdout.split("Configs:", 1)[-1].split("\n")[0]))
        return {"partitions": int(m.group(1)), "replication_factor": int(m.group(2)),
                "configs": cfgs}

    def add_partitions(self, name: str, partitions: int) -> None:
        # A shrink is sent to the broker on purpose: whether it is refused is
        # the system's decision, not the harness's, so a variant without the
        # gate meets the same failure a real deployment would.
        if partitions == self.describe_topic(name)["partitions"]:
            return
        r = self._run([self._bin("kafka-topics.sh"), "--alter", "--bootstrap-server",
                       self.bootstrap, "--topic", name, "--partitions", str(partitions)])
        if r.returncode != 0:
            raise RuntimeError(f"alter partitions {name}: {r.stderr[-500:]}")

    def alter_topic(self, name: str, configs: dict) -> None:
        if not configs:
            return
        add = ",".join(f"{k}={v}" for k, v in configs.items())
        r = self._run([self._bin("kafka-configs.sh"), "--bootstrap-server", self.bootstrap,
                       "--alter", "--entity-type", "topics", "--entity-name", name,
                       "--add-config", add])
        if r.returncode != 0:
            raise RuntimeError(f"alter topic {name}: {r.stderr[-500:]}")

    def alter_brokers(self, configs: dict) -> None:
        """Cluster-wide dynamic broker config (thread pools resize in place)."""
        if not configs:
            return
        add = ",".join(f"{k}={v}" for k, v in configs.items())
        r = self._run([self._bin("kafka-configs.sh"), "--bootstrap-server", self.bootstrap,
                       "--alter", "--entity-type", "brokers", "--entity-default",
                       "--add-config", add])
        if r.returncode != 0:
            raise RuntimeError(f"alter brokers: {r.stderr[-500:]}")

    def broker_config(self) -> dict:
        r = self._run([self._bin("kafka-configs.sh"), "--bootstrap-server", self.bootstrap,
                       "--describe", "--entity-type", "brokers", "--entity-default"])
        return dict(re.findall(r"([a-z.]+)=([^\s]+)", r.stdout))

    # ------------------------------------------------------------ verification
    def verify(self, topics: list[str]) -> dict:
        k = self.config
        out, problems = {"topics": {}, "brokers": self.broker_config()}, []
        for t in topics:
            d = self.describe_topic(t)
            out["topics"][t] = d
            if d["configs"].get("message.timestamp.type") != "LogAppendTime":
                problems.append(f"{t}: timestamp type is not LogAppendTime")
        src = topics[0]
        if out["topics"][src]["partitions"] != int(k["partitions"]):
            problems.append(f"{src}: {out['topics'][src]['partitions']} partitions "
                            f"!= {k['partitions']}")
        io = out["brokers"].get("num.io.threads")
        if io is not None and int(io) != int(k["num_io_threads"]):
            problems.append(f"num.io.threads {io} != {k['num_io_threads']}")
        out["verified"], out["problems"] = not problems, problems
        if problems:
            from flink_cluster import ConfigMismatch
            raise ConfigMismatch("kafka: " + "; ".join(problems))
        return out
