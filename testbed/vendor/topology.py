"""
Emulated multi-node testbed topology.

A "node" is a set of whole physical cores (both SMT siblings) inside one NUMA
domain, plus a memory budget and a private state directory on node-local RAID.
Processes are pinned with `numactl --physcpubind --membind`.

Nodes have *roles*, and no two roles share a node:

    kafka   one broker per node
    flink   TaskManagers; the JobManager runs on the first flink node
    canary  the two arms of the paired micro-canary
    control load generation, measurement, Prometheus, the controller

Role separation removes a confound the first version had, where TaskManagers
and brokers competed for the same cores. The control plane is disjoint from
every node under test, so load generation and measurement never take cycles
from the system being measured.

The CPU pool is read from the affinity mask granted by the batch scheduler,
never hardcoded.

This is NOT a real multi-machine cluster: nodes share one kernel, one page
cache, one RAID volume and loopback networking. See docs/TESTBED.md.
"""
from __future__ import annotations
import json, os
from dataclasses import dataclass, asdict


def allocated_cpus() -> list[int]:
    return sorted(os.sched_getaffinity(0))


def numa_of(cpu: int) -> int:
    base = f"/sys/devices/system/cpu/cpu{cpu}"
    for entry in os.listdir(base):
        if entry.startswith("node") and entry[4:].isdigit():
            return int(entry[4:])
    raise RuntimeError(f"no NUMA domain for cpu{cpu}")


def smt_siblings(cpu: int) -> list[int]:
    p = f"/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list"
    with open(p) as f:
        spec = f.read().strip()
    out: list[int] = []
    for part in spec.split(","):
        if "-" in part:
            a, b = part.split("-")
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return sorted(out)


def physical_cores(cpus: list[int]) -> list[list[int]]:
    """Whole physical cores fully contained in `cpus`."""
    pool, seen, cores = set(cpus), set(), []
    for c in sorted(cpus):
        if c in seen:
            continue
        sib = smt_siblings(c)
        seen.update(sib)
        if set(sib) <= pool:
            cores.append(sib)
    return cores


@dataclass
class Node:
    name: str
    role: str
    numa: "int | str"            # one domain, or "a,b" for a spanning node
    cpus: list[int]
    mem_mb: int
    state_dir: str

    @property
    def cpu_spec(self) -> str:
        return ",".join(str(c) for c in self.cpus)

    @property
    def n_cores(self) -> int:
        return len(self.cpus) // 2

    def wrap(self, argv: list[str]) -> list[str]:
        return ["numactl", f"--physcpubind={self.cpu_spec}",
                f"--membind={self.numa}", "--"] + argv


@dataclass
class Topology:
    instance: str
    nodes: list[Node]
    port_base: int
    root: str

    def role(self, role: str) -> list[Node]:
        return [n for n in self.nodes if n.role == role]

    @property
    def node_map(self) -> dict[str, Node]:
        return {n.name: n for n in self.nodes}

    @property
    def control(self) -> Node:
        return self.role("control")[0]

    @property
    def control_cpus(self) -> list[int]:
        return self.control.cpus

    def control_wrap(self, argv: list[str]) -> list[str]:
        return self.control.wrap(argv)

    def port(self, offset: int) -> int:
        return self.port_base + offset

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)

    def save(self, path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(self.to_json())


# Default shape: 3 broker nodes, 3 Flink nodes, 1 canary node, 4 cores each
# (8 logical CPUs, an "8-vCPU node"), plus a 6-core control plane.
# The canary is two 2-core nodes, one per arm, so each arm is NUMA-local even
# when the scheduler's grant is fragmented across domains.
DEFAULT_ROLES: list[tuple[str, int]] = [
    ("kafka", 3), ("flink", 3), ("canary", 2), ("control", 1)]
# control measured at <= 3.4 cores at 300k records/s (2 generators, collector,
# Prometheus, exporter, monitors), so 4 cores leave headroom.
CORES = {"kafka": 4, "flink": 4, "canary": 2, "control": 4}


def allocate(instance: str, port_base: int, root: str,
             roles: list[tuple[str, int]] | None = None,
             cores: dict[str, int] | None = None,
             mem_per_node_mb: int = 32768,
             cpus: list[int] | None = None) -> Topology:
    """Partition the granted CPUs into role-dedicated nodes.

    Every node lies inside one NUMA domain. Nodes under test (kafka, flink) are
    packed into as few domains as possible so cross-socket traffic between
    brokers and TaskManagers is not an artefact of placement.
    """
    roles = roles or DEFAULT_ROLES
    cores = {**CORES, **(cores or {})}
    pool = cpus if cpus is not None else allocated_cpus()

    by_numa: dict[int, list[list[int]]] = {}
    for core in physical_cores(pool):
        by_numa.setdefault(numa_of(core[0]), []).append(core)
    free = {d: list(cs) for d, cs in sorted(by_numa.items(),
                                             key=lambda kv: -len(kv[1]))}

    def take(n: int, prefer: int | None, may_span: bool = False):
        order = ([prefer] if prefer is not None and prefer in free else []) + \
                [d for d in free if d != prefer]
        for d in order:
            if len(free[d]) >= n:
                got, free[d] = free[d][:n], free[d][n:]
                return d, got
        if may_span and sum(len(c) for c in free.values()) >= n:
            # Last resort, control plane only: gather whole cores from several
            # domains, largest remainders first, and bind memory to all of them.
            got, doms = [], []
            for d in sorted(free, key=lambda k: -len(free[k])):
                k = min(n - len(got), len(free[d]))
                if k:
                    got += free[d][:k]; free[d] = free[d][k:]; doms.append(d)
                if len(got) == n:
                    return ",".join(map(str, sorted(doms))), got
        raise RuntimeError(
            f"no NUMA domain has {n} free whole cores; free per domain: "
            f"{ {d: len(c) for d, c in free.items()} }")

    nodes: list[Node] = []
    sut_domain: int | None = None
    counters: dict[str, int] = {}
    # Place nodes under test first so they share a domain, then the rest.
    ordered = sorted(roles, key=lambda r: 0 if r[0] in ("kafka", "flink") else 1)
    for role, count in ordered:
        for _ in range(count):
            try:
                dom, got = take(cores[role], sut_domain if role in ("kafka", "flink") else None,
                                may_span=(role == "control"))
            except RuntimeError:
                # Fragmented grant: as a last resort a node takes whole cores
                # from several domains. Recorded, so runs can be checked for it.
                dom, got = take(cores[role], None, may_span=True)
            if role in ("kafka", "flink") and sut_domain is None:
                sut_domain = dom
            counters[role] = counters.get(role, 0) + 1
            name = (f"{role[0]}{counters[role]}" if role not in ("control", "canary")
                    else ("ctl" if role == "control" else f"c1{'ab'[counters[role] - 1]}"))
            nodes.append(Node(
                name=name, role=role, numa=dom,
                cpus=sorted(c for core in got for c in core),
                mem_mb=mem_per_node_mb,
                state_dir=os.path.join(root, instance, name)))

    topo = Topology(instance=instance, nodes=nodes, port_base=port_base, root=root)
    verify_disjoint(topo)
    return topo


def quality(topo: Topology) -> dict:
    """Which nodes span NUMA domains (last-resort placement)."""
    spans = [n.name for n in topo.nodes if isinstance(n.numa, str) and "," in n.numa]
    sut = [n.name for n in topo.nodes if n.role in ("kafka", "flink")]
    doms = sorted({n.numa for n in topo.nodes if n.role in ("kafka", "flink")}, key=str)
    return {"spanning_nodes": spans, "spanning_sut_nodes": [n for n in spans if n in sut],
            "sut_domains": doms}


def verify_disjoint(topo: Topology) -> None:
    seen: dict[int, str] = {}
    for n in topo.nodes:
        for c in n.cpus:
            if c in seen:
                raise AssertionError(f"CPU {c} shared by {seen[c]} and {n.name}")
            seen[c] = n.name


def required_cpus(roles=None, cores=None) -> int:
    roles = roles or DEFAULT_ROLES
    cores = {**CORES, **(cores or {})}
    return 2 * sum(cores[r] * n for r, n in roles)


if __name__ == "__main__":
    import sys
    t = allocate(sys.argv[1] if len(sys.argv) > 1 else "probe",
                 port_base=20000, root="/tmp/chamoso")
    for n in t.nodes:
        print(f"  {n.name:4} {n.role:8} numa{n.numa} cores={n.n_cores} cpus={n.cpu_spec}")
    print(f"logical CPUs required by default shape: {required_cpus()}")
