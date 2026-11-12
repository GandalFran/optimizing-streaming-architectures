"""
Per-second system monitor: component resources and interference indicators.

Component accounting reads utime+stime and RSS from /proc for each tracked
process tree, so CPU is in core-seconds per second and memory in MiB (the
first version reported JVM CPU *load*, a 0-1 fraction, as "cores").

Interference indicators matter because the host is shared with other users'
jobs. Every run records:
  * PSI (pressure stall information) for CPU, memory and IO, both for this
    job's cgroup and for the whole host -- the fraction of time tasks were
    stalled waiting for a resource;
  * utilisation of the RAID device holding broker logs and Flink state;
  * load average.
A run whose indicators exceed the validity thresholds is flagged by the
analysis rather than silently pooled with clean runs.
"""
from __future__ import annotations
import json, os, threading, time
from pathlib import Path

CLK = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
PAGE = os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else 4096


def _proc_stat(pid: int):
    try:
        with open(f"/proc/{pid}/stat") as f:
            parts = f.read().rsplit(") ", 1)[1].split()
        return (int(parts[11]) + int(parts[12])) / CLK, int(parts[21]) * PAGE
    except (OSError, IndexError, ValueError):
        return None


def _children(pid: int) -> list[int]:
    out = []
    try:
        for tid in os.listdir(f"/proc/{pid}/task"):
            try:
                with open(f"/proc/{pid}/task/{tid}/children") as f:
                    out.extend(int(x) for x in f.read().split())
            except OSError:
                pass
    except OSError:
        pass
    return out


def _tree(pid: int, depth: int = 0) -> list[int]:
    if depth > 8:
        return [pid]
    out = [pid]
    for c in _children(pid):
        out.extend(_tree(c, depth + 1))
    return out


def _psi(path: str) -> dict:
    out = {}
    try:
        with open(path) as f:
            for line in f:
                kind, *fields = line.split()
                vals = dict(x.split("=") for x in fields)
                out[kind] = {"avg10": float(vals["avg10"]), "total_us": int(vals["total"])}
    except OSError:
        pass
    return out


def _proc_stat_cpu():
    """Aggregate host CPU jiffies: (busy, iowait, total)."""
    try:
        with open("/proc/stat") as f:
            parts = f.readline().split()[1:]
        v = [int(x) for x in parts[:8]]
        user, nice, system, idle, iowait, irq, softirq, steal = v
        busy = user + nice + system + irq + softirq + steal
        return busy, iowait, busy + idle + iowait
    except (OSError, ValueError):
        return None


def _job_cgroup() -> str | None:
    try:
        with open("/proc/self/cgroup") as f:
            for line in f:
                if line.startswith("0::"):
                    rel = line.strip()[3:]
                    # climb to the job level (…/job_N), not this step/task
                    parts = rel.split("/")
                    for i in range(len(parts), 0, -1):
                        if parts[i - 1].startswith("job_"):
                            return "/sys/fs/cgroup" + "/".join(parts[:i])
                    return "/sys/fs/cgroup" + rel
    except OSError:
        pass
    return None


def _diskstats(devices: tuple[str, ...]) -> dict:
    out = {}
    try:
        with open("/proc/diskstats") as f:
            for line in f:
                p = line.split()
                if p[2] in devices:
                    # field 13 (index 12): ms spent doing IO
                    out[p[2]] = {"io_ms": int(p[12]), "rd_sect": int(p[5]),
                                 "wr_sect": int(p[9])}
    except OSError:
        pass
    return out


class SysMon(threading.Thread):
    def __init__(self, out: Path, interval: float = 1.0, devices=("md0",)):
        super().__init__(daemon=True)
        self.out = Path(out)
        self.interval = interval
        self.devices = tuple(devices)
        self.components: dict[str, int] = {}
        self.groups: dict[str, str] = {}          # component -> group label
        self._prev: dict[str, tuple[float, float]] = {}
        self._prev_disk: dict | None = None
        self._prev_host = None
        self._stop = threading.Event()
        self.cg = _job_cgroup()
        self.lock = threading.Lock()
        self.latest: dict = {}

    def track(self, name: str, pid: int, group: str) -> None:
        with self.lock:
            self.components[name] = pid
            self.groups[name] = group
            self._prev.pop(name, None)

    def untrack(self, name: str) -> None:
        with self.lock:
            self.components.pop(name, None)
            self.groups.pop(name, None)
            self._prev.pop(name, None)

    def stop(self) -> None:
        self._stop.set()

    def sample(self) -> dict:
        now = time.time()
        row: dict = {"t": round(now, 3), "c": {}}
        with self.lock:
            items = list(self.components.items())
        for name, pid in items:
            cpu, rss = 0.0, 0
            for p in _tree(pid):
                st = _proc_stat(p)
                if st:
                    cpu += st[0]
                    rss += st[1]
            prev = self._prev.get(name)
            cores = (cpu - prev[0]) / (now - prev[1]) if prev and now > prev[1] else None
            self._prev[name] = (cpu, now)
            row["c"][name] = {"cores": None if cores is None else round(max(0.0, cores), 4),
                              "rss_mb": round(rss / 1048576, 1),
                              "g": self.groups.get(name, "")}
        # PSI is compiled out / disabled on RHEL 9 kernels unless booted with
        # psi=1; keep it when present, and always record host CPU from
        # /proc/stat. Other jobs cannot run on this job's cores (SLURM cpuset),
        # so host-level numbers indicate shared-resource pressure (disk,
        # memory bandwidth), not CPU theft.
        psi = {k: _psi(f"/proc/pressure/{k}") for k in ("cpu", "memory", "io")}
        if any(psi.values()):
            row["psi_host"] = psi
        if self.cg:
            pj = {k: _psi(f"{self.cg}/{k}.pressure") for k in ("cpu", "memory", "io")}
            if any(pj.values()):
                row["psi_job"] = pj
        hs = _proc_stat_cpu()
        if hs and self._prev_host:
            db, di, dt = (hs[0] - self._prev_host[0], hs[1] - self._prev_host[1],
                          hs[2] - self._prev_host[2])
            if dt > 0:
                row["host_cpu"] = {"busy": round(db / dt, 4), "iowait": round(di / dt, 4)}
        self._prev_host = hs
        disk = _diskstats(self.devices)
        if self._prev_disk is not None:
            row["disk"] = {}
            for d, v in disk.items():
                pv = self._prev_disk["v"].get(d)
                dt = now - self._prev_disk["t"]
                if pv and dt > 0:
                    row["disk"][d] = {
                        "util": round(min(1.0, (v["io_ms"] - pv["io_ms"]) / (dt * 1000)), 4),
                        "wr_mb_s": round((v["wr_sect"] - pv["wr_sect"]) * 512 / 1048576 / dt, 2),
                        "rd_mb_s": round((v["rd_sect"] - pv["rd_sect"]) * 512 / 1048576 / dt, 2)}
        self._prev_disk = {"t": now, "v": disk}
        try:
            row["load1"] = float(open("/proc/loadavg").read().split()[0])
        except OSError:
            pass
        return row

    def run(self) -> None:
        self.out.parent.mkdir(parents=True, exist_ok=True)
        with open(self.out, "a", buffering=1) as f:
            while not self._stop.is_set():
                t0 = time.time()
                try:
                    row = self.sample()
                    self.latest = row
                    f.write(json.dumps(row) + "\n")
                except Exception as exc:
                    f.write(json.dumps({"t": round(t0, 3), "error": str(exc)[:200]}) + "\n")
                self._stop.wait(max(0.0, self.interval - (time.time() - t0)))
