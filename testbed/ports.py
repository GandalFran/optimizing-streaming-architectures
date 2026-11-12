"""Local loopback port-block allocator for the executable-validation testbed."""
from __future__ import annotations

import socket


SPAN = 200
BASES = range(12000, 32000 - SPAN, 500)


def ephemeral_range() -> tuple[int, int]:
    try:
        lo, hi = open("/proc/sys/net/ipv4/ip_local_port_range").read().split()
        return int(lo), int(hi)
    except (OSError, ValueError):
        return 32768, 60999


def check_ports(base: int, span: int = SPAN - 1) -> None:
    """Require a free, non-ephemeral loopback port block."""
    lo, hi = ephemeral_range()
    if not (base + span < lo or base > hi):
        raise ValueError(f"port block {base}-{base + span} overlaps the ephemeral range {lo}-{hi}")
    busy = []
    for port in range(base, base + span):
        with socket.socket() as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("127.0.0.1", port))
            except OSError:
                busy.append(port)
    if busy:
        raise RuntimeError(f"ports already in use in block {base}-{base + span}: {busy[:10]}")


def acquire_port_block(preferred: int = 0):
    """Reserve a loopback port block and return its base and sentinel socket."""
    candidates = [preferred] if preferred else list(BASES)
    for base in candidates:
        sentinel = socket.socket()
        try:
            sentinel.bind(("127.0.0.1", base + SPAN - 1))
            sentinel.listen(1)
            check_ports(base)
            return base, sentinel
        except (OSError, RuntimeError, ValueError):
            sentinel.close()
            if preferred:
                raise
    raise RuntimeError("no free port block on this node")
