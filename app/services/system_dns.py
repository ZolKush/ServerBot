import asyncio
import re
import socket
from typing import List, Optional, Tuple

from ..config import PING_BIN
from .system_process import run_exec

try:
    import aiodns  # type: ignore
except Exception:  # pragma: no cover
    aiodns = None


def dns_supports_custom_resolver() -> bool:
    return aiodns is not None


_HOST_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9.\-]{0,252}$")


async def ping_host(host: str, count: int, timeout_sec: int) -> Tuple[bool, Optional[float]]:
    if not host or not _HOST_RE.fullmatch(host):
        return False, None

    args = [PING_BIN, "-c", str(max(1, count)), "-W", str(max(1, timeout_sec)), host]
    rc, out, _ = await run_exec(args, timeout=max(2, timeout_sec * (count + 2)))
    if rc != 0:
        return False, None

    rtt = None
    for line in out.splitlines():
        if "rtt min/avg/max" in line or "round-trip min/avg/max" in line:
            try:
                part = line.split("=")[1].strip().split(" ")[0]
                rtt = float(part.split("/")[1])
            except Exception:
                rtt = None
            break
    return True, rtt


async def resolve_a_record(domain: str, resolver: Optional[str] = None, timeout: float = 2.0) -> List[str]:
    dom = (domain or "").strip()
    if not dom or not _HOST_RE.fullmatch(dom):
        return []

    if aiodns is not None:
        try:
            if resolver:
                res = aiodns.DNSResolver(nameservers=[resolver], timeout=timeout)
            else:
                res = aiodns.DNSResolver(timeout=timeout)
            ans = await res.query(dom, "A")
            ips = [a.host for a in ans if getattr(a, "host", None)]
            return list(dict.fromkeys(ips))
        except Exception:
            pass

    try:
        infos = await asyncio.get_running_loop().getaddrinfo(dom, None, family=socket.AF_INET)
        ips: List[str] = []
        for info in infos:
            addr = info[4]
            if addr and addr[0] not in ips:
                ips.append(addr[0])
        return ips
    except Exception:
        return []
