import asyncio
import logging
import re
import socket

try:
    import aiodns
except Exception:  # pragma: no cover
    aiodns = None  # type: ignore[assignment]

logger = logging.getLogger("maint-bot")


def dns_supports_custom_resolver() -> bool:
    return aiodns is not None


_HOST_RE = re.compile(r"^(?=.{1,253}$)([a-zA-Z0-9_]([a-zA-Z0-9_\-]{0,61}[a-zA-Z0-9_])?)(\.[a-zA-Z0-9_]([a-zA-Z0-9_\-]{0,61}[a-zA-Z0-9_])?)*$")


async def resolve_a_record(domain: str, resolver: str | None = None, timeout: float = 2.0) -> list[str]:
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
        except Exception as e:
            if resolver:
                logger.debug("DNS resolve %s via %s failed: %s", dom, resolver, e)
                return []
            logger.debug("DNS resolve %s via aiodns failed, fallback to getaddrinfo: %s", dom, e)

    try:
        infos = await asyncio.get_running_loop().getaddrinfo(dom, None, family=socket.AF_INET)
        found: list[str] = []
        for info in infos:
            addr = info[4]
            if addr and addr[0] not in found:
                found.append(str(addr[0]))
        return found
    except Exception as e:
        logger.debug("DNS resolve %s via getaddrinfo failed: %s", dom, e)
        return []
