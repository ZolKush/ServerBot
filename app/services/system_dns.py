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


_HOST_RE = re.compile(
    r"^(?=.{1,253}$)([a-zA-Z0-9_]([a-zA-Z0-9_\-]{0,61}[a-zA-Z0-9_])?)(\.[a-zA-Z0-9_]([a-zA-Z0-9_\-]{0,61}[a-zA-Z0-9_])?)*$"
)


async def resolve_a_record(domain: str, resolver: str | None = None, timeout: float = 2.0) -> list[str]:
    dom = (domain or "").strip()
    if not dom or not _HOST_RE.fullmatch(dom):
        return []

    if aiodns is not None:
        try:
            if resolver:
                async with aiodns.DNSResolver(nameservers=[resolver], timeout=timeout) as res:
                    answer = await res.query_dns(dom, "A")
            else:
                async with aiodns.DNSResolver(timeout=timeout) as res:
                    answer = await res.query_dns(dom, "A")
            ips = [str(address) for record in answer.answer if (address := getattr(record.data, "addr", None))]
            return list(dict.fromkeys(ips))
        except Exception as e:
            if resolver:
                logger.debug("DNS resolve %s via %s failed: %s", dom, resolver, e)
                return []
            logger.debug("DNS resolve %s via aiodns failed, fallback to getaddrinfo: %s", dom, e)

    try:
        lookup = asyncio.get_running_loop().getaddrinfo(dom, None, family=socket.AF_INET)
        infos = await asyncio.wait_for(lookup, timeout=max(0.1, float(timeout)))
        found: list[str] = []
        for info in infos:
            addr = info[4]
            if addr and addr[0] not in found:
                found.append(str(addr[0]))
        return found
    except Exception as e:
        logger.debug("DNS resolve %s via getaddrinfo failed: %s", dom, e)
        return []
