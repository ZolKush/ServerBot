"""Bounded asynchronous A/AAAA resolution for monitoring checks."""

from __future__ import annotations

import asyncio
import logging
import re
import socket

try:
    import aiodns
except Exception:  # pragma: no cover
    aiodns = None  # type: ignore[assignment]

logger = logging.getLogger("maint-bot")

_HOST_RE = re.compile(
    r"^(?=.{1,253}$)([a-zA-Z0-9_]([a-zA-Z0-9_\-]{0,61}[a-zA-Z0-9_])?)"
    r"(\.[a-zA-Z0-9_]([a-zA-Z0-9_\-]{0,61}[a-zA-Z0-9_])?)*$"
)


def dns_supports_custom_resolver() -> bool:
    return aiodns is not None


async def resolve_a_record(domain: str, resolver: str | None = None, timeout: float = 2.0) -> list[str]:
    return await _resolve_record(domain, resolver, timeout, record_type="A", family=socket.AF_INET)


async def resolve_aaaa_record(domain: str, resolver: str | None = None, timeout: float = 2.0) -> list[str]:
    return await _resolve_record(domain, resolver, timeout, record_type="AAAA", family=socket.AF_INET6)


async def _resolve_record(
    domain: str, resolver: str | None, timeout: float, *, record_type: str, family: int
) -> list[str]:
    normalized_domain = (domain or "").strip()
    if not normalized_domain or not _HOST_RE.fullmatch(normalized_domain):
        return []
    deadline = asyncio.get_running_loop().time() + max(0.1, timeout)

    if aiodns is not None:
        try:
            async with aiodns.DNSResolver(
                nameservers=[resolver] if resolver else None, timeout=timeout
            ) as dns_resolver:
                answer = await asyncio.wait_for(
                    dns_resolver.query_dns(normalized_domain, record_type), timeout=max(0.1, timeout)
                )
            addresses = [str(address) for record in answer.answer if (address := getattr(record.data, "addr", None))]
            return list(dict.fromkeys(addresses))
        except Exception as exc:
            if resolver:
                logger.debug(
                    "DNS resolve %s via %s failed: %s",
                    normalized_domain,
                    resolver,
                    exc,
                )
                return []
            logger.debug(
                "DNS resolve %s via aiodns failed, fallback to getaddrinfo: %s",
                normalized_domain,
                exc,
            )

    try:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return []
        lookup = asyncio.get_running_loop().getaddrinfo(
            normalized_domain,
            None,
            family=family,
        )
        infos = await asyncio.wait_for(lookup, timeout=remaining)
        found: list[str] = []
        for info in infos:
            address = info[4]
            if address and address[0] not in found:
                found.append(str(address[0]))
        return found
    except Exception as exc:
        logger.debug(
            "DNS resolve %s via getaddrinfo failed: %s",
            normalized_domain,
            exc,
        )
        return []


__all__ = ["dns_supports_custom_resolver", "resolve_a_record", "resolve_aaaa_record"]
