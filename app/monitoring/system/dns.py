"""Asynchronous IPv4 DNS resolution for monitoring checks."""

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
    normalized_domain = (domain or "").strip()
    if not normalized_domain or not _HOST_RE.fullmatch(normalized_domain):
        return []

    if aiodns is not None:
        try:
            if resolver:
                async with aiodns.DNSResolver(nameservers=[resolver], timeout=timeout) as dns_resolver:
                    answer = await dns_resolver.query_dns(normalized_domain, "A")
            else:
                async with aiodns.DNSResolver(timeout=timeout) as dns_resolver:
                    answer = await dns_resolver.query_dns(normalized_domain, "A")
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
        lookup = asyncio.get_running_loop().getaddrinfo(
            normalized_domain,
            None,
            family=socket.AF_INET,
        )
        infos = await asyncio.wait_for(lookup, timeout=max(0.1, float(timeout)))
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


__all__ = ["dns_supports_custom_resolver", "resolve_a_record"]
