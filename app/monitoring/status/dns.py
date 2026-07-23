"""DNS status collection and persistent cache projection."""

from __future__ import annotations

import asyncio
from datetime import datetime

from ...bot.ui import html_escape, ui_info_text
from ...config import DNS_RESOLVERS, TZ, ServerTarget
from ...monitoring.system.dns import (
    dns_supports_custom_resolver,
    resolve_a_record,
)
from ...storage import get_dns_status_cache
from .common import safe_nonnegative_int

_DNS_QUERY_SEMAPHORE: asyncio.Semaphore | None = None


def dns_query_semaphore() -> asyncio.Semaphore:
    global _DNS_QUERY_SEMAPHORE
    if _DNS_QUERY_SEMAPHORE is None:
        _DNS_QUERY_SEMAPHORE = asyncio.Semaphore(16)
    return _DNS_QUERY_SEMAPHORE


async def build_dns_status_payload_live(
    server: ServerTarget,
) -> dict[str, object]:
    domains = list(server.check_a_domains)
    if not domains:
        return {
            "server_key": server.key,
            "updated_at": datetime.now(TZ).isoformat(),
            "total": 0,
            "ok": 0,
            "bad": 0,
            "unknown": 0,
            "details": [],
        }
    expected_ip = (server.expected_a_ip or "").strip()
    custom_resolvers_supported = dns_supports_custom_resolver()

    async def resolve_limited(domain: str, resolver: str | None) -> list[str]:
        async with dns_query_semaphore():
            return await resolve_a_record(domain, resolver=resolver)

    async def check_domain(domain: str) -> tuple[str, str | None]:
        if custom_resolvers_supported and DNS_RESOLVERS:
            results = await asyncio.gather(
                *(resolve_limited(domain, resolver) for resolver in DNS_RESOLVERS),
                return_exceptions=True,
            )
            ip_lists = [result for result in results if isinstance(result, list)]
            merged: list[str] = []
            for addresses in ip_lists:
                for address in addresses:
                    if address not in merged:
                        merged.append(address)
        else:
            try:
                merged = await resolve_limited(domain, None)
            except Exception:
                merged = []
        if not merged:
            return "unknown", f"• <code>{html_escape(domain)}</code>: ⚠️ нет ответа"
        if expected_ip and expected_ip not in merged:
            shown = merged[:10]
            suffix = f", … ещё {len(merged) - len(shown)}" if len(merged) > len(shown) else ""
            return (
                "bad",
                f"• <code>{html_escape(domain)}</code>: 🔴 ожидался "
                f"<code>{html_escape(expected_ip)}</code>, получено "
                f"<code>{html_escape(', '.join(shown))}</code>{html_escape(suffix)}",
            )
        return "ok", None

    checks = await asyncio.gather(*(check_domain(domain) for domain in domains))
    return {
        "server_key": server.key,
        "updated_at": datetime.now(TZ).isoformat(),
        "total": len(domains),
        "ok": sum(1 for status, _ in checks if status == "ok"),
        "bad": sum(1 for status, _ in checks if status == "bad"),
        "unknown": sum(1 for status, _ in checks if status == "unknown"),
        "details": [detail for _, detail in checks if detail],
    }


def dns_payload_from_cache_or_empty(server: ServerTarget) -> dict[str, object]:
    raw = get_dns_status_cache(server.key) or {}
    details = raw.get("details", [])
    if not isinstance(details, list):
        details = []
    total = safe_nonnegative_int(raw.get("total"))
    if total <= 0 and server.check_a_domains:
        return {
            "total": len(server.check_a_domains),
            "ok": 0,
            "bad": 0,
            "unknown": 0,
            "details": [
                f"• {html_escape(ui_info_text('DNS статус ещё не обновлялся. Нажмите «Обновить DNS статус».'))}"
            ],
        }
    return {
        "total": total,
        "ok": safe_nonnegative_int(raw.get("ok")),
        "bad": safe_nonnegative_int(raw.get("bad")),
        "unknown": safe_nonnegative_int(raw.get("unknown")),
        "details": [str(value)[:500] for value in details],
    }
