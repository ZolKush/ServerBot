from __future__ import annotations

import asyncio
import html
from datetime import datetime, timezone
from typing import Any

from ...config import SERVERS, logger
from ...messaging.outbox import message_payload
from ...storage import (
    ImportantData,
    authorized_users_snapshot,
    enqueue_important_outbox,
    make_outbox_event,
    tls_certificates_snapshot,
    update_important_data,
)
from .checks import TLS_EXPIRY_WARNING, TLS_PORT, check_tls_endpoint

TLS_CHECK_CONCURRENCY = 6


def configured_tls_endpoints() -> dict[tuple[str, int], list[str]]:
    endpoints: dict[tuple[str, int], list[str]] = {}
    for server_key, server in SERVERS.items():
        for raw_domain in server.check_a_domains:
            domain = str(raw_domain or "").strip().lower().rstrip(".")
            if not domain:
                continue
            try:
                ascii_domain = domain.encode("idna").decode("ascii")
            except UnicodeError:
                logger.warning("TLS certificate check skipped invalid domain=%s", domain)
                continue
            endpoint = (ascii_domain, TLS_PORT)
            endpoints.setdefault(endpoint, []).append(server_key)
    return {endpoint: list(dict.fromkeys(server_keys)) for endpoint, server_keys in endpoints.items()}


def _approved_admin_ids() -> list[int]:
    result: list[int] = []
    for key, meta in authorized_users_snapshot().items():
        if meta.get("role") != "admin" or meta.get("access_state") != "approved" or not bool(meta.get("enabled", True)):
            continue
        try:
            result.append(int(meta.get("user_id", key)))
        except (TypeError, ValueError, OverflowError):
            continue
    return sorted(set(uid for uid in result if uid > 0))


def _alert_text(item: dict[str, Any], level: str) -> str:
    domain = str(item.get("domain") or "-")
    port = int(item.get("port", TLS_PORT) or TLS_PORT)
    not_after = str(item.get("not_after") or "-")
    try:
        end = datetime.fromisoformat(not_after).astimezone(timezone.utc)
        end_text = end.strftime("%d.%m.%Y %H:%M UTC")
    except (TypeError, ValueError):
        end_text = not_after
    if level == "expired":
        heading = "‼️‼️‼️ СЕРТИФИКАТ ПРОСРОЧЕН ‼️‼️‼️"
        detail = "TLS-сертификат уже недействителен. Требуется немедленное обновление."
    else:
        heading = "‼️‼️‼️ СЕРТИФИКАТ ИСТЕКАЕТ ‼️‼️‼️"
        remaining_hours = max(0, int(item.get("remaining_seconds", 0) or 0) // 3600)
        detail = f"До окончания TLS-сертификата осталось не более 3 суток ({remaining_hours} ч.)."
    servers = ", ".join(str(server) for server in item.get("servers", []) if str(server).strip()) or "-"
    return (
        f"<b>{heading}</b>\n\n"
        f"{detail}\n\n"
        f"• Домен: <code>{html.escape(domain)}:{port}</code>\n"
        f"• Серверы: <code>{html.escape(servers)}</code>\n"
        f"• Действителен до: <code>{html.escape(end_text)}</code>"
    )


async def refresh_tls_certificates() -> dict[str, dict[str, Any]]:
    endpoints = configured_tls_endpoints()
    semaphore = asyncio.Semaphore(TLS_CHECK_CONCURRENCY)

    async def check(endpoint: tuple[str, int], server_keys: list[str]) -> tuple[str, dict[str, Any]]:
        domain, port = endpoint
        async with semaphore:
            result = await check_tls_endpoint(domain, port, server_keys)
        return f"{domain}:{port}", result

    checked = await asyncio.gather(*(check(endpoint, keys) for endpoint, keys in endpoints.items()))
    fresh = dict(checked)
    admin_ids = _approved_admin_ids()

    def save(aggregate: ImportantData) -> dict[str, dict[str, Any]]:
        previous = dict(aggregate.tls_certificates or {})
        updated: dict[str, dict[str, Any]] = {}
        for key, raw_item in fresh.items():
            item = dict(raw_item)
            old_value = previous.get(key)
            old: dict[str, Any] = dict(old_value) if isinstance(old_value, dict) else {}
            fingerprint = str(item.get("fingerprint") or "")
            old_notified_fingerprint = str(old.get("notified_fingerprint") or "")
            if fingerprint and fingerprint == old_notified_fingerprint:
                notified_levels = [
                    str(level) for level in old.get("notified_levels", []) if str(level) in {"expiring", "expired"}
                ]
            elif not fingerprint:
                item["notified_fingerprint"] = old.get("notified_fingerprint")
                notified_levels = list(old.get("notified_levels", []) or [])
            else:
                notified_levels = []

            remaining_seconds = int(item.get("remaining_seconds", 0) or 0)
            has_expiry = bool(item.get("not_after"))
            level = (
                "expired"
                if has_expiry and remaining_seconds <= 0
                else ("expiring" if has_expiry and remaining_seconds <= int(TLS_EXPIRY_WARNING.total_seconds()) else "")
            )
            if level in {"expiring", "expired"} and level not in notified_levels and admin_ids and fingerprint:
                enqueue_important_outbox(
                    aggregate,
                    make_outbox_event(
                        kind=f"tls_certificate_{level}",
                        recipient_ids=admin_ids,
                        payload=message_payload(_alert_text(item, level)),
                    ),
                )
                notified_levels.append(level)
                item["notified_fingerprint"] = fingerprint
            elif fingerprint:
                item["notified_fingerprint"] = (
                    old_notified_fingerprint if fingerprint == old_notified_fingerprint else None
                )
            item["notified_levels"] = list(dict.fromkeys(notified_levels))
            updated[key] = item
        aggregate.tls_certificates = updated
        return {key: dict(value) for key, value in updated.items()}

    result = await update_important_data(save)
    counts: dict[str, int] = {}
    for item in result.values():
        status = str(item.get("status") or "error")
        counts[status] = counts.get(status, 0) + 1
    logger.info(
        "TLS certificates refreshed: total=%s statuses=%s", len(result), counts, extra={"action": "tls_refresh"}
    )
    return result


def tls_snapshot_for_server(server_key: str) -> list[dict[str, Any]]:
    items = [
        item
        for item in tls_certificates_snapshot().values()
        if server_key in [str(value) for value in item.get("servers", [])]
    ]
    return sorted(items, key=lambda item: (str(item.get("status") or ""), str(item.get("domain") or "")))


__all__ = [
    "configured_tls_endpoints",
    "refresh_tls_certificates",
    "tls_snapshot_for_server",
]
