from __future__ import annotations

import asyncio
import html
import time
from datetime import datetime, timezone
from typing import Any

from ...config import logger
from ...messaging.outbox import message_payload
from ...storage import (
    ImportantData,
    authorized_users_snapshot,
    enqueue_important_outbox,
    make_outbox_event,
    tls_certificates_snapshot,
    update_important_data,
)
from .checks import TLS_EXPIRY_WARNING, TLS_PORT
from .policy import ConfiguredTLSEndpoint, check_tls_with_fallback, configured_tls_endpoints

TLS_CHECK_CONCURRENCY = 6


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


def _parse_time(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or ""))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _evaluate_deadline(item: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    result = dict(item)
    not_after = _parse_time(result.get("not_after"))
    if not_after is None:
        return result
    remaining = int((not_after - now).total_seconds())
    result["remaining_seconds"] = remaining
    status = str(result.get("status") or "unknown")
    if remaining <= 0:
        result["status"] = "expired"
    elif status not in {"error", "invalid"}:
        result["status"] = "expiring" if remaining <= int(TLS_EXPIRY_WARNING.total_seconds()) else "ok"
    return result


def _alert_text(item: dict[str, Any], level: str) -> str:
    domain = str(item.get("domain") or "-")
    port = int(item.get("effective_port", item.get("port", TLS_PORT)) or TLS_PORT)
    end = _parse_time(item.get("not_after"))
    end_text = end.strftime("%d.%m.%Y %H:%M UTC") if end else str(item.get("not_after") or "-")
    if level == "expired":
        heading = "‼️‼️‼️ СЕРТИФИКАТ ПРОСРОЧЕН ‼️‼️‼️"
        detail = "TLS-сертификат уже недействителен. Требуется немедленное обновление."
    else:
        heading = "‼️‼️‼️ СЕРТИФИКАТ ИСТЕКАЕТ ‼️‼️‼️"
        remaining_hours = max(0, int(item.get("remaining_seconds", 0) or 0) // 3600)
        detail = f"До окончания TLS-сертификата осталось не более 3 суток ({remaining_hours} ч.)."
    servers = ", ".join(str(server) for server in item.get("servers", []) if str(server).strip()) or "-"
    return (
        f"<b>{heading}</b>\n\n{detail}\n\n"
        f"• Домен: <code>{html.escape(domain)}:{port}</code>\n"
        f"• Серверы: <code>{html.escape(servers)}</code>\n"
        f"• Действителен до: <code>{html.escape(end_text)}</code>"
    )


def _apply_notification_state(
    aggregate: ImportantData,
    item: dict[str, Any],
    old: dict[str, Any],
    admin_ids: list[int],
) -> dict[str, Any]:
    fingerprint = str(item.get("fingerprint") or "")
    old_fingerprint = str(old.get("notified_fingerprint") or "")
    levels = (
        [str(level) for level in old.get("notified_levels", []) if str(level) in {"expiring", "expired"}]
        if fingerprint and fingerprint == old_fingerprint
        else []
    )
    remaining = int(item.get("remaining_seconds", 0) or 0)
    has_expiry = bool(item.get("not_after"))
    level = "expired" if has_expiry and remaining <= 0 else "expiring" if has_expiry and remaining <= 259200 else ""
    if level and level not in levels and admin_ids and fingerprint:
        enqueue_important_outbox(
            aggregate,
            make_outbox_event(
                kind=f"tls_certificate_{level}",
                recipient_ids=admin_ids,
                payload=message_payload(_alert_text(item, level)),
            ),
        )
        levels.append(level)
    item["notified_fingerprint"] = fingerprint or old_fingerprint or None
    item["notified_levels"] = list(dict.fromkeys(levels))
    return item


def _merge_network_result(raw: dict[str, Any], old: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    item = dict(raw)
    attempted_at = str(item.get("checked_at") or now.isoformat())
    item["last_attempt_at"] = attempted_at
    if item.get("status") != "error":
        item["last_success_at"] = attempted_at
        return _evaluate_deadline(item, now=now)

    item["last_success_at"] = str(old.get("last_success_at") or old.get("checked_at") or "")
    for field in (
        "not_before",
        "not_after",
        "fingerprint",
        "issuer",
        "hostname_valid",
        "trust_valid",
        "remaining_seconds",
    ):
        if not item.get(field) and old.get(field) not in {None, ""}:
            item[field] = old[field]
    return _evaluate_deadline(item, now=now)


async def refresh_tls_certificates(*, source: str = "manual") -> dict[str, dict[str, Any]]:
    started = time.monotonic()
    targets = configured_tls_endpoints()
    semaphore = asyncio.Semaphore(TLS_CHECK_CONCURRENCY)

    async def check(target: ConfiguredTLSEndpoint) -> tuple[str, dict[str, Any]]:
        async with semaphore:
            return target.storage_key, await check_tls_with_fallback(target)

    fresh = dict(await asyncio.gather(*(check(target) for target in targets)))
    for item in fresh.values():
        domain = str(item.get("domain") or "-")
        primary_port = int(item.get("primary_port", TLS_PORT) or TLS_PORT)
        effective_port = int(item.get("effective_port", primary_port) or primary_port)
        if item.get("used_fallback") and item.get("status") not in {"error", "invalid"}:
            logger.info(
                "TLS fallback succeeded domain=%s primary_port=%s effective_port=%s",
                domain,
                primary_port,
                effective_port,
                extra={
                    "action": "tls_fallback_succeeded",
                    "source": source,
                    "primary_port": primary_port,
                    "effective_port": effective_port,
                },
            )
        if item.get("status") in {"error", "invalid"}:
            detail = " ".join(str(item.get("error") or "unknown error").split())[:300]
            logger.warning(
                "TLS endpoint problem domain=%s primary_port=%s effective_port=%s used_fallback=%s "
                "failure_kind=%s detail=%s",
                domain,
                primary_port,
                effective_port,
                bool(item.get("used_fallback")),
                str(item.get("failure_kind") or "certificate"),
                detail,
                extra={
                    "action": "tls_endpoint_problem",
                    "source": source,
                    "primary_port": primary_port,
                    "effective_port": effective_port,
                },
            )
    admin_ids = _approved_admin_ids()
    now = datetime.now(timezone.utc)

    def save(aggregate: ImportantData) -> dict[str, dict[str, Any]]:
        previous = dict(aggregate.tls_certificates or {})
        updated: dict[str, dict[str, Any]] = {}
        for key, raw_item in fresh.items():
            old_value = previous.get(key)
            old = dict(old_value) if isinstance(old_value, dict) else {}
            item = _merge_network_result(raw_item, old, now=now)
            updated[key] = _apply_notification_state(aggregate, item, old, admin_ids)
        aggregate.tls_certificates = updated
        return {key: dict(value) for key, value in updated.items()}

    result = await update_important_data(save)
    counts: dict[str, int] = {}
    for item in result.values():
        status = str(item.get("status") or "error")
        counts[status] = counts.get(status, 0) + 1
    logger.info(
        "TLS certificates refreshed source=%s total=%s statuses=%s duration_ms=%s",
        source,
        len(result),
        counts,
        round((time.monotonic() - started) * 1000),
        extra={
            "action": "tls_refresh",
            "source": source,
            "total": len(result),
            "duration_ms": round((time.monotonic() - started) * 1000),
        },
    )
    return result


async def evaluate_tls_deadlines(*, source: str = "scheduled-local") -> dict[str, dict[str, Any]]:
    """Recalculate persisted expiries without opening a network connection."""
    now = datetime.now(timezone.utc)
    admin_ids = _approved_admin_ids()

    def save(aggregate: ImportantData) -> dict[str, dict[str, Any]]:
        updated: dict[str, dict[str, Any]] = {}
        for key, raw_item in dict(aggregate.tls_certificates or {}).items():
            old = dict(raw_item) if isinstance(raw_item, dict) else {}
            item = _evaluate_deadline(old, now=now)
            updated[key] = _apply_notification_state(aggregate, item, old, admin_ids)
        aggregate.tls_certificates = updated
        return {key: dict(value) for key, value in updated.items()}

    result = await update_important_data(save)
    logger.info(
        "TLS deadlines evaluated source=%s total=%s",
        source,
        len(result),
        extra={"action": "tls_deadline_evaluation", "source": source, "total": len(result)},
    )
    return result


def tls_snapshot_for_server(server_key: str) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    items = [
        _evaluate_deadline(dict(item), now=now)
        for item in tls_certificates_snapshot().values()
        if server_key in [str(value) for value in item.get("servers", [])]
    ]
    return sorted(items, key=lambda item: (str(item.get("status") or ""), str(item.get("domain") or "")))


__all__ = [
    "ConfiguredTLSEndpoint",
    "check_tls_with_fallback",
    "configured_tls_endpoints",
    "evaluate_tls_deadlines",
    "refresh_tls_certificates",
    "tls_snapshot_for_server",
]
