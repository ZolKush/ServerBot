"""Maintenance scope, duration, and notification-threshold policy."""

from __future__ import annotations

from ..bot.ui import html_escape
from ..config import SERVERS

MAINT_SCOPE_ALL = "all"
MAX_MAINT_HOURS = 72
MAINT_WARN_THRESHOLDS_MIN = (4320, 720, 30)


def initial_notified_thresholds(remaining_min: int) -> list[int]:
    thresholds = set(MAINT_WARN_THRESHOLDS_MIN)
    armed = {threshold for threshold in thresholds if remaining_min > threshold}
    if not armed and remaining_min > 0:
        armed = {min(thresholds)}
    return sorted(thresholds - armed)


def due_thresholds(notified: list[int], remaining_min: int) -> list[int]:
    sent = set(notified)
    return [
        threshold for threshold in MAINT_WARN_THRESHOLDS_MIN if threshold not in sent and remaining_min <= threshold
    ]


def server_items() -> list[tuple[str, str]]:
    return [(key, value.label) for key, value in SERVERS.items()]


def normalize_scope(scope: str | None) -> str:
    value = (scope or "").strip().lower()
    if value == MAINT_SCOPE_ALL:
        return MAINT_SCOPE_ALL
    return value if value in SERVERS else MAINT_SCOPE_ALL


def scope_label(scope: str | None) -> str:
    normalized = normalize_scope(scope)
    if normalized == MAINT_SCOPE_ALL:
        labels = [label for _, label in server_items()]
        return ", ".join(labels) if labels else "Все серверы"
    server = SERVERS.get(normalized)
    return server.label if server else normalized


def scope_line(scope: str | None) -> str:
    normalized = normalize_scope(scope)
    prefix = "Серверы" if normalized == MAINT_SCOPE_ALL else "Сервер"
    return f"• {prefix}: <b>{html_escape(scope_label(normalized))}</b>"


def maint_heading(urgency: object, status: str) -> str:
    if str(urgency or "").lower() == "urgent":
        return f"🚨🚨 <b>СРОЧНЫЕ ТЕХНИЧЕСКИЕ РАБОТЫ</b> 🚨🚨\n⚠️ <b>{html_escape(status)}</b>"
    return f"🗓 <b>ПЛАНОВЫЕ ТЕХНИЧЕСКИЕ РАБОТЫ</b>\nℹ️ <b>{html_escape(status)}</b>"


def hhmm_to_minutes(hours: int, minutes: int) -> int:
    return max(0, (int(hours) * 60) + int(minutes))


def minutes_to_hhmm(total: int) -> tuple[int, int]:
    value = max(0, int(total))
    return value // 60, value % 60
