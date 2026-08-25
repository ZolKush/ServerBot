"""Atomic storage operations for maintenance workflows."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any

from ..config import TZ
from ..storage import ImportantData, enqueue_important_outbox, update_important_data
from .models import coerce_scheduled_maintenance
from .notifications import enqueue_maintenance_notice, make_maintenance_notice_event
from .records import scheduled_to_active_record
from .views import maintenance_end_notice, maintenance_extend_notice, maintenance_scheduled_cancel_notice


async def start_maintenance(
    maintenance: Mapping[str, Any],
    *,
    expected_schedule_id: str,
    notice_event: dict[str, Any] | None,
) -> bool:
    def apply(cfg: ImportantData) -> bool:
        active = cfg.maintenance if isinstance(cfg.maintenance, dict) else {}
        if active.get("active"):
            return False
        current_schedule = cfg.scheduled_maintenance if isinstance(cfg.scheduled_maintenance, dict) else {}
        if str(current_schedule.get("id") or "") != expected_schedule_id:
            return False
        cfg.maintenance = dict(maintenance)
        # An immediate announcement supersedes the schedule atomically.
        cfg.scheduled_maintenance = {}
        enqueue_maintenance_notice(cfg, notice_event)
        return True

    return await update_important_data(apply)


async def schedule_maintenance(scheduled: Mapping[str, Any]) -> bool:
    def apply(cfg: ImportantData) -> bool:
        active = cfg.maintenance if isinstance(cfg.maintenance, dict) else {}
        existing = cfg.scheduled_maintenance if isinstance(cfg.scheduled_maintenance, dict) else {}
        if active.get("active") or existing.get("id"):
            return False
        cfg.scheduled_maintenance = dict(scheduled)
        return True

    return await update_important_data(apply)


async def extend_maintenance(
    maintenance_id: object,
    *,
    duration_min: int,
    hours: int,
    minutes: int,
    author: str,
    author_id: int | None,
) -> tuple[dict[str, Any], int, int]:
    users_count = admins_count = 0

    def apply(cfg: ImportantData) -> dict[str, Any]:
        nonlocal users_count, admins_count
        current = cfg.maintenance if isinstance(cfg.maintenance, dict) else {}
        if not current.get("active"):
            raise RuntimeError("maintenance_not_active")
        if str(current.get("id")) != str(maintenance_id):
            raise RuntimeError("maintenance_changed")
        now = datetime.now(TZ)
        updated = dict(current)
        updated["duration_min"] = duration_min
        updated["expected_end"] = (now + timedelta(minutes=duration_min)).isoformat()
        updated["updated_at"] = now.isoformat()
        cfg.maintenance = updated
        event, users_count, admins_count = make_maintenance_notice_event(
            author_id=author_id,
            text=maintenance_extend_notice(updated, hours, minutes, author),
            kind="maintenance_extended",
        )
        enqueue_maintenance_notice(cfg, event)
        return updated

    updated = await update_important_data(apply)
    return updated, users_count, admins_count


async def end_maintenance(
    maintenance_id: str,
    *,
    author: str,
    author_id: int | None,
    ended_at: datetime,
) -> tuple[dict[str, Any] | None, int, int]:
    users_count = admins_count = 0

    def apply(cfg: ImportantData) -> dict[str, Any] | None:
        nonlocal users_count, admins_count
        current = cfg.maintenance if isinstance(cfg.maintenance, dict) else {}
        if not current.get("active") or str(current.get("id") or "") != maintenance_id:
            return None
        previous = dict(current)
        event, users_count, admins_count = make_maintenance_notice_event(
            author_id=author_id,
            text=maintenance_end_notice(previous, author, ended_at=ended_at),
            kind="maintenance_ended",
        )
        cfg.maintenance = {}
        reminder_kind = f"maintenance_admin_reminder_{maintenance_id}"
        cfg.outbox = {
            event_id: pending
            for event_id, pending in cfg.outbox.items()
            if not (isinstance(pending, dict) and pending.get("kind") == reminder_kind)
        }
        enqueue_maintenance_notice(cfg, event)
        return previous

    previous = await update_important_data(apply)
    return previous, users_count, admins_count


async def cancel_scheduled_maintenance(
    schedule_id: str,
    *,
    actor_id: int | None,
) -> tuple[dict[str, Any] | None, int, int, bool]:
    users_count = admins_count = 0
    queued_notice = False

    def apply(cfg: ImportantData) -> dict[str, Any] | None:
        nonlocal users_count, admins_count, queued_notice
        current = cfg.scheduled_maintenance if isinstance(cfg.scheduled_maintenance, dict) else {}
        if str(current.get("id") or "") != schedule_id:
            return None
        previous = dict(current)
        announced = previous.get("announced_thresholds")
        already_warned = (
            bool(announced) or bool(previous.get("notified_start")) or bool(previous.get("notified_before"))
        )
        cfg.scheduled_maintenance = {}
        if already_warned:
            event, users_count, admins_count = make_maintenance_notice_event(
                author_id=actor_id,
                text=maintenance_scheduled_cancel_notice(previous),
                kind="maintenance_schedule_cancelled",
            )
            enqueue_maintenance_notice(cfg, event)
            queued_notice = event is not None
        return previous

    previous = await update_important_data(apply)
    return previous, users_count, admins_count, queued_notice


async def queue_active_reminder(
    maintenance_id: str,
    reminder_kind: str,
    event: dict[str, Any],
) -> bool:
    def apply(cfg: ImportantData) -> bool:
        current = cfg.maintenance if isinstance(cfg.maintenance, dict) else {}
        if not current.get("active") or str(current.get("id") or "") != maintenance_id:
            return False
        if any(isinstance(pending, dict) and pending.get("kind") == reminder_kind for pending in cfg.outbox.values()):
            return False
        enqueue_important_outbox(cfg, event)
        return True

    return await update_important_data(apply)


async def clear_invalid_schedule(schedule_id: str) -> None:
    def apply(cfg: ImportantData) -> None:
        current = cfg.scheduled_maintenance if isinstance(cfg.scheduled_maintenance, dict) else {}
        if str(current.get("id") or "") == schedule_id:
            cfg.scheduled_maintenance = {}

    await update_important_data(apply)


async def expire_schedule(
    scheduled: dict[str, Any],
    notice_event: dict[str, Any] | None,
) -> bool:
    def apply(cfg: ImportantData) -> bool:
        current = cfg.scheduled_maintenance if isinstance(cfg.scheduled_maintenance, dict) else {}
        if str(current.get("id") or "") != str(scheduled.get("id") or ""):
            return False
        cfg.scheduled_maintenance = {}
        enqueue_maintenance_notice(cfg, notice_event)
        return True

    return await update_important_data(apply)


async def mark_schedule_thresholds(
    scheduled: dict[str, Any],
    *,
    updated_notified: list[int],
    due: list[int],
    updated_at: datetime,
    notice_event: dict[str, Any] | None,
) -> dict[str, Any] | None:
    def apply(cfg: ImportantData) -> dict[str, Any] | None:
        current = dict(cfg.scheduled_maintenance or {})
        if str(current.get("id") or "") != str(scheduled.get("id") or ""):
            return None
        current["notified_thresholds"] = updated_notified
        announced_raw = current.get("announced_thresholds")
        announced = (
            [int(value) for value in announced_raw if isinstance(value, int)] if isinstance(announced_raw, list) else []
        )
        current["announced_thresholds"] = sorted(set(announced) | set(due))
        current.pop("notified_before", None)
        current["updated_at"] = updated_at.isoformat()
        cfg.scheduled_maintenance = current
        enqueue_maintenance_notice(cfg, notice_event)
        return current

    return await update_important_data(apply)


async def activate_scheduled_maintenance(
    scheduled: dict[str, Any],
    notice_event: dict[str, Any] | None,
) -> dict[str, Any] | None:
    def apply(cfg: ImportantData) -> dict[str, Any] | None:
        current = dict(cfg.scheduled_maintenance or {})
        if str(current.get("id") or "") != str(scheduled.get("id") or ""):
            return None
        existing = cfg.maintenance if isinstance(cfg.maintenance, dict) else {}
        if existing.get("active"):
            return None
        cfg.maintenance = dict(scheduled_to_active_record(coerce_scheduled_maintenance(current)))
        cfg.scheduled_maintenance = {}
        enqueue_maintenance_notice(cfg, notice_event)
        return dict(cfg.maintenance)

    return await update_important_data(apply)


__all__ = [
    "activate_scheduled_maintenance",
    "cancel_scheduled_maintenance",
    "clear_invalid_schedule",
    "end_maintenance",
    "expire_schedule",
    "extend_maintenance",
    "mark_schedule_thresholds",
    "queue_active_reminder",
    "schedule_maintenance",
    "start_maintenance",
]
