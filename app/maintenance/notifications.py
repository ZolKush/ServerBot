"""Reliable maintenance notification creation."""

from __future__ import annotations

from typing import Any

from ..bot.guards import authorized_ids
from ..messaging.outbox import message_payload
from ..storage import ImportantData, enqueue_important_outbox, make_outbox_event


def make_maintenance_notice_event(
    *,
    author_id: int | None,
    text: str,
    kind: str,
) -> tuple[dict[str, Any] | None, int, int]:
    user_ids = authorized_ids(role_filter="user", exclude=set())
    admin_ids = authorized_ids(role_filter="admin", exclude={author_id} if author_id else set())
    recipient_ids = user_ids + admin_ids
    if not recipient_ids:
        return None, 0, 0
    event = make_outbox_event(
        kind=kind,
        recipient_ids=recipient_ids,
        payload=message_payload(
            text,
            reply_markup=[[{"text": "🏠 Меню", "callback_data": "menu:home"}]],
        ),
    )
    return event, len(user_ids), len(admin_ids)


def enqueue_maintenance_notice(cfg: ImportantData, event: dict[str, Any] | None) -> None:
    if event:
        enqueue_important_outbox(cfg, event)


__all__ = ["enqueue_maintenance_notice", "make_maintenance_notice_event"]
