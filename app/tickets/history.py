"""Ticket message history and attachment snapshots."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, cast

from ..config import TZ
from .models import TicketMessage

MAX_TICKET_HISTORY_ITEMS = 6
MAX_TICKET_HISTORY_CHARS = 2500
MAX_TICKET_HISTORY_ITEM_CHARS = 900
MAX_TICKET_MESSAGES_STORED = 100


def _now_iso() -> str:
    return datetime.now(TZ).isoformat()


def _ticket_messages(ticket: Mapping[str, Any]) -> list[TicketMessage]:
    items = ticket.get("messages", [])
    return (
        [cast(TicketMessage, dict(item)) for item in items if isinstance(item, dict)] if isinstance(items, list) else []
    )


def _append_ticket_message(
    ticket: Mapping[str, Any],
    *,
    sender_role: str,
    sender_id: int | None,
    sender_name: str,
    text: str,
    kind: str,
    attachment: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    updated = dict(ticket)
    messages = _ticket_messages(updated)
    item: TicketMessage = {
        "ts": _now_iso(),
        "sender_role": sender_role,  # type: ignore[typeddict-item]
        "sender_id": sender_id,
        "sender_name": sender_name,
        "text": text,
        "kind": kind,
    }
    if attachment:
        item["attachment"] = cast(Any, dict(attachment))
    if sender_role == "admin" and sender_id is not None:
        item["sender_signature_version"] = 1
    messages.append(item)
    if len(messages) > MAX_TICKET_MESSAGES_STORED:
        first = messages[0]
        if first.get("kind") == "initial":
            messages = [first, *messages[-(MAX_TICKET_MESSAGES_STORED - 1) :]]
        else:
            messages = messages[-MAX_TICKET_MESSAGES_STORED:]
    updated["messages"] = messages
    updated["updated_at"] = _now_iso()
    return updated


def _last_attachment(ticket: Mapping[str, Any]) -> dict[str, Any] | None:
    messages = _ticket_messages(ticket)
    if not messages:
        return None
    attachment = messages[-1].get("attachment")
    return dict(attachment) if isinstance(attachment, dict) else None


__all__ = [
    "MAX_TICKET_HISTORY_CHARS",
    "MAX_TICKET_HISTORY_ITEM_CHARS",
    "MAX_TICKET_HISTORY_ITEMS",
    "MAX_TICKET_MESSAGES_STORED",
]
