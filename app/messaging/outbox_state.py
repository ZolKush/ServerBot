"""Pure state transitions for durable Telegram outbox recipients."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

ACTIVE_RECIPIENT_STATUSES = frozenset({"pending", "delivered_pending_registration"})
DEAD_LETTER_STATUS = "dead_letter"


def parse_time(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value or ""))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return datetime.min.replace(tzinfo=timezone.utc)


def delivery_coordinates(message: Any, recipient_id: int) -> tuple[int, int] | None:
    try:
        message_id = int(getattr(message, "message_id", 0) or 0)
        chat_id = int(getattr(message, "chat_id", 0) or 0)
        if not chat_id:
            chat_id = int(getattr(getattr(message, "chat", None), "id", 0) or recipient_id)
    except (TypeError, ValueError, OverflowError):
        return None
    return (chat_id, message_id) if chat_id and message_id > 0 else None


def recipient_mutation(
    uid: int,
    *,
    status: str,
    attempts: int,
    error: str = "",
    retry_after: float = 0,
    chat_id: int | None = None,
    message_id: int | None = None,
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    def apply(event: dict[str, Any]) -> dict[str, Any]:
        recipients = event.get("recipients")
        if not isinstance(recipients, dict) or not isinstance(recipients.get(str(uid)), dict):
            return event
        state = dict(recipients[str(uid)])
        state.update(
            {
                "status": status,
                "attempts": max(0, int(attempts)),
                "last_error": str(error)[:500],
            }
        )
        now = datetime.now(timezone.utc)
        if status in ACTIVE_RECIPIENT_STATUSES:
            state["next_attempt_at"] = (now + timedelta(seconds=max(0.0, retry_after))).isoformat()
        if status in {"delivered", "delivered_pending_registration"}:
            state["delivered_at"] = state.get("delivered_at") or now.isoformat()
        if status == "delivered_pending_registration":
            state["delivered_chat_id"] = int(chat_id or 0)
            state["delivered_message_id"] = int(message_id or 0)
        if status == DEAD_LETTER_STATUS:
            state["dead_lettered_at"] = now.isoformat()
            state["next_attempt_at"] = ""
        elif status == "pending":
            state["dead_lettered_at"] = ""
        recipients[str(uid)] = state
        event["recipients"] = recipients
        return event

    return apply


def should_dead_letter(event: dict[str, Any], *, attempts: int, now: datetime) -> bool:
    created_at = parse_time(event.get("created_at"))
    age = now - created_at if created_at != datetime.min.replace(tzinfo=timezone.utc) else timedelta.max
    return attempts >= 72 or age >= timedelta(days=7)


__all__ = [
    "ACTIVE_RECIPIENT_STATUSES",
    "DEAD_LETTER_STATUS",
    "delivery_coordinates",
    "parse_time",
    "recipient_mutation",
    "should_dead_letter",
]
