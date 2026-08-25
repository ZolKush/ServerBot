"""Durable completion of already-sent Telegram review cards."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

from telegram.error import NetworkError, RetryAfter, TimedOut

from ..config import logger
from ..storage import mutate_outbox_event
from .outbox_state import DEAD_LETTER_STATUS, recipient_mutation, should_dead_letter
from .telegram_rate import extend_flood_gate, retry_after_seconds


async def complete_review_registration(
    bot: Any,
    *,
    source: str,
    event_id: str,
    event: dict[str, Any],
    uid: int,
    state: dict[str, Any],
    attempts: int,
) -> dict[str, Any] | None:
    """Register persisted Telegram coordinates without sending the card again."""

    completion = event.get("completion")
    try:
        chat_id = int(state.get("delivered_chat_id", 0) or 0)
        message_id = int(state.get("delivered_message_id", 0) or 0)
    except (TypeError, ValueError, OverflowError):
        chat_id = message_id = 0
    if not isinstance(completion, dict) or completion.get("type") != "review_card" or not chat_id or message_id <= 0:
        return await mutate_outbox_event(
            source,
            event_id,
            recipient_mutation(
                uid,
                status=DEAD_LETTER_STATUS,
                attempts=attempts,
                error="review delivery has no durable Telegram coordinates",
            ),
        )

    try:
        from .review_sync import record_review_delivery

        completed = await record_review_delivery(
            bot,
            completion,
            uid,
            SimpleNamespace(chat_id=chat_id, message_id=message_id),
        )
        if not completed:
            raise NetworkError("review card refresh deferred")
    except RetryAfter as exc:
        delay = retry_after_seconds(exc, minimum=1.0) + 0.5
        await extend_flood_gate(delay)
        error = "RetryAfter"
    except (TimedOut, NetworkError, OSError) as exc:
        delay = min(3600.0, 2.0 ** min(attempts, 10))
        error = exc.__class__.__name__
    except Exception as exc:
        delay = min(3600.0, 2.0 ** min(attempts, 10))
        error = exc.__class__.__name__
        logger.exception("Could not register delivered review card event=%s recipient=%s", event_id, uid)
    else:
        return await mutate_outbox_event(
            source,
            event_id,
            recipient_mutation(uid, status="delivered", attempts=attempts),
        )

    dead_letter = should_dead_letter(event, attempts=attempts, now=datetime.now(timezone.utc))
    return await mutate_outbox_event(
        source,
        event_id,
        recipient_mutation(
            uid,
            status=DEAD_LETTER_STATUS if dead_letter else "delivered_pending_registration",
            attempts=attempts,
            error=error,
            retry_after=delay,
            chat_id=chat_id,
            message_id=message_id,
        ),
    )


__all__ = ["complete_review_registration"]
