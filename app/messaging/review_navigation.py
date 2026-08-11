"""Prevent review synchronization from overwriting a message after navigation."""

from __future__ import annotations

import re
from typing import Any

from ..config import logger
from .review_refs import remove_review_references_for_message

_REVIEW_CARD_CALLBACK = re.compile(
    r"^(?:access:(?:approve|reject|block):\d+|"
    r"product:req:(?:view|approve|approve24|custom|reject|requisites|confirm|notfound):\d+)$"
)


def keeps_review_card_reference(callback_data: object) -> bool:
    """Return true for callbacks that still operate on the same review card."""

    return bool(_REVIEW_CARD_CALLBACK.fullmatch(str(callback_data or "")))


async def retire_review_card_message(update: Any) -> int:
    """Drop persisted coordinates when a review card becomes another screen."""

    query = getattr(update, "callback_query", None)
    user = getattr(update, "effective_user", None)
    message = getattr(query, "message", None)
    if query is None or user is None or message is None:
        return 0
    try:
        admin_id = int(user.id)
        chat_id = int(getattr(message, "chat_id", 0) or getattr(getattr(message, "chat", None), "id", 0) or 0)
        message_id = int(getattr(message, "message_id", 0) or 0)
    except (TypeError, ValueError, OverflowError):
        return 0
    removed = await remove_review_references_for_message(
        admin_id=admin_id,
        chat_id=chat_id,
        message_id=message_id,
    )
    if removed:
        logger.info(
            "Review card references retired by navigation user_id=%s count=%s",
            admin_id,
            removed,
            extra={"action": "review_card_retired", "user_id": admin_id, "total": removed},
        )
    return removed


async def retire_review_card_for_navigation(update: Any) -> int:
    """Drop persisted coordinates before ordinary navigation reuses a review card."""

    query = getattr(update, "callback_query", None)
    if query is None or keeps_review_card_reference(query.data):
        return 0
    return await retire_review_card_message(update)


__all__ = [
    "keeps_review_card_reference",
    "retire_review_card_for_navigation",
    "retire_review_card_message",
]
