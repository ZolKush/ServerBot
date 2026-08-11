"""Persist and refresh Telegram review cards across all administrators."""

from __future__ import annotations

import asyncio
from collections.abc import Collection
from datetime import timedelta
from typing import Any

from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TimedOut

from ..config import logger
from ..storage import get_user_meta_copy, service_requests_snapshot
from ..users.staff import is_admin_meta
from .review_refs import (
    REVIEW_COMPLETION_TYPE,
    register_review_reference,
    remove_review_reference,
    review_completion,
)


async def record_review_delivery(
    bot: Any,
    completion: object,
    recipient_id: int,
    message: Any,
) -> None:
    """Attach a delivered outbox message to its canonical request generation."""

    delivery = await register_review_reference(completion, recipient_id, message)
    if delivery is None:
        return
    scope, target_id, chat_id, message_id, registered = delivery
    if registered:
        if scope == "access":
            await refresh_access_review_message(bot, target_id, recipient_id, chat_id, message_id)
        else:
            await refresh_service_review_message(bot, target_id, recipient_id, chat_id, message_id)
        return
    await _edit_stale_delivery(bot, chat_id=chat_id, message_id=message_id)


async def _edit_stale_delivery(bot: Any, *, chat_id: int, message_id: int) -> None:
    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="⚠️ Эта карточка относится к уже завершённой или заменённой заявке.",
            reply_markup=None,
        )
    except (BadRequest, Forbidden, NetworkError, RetryAfter, TimedOut):
        logger.warning(
            "Could not retire stale review card chat=%s message=%s",
            chat_id,
            message_id,
            extra={"action": "review_card_stale"},
        )


def _is_not_modified(exc: BadRequest) -> bool:
    return "message is not modified" in str(exc).lower()


def _is_terminal_edit_error(exc: BaseException) -> bool:
    if isinstance(exc, Forbidden):
        return True
    if not isinstance(exc, BadRequest):
        return False
    text = str(exc).lower()
    return any(
        marker in text
        for marker in (
            "message to edit not found",
            "message can't be edited",
            "message can\u2019t be edited",
            "chat not found",
            "message_id_invalid",
        )
    )


async def _edit_card(
    bot: Any,
    *,
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup: Any,
) -> str:
    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
            )
        except BadRequest as exc:
            if _is_not_modified(exc):
                return "ok"
            if _is_terminal_edit_error(exc):
                return "terminal"
            error: BaseException = exc
        except Forbidden:
            return "terminal"
        except (NetworkError, RetryAfter, TimedOut, OSError) as exc:
            error = exc
        else:
            return "ok"

        if attempt == attempts:
            logger.warning(
                "Review card refresh deferred chat=%s message=%s type=%s attempts=%s",
                chat_id,
                message_id,
                error.__class__.__name__,
                attempts,
                extra={"action": "review_card_edit"},
            )
            return "retryable"
        await asyncio.sleep(_retry_delay(error, attempt))
    return "retryable"


def _retry_delay(error: BaseException, attempt: int) -> float:
    if isinstance(error, RetryAfter):
        raw_delay = error.retry_after
        seconds = raw_delay.total_seconds() if isinstance(raw_delay, timedelta) else float(raw_delay)
        return max(0.05, min(seconds, 2.0))
    return 0.2 * attempt


async def refresh_access_review_message(
    bot: Any,
    target_user_id: int,
    admin_id: int,
    chat_id: int,
    message_id: int,
) -> None:
    from ..access.views import access_request_card, access_request_markup

    meta = get_user_meta_copy(target_user_id)
    if not isinstance(meta, dict):
        return
    result = await _edit_card(
        bot,
        chat_id=chat_id,
        message_id=message_id,
        text=access_request_card(meta),
        reply_markup=access_request_markup(meta),
    )
    if result == "terminal":
        await remove_review_reference(
            scope="access",
            target_id=target_user_id,
            admin_id=admin_id,
            chat_id=chat_id,
            message_id=message_id,
        )


async def sync_access_review_messages(bot: Any, target_user_id: int) -> None:
    meta = get_user_meta_copy(target_user_id)
    if not isinstance(meta, dict):
        return
    refs = meta.get("review_messages")
    if not isinstance(refs, dict):
        return
    for raw_admin_id, raw_refs in list(refs.items()):
        try:
            admin_id = int(raw_admin_id)
        except (TypeError, ValueError, OverflowError):
            continue
        admin_refs = raw_refs if isinstance(raw_refs, list) else [raw_refs]
        for ref in list(admin_refs):
            if not isinstance(ref, dict):
                continue
            try:
                chat_id = int(ref.get("chat_id", 0) or 0)
                message_id = int(ref.get("message_id", 0) or 0)
            except (TypeError, ValueError, OverflowError):
                continue
            await refresh_access_review_message(bot, target_user_id, admin_id, chat_id, message_id)


async def refresh_service_review_message(
    bot: Any,
    request_id: int,
    admin_id: int,
    chat_id: int,
    message_id: int,
) -> None:
    from ..subscriptions.requests.views import request_card, request_markup

    request = service_requests_snapshot().get(str(request_id))
    if not isinstance(request, dict):
        return
    user_meta = get_user_meta_copy(int(request.get("user_id", 0) or 0)) or {}
    actor_meta = get_user_meta_copy(admin_id) or {}
    markup = request_markup(request, actor_meta) if is_admin_meta(actor_meta) else None
    result = await _edit_card(
        bot,
        chat_id=chat_id,
        message_id=message_id,
        text=request_card(request, user_meta),
        reply_markup=markup,
    )
    if result == "terminal":
        await remove_review_reference(
            scope="service",
            target_id=request_id,
            admin_id=admin_id,
            chat_id=chat_id,
            message_id=message_id,
        )


async def sync_service_review_messages(
    bot: Any,
    request_id: int,
    *,
    exclude: Collection[tuple[int, int]] = (),
) -> None:
    request = service_requests_snapshot().get(str(request_id))
    if not isinstance(request, dict):
        return
    refs = request.get("review_messages")
    if not isinstance(refs, dict):
        return
    for raw_admin_id, raw_refs in list(refs.items()):
        try:
            admin_id = int(raw_admin_id)
        except (TypeError, ValueError, OverflowError):
            continue
        admin_refs = raw_refs if isinstance(raw_refs, list) else [raw_refs]
        for ref in list(admin_refs):
            if not isinstance(ref, dict):
                continue
            try:
                chat_id = int(ref.get("chat_id", 0) or 0)
                message_id = int(ref.get("message_id", 0) or 0)
            except (TypeError, ValueError, OverflowError):
                continue
            if (chat_id, message_id) in exclude:
                continue
            await refresh_service_review_message(bot, request_id, admin_id, chat_id, message_id)


async def sync_service_review_messages_for_user(
    bot: Any,
    user_id: int,
    *,
    exclude: Collection[tuple[int, int]] = (),
) -> None:
    """Refresh every request card affected by a mutation of one user."""

    requests = service_requests_snapshot()
    request_ids = sorted(
        int(request.get("id", 0) or 0)
        for request in requests.values()
        if isinstance(request, dict) and int(request.get("user_id", 0) or 0) == user_id
    )
    for request_id in request_ids:
        if request_id > 0:
            await sync_service_review_messages(bot, request_id, exclude=exclude)


__all__ = [
    "REVIEW_COMPLETION_TYPE",
    "record_review_delivery",
    "refresh_access_review_message",
    "refresh_service_review_message",
    "review_completion",
    "sync_access_review_messages",
    "sync_service_review_messages",
    "sync_service_review_messages_for_user",
]
