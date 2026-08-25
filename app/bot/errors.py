"""Global PTB guards, fallbacks, and exception reporting."""

from __future__ import annotations

import asyncio
import contextlib
import time

from telegram import Update
from telegram.constants import ParseMode
from telegram.error import RetryAfter
from telegram.ext import ApplicationHandlerStop

from ..config import ERROR_NOTIFY_INTERVAL_SEC, logger
from ..messaging.telegram_rate import extend_flood_gate, retry_after_seconds, wait_flood_gate
from ..storage import get_user_meta_copy
from .guards import authorized_ids, is_authorized, is_enabled, reply_need_auth
from .ui import clip_html_message, html_escape

_LAST_ERROR_NOTIFY_AT = 0.0
_ERROR_NOTIFY_LOCK: asyncio.Lock | None = None


def _notification_lock() -> asyncio.Lock:
    global _ERROR_NOTIFY_LOCK
    if _ERROR_NOTIFY_LOCK is None:
        _ERROR_NOTIFY_LOCK = asyncio.Lock()
    return _ERROR_NOTIFY_LOCK


async def on_error(update: object, context) -> None:
    try:
        callback_data = getattr(getattr(update, "callback_query", None), "data", None)
        user_id = getattr(getattr(update, "effective_user", None), "id", None)
        chat_id = getattr(getattr(update, "effective_chat", None), "id", None)
    except Exception:
        callback_data = user_id = chat_id = None
    error = context.error
    error_type = error.__class__.__name__ if isinstance(error, BaseException) else type(error).__name__
    logger.error(
        "Unhandled exception in handler type=%s (user_id=%s chat_id=%s cb=%s)",
        error_type,
        user_id,
        chat_id,
        callback_data,
    )

    global _LAST_ERROR_NOTIFY_AT
    now = time.monotonic()
    lock = _notification_lock()
    if lock.locked() or now - _LAST_ERROR_NOTIFY_AT < ERROR_NOTIFY_INTERVAL_SEC:
        return

    async with lock:
        now = time.monotonic()
        if now - _LAST_ERROR_NOTIFY_AT < ERROR_NOTIFY_INTERVAL_SEC:
            return
        # Start the cooldown before network fan-out. A total Telegram outage
        # must not turn every handler exception into another notification storm.
        _LAST_ERROR_NOTIFY_AT = now
        try:
            admins = authorized_ids(role_filter="admin")
            if not admins:
                return
            error_text = clip_html_message(
                f"⚠️ <b>Необработанная ошибка в боте</b>\nТип: <code>{html_escape(error_type)}</code>"
            )
            semaphore = asyncio.Semaphore(4)

            async def notify(admin_id: int) -> None:
                async with semaphore:
                    await wait_flood_gate()
                    try:
                        await asyncio.wait_for(
                            context.bot.send_message(
                                chat_id=admin_id,
                                text=error_text,
                                parse_mode=ParseMode.HTML,
                            ),
                            timeout=10,
                        )
                    except RetryAfter as exc:
                        await extend_flood_gate(retry_after_seconds(exc, minimum=1.0) + 0.5)
                        logger.warning("Отложено уведомление об ошибке администратору %s: RetryAfter", admin_id)
                    except Exception:
                        logger.warning("Не удалось отправить уведомление об ошибке администратору %s", admin_id)

            await asyncio.gather(*(notify(admin_id) for admin_id in admins))
        except Exception:
            logger.exception("Не удалось уведомить админов об ошибке")


async def fallback_text(update, context) -> None:
    message = update.effective_message
    if not message:
        return
    if not is_authorized(update):
        await reply_need_auth(update)
        return
    if not is_enabled(update):
        return
    await message.reply_text("Не понимаю команду. Используйте /menu для меню или /help для подсказок.")


async def blocked_user_guard(update: Update, context) -> None:
    """Completely ignore every update produced by a blocked account."""
    user = update.effective_user
    if user is None:
        return
    meta = get_user_meta_copy(user.id)
    if isinstance(meta, dict) and meta.get("access_state") == "blocked":
        raise ApplicationHandlerStop


async def unhandled_callback(update: Update, context) -> None:
    query = update.callback_query
    if query is None:
        return
    user_id = update.effective_user.id if update.effective_user else None
    logger.warning(
        "Unhandled callback user_id=%s data=%s",
        user_id,
        str(query.data)[:100],
        extra={"user_id": user_id, "action": "unhandled_callback"},
    )
    with contextlib.suppress(Exception):
        await query.answer(
            "Эта кнопка устарела. Откройте меню заново командой /menu.",
            show_alert=True,
        )
