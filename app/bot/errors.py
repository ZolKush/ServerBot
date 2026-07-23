"""Global PTB guards, fallbacks, and exception reporting."""

from __future__ import annotations

import contextlib
import time

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ApplicationHandlerStop

from ..config import ERROR_NOTIFY_INTERVAL_SEC, logger
from ..storage import get_user_meta_copy
from .guards import authorized_ids, is_authorized, is_enabled, reply_need_auth
from .ui import clip_html_message, html_escape

_LAST_ERROR_NOTIFY_AT = 0.0


async def on_error(update: object, context) -> None:
    try:
        callback_data = getattr(getattr(update, "callback_query", None), "data", None)
        user_id = getattr(getattr(update, "effective_user", None), "id", None)
        chat_id = getattr(getattr(update, "effective_chat", None), "id", None)
    except Exception:
        callback_data = user_id = chat_id = None
    error = context.error
    exc_info = None
    if isinstance(error, BaseException):
        exc_info = (type(error), error, error.__traceback__)
    logger.error(
        "Unhandled exception in handler: %s (user_id=%s chat_id=%s cb=%s)",
        error,
        user_id,
        chat_id,
        callback_data,
        exc_info=exc_info,
    )

    global _LAST_ERROR_NOTIFY_AT
    now = time.monotonic()
    if now - _LAST_ERROR_NOTIFY_AT < ERROR_NOTIFY_INTERVAL_SEC:
        return
    try:
        admins = authorized_ids(role_filter="admin")
        if not admins:
            return
        error_text = clip_html_message(
            f"⚠️ <b>Необработанная ошибка в боте</b>\n<code>{html_escape(str(error))[:500]}</code>"
        )
        delivered = False
        for admin_id in admins:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=error_text,
                    parse_mode=ParseMode.HTML,
                )
                delivered = True
            except Exception:
                logger.warning("Не удалось отправить уведомление об ошибке администратору %s", admin_id)
        if delivered:
            _LAST_ERROR_NOTIFY_AT = now
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
