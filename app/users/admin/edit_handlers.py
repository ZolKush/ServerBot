"""Text-input handlers for nickname and subscription-link edits."""

from __future__ import annotations

from datetime import datetime

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ...bot.guards import get_user_id, get_user_meta, require_admin, staff_title
from ...bot.ui import ui_error_text, ui_ok_text
from ...config import TZ
from ...runtime.logging import logger
from ...subscriptions.connections import MAX_CONNECTION_BYTES, is_valid_connection_url
from ..states import (
    ADMIN_PICK,
    ADMIN_USER_CFG_TEXT,
    ADMIN_USER_MENU,
    ADMIN_USER_NICK_TEXT,
    MAX_USER_NICK_LEN,
)
from ..views import (
    format_user_card,
    user_card_kb,
    users_list_kb,
    users_list_title,
)
from .navigation import conversation_data, get_users_filter
from .operations import assign_connection, update_nickname


@require_admin
async def users_user_nick_text(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    user_id = conversation_data(context).get("selected_uid")
    message = update.effective_message
    if not isinstance(user_id, int):
        if message:
            active_filter = get_users_filter(context)
            await message.reply_text(ui_error_text("пользователь не выбран."))
            await message.reply_text(
                users_list_title(active_filter),
                parse_mode=ParseMode.HTML,
                reply_markup=users_list_kb(active_filter),
            )
        return ADMIN_PICK

    meta = get_user_meta(user_id)
    if not meta:
        if message:
            active_filter = get_users_filter(context)
            await message.reply_text(ui_error_text("пользователь не найден."))
            await message.reply_text(
                users_list_title(active_filter),
                parse_mode=ParseMode.HTML,
                reply_markup=users_list_kb(active_filter),
            )
        return ADMIN_PICK

    nickname = ((message.text if message else "") or "").strip()
    if len(nickname) < 2:
        if message:
            await message.reply_text(ui_error_text("ник слишком короткий. Введите минимум 2 символа:"))
        return ADMIN_USER_NICK_TEXT
    if len(nickname) > MAX_USER_NICK_LEN:
        if message:
            await message.reply_text(ui_error_text(f"ник слишком длинный. Максимум {MAX_USER_NICK_LEN} символов:"))
        return ADMIN_USER_NICK_TEXT

    actor_id = get_user_id(update)
    updated = await update_nickname(
        target_user_id=user_id,
        nickname=nickname,
        actor_id=actor_id,
    )
    if updated is None:
        if message:
            await message.reply_text(ui_error_text("пользователь не найден."))
        return ADMIN_PICK
    logger.info(
        "Admin user_id=%s updated nickname target_uid=%s",
        actor_id,
        user_id,
    )

    if message:
        await message.reply_text(ui_ok_text("Никнейм сохранён"))
        await message.reply_text(
            format_user_card(updated),
            parse_mode=ParseMode.HTML,
            reply_markup=user_card_kb(user_id),
        )
    return ADMIN_USER_MENU


@require_admin
async def users_user_cfg_text(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    data = conversation_data(context)
    user_id = data.get("selected_uid")
    message = update.effective_message
    if not isinstance(user_id, int):
        if message:
            await message.reply_text(ui_error_text("пользователь не выбран."))
        return ADMIN_PICK

    connection_url = ((message.text if message else "") or "").strip()
    if not connection_url.strip():
        if message:
            await message.reply_text(ui_error_text("пустая ссылка. Вставьте её одним сообщением."))
        return ADMIN_USER_CFG_TEXT
    if len(connection_url.encode("utf-8")) > MAX_CONNECTION_BYTES:
        if message:
            await message.reply_text(ui_error_text("ссылка превышает лимит 1 МБ."))
        return ADMIN_USER_CFG_TEXT
    if not is_valid_connection_url(connection_url):
        if message:
            await message.reply_text(ui_error_text("нужна полная ссылка, начинающаяся с http:// или https://."))
        return ADMIN_USER_CFG_TEXT

    delivery_mode = str(data.get("subscription_delivery_mode", "send"))
    actor_id = get_user_id(update)
    updated = await assign_connection(
        target_user_id=user_id,
        connection_url=connection_url,
        delivery_mode=delivery_mode,
        actor_id=actor_id,
        actor_name=staff_title(update),
        changed_at=datetime.now(TZ).isoformat(),
    )
    if updated is None:
        if message:
            await message.reply_text(ui_error_text("пользователь не найден (возможно, удалён из списка)."))
        return ADMIN_PICK

    if delivery_mode == "assign":
        if message:
            await message.reply_text(ui_ok_text("Персональная ссылка сохранена без отправки пользователю"))
        logger.info(
            "Admin user_id=%s assigned connection target_uid=%s mode=assign",
            actor_id,
            user_id,
        )
    else:
        logger.info(
            "Admin user_id=%s assigned connection target_uid=%s mode=queued",
            actor_id,
            user_id,
        )
        if message:
            await message.reply_text(ui_ok_text("Персональная ссылка сохранена и поставлена в очередь отправки"))

    data.pop("subscription_delivery_mode", None)
    if message:
        await message.reply_text(
            format_user_card(updated),
            parse_mode=ParseMode.HTML,
            reply_markup=user_card_kb(user_id),
        )
    return ADMIN_USER_MENU
