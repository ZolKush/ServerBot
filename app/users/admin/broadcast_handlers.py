"""Mass and direct messaging handlers for user administration."""

from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ...bot.guards import (
    authorized_ids,
    get_user_id,
    get_user_meta,
    require_admin,
    staff_title,
)
from ...bot.ui import (
    breadcrumbs,
    clip_html,
    clip_text,
    html_escape,
    ui_error_text,
    ui_ok_text,
    ui_warn_text,
    wrap_as_codeblock_html,
)
from ...messaging.outbox import message_payload
from ...runtime.logging import logger
from ...storage import make_outbox_event
from ..states import (
    ADMIN_ALL_MENU,
    ADMIN_ALL_MSG_CONFIRM,
    ADMIN_ALL_MSG_TEXT,
    ADMIN_PICK,
    ADMIN_USER_MENU,
    ADMIN_USER_MSG_TEXT,
)
from ..views import (
    format_user_card,
    user_card_kb,
    users_all_confirm_kb,
    users_all_kb,
    users_list_kb,
    users_list_title,
)
from .navigation import conversation_data, get_users_filter
from .operations import queue_broadcast, queue_direct_message


@require_admin
async def users_all_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()

    if query.data == "users:back":
        active_filter = get_users_filter(context)
        await query.edit_message_text(
            users_list_title(active_filter),
            parse_mode=ParseMode.HTML,
            reply_markup=users_list_kb(active_filter),
        )
        return ADMIN_PICK
    if query.data == "users:allmsg":
        await query.edit_message_text(
            f"<b>{html_escape(breadcrumbs('Админ-панель', 'Пользователи', 'Рассылка', 'Текст'))}</b>\n\n"
            "Введите текст сообщения всем пользователям:",
            parse_mode=ParseMode.HTML,
        )
        return ADMIN_ALL_MSG_TEXT

    await query.edit_message_text(
        f"<b>{html_escape(breadcrumbs('Админ-панель', 'Пользователи', 'Рассылка'))}</b>\n\nВыберите действие:",
        parse_mode=ParseMode.HTML,
        reply_markup=users_all_kb(),
    )
    return ADMIN_ALL_MENU


@require_admin
async def users_all_msg_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    text = ((message.text if message else "") or "").strip()
    if not text:
        if message:
            await message.reply_text(ui_error_text("пустой текст. Введите сообщение:"))
        return ADMIN_ALL_MSG_TEXT
    conversation_data(context)["users_all_broadcast_text"] = text
    preview = (
        f"<b>{html_escape(breadcrumbs('Админ-панель', 'Пользователи', 'Рассылка', 'Проверка'))}</b>\n\n"
        "Сообщение будет отправлено всем авторизованным пользователям (кроме вас).\n\n"
        + wrap_as_codeblock_html(clip_text(text, limit=3000))
        + "\n\nПодтвердите действие:"
    )
    if message:
        await message.reply_text(
            preview,
            parse_mode=ParseMode.HTML,
            reply_markup=users_all_confirm_kb(),
        )
    return ADMIN_ALL_MSG_CONFIRM


@require_admin
async def users_all_msg_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()
    callback_data = query.data or ""
    if callback_data in ("users:back", "users:all"):
        await query.edit_message_text(
            f"<b>{html_escape(breadcrumbs('Админ-панель', 'Пользователи', 'Рассылка'))}</b>\n\nВыберите действие:",
            parse_mode=ParseMode.HTML,
            reply_markup=users_all_kb(),
        )
        return ADMIN_ALL_MENU
    if callback_data != "users:allsend":
        return ADMIN_ALL_MSG_CONFIRM

    user_data = conversation_data(context)
    text = str(user_data.get("users_all_broadcast_text", "")).strip()
    if not text:
        await query.edit_message_text(ui_error_text("текст рассылки потерян. Повторите позже."))
        return ADMIN_PICK

    sender_id = get_user_id(update)
    recipients = authorized_ids(
        role_filter=None,
        exclude={sender_id} if sender_id else set(),
    )
    if not recipients:
        await query.edit_message_text(ui_warn_text("нет получателей для рассылки."))
        return ADMIN_PICK

    payload = (
        "📣 <b>Массовая рассылка</b>\n\n"
        f"Отправитель: <b>{html_escape(staff_title(update))}</b>\n\n"
        f"{clip_html(text, limit=3000)}"
    )
    event = make_outbox_event(
        kind="admin_broadcast",
        recipient_ids=recipients,
        payload=message_payload(payload),
    )
    await queue_broadcast(
        event,
        sender_id=sender_id,
        recipient_count=len(recipients),
    )
    logger.info(
        "Admin user_id=%s queued broadcast recipients=%s",
        sender_id,
        len(recipients),
    )
    user_data.pop("users_all_broadcast_text", None)
    await query.edit_message_text(
        ui_ok_text(f"Рассылка сохранена в очереди для {len(recipients)} получателей."),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]]),
    )
    return ADMIN_PICK


@require_admin
async def users_user_msg_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
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

    text = ((message.text if message else "") or "").strip()
    if not text:
        if message:
            await message.reply_text(ui_error_text("пустой текст. Введите сообщение:"))
        return ADMIN_USER_MSG_TEXT

    actor_id = get_user_id(update)
    payload = (
        "✉️ <b>Персональное сообщение</b>\n\n"
        f"Отправитель: <b>{html_escape(staff_title(update))}</b>\n\n"
        f"{clip_html(text, limit=3000)}"
    )
    event = make_outbox_event(
        kind="admin_direct_message",
        recipient_ids=[user_id],
        payload=message_payload(payload),
    )
    await queue_direct_message(
        event,
        actor_id=actor_id,
        target_user_id=user_id,
    )
    logger.info(
        "Admin user_id=%s queued direct message target_uid=%s",
        actor_id,
        user_id,
    )
    if message:
        await message.reply_text(ui_ok_text("Сообщение сохранено в очереди отправки"))
        await message.reply_text(
            format_user_card(meta),
            parse_mode=ParseMode.HTML,
            reply_markup=user_card_kb(user_id),
        )
    return ADMIN_USER_MENU
