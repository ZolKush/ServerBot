"""User-list entry point, filtering, paging, and user selection."""

from __future__ import annotations

import re

from telegram import Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import ContextTypes, ConversationHandler

from ...bot.guards import get_user_meta, require_admin
from ...bot.menu import show_main_menu
from ...bot.ui import breadcrumbs, html_escape, ui_error_text
from ...messaging.message_cleanup import record_navigation_result
from ..states import ADMIN_ALL_MENU, ADMIN_PICK, ADMIN_USER_MENU
from ..views import format_user_card, user_card_kb, users_all_kb, users_list_kb, users_list_title
from .navigation import conversation_data, get_users_filter, set_users_filter


@require_admin
async def users_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    message = update.effective_message
    active_filter = get_users_filter(context)
    title = users_list_title(active_filter)
    if query and message:
        await query.answer()
        result = await query.edit_message_text(
            title,
            parse_mode=ParseMode.HTML,
            reply_markup=users_list_kb(active_filter),
        )
        await record_navigation_result(update, result)
    elif message:
        result = await message.reply_text(
            title,
            parse_mode=ParseMode.HTML,
            reply_markup=users_list_kb(active_filter),
        )
        await record_navigation_result(update, result)
    return ADMIN_PICK


@require_admin
async def users_pick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()
    data = query.data or ""

    if data == "users:main":
        await show_main_menu(update)
        return ConversationHandler.END

    if data == "users:all":
        await query.edit_message_text(
            f"<b>{html_escape(breadcrumbs('Админ-панель', 'Пользователи', 'Рассылка'))}</b>\n\nВыберите действие:",
            parse_mode=ParseMode.HTML,
            reply_markup=users_all_kb(),
        )
        return ADMIN_ALL_MENU

    filter_match = re.fullmatch(r"users:filter:(all|active|disabled|unpaid|admins|blocked)", data)
    if filter_match:
        active_filter = set_users_filter(context, filter_match.group(1))
        await query.edit_message_text(
            users_list_title(active_filter),
            parse_mode=ParseMode.HTML,
            reply_markup=users_list_kb(active_filter),
        )
        return ADMIN_PICK

    page_match = re.fullmatch(r"users:page:(\d+)", data)
    if page_match:
        active_filter = get_users_filter(context)
        try:
            await query.edit_message_text(
                users_list_title(active_filter),
                parse_mode=ParseMode.HTML,
                reply_markup=users_list_kb(active_filter, page=int(page_match.group(1))),
            )
        except BadRequest as error:
            if "message is not modified" not in str(error).lower():
                raise
        return ADMIN_PICK

    user_match = re.fullmatch(r"users:user:(\d+)", data)
    if user_match:
        user_id = int(user_match.group(1))
        meta = get_user_meta(user_id)
        if not meta:
            active_filter = get_users_filter(context)
            await query.edit_message_text(
                ui_error_text("пользователь не найден (возможно, удалён из списка)."),
                reply_markup=users_list_kb(active_filter),
            )
            return ADMIN_PICK
        conversation_data(context)["selected_uid"] = user_id
        await query.edit_message_text(
            format_user_card(meta),
            parse_mode=ParseMode.HTML,
            reply_markup=user_card_kb(user_id),
        )
        return ADMIN_USER_MENU

    active_filter = get_users_filter(context)
    await query.edit_message_text(
        users_list_title(active_filter),
        parse_mode=ParseMode.HTML,
        reply_markup=users_list_kb(active_filter),
    )
    return ADMIN_PICK
