"""Main menu presentation and callbacks."""

from __future__ import annotations

from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from ..config import (
    MENU_ADMINISTRATION,
    MENU_MAINT,
    MENU_REQUESTS,
    MENU_STATUS,
    MENU_SUBSCRIPTION,
    MENU_TICKET,
    MENU_USERS,
)
from ..messaging.message_cleanup import record_navigation_result
from ..users.staff import can_manage_maintenance_meta
from .guards import get_user_id, get_user_meta, has_subscriber_access, is_admin, require_auth


def main_menu_inline_kb_for_meta(meta: dict[str, Any] | None) -> InlineKeyboardMarkup:
    is_admin_user = bool(meta and meta.get("role") == "admin")
    rows: list[list[InlineKeyboardButton]] = []
    if has_subscriber_access(meta):
        rows.append(
            [
                InlineKeyboardButton(MENU_STATUS, callback_data="menu:status"),
                InlineKeyboardButton(MENU_SUBSCRIPTION, callback_data="menu:subscription"),
            ]
        )
    else:
        rows.append([InlineKeyboardButton(MENU_SUBSCRIPTION, callback_data="menu:subscription")])
    rows.append([InlineKeyboardButton(MENU_TICKET, callback_data="menu:ticket")])
    if is_admin_user:
        rows.append(
            [
                InlineKeyboardButton(MENU_USERS, callback_data="menu:users"),
                InlineKeyboardButton(MENU_REQUESTS, callback_data="product:requests"),
            ]
        )
        administration_row: list[InlineKeyboardButton] = []
        if can_manage_maintenance_meta(meta):
            administration_row.append(InlineKeyboardButton(MENU_MAINT, callback_data="menu:maint"))
        administration_row.append(InlineKeyboardButton(MENU_ADMINISTRATION, callback_data="administration:show"))
        rows.append(administration_row)
    rows.append([InlineKeyboardButton("ℹ️ Помощь", callback_data="menu:help")])
    return InlineKeyboardMarkup(rows)


def main_menu_inline_kb_for_admin(is_admin_user: bool) -> InlineKeyboardMarkup:
    meta = {"role": "admin", "service_tier": "subscriber"} if is_admin_user else {"service_tier": "subscriber"}
    return main_menu_inline_kb_for_meta(meta)


def main_menu_inline_kb(update: Update) -> InlineKeyboardMarkup:
    uid = get_user_id(update)
    return main_menu_inline_kb_for_meta(get_user_meta(uid) if uid is not None else None)


def main_menu_text(is_admin_user: bool, text: str = "Меню:") -> str:
    if text == "Меню:":
        title = "👑 <b>Админ-панель</b>" if is_admin_user else "👤 <b>Главное меню</b>"
        return f"{title}\n\nВыберите раздел:"
    return text


async def show_main_menu(update: Update, text: str = "Меню:") -> None:
    query = update.callback_query
    markup = main_menu_inline_kb(update)
    text = main_menu_text(is_admin(update), text=text)
    if query:
        await query.answer()
        try:
            result = await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
        except BadRequest as error:
            if "message is not modified" in str(error).lower():
                return
            raise
        await record_navigation_result(update, result)
        return
    message = update.effective_message
    if message:
        result = await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
        await record_navigation_result(update, result)


@require_auth
async def menu_home_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await show_main_menu(update)
