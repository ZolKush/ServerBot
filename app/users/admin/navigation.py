"""Conversation-local navigation and selection helpers."""

from __future__ import annotations

from typing import Any

from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ...bot.guards import get_user_meta
from ...bot.ui import ui_error_text
from ..states import ADMIN_PICK
from ..views import USER_FILTER_ALL, USER_FILTERS, users_list_kb, users_list_title


def conversation_data(context: ContextTypes.DEFAULT_TYPE) -> dict[Any, Any]:
    data = context.user_data
    if data is None:
        raise RuntimeError("Telegram user_data is unavailable")
    return data


def get_users_filter(context: ContextTypes.DEFAULT_TYPE) -> str:
    current = str(conversation_data(context).get("users_filter", USER_FILTER_ALL))
    return current if current in USER_FILTERS else USER_FILTER_ALL


def set_users_filter(context: ContextTypes.DEFAULT_TYPE, value: str) -> str:
    selected = value if value in USER_FILTERS else USER_FILTER_ALL
    conversation_data(context)["users_filter"] = selected
    return selected


async def back_to_user_list(query: Any, context: ContextTypes.DEFAULT_TYPE) -> int:
    active_filter = get_users_filter(context)
    await query.edit_message_text(
        users_list_title(active_filter),
        parse_mode=ParseMode.HTML,
        reply_markup=users_list_kb(active_filter),
    )
    return ADMIN_PICK


async def resolve_user_or_redirect(
    query: Any,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
) -> dict[str, Any] | None:
    meta = get_user_meta(user_id)
    if meta:
        return meta
    active_filter = get_users_filter(context)
    await query.edit_message_text(
        ui_error_text("пользователь не найден."),
        reply_markup=users_list_kb(active_filter),
    )
    return None
