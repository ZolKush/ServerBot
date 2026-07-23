"""Administrative user-card dispatcher and action prompts."""

from __future__ import annotations

import re
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ...bot.guards import get_user_meta, require_admin
from ..states import (
    ADMIN_PICK,
    ADMIN_USER_CFG_TEXT,
    ADMIN_USER_MENU,
    ADMIN_USER_MSG_TEXT,
    ADMIN_USER_NICK_TEXT,
)
from ..views import format_user_card, user_card_kb
from .access_handlers import action_access, action_toggle, action_toggle_apply
from .navigation import (
    back_to_user_list,
    conversation_data,
    resolve_user_or_redirect,
)

USER_ACTION_RE = re.compile(
    r"^users:(?P<action>toggle|toggleapply|msg|nick|subassign|subsend):"
    r"(?P<uid>\d+)$"
)
USER_ACCESS_ACTION_RE = re.compile(
    r"^users:(?P<stage>access|accessapply):"
    r"(?P<decision>approve|block):(?P<uid>\d+)$"
)


def subscription_mode_prompt(mode: str) -> str:
    if mode == "assign":
        return (
            "Вставьте персональную ссылку подключения одним сообщением. "
            "Она будет только сохранена за пользователем в хранилище данных "
            "без отправки уведомления."
            "\n\nСсылка должна начинаться с http:// или https://."
        )
    return (
        "Вставьте персональную ссылку подключения одним сообщением. "
        "Она будет сохранена за пользователем в хранилище данных и сразу "
        "отправлена ему уведомлением."
        "\n\nСсылка должна начинаться с http:// или https://."
    )


async def action_message(
    query: Any,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
) -> int:
    conversation_data(context)["selected_uid"] = user_id
    await query.edit_message_text("Введите текст личного сообщения пользователю:")
    return ADMIN_USER_MSG_TEXT


async def action_nickname(
    query: Any,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
) -> int:
    conversation_data(context)["selected_uid"] = user_id
    await query.edit_message_text("Введите никнейм (как должен отображаться в списке):")
    return ADMIN_USER_NICK_TEXT


async def action_subscription(
    query: Any,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    mode: str,
) -> int:
    data = conversation_data(context)
    data["selected_uid"] = user_id
    data["subscription_delivery_mode"] = mode
    await query.edit_message_text(
        subscription_mode_prompt(mode),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "⬅️ Назад",
                        callback_data=f"users:user:{user_id}",
                    )
                ],
                [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
            ]
        ),
    )
    return ADMIN_USER_CFG_TEXT


@require_admin
async def users_user_menu(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()
    data = query.data or ""

    if data == "users:back":
        return await back_to_user_list(query, context)

    access_match = USER_ACCESS_ACTION_RE.fullmatch(data)
    if access_match:
        user_id = int(access_match.group("uid"))
        meta = await resolve_user_or_redirect(query, context, user_id)
        if meta is None:
            return ADMIN_PICK
        desired_state = "approved" if access_match.group("decision") == "approve" else "blocked"
        if access_match.group("stage") == "access":
            return await action_access(
                query,
                user_id,
                meta,
                desired_state=desired_state,
            )
        return await action_toggle_apply(
            update,
            query,
            context,
            user_id,
            meta,
            desired_state=desired_state,
        )

    action_match = USER_ACTION_RE.fullmatch(data)
    if action_match:
        action = action_match.group("action")
        user_id = int(action_match.group("uid"))

        if action == "msg":
            return await action_message(query, context, user_id)
        if action == "nick":
            return await action_nickname(query, context, user_id)
        if action == "subassign":
            return await action_subscription(query, context, user_id, "assign")
        if action == "subsend":
            return await action_subscription(query, context, user_id, "send")

        meta = await resolve_user_or_redirect(query, context, user_id)
        if meta is None:
            return ADMIN_PICK
        if action == "toggle":
            return await action_toggle(query, context, user_id, meta)
        if action == "toggleapply":
            return await action_toggle_apply(
                update,
                query,
                context,
                user_id,
                meta,
            )

    selected = conversation_data(context).get("selected_uid")
    if isinstance(selected, int):
        meta = get_user_meta(selected)
        if meta:
            await query.edit_message_text(
                format_user_card(meta),
                parse_mode=ParseMode.HTML,
                reply_markup=user_card_kb(selected),
            )
            return ADMIN_USER_MENU

    return await back_to_user_list(query, context)
