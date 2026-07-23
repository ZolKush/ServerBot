"""Access-state actions shown on an administrative user card."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ...bot.guards import display_name, get_user_id
from ...bot.ui import ui_ok_text, ui_warn_text
from ...config import TZ
from ...runtime.logging import logger
from ..states import ADMIN_USER_MENU
from ..views import (
    confirm_access_kb,
    confirm_toggle_kb,
    format_user_card,
    user_card_kb,
)
from .navigation import back_to_user_list
from .operations import update_access_state


async def action_access(
    query: Any,
    user_id: int,
    meta: dict[str, Any],
    *,
    desired_state: str,
) -> int:
    if meta.get("role") == "admin":
        await query.edit_message_text(
            format_user_card(meta) + "\n\n" + ui_warn_text("доступ администраторов здесь изменять нельзя."),
            parse_mode=ParseMode.HTML,
            reply_markup=user_card_kb(user_id),
        )
        return ADMIN_USER_MENU
    current_state = str(meta.get("access_state") or ("approved" if meta.get("enabled", True) else "blocked"))
    if desired_state == "blocked":
        action = "забанить"
    else:
        action = "разбанить" if current_state == "blocked" else "одобрить доступ"
    await query.edit_message_text(
        format_user_card(meta) + f"\n\n{ui_warn_text(f'Подтвердите действие: {action}.')}",
        parse_mode=ParseMode.HTML,
        reply_markup=confirm_access_kb(
            user_id,
            desired_state=desired_state,
            current_state=current_state,
        ),
    )
    return ADMIN_USER_MENU


async def action_toggle(
    query: Any,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    meta: dict[str, Any],
) -> int:
    del context
    if meta.get("role") == "admin":
        await query.edit_message_text(
            format_user_card(meta) + "\n\n" + ui_warn_text("администраторов банить нельзя."),
            parse_mode=ParseMode.HTML,
            reply_markup=user_card_kb(user_id),
        )
        return ADMIN_USER_MENU
    state = str(meta.get("access_state") or ("approved" if meta.get("enabled", True) else "blocked"))
    action = "забанить" if state == "approved" else ("разбанить" if state == "blocked" else "одобрить доступ")
    await query.edit_message_text(
        format_user_card(meta) + f"\n\n{ui_warn_text(f'Подтвердите действие: {action}.')}",
        parse_mode=ParseMode.HTML,
        reply_markup=confirm_toggle_kb(user_id, access_state=state),
    )
    return ADMIN_USER_MENU


async def action_toggle_apply(
    update: Update,
    query: Any,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    meta: dict[str, Any],
    *,
    desired_state: str | None = None,
) -> int:
    del meta
    actor_id = get_user_id(update)
    outcome, updated = await update_access_state(
        target_user_id=user_id,
        actor_id=actor_id,
        actor_name=display_name(update),
        changed_at=datetime.now(TZ).isoformat(),
        desired_state=desired_state,
    )
    if outcome == "missing" or updated is None:
        return await back_to_user_list(query, context)
    if outcome == "admin":
        await query.edit_message_text(
            format_user_card(updated) + "\n\n" + ui_warn_text("администраторов банить нельзя."),
            parse_mode=ParseMode.HTML,
            reply_markup=user_card_kb(user_id),
        )
        return ADMIN_USER_MENU
    if outcome in {"already", "invalid"}:
        note = "Статус пользователя уже установлен." if outcome == "already" else "Некорректное действие."
        await query.edit_message_text(
            format_user_card(updated) + "\n\n" + ui_warn_text(note),
            parse_mode=ParseMode.HTML,
            reply_markup=user_card_kb(user_id),
        )
        return ADMIN_USER_MENU

    logger.info(
        "Admin user_id=%s changed access_state=%s target_uid=%s",
        actor_id,
        updated.get("access_state"),
        user_id,
        extra={"user_id": actor_id, "action": "access_toggle"},
    )
    await query.edit_message_text(
        format_user_card(updated) + "\n\n" + ui_ok_text("Статус пользователя обновлён."),
        parse_mode=ParseMode.HTML,
        reply_markup=user_card_kb(user_id),
    )
    return ADMIN_USER_MENU
