from __future__ import annotations

import re
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..bot.guards import get_user_id, require_admin
from ..bot.ui import html_escape, ui_ok_text
from ..storage import get_user_meta_copy
from ..users.staff import (
    REGULAR_STAFF_TITLES,
    STAFF_DISPLAY_TITLE_ALIAS,
    STAFF_TITLE_LABELS,
    is_owner_meta,
    staff_title_label,
)
from .operations import (
    change_staff_display_mode,
    change_staff_title,
)
from .views import administration_markup, administration_text


def actor_meta(update: Update) -> dict[str, Any] | None:
    user_id = get_user_id(update)
    return get_user_meta_copy(user_id) if user_id is not None else None


@require_admin
async def administration_show_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = actor_meta(update)
    if not query or not actor:
        return
    await query.answer()
    await query.edit_message_text(
        administration_text(actor),
        parse_mode=ParseMode.HTML,
        reply_markup=administration_markup(actor),
    )


@require_admin
async def administration_signature_mode_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    actor = actor_meta(update)
    if not query or not actor:
        return
    match = re.fullmatch(
        r"(?:administration:signature|staff:mode):(title|title_alias)",
        query.data or "",
    )
    if not match:
        return
    await query.answer()
    mode = match.group(1)
    if mode == STAFF_DISPLAY_TITLE_ALIAS and not actor.get("staff_alias"):
        await query.edit_message_text(
            "Сначала задайте псевдоним.",
            reply_markup=administration_markup(actor),
        )
        return
    updated = await change_staff_display_mode(
        user_id=int(actor.get("user_id") or 0),
        mode=mode,
    )
    await query.edit_message_text(
        administration_text(updated),
        parse_mode=ParseMode.HTML,
        reply_markup=administration_markup(updated),
    )


@require_admin
async def administration_staff_title_menu_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    actor = actor_meta(update)
    if not query or not actor:
        return
    match = re.fullmatch(
        r"(?:administration:title|product:titlemenu):(\d+)",
        query.data or "",
    )
    if not match:
        return
    await query.answer()
    if not is_owner_meta(actor):
        await query.edit_message_text("Доступно только руководителю сервиса.")
        return
    user_id = int(match.group(1))
    target = get_user_meta_copy(user_id)
    if not target or target.get("role") != "admin" or is_owner_meta(target):
        await query.edit_message_text("Должность этого пользователя изменить нельзя.")
        return
    rows = [
        [
            InlineKeyboardButton(
                STAFF_TITLE_LABELS[code],
                callback_data=f"administration:title:{user_id}:{code}",
            )
        ]
        for code in REGULAR_STAFF_TITLES
    ]
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"product:manage:{user_id}")])
    await query.edit_message_text(
        f"🪪 <b>Должность сотрудника</b>\n\nТекущая: <b>{html_escape(staff_title_label(target))}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


@require_admin
async def administration_staff_title_apply_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    actor = actor_meta(update)
    if not query or not actor:
        return
    match = re.fullmatch(
        r"(?:administration:title|product:title):(\d+):([a-z_]+)",
        query.data or "",
    )
    if not match:
        return
    await query.answer()
    if not is_owner_meta(actor):
        await query.edit_message_text("Доступно только руководителю сервиса.")
        return
    user_id = int(match.group(1))
    title_code = match.group(2)
    if title_code not in REGULAR_STAFF_TITLES:
        await query.edit_message_text("Неизвестная должность.")
        return

    updated = await change_staff_title(
        actor=actor,
        target_user_id=user_id,
        title_code=title_code,
    )
    if not updated:
        await query.edit_message_text("Сотрудник не найден.")
        return
    await query.edit_message_text(
        ui_ok_text(f"Должность изменена: {STAFF_TITLE_LABELS[title_code]}"),
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Назад", callback_data=f"product:manage:{user_id}")]]
        ),
    )


__all__ = [
    "actor_meta",
    "administration_show_cb",
    "administration_signature_mode_cb",
    "administration_staff_title_apply_cb",
    "administration_staff_title_menu_cb",
]
