"""Entry points for subscription-management text input."""

from __future__ import annotations

import re

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ...bot.guards import require_admin
from ...storage import get_user_meta_copy
from ...users.staff import (
    is_billing_exempt_meta,
    is_lead_or_owner_meta,
    is_owner_meta,
)
from . import state


@require_admin
async def product_input_start_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    query = update.callback_query
    actor = state.actor_meta(update)
    if not query or not actor:
        return ConversationHandler.END
    data = query.data or ""
    action = ""
    target_user_id: int | None = None
    prompt = ""
    if data == "product:input:massdate":
        if not is_lead_or_owner_meta(actor):
            await query.answer("Недостаточно прав.", show_alert=True)
            return ConversationHandler.END
        action = "mass_date"
        prompt = (
            "Введите новую дату в формате <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>.\n\n"
            "Для выбранных пользователей добавьте после вертикальной черты "
            "Telegram ID через запятую:\n"
            "<code>31.10.2026 23:59 | 123456, 789012</code>\n\n"
            "Без списка дата будет назначена всем оплаченным подписчикам."
        )
    elif data == "product:input:massremind":
        if not is_lead_or_owner_meta(actor):
            await query.answer("Недостаточно прав.", show_alert=True)
            return ConversationHandler.END
        action = "mass_reminder"
        prompt = (
            "Укажите получателей:\n\n"
            "• <code>все</code> — все оплаченные подписчики;\n"
            "• <code>до 31.10.2026 23:59</code> — подписчики "
            "с окончанием не позднее даты;\n"
            "• <code>123456, 789012</code> — конкретные Telegram ID."
        )
    else:
        match = re.fullmatch(
            r"product:input:(user_end|manualpay):(\d+)",
            data,
        )
        if not match:
            return ConversationHandler.END
        action, user_id_text = match.groups()
        target_user_id = int(user_id_text)
        if action == "user_end" and not is_lead_or_owner_meta(actor):
            await query.answer("Недостаточно прав.", show_alert=True)
            return ConversationHandler.END
        if action == "manualpay" and not is_owner_meta(actor):
            await query.answer(
                "Оплату подтверждает только руководитель сервиса.",
                show_alert=True,
            )
            return ConversationHandler.END
        target = get_user_meta_copy(target_user_id)
        if action == "manualpay" and is_billing_exempt_meta(target):
            await query.answer(
                "У руководителя сервиса бессрочный оплаченный доступ.",
                show_alert=True,
            )
            return ConversationHandler.END
        prompt = (
            "Введите новую дату окончания в формате <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>:"
            if action == "user_end"
            else "Введите дату окончания оплаченного доступа в формате <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>:"
        )
    await query.answer()
    state.clear_request_context(context)
    context_state = state.context_data(context)
    context_state[state.CTX_ACTION] = action
    if target_user_id is not None:
        context_state[state.CTX_TARGET_UID] = target_user_id
    await query.edit_message_text(
        prompt,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="product:cancel")]]),
    )
    return state.PRODUCT_INPUT


__all__ = ["product_input_start_cb"]
