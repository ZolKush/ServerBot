"""Administrative callback orchestration for subscription requests."""

from __future__ import annotations

import re

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, ConversationHandler

from ...bot.guards import require_admin
from ...storage import update_user_data
from ...users.staff import is_owner_meta
from . import state
from .review_operations import (
    approve_trial,
    confirm_payment,
    reject_request,
    reset_unconfirmed_payment,
    send_requisites,
)


def _prepare_connection_input(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    action: str,
    request_id: int,
) -> None:
    state.clear_request_context(context)
    data = state.context_data(context)
    data[state.CTX_ACTION] = action
    data[state.CTX_REQUEST_ID] = request_id


@require_admin
async def product_request_action_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    query = update.callback_query
    actor = state.actor_meta(update)
    if not query or not actor:
        return ConversationHandler.END
    match = re.fullmatch(
        r"product:req:(approve|reject|requisites|confirm|notfound):(\d+)",
        query.data or "",
    )
    if not match:
        return ConversationHandler.END
    action, request_id_text = match.groups()
    request_id = int(request_id_text)
    await query.answer()

    if action == "approve":
        outcome, _request = await update_user_data(
            lambda config: approve_trial(
                config,
                request_id=request_id,
                actor=actor,
            )
        )
        if outcome == "need_link":
            _prepare_connection_input(
                context,
                action="request_link",
                request_id=request_id,
            )
            await query.edit_message_text(
                "🔗 Вставьте персональную ссылку подключения одним сообщением. "
                "Поддерживаются только ссылки HTTP/HTTPS.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("❌ Отмена", callback_data="product:cancel")]]
                ),
            )
            return state.PRODUCT_INPUT
        messages = {
            "completed": ("Тестовый доступ одобрен, существующая ссылка отправлена пользователю."),
            "claimed": "Заявку уже обрабатывает другой сотрудник.",
            "stale": "Заявка уже обработана.",
            "tier_changed": ("Уровень пользователя уже изменился; заявка на тест отменена."),
            "already_issued": "Тестовый доступ уже выдавался.",
            "missing": "Заявка или пользователь не найдены.",
        }
        await query.edit_message_text(messages.get(outcome, "Заявка не обработана."))
        return ConversationHandler.END

    if action == "requisites":
        outcome, _request = await update_user_data(
            lambda config: send_requisites(
                config,
                request_id=request_id,
                actor=actor,
            )
        )
        messages = {
            "sent": "Реквизиты поставлены в очередь отправки пользователю.",
            "already_sent": "Реквизиты уже были отправлены пользователю.",
            "tier_changed": "Уровень пользователя уже изменился; заявка отменена.",
            "not_configured": "Реквизиты или дата периода заполнены не полностью.",
            "stale": "Заявка уже перешла на другой этап.",
            "missing": "Заявка не найдена.",
        }
        await query.edit_message_text(messages[outcome])
        return ConversationHandler.END

    if action == "reject":
        outcome = await update_user_data(
            lambda config: reject_request(
                config,
                request_id=request_id,
                actor=actor,
            )
        )
        await query.edit_message_text(
            {
                "rejected": "Заявка отклонена.",
                "owner_only": ("Платёжное решение может принять только руководитель сервиса."),
                "claimed": "Заявку уже обрабатывает другой сотрудник.",
                "stale": "Заявка уже обработана.",
            }[outcome]
        )
        return ConversationHandler.END

    if not is_owner_meta(actor):
        await query.edit_message_text("Подтверждать оплату может только руководитель сервиса.")
        return ConversationHandler.END

    if action == "notfound":
        outcome = await update_user_data(
            lambda config: reset_unconfirmed_payment(
                config,
                request_id=request_id,
                actor=actor,
            )
        )
        text = "Пользователь уведомлён." if outcome == "reset" else "Заявка уже обработана."
        await query.edit_message_text(text)
        return ConversationHandler.END

    outcome, _request = await update_user_data(
        lambda config: confirm_payment(
            config,
            request_id=request_id,
            actor=actor,
        )
    )
    if outcome == "need_link":
        _prepare_connection_input(
            context,
            action="payment_link",
            request_id=request_id,
        )
        await query.edit_message_text(
            "Оплата найдена, но у пользователя нет персональной ссылки. "
            "Вставьте ссылку HTTP/HTTPS для завершения активации.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="product:cancel")]]),
        )
        return state.PRODUCT_INPUT
    messages = {
        "completed": "Оплата подтверждена, доступ пользователя активирован.",
        "claimed": "Ввод ссылки уже выполняет другой руководитель сервиса.",
        "invalid_target": (
            "Дата оплачиваемого периода уже истекла. Исправьте период или зарегистрируйте оплату вручную."
        ),
        "stale": "Заявка уже обработана.",
        "missing": "Заявка или пользователь не найдены.",
    }
    await query.edit_message_text(messages[outcome])
    return ConversationHandler.END


__all__ = ["product_request_action_cb"]
