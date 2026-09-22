"""Administrative input and confirmation for one purchase's price and duration."""

from __future__ import annotations

import re
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ...bot.guards import require_admin
from ...bot.ui import clip_html_message, html_escape
from ...config import TZ
from ...messaging.message_cleanup import record_navigation_result
from ...messaging.review_navigation import retire_review_card_message
from ...messaging.review_sync import record_review_delivery, review_completion, sync_service_review_messages
from ...runtime.logging import logger
from ...storage import get_user_meta_copy, service_requests_snapshot, update_user_data
from . import state
from .terms import parse_terms, terms_version, update_request_terms
from .views import request_card, request_markup


def _cancel_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="product:cancel")]])


@require_admin
async def terms_input_start_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    match = re.fullmatch(r"product:input:terms:(\d+)", query.data or "")
    if not match:
        return ConversationHandler.END
    await query.answer()
    request_id = int(match.group(1))
    request = service_requests_snapshot().get(str(request_id))
    if not request or request.get("kind") != "purchase" or request.get("status") != "pending":
        await query.edit_message_text("Цена и срок доступны для изменения только до отправки реквизитов.")
        return ConversationHandler.END
    state.clear_request_context(context)
    data = state.context_data(context)
    data[state.CTX_ACTION] = "request_terms"
    data[state.CTX_REQUEST_ID] = request_id
    data[state.CTX_PENDING] = {"expected": terms_version(request)}
    await retire_review_card_message(update)
    await query.edit_message_text(
        clip_html_message(request_card(request, get_user_meta_copy(int(request["user_id"])) or {}), limit=3000)
        + "\n\n✏️ <b>Цена и срок этой заявки</b>\n"
        "Введите сумму в рублях и срок в месяцах через вертикальную черту:\n"
        "<code>500 | 2</code>\n"
        "Цена — целое число от 1 до 1000000, срок — от 1 до 36 месяцев.\n\n"
        "Дата окончания будет рассчитана от текущего момента. "
        "Чтобы задать её точно, добавьте дату и время:\n"
        "<code>500 | 2 | 31.12.2026 23:59</code>\n"
        f"Часовой пояс: {html_escape(str(TZ))}.\n\n"
        "Изменения действуют только для этой оплаты. Следующий период — по стандартной цене.",
        parse_mode=ParseMode.HTML,
        reply_markup=_cancel_markup(),
    )
    await record_navigation_result(update, True)
    return state.PRODUCT_INPUT


async def handle_terms_input(update: Update, data: dict[str, Any], text: str) -> int:
    message = update.effective_message
    if not message:
        return ConversationHandler.END
    terms = parse_terms(text)
    if terms is None:
        await message.reply_text(
            "Введите: сумма | месяцы | ДД.ММ.ГГГГ ЧЧ:ММ. "
            "Сумма — целое число от 1 до 1000000, срок — от 1 до 36 месяцев, дата — в будущем. "
            "Дату можно опустить: 500 | 2."
        )
        return state.PRODUCT_INPUT
    pending = data.get(state.CTX_PENDING)
    if not isinstance(pending, dict) or not isinstance(pending.get("expected"), dict):
        await message.reply_text("Ввод устарел. Откройте заявку заново.")
        return ConversationHandler.END
    pending.update(kind="request_terms", terms=terms, request_id=data[state.CTX_REQUEST_ID])
    result = await message.reply_text(
        f"Сохранить условия заявки #{data[state.CTX_REQUEST_ID]}?\n\n"
        f"Стоимость: {terms['amount_rub']} ₽\n"
        f"Срок, мес.: {terms['period_months']}\n"
        f"Доступ до: {state.datetime_text(terms['target_end_at'])}\n\n"
        "Условия действуют только для этой оплаты. После сохранения нажмите «Отправить реквизиты» в заявке.",
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("✅ Сохранить", callback_data="product:confirm:apply")],
                [InlineKeyboardButton("❌ Отмена", callback_data="product:cancel")],
            ]
        ),
    )
    await record_navigation_result(update, result)
    return state.PRODUCT_CONFIRM


async def confirm_terms(
    update: Update, context: ContextTypes.DEFAULT_TYPE, actor: dict[str, Any], pending: dict[str, Any]
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    request_id = int(pending.get("request_id", 0) or 0)
    outcome = await update_user_data(
        lambda config: update_request_terms(
            config,
            request_id=request_id,
            actor=actor,
            expected=pending.get("expected", {}),
            terms=pending.get("terms", {}),
        )
    )
    state.clear_request_context(context)
    if outcome == "updated":
        request = service_requests_snapshot()[str(request_id)]
        await query.edit_message_text(
            "✅ Условия сохранены. Можно отправить реквизиты.\n\n"
            + clip_html_message(request_card(request, get_user_meta_copy(int(request["user_id"])) or {}), limit=3900),
            parse_mode=ParseMode.HTML,
            reply_markup=request_markup(request, actor),
        )
        bot = getattr(context, "bot", None)
        if bot is not None:
            try:
                if update.effective_user and query.message:
                    await record_review_delivery(
                        bot,
                        review_completion(
                            scope="service", target_id=request_id, generation=str(request.get("created_at") or "")
                        ),
                        update.effective_user.id,
                        query.message,
                    )
                await sync_service_review_messages(bot, request_id)
            except Exception:
                logger.exception("Could not synchronize purchase terms request_id=%s", request_id)
    else:
        await query.edit_message_text(
            {
                "forbidden": "Недостаточно прав для изменения условий.",
                "stale": "Заявка уже обработана или реквизиты отправлены. Изменения не сохранены.",
                "changed": "Другой сотрудник изменил условия. Откройте заявку заново.",
                "invalid": "Некорректные условия или дата уже истекла. Откройте заявку заново.",
            }[outcome],
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ К заявке", callback_data=f"product:req:view:{request_id}")]]
            ),
        )
    return ConversationHandler.END
