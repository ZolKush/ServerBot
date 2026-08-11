"""Administrative callback orchestration for subscription requests."""

from __future__ import annotations

import re
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, ConversationHandler

from ...bot.guards import require_admin
from ...messaging.message_cleanup import record_navigation_result
from ...messaging.review_navigation import retire_review_card_message
from ...messaging.review_sync import (
    sync_service_review_messages,
    sync_service_review_messages_for_user,
)
from ...runtime.logging import logger
from ...storage import service_requests_snapshot, update_user_data
from ...users.staff import is_owner_meta
from ..policy import DEFAULT_TRIAL_DURATION_HOURS
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


def _query_coordinates(query: Any) -> tuple[int, int] | None:
    message = getattr(query, "message", None)
    try:
        chat_id = int(getattr(message, "chat_id", 0) or getattr(getattr(message, "chat", None), "id", 0) or 0)
        message_id = int(getattr(message, "message_id", 0) or 0)
    except (TypeError, ValueError, OverflowError):
        return None
    return (chat_id, message_id) if chat_id and message_id > 0 else None


async def _sync_request_cards(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    query: Any,
    request_id: int,
    *,
    keep_input_prompt: bool = False,
) -> None:
    coordinates = _query_coordinates(query) if keep_input_prompt else None
    if keep_input_prompt:
        try:
            # The original review card has become an input prompt. Forget its
            # persisted coordinates before any later user-wide synchronization
            # can overwrite that prompt with a request card again.
            await retire_review_card_message(update)
        except Exception:
            logger.exception("Could not retire input-prompt review card request_id=%s", request_id)
        try:
            await record_navigation_result(update, True)
        except Exception:
            logger.exception("Could not register review input prompt request_id=%s", request_id)

    bot = getattr(context, "bot", None)
    if bot is None:
        return
    try:
        excluded = {coordinates} if coordinates else ()
        request = service_requests_snapshot().get(str(request_id))
        user_id = int(request.get("user_id", 0) or 0) if isinstance(request, dict) else 0
        if user_id:
            await sync_service_review_messages_for_user(bot, user_id, exclude=excluded)
        else:
            await sync_service_review_messages(bot, request_id, exclude=excluded)
    except Exception:
        # The business mutation is already durable; Telegram synchronization
        # must never turn a successful decision into an apparent failure.
        logger.exception("Could not synchronize service request cards request_id=%s", request_id)


async def _approve_trial_for_duration(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    actor: dict[str, Any],
    request_id: int,
    duration_hours: int,
) -> int:
    query = update.callback_query
    if query is None:
        return ConversationHandler.END
    outcome, request = await update_user_data(
        lambda config: approve_trial(
            config,
            request_id=request_id,
            actor=actor,
            duration_hours=duration_hours,
        )
    )
    if outcome == "need_link":
        _prepare_connection_input(
            context,
            action="request_link",
            request_id=request_id,
        )
        deadline = state.datetime_text((request or {}).get("target_end_at"))
        await query.edit_message_text(
            "🔗 Вставьте персональную ссылку подключения одним сообщением. "
            "Поддерживаются только ссылки HTTP/HTTPS.\n\n"
            f"Ссылка должна быть ограничена сроком до: {deadline} ({duration_hours} ч).",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="product:cancel")]]),
        )
        await _sync_request_cards(update, context, query, request_id, keep_input_prompt=True)
        return state.PRODUCT_INPUT
    messages = {
        "completed": f"Тестовый доступ одобрен на {duration_hours} ч, существующая ссылка отправлена пользователю.",
        "claimed": "Заявку уже обрабатывает другой сотрудник.",
        "stale": "Заявка уже обработана.",
        "tier_changed": "Уровень пользователя уже изменился; заявка на тест отменена.",
        "already_issued": "Тестовый доступ уже выдавался.",
        "duration_forbidden": "Изменять срок теста может только руководитель сервиса.",
        "invalid_duration": "Некорректная длительность теста.",
        "admin_required": "Недостаточно прав.",
        "missing": "Заявка или пользователь не найдены.",
    }
    await query.edit_message_text(messages.get(outcome, "Заявка не обработана."))
    await _sync_request_cards(update, context, query, request_id)
    return ConversationHandler.END


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
        r"product:req:(approve|approve24|custom|reject|requisites|confirm|notfound):(\d+)",
        query.data or "",
    )
    if not match:
        return ConversationHandler.END
    action, request_id_text = match.groups()
    request_id = int(request_id_text)
    await query.answer()

    if action == "approve" and is_owner_meta(actor):
        await query.edit_message_text(
            "🧪 Выберите срок тестового доступа. Стандартный срок для сотрудников — 24 часа.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "✅ 24 часа",
                            callback_data=f"product:req:approve24:{request_id}",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "⏱ Другой срок",
                            callback_data=f"product:req:custom:{request_id}",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "⬅️ К заявке",
                            callback_data=f"product:req:view:{request_id}",
                        )
                    ],
                ]
            ),
        )
        # The chooser is still synchronized as a review card, but it is also a
        # transient inline panel that must be removed after a restart.
        await record_navigation_result(update, True)
        return ConversationHandler.END

    if action == "custom":
        if not is_owner_meta(actor):
            await query.edit_message_text("Изменять срок теста может только руководитель сервиса.")
            return ConversationHandler.END
        state.clear_request_context(context)
        data = state.context_data(context)
        data[state.CTX_ACTION] = "trial_duration"
        data[state.CTX_REQUEST_ID] = request_id
        await query.edit_message_text(
            "Введите длительность теста целым числом часов (от 1 до 8760).",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="product:cancel")]]),
        )
        await _sync_request_cards(update, context, query, request_id, keep_input_prompt=True)
        return state.PRODUCT_INPUT

    if action in {"approve", "approve24"}:
        return await _approve_trial_for_duration(
            update,
            context,
            actor=actor,
            request_id=request_id,
            duration_hours=DEFAULT_TRIAL_DURATION_HOURS,
        )

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
            "admin_required": "Недостаточно прав.",
        }
        await query.edit_message_text(messages[outcome])
        await _sync_request_cards(update, context, query, request_id)
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
                "admin_required": "Недостаточно прав.",
                "claimed": "Заявку уже обрабатывает другой сотрудник.",
                "stale": "Заявка уже обработана.",
            }[outcome]
        )
        await _sync_request_cards(update, context, query, request_id)
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
        text = {
            "reset": "Пользователь уведомлён.",
            "owner_only": "Действие доступно только руководителю сервиса.",
            "stale": "Заявка уже обработана.",
        }[outcome]
        await query.edit_message_text(text)
        await _sync_request_cards(update, context, query, request_id)
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
        await _sync_request_cards(update, context, query, request_id, keep_input_prompt=True)
        return state.PRODUCT_INPUT
    messages = {
        "completed": "Оплата подтверждена, доступ пользователя активирован.",
        "claimed": "Ввод ссылки уже выполняет другой руководитель сервиса.",
        "invalid_target": (
            "Дата оплачиваемого периода уже истекла. Исправьте период или зарегистрируйте оплату вручную."
        ),
        "stale": "Заявка уже обработана.",
        "missing": "Заявка или пользователь не найдены.",
        "owner_only": "Подтверждать оплату может только руководитель сервиса.",
    }
    await query.edit_message_text(messages[outcome])
    await _sync_request_cards(update, context, query, request_id)
    return ConversationHandler.END


__all__ = ["product_request_action_cb"]
