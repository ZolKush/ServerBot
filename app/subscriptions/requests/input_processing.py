"""Validation and staging of subscription request text input."""

from __future__ import annotations

from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, ConversationHandler

from ...bot.guards import require_auth
from ...bot.menu import main_menu_inline_kb
from ...messaging.message_cleanup import record_navigation_result
from ...messaging.review_sync import (
    sync_service_review_messages,
    sync_service_review_messages_for_user,
)
from ...runtime.logging import logger
from ...storage import UserData, get_user_meta_copy, service_requests_snapshot, update_user_data
from ...users.staff import is_admin_meta, is_owner_meta
from ..connections import is_valid_connection_url
from ..policy import MAX_CUSTOM_TRIAL_DURATION_HOURS, MIN_CUSTOM_TRIAL_DURATION_HOURS
from . import state
from .admin_input import (
    handle_mass_date_input,
    handle_mass_reminder_input,
    handle_user_date_input,
)
from .customer import apply_trial_comment
from .operations import finalize_payment, finalize_trial
from .review_operations import approve_trial


async def _sync_request_cards(context: ContextTypes.DEFAULT_TYPE, request_id: int) -> None:
    bot = getattr(context, "bot", None)
    if bot is None:
        return
    try:
        request = service_requests_snapshot().get(str(request_id))
        user_id = int(request.get("user_id", 0) or 0) if isinstance(request, dict) else 0
        if user_id:
            await sync_service_review_messages_for_user(bot, user_id)
        else:
            await sync_service_review_messages(bot, request_id)
    except Exception:
        logger.exception("Could not synchronize service request cards request_id=%s", request_id)


async def complete_connection_input(
    *,
    action: str,
    request_id: int,
    connection_url: str,
    actor: dict[str, Any],
) -> str:
    def apply(config: UserData) -> str:
        request = config.service_requests.get(str(request_id))
        if not isinstance(request, dict) or request.get("status") != "awaiting_link":
            return "stale"
        if int(request.get("claimed_by_id", 0) or 0) != int(actor.get("user_id", 0) or 0):
            return "claimed"
        if action == "request_link" and request.get("kind") == "trial":
            try:
                finalize_trial(config, request, actor, connection_url)
            except ValueError as exc:
                code = str(exc)
                updated = dict(request)
                updated.update(
                    {
                        "status": (
                            "cancelled" if code in {"tier_changed", "already_issued", "user_missing"} else "pending"
                        ),
                        "decision_reason": code,
                        "claimed_by_id": None,
                        "claimed_at": None,
                        "updated_at": state.now_iso(),
                    }
                )
                config.service_requests[str(request_id)] = updated
                return code
            return "completed"
        if action == "payment_link" and request.get("kind") in {"purchase", "renewal"} and is_owner_meta(actor):
            try:
                finalize_payment(
                    config,
                    request,
                    actor,
                    connection_url=connection_url,
                )
            except ValueError as exc:
                code = str(exc)
                updated = dict(request)
                updated.update(
                    {
                        "status": (
                            "cancelled"
                            if code == "user_missing"
                            else str(request.get("resume_status") or "payment_reported")
                        ),
                        "decision_reason": code,
                        "claimed_by_id": None,
                        "claimed_at": None,
                        "updated_at": state.now_iso(),
                    }
                )
                config.service_requests[str(request_id)] = updated
                return code
            return "completed"
        return "stale"

    return await update_user_data(apply)


@require_auth
async def product_text_input(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    message = update.effective_message
    actor = state.actor_meta(update)
    if not message or not actor:
        return ConversationHandler.END
    text = (message.text or "").strip()
    data = state.context_data(context)
    action = str(data.get(state.CTX_ACTION) or "")
    if not text:
        await message.reply_text("Пустое значение. Повторите ввод.")
        return state.PRODUCT_INPUT

    if action == "trial_comment":
        if len(text) > 1000:
            await message.reply_text("Комментарий слишком длинный. Максимум 1000 символов.")
            return state.PRODUCT_INPUT
        outcome, _request_id = await update_user_data(
            lambda config: apply_trial_comment(
                config,
                user_id=int(actor.get("user_id") or 0),
                comment=text,
            )
        )
        state.clear_request_context(context)
        result = await message.reply_text(
            {
                "created": "✅ Заявка на тестовый доступ отправлена.",
                "exists": "Заявка уже ожидает решения.",
                "issued": "Тестовый доступ уже выдавался ранее.",
                "denied": "Запрос недоступен для текущего уровня.",
            }[outcome],
            reply_markup=main_menu_inline_kb(update),
        )
        await record_navigation_result(update, result)
        return ConversationHandler.END

    if action == "trial_duration":
        if not is_owner_meta(actor):
            state.clear_request_context(context)
            await message.reply_text("Изменять срок теста может только руководитель сервиса.")
            return ConversationHandler.END
        try:
            duration_hours = int(text)
        except (TypeError, ValueError, OverflowError):
            duration_hours = 0
        if not MIN_CUSTOM_TRIAL_DURATION_HOURS <= duration_hours <= MAX_CUSTOM_TRIAL_DURATION_HOURS:
            await message.reply_text(
                f"Введите целое число часов от {MIN_CUSTOM_TRIAL_DURATION_HOURS} до {MAX_CUSTOM_TRIAL_DURATION_HOURS}."
            )
            return state.PRODUCT_INPUT
        request_id = int(data.get(state.CTX_REQUEST_ID, 0) or 0)
        outcome, request = await update_user_data(
            lambda config: approve_trial(
                config,
                request_id=request_id,
                actor=actor,
                duration_hours=duration_hours,
            )
        )
        if outcome == "need_link":
            data[state.CTX_ACTION] = "request_link"
            deadline = state.datetime_text((request or {}).get("target_end_at"))
            result = await message.reply_text(
                "🔗 Вставьте персональную ссылку подключения одним сообщением. "
                "Поддерживаются только ссылки HTTP/HTTPS.\n\n"
                f"Ссылка должна быть ограничена сроком до: {deadline} ({duration_hours} ч).",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("❌ Отмена", callback_data="product:cancel")]]
                ),
            )
            await record_navigation_result(update, result)
            await _sync_request_cards(context, request_id)
            return state.PRODUCT_INPUT
        state.clear_request_context(context)
        result = await message.reply_text(
            {
                "completed": f"✅ Тестовый доступ одобрен на {duration_hours} ч.",
                "claimed": "Заявку уже обрабатывает другой сотрудник.",
                "stale": "Заявка уже обработана.",
                "tier_changed": "Уровень пользователя изменился; заявка отменена.",
                "already_issued": "Тестовый доступ уже выдавался.",
                "invalid_duration": "Некорректная длительность теста.",
                "missing": "Заявка или пользователь не найдены.",
            }.get(outcome, "Заявка не обработана."),
            reply_markup=main_menu_inline_kb(update),
        )
        await record_navigation_result(update, result)
        await _sync_request_cards(context, request_id)
        return ConversationHandler.END

    if action in {"request_link", "payment_link"}:
        request_id = int(data.get(state.CTX_REQUEST_ID, 0) or 0)
        if not is_valid_connection_url(text):
            await message.reply_text(
                "Некорректная ссылка. Вставьте полную ссылку, начинающуюся с http:// или https://."
            )
            return state.PRODUCT_INPUT
        if action == "request_link":
            request = service_requests_snapshot().get(str(request_id))
            user_id = int(request.get("user_id", 0) or 0) if isinstance(request, dict) else 0
            current = get_user_meta_copy(user_id) if user_id else None
            previous_url = str((current or {}).get("connection_url") or "").strip()
            if previous_url and text == previous_url:
                await message.reply_text(
                    "Для теста нужна новая ссылка, ограниченная указанным сроком. "
                    "Существующую постоянную ссылку использовать нельзя."
                )
                return state.PRODUCT_INPUT
        outcome = await complete_connection_input(
            action=action,
            request_id=request_id,
            connection_url=text,
            actor=actor,
        )
        state.clear_request_context(context)
        result = await message.reply_text(
            {
                "completed": ("✅ Ссылка сохранена, заявка завершена и уведомление поставлено в очередь."),
                "claimed": "Заявку уже обрабатывает другой сотрудник.",
                "stale": "Заявка уже обработана или отменена.",
                "invalid_target": (
                    "Дата оплачиваемого периода уже истекла. Настройте период и повторите подтверждение."
                ),
                "user_missing": "Пользователь больше не найден.",
                "connection_missing": "Не удалось сохранить персональную ссылку.",
                "connection_not_fresh": "Для теста требуется новая ограниченная по сроку ссылка.",
                "tier_changed": ("Уровень пользователя уже изменился; заявка на тест отменена."),
                "already_issued": "Тестовый доступ уже был выдан ранее.",
            }.get(outcome, "Заявку не удалось завершить. Откройте её заново."),
            reply_markup=main_menu_inline_kb(update),
        )
        await record_navigation_result(update, result)
        await _sync_request_cards(context, request_id)
        return ConversationHandler.END

    if not is_admin_meta(actor):
        state.clear_request_context(context)
        await message.reply_text("Административное действие больше недоступно.")
        return ConversationHandler.END
    if action == "mass_reminder":
        return await handle_mass_reminder_input(update, message, data, text)
    if action == "mass_date":
        return await handle_mass_date_input(update, message, data, text)
    if action in {"user_end", "manualpay"}:
        return await handle_user_date_input(update, message, data, action, text)

    state.clear_request_context(context)
    await message.reply_text("Сценарий ввода устарел. Начните действие заново.")
    return ConversationHandler.END


__all__ = ["complete_connection_input", "product_text_input"]
