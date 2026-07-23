"""Validation and staging of subscription request text input."""

from __future__ import annotations

from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ...bot.guards import require_auth
from ...bot.menu import main_menu_inline_kb
from ...bot.ui import html_escape
from ...storage import (
    UserData,
    authorized_users_snapshot,
    get_user_meta_copy,
    update_user_data,
)
from ...users.staff import is_admin_meta, is_billing_exempt_meta, is_owner_meta
from ..connections import has_connection, is_valid_connection_url
from . import state
from .customer import apply_trial_comment
from .eligibility import (
    is_eligible_paid_subscriber,
    is_paid_subscriber,
    parse_id_list,
)
from .operations import finalize_payment, finalize_trial


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


def _confirmation_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Подтвердить",
                    callback_data="product:confirm:apply",
                )
            ],
            [InlineKeyboardButton("❌ Отмена", callback_data="product:cancel")],
        ]
    )


async def _handle_mass_reminder_input(
    message: Any,
    data: dict[str, Any],
    text: str,
) -> int:
    targets: list[int] = []
    snapshot = authorized_users_snapshot()
    lowered = text.lower()
    if lowered == "все":
        targets = [int(meta.get("user_id", key)) for key, meta in snapshot.items() if is_eligible_paid_subscriber(meta)]
    elif lowered.startswith("до "):
        cutoff = state.parse_input_datetime(text[3:].strip())
        if cutoff is None:
            await message.reply_text("Некорректная дата. Используйте ДД.ММ.ГГГГ ЧЧ:ММ.")
            return state.PRODUCT_INPUT
        for key, meta in snapshot.items():
            end = state.parse_datetime(meta.get("subscription_end_at"))
            if is_eligible_paid_subscriber(meta) and end and end <= cutoff:
                targets.append(int(meta.get("user_id", key)))
    else:
        parsed_ids = parse_id_list(text)
        if parsed_ids is None:
            await message.reply_text("Введите «все», условие с датой или Telegram ID через запятую.")
            return state.PRODUCT_INPUT
        targets = sorted(parsed_ids)
    if not targets:
        await message.reply_text("Подходящих получателей нет. Повторите ввод.")
        return state.PRODUCT_INPUT
    data[state.CTX_PENDING] = {
        "kind": "mass_reminder",
        "target_ids": sorted(set(targets)),
    }
    await message.reply_text(
        f"Будет подготовлено напоминаний: <b>{len(set(targets))}</b>. Подтвердите отправку.",
        parse_mode=ParseMode.HTML,
        reply_markup=_confirmation_markup(),
    )
    return state.PRODUCT_CONFIRM


async def _handle_mass_date_input(
    message: Any,
    data: dict[str, Any],
    text: str,
) -> int:
    date_part, separator, ids_part = text.partition("|")
    target = state.parse_input_datetime(date_part.strip())
    if target is None:
        await message.reply_text("Некорректная дата. Используйте ДД.ММ.ГГГГ ЧЧ:ММ.")
        return state.PRODUCT_INPUT
    selected_ids = parse_id_list(ids_part.strip()) if separator else None
    if separator and selected_ids is None:
        await message.reply_text("После | укажите корректные Telegram ID через запятую.")
        return state.PRODUCT_INPUT
    snapshot = authorized_users_snapshot()
    candidates = [
        int(meta.get("user_id", key))
        for key, meta in snapshot.items()
        if is_eligible_paid_subscriber(meta) and (selected_ids is None or int(meta.get("user_id", key)) in selected_ids)
    ]
    skipped = (len(selected_ids) - len(candidates)) if selected_ids is not None else 0
    if not candidates:
        await message.reply_text("Нет оплаченных подписчиков, которым можно назначить эту дату.")
        return state.PRODUCT_INPUT
    data[state.CTX_PENDING] = {
        "kind": "mass_date",
        "target_ids": sorted(set(candidates)),
        "target_end_at": target.isoformat(),
        "skipped": max(0, skipped),
    }
    await message.reply_text(
        "📅 <b>Проверка массового изменения</b>\n\n"
        f"• Новая дата: <code>{html_escape(state.datetime_text(target.isoformat()))}</code>\n"
        f"• Будет изменено: <b>{len(set(candidates))}</b>\n"
        f"• Пропущено: <b>{max(0, skipped)}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=_confirmation_markup(),
    )
    return state.PRODUCT_CONFIRM


async def _handle_user_date_input(
    message: Any,
    data: dict[str, Any],
    action: str,
    text: str,
) -> int:
    target = state.parse_input_datetime(text)
    if target is None:
        await message.reply_text("Некорректная дата. Используйте ДД.ММ.ГГГГ ЧЧ:ММ.")
        return state.PRODUCT_INPUT
    if action == "manualpay" and target <= state.now():
        await message.reply_text("Для этого действия дата должна находиться в будущем.")
        return state.PRODUCT_INPUT
    pending: dict[str, Any] = {
        "kind": action,
        "target_end_at": target.isoformat(),
    }
    target_user_id = data.get(state.CTX_TARGET_UID)
    if isinstance(target_user_id, int):
        pending["target_uid"] = target_user_id
    user = get_user_meta_copy(int(target_user_id or 0))
    if action == "user_end" and (not user or not is_paid_subscriber(user)):
        await message.reply_text(
            "⛔ Невозможно изменить дату окончания\n\n"
            "Оплата пользователя не подтверждена. Сначала руководитель "
            "сервиса должен подтвердить оплату."
        )
        return state.PRODUCT_INPUT
    if action == "manualpay":
        if not user:
            await message.reply_text("Пользователь не найден.")
            return state.PRODUCT_INPUT
        if is_billing_exempt_meta(user):
            await message.reply_text("У руководителя сервиса бессрочный оплаченный доступ.")
            return state.PRODUCT_INPUT
        if not has_connection(user):
            await message.reply_text("Сначала назначьте пользователю персональную ссылку подключения.")
            return state.PRODUCT_INPUT
    data[state.CTX_PENDING] = pending
    labels = {
        "user_end": "Дата окончания пользователя",
        "manualpay": "Ручное подтверждение оплаты",
    }
    await message.reply_text(
        f"<b>{html_escape(labels[action])}</b>\n\n"
        f"Новое значение: <code>{html_escape(state.datetime_text(target.isoformat()))}</code>\n\n"
        "Подтвердите изменение.",
        parse_mode=ParseMode.HTML,
        reply_markup=_confirmation_markup(),
    )
    return state.PRODUCT_CONFIRM


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
        await message.reply_text(
            {
                "created": "✅ Заявка на тестовый доступ отправлена.",
                "exists": "Заявка уже ожидает решения.",
                "issued": "Тестовый доступ уже выдавался ранее.",
                "denied": "Запрос недоступен для текущего уровня.",
            }[outcome],
            reply_markup=main_menu_inline_kb(update),
        )
        return ConversationHandler.END

    if action in {"request_link", "payment_link"}:
        request_id = int(data.get(state.CTX_REQUEST_ID, 0) or 0)
        if not is_valid_connection_url(text):
            await message.reply_text(
                "Некорректная ссылка. Вставьте полную ссылку, начинающуюся с http:// или https://."
            )
            return state.PRODUCT_INPUT
        outcome = await complete_connection_input(
            action=action,
            request_id=request_id,
            connection_url=text,
            actor=actor,
        )
        state.clear_request_context(context)
        await message.reply_text(
            {
                "completed": ("✅ Ссылка сохранена, заявка завершена и уведомление поставлено в очередь."),
                "claimed": "Заявку уже обрабатывает другой сотрудник.",
                "stale": "Заявка уже обработана или отменена.",
                "invalid_target": (
                    "Дата оплачиваемого периода уже истекла. Настройте период и повторите подтверждение."
                ),
                "user_missing": "Пользователь больше не найден.",
                "connection_missing": "Не удалось сохранить персональную ссылку.",
                "tier_changed": ("Уровень пользователя уже изменился; заявка на тест отменена."),
                "already_issued": "Тестовый доступ уже был выдан ранее.",
            }.get(outcome, "Заявку не удалось завершить. Откройте её заново."),
            reply_markup=main_menu_inline_kb(update),
        )
        return ConversationHandler.END

    if not is_admin_meta(actor):
        state.clear_request_context(context)
        await message.reply_text("Административное действие больше недоступно.")
        return ConversationHandler.END
    if action == "mass_reminder":
        return await _handle_mass_reminder_input(message, data, text)
    if action == "mass_date":
        return await _handle_mass_date_input(message, data, text)
    if action in {"user_end", "manualpay"}:
        return await _handle_user_date_input(message, data, action, text)

    state.clear_request_context(context)
    await message.reply_text("Сценарий ввода устарел. Начните действие заново.")
    return ConversationHandler.END


__all__ = ["complete_connection_input", "product_text_input"]
