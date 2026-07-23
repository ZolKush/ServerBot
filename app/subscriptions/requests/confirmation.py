"""Application of staged subscription-management changes."""

from __future__ import annotations

from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, ConversationHandler

from ...bot.guards import require_admin
from ...bot.ui import ui_ok_text
from ...storage import UserData, append_audit_entry, update_user_data
from ...users.staff import (
    is_billing_exempt_meta,
    is_lead_or_owner_meta,
    is_owner_meta,
)
from ..connections import has_connection
from . import state
from .eligibility import is_paid_subscriber
from .operations import create_request, finalize_payment
from .reminders import queue_manual_reminders


def _back_to_user_markup(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "⬅️ Назад",
                    callback_data=f"product:manage:{user_id}",
                )
            ]
        ]
    )


@require_admin
async def product_confirm_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    query = update.callback_query
    actor = state.actor_meta(update)
    if not query or not actor:
        return ConversationHandler.END
    await query.answer()
    pending = state.context_data(context).get(state.CTX_PENDING)
    if not isinstance(pending, dict):
        await query.edit_message_text("Подтверждение устарело. Начните действие заново.")
        return ConversationHandler.END
    kind = str(pending.get("kind") or "")
    target = state.parse_datetime(pending.get("target_end_at"))

    if kind == "user_end":
        if not is_lead_or_owner_meta(actor) or target is None:
            await query.edit_message_text("Недостаточно прав или некорректная дата.")
            state.clear_request_context(context)
            return ConversationHandler.END
        user_id = int(pending.get("target_uid", 0) or 0)

        def change_user_end(
            config: UserData,
        ) -> tuple[str, dict[str, Any] | None]:
            current = config.authorized_users.get(str(user_id))
            if not isinstance(current, dict):
                return "missing", None
            if not is_paid_subscriber(current):
                return "unpaid", current
            old = current.get("subscription_end_at")
            updated = UserData._normalize_user(
                {
                    **current,
                    "subscription_end_at": target.isoformat(),
                    "payment_auto_reminders": {},
                }
            )
            config.authorized_users[str(user_id)] = updated
            append_audit_entry(
                config,
                action="subscription_end_changed",
                actor_meta=actor,
                target_user_id=user_id,
                details={"old": old, "new": target.isoformat()},
            )
            return "updated", updated

        outcome, _updated = await update_user_data(change_user_end)
        state.clear_request_context(context)
        if outcome == "unpaid":
            await query.edit_message_text(
                "⛔ Невозможно изменить дату окончания\n\n"
                "Оплата пользователя не подтверждена. Сначала руководитель "
                "сервиса должен подтвердить оплату."
            )
        elif outcome == "missing":
            await query.edit_message_text("Пользователь не найден.")
        else:
            await query.edit_message_text(
                ui_ok_text(f"Дата окончания изменена: {state.datetime_text(target.isoformat())}"),
                reply_markup=_back_to_user_markup(user_id),
            )
        return ConversationHandler.END

    if kind == "manualpay":
        if not is_owner_meta(actor) or target is None or target <= state.now():
            await query.edit_message_text("Ручное подтверждение оплаты больше недоступно.")
            state.clear_request_context(context)
            return ConversationHandler.END
        user_id = int(pending.get("target_uid", 0) or 0)

        def register_payment(
            config: UserData,
        ) -> tuple[str, dict[str, Any] | None]:
            current = config.authorized_users.get(str(user_id))
            if not isinstance(current, dict):
                return "missing", None
            if is_billing_exempt_meta(current):
                return "billing_exempt", current
            if not has_connection(current):
                return "connection_missing", current
            request = create_request(
                config,
                kind="purchase",
                user_id=user_id,
                status="payment_reported",
                target_end_at=target.isoformat(),
                comment="Ручная регистрация оплаты руководителем",
            )
            return "updated", finalize_payment(config, request, actor)

        outcome, _updated = await update_user_data(register_payment)
        state.clear_request_context(context)
        messages = {
            "updated": "Оплата зарегистрирована, доступ пользователя активирован.",
            "connection_missing": ("Сначала назначьте персональную ссылку подключения."),
            "billing_exempt": ("У руководителя сервиса бессрочный оплаченный доступ."),
            "missing": "Пользователь не найден.",
        }
        await query.edit_message_text(
            messages[outcome],
            reply_markup=_back_to_user_markup(user_id),
        )
        return ConversationHandler.END

    if kind == "mass_date":
        if not is_lead_or_owner_meta(actor) or target is None:
            await query.edit_message_text("Массовое изменение больше недоступно.")
            state.clear_request_context(context)
            return ConversationHandler.END
        target_ids = [int(user_id) for user_id in pending.get("target_ids", []) if str(user_id).isdigit()]

        def change_mass_date(config: UserData) -> tuple[int, int]:
            changed = skipped = 0
            for user_id in sorted(set(target_ids)):
                current = config.authorized_users.get(str(user_id))
                if not isinstance(current, dict) or not is_paid_subscriber(current):
                    skipped += 1
                    continue
                old = current.get("subscription_end_at")
                config.authorized_users[str(user_id)] = UserData._normalize_user(
                    {
                        **current,
                        "subscription_end_at": target.isoformat(),
                        "payment_auto_reminders": {},
                    }
                )
                append_audit_entry(
                    config,
                    action="subscription_end_changed_mass",
                    actor_meta=actor,
                    target_user_id=user_id,
                    details={"old": old, "new": target.isoformat()},
                )
                changed += 1
            return changed, skipped

        changed, skipped = await update_user_data(change_mass_date)
        state.clear_request_context(context)
        await query.edit_message_text(ui_ok_text(f"Дата изменена у {changed} пользователей. Пропущено: {skipped}."))
        return ConversationHandler.END

    if kind == "mass_reminder":
        if not is_lead_or_owner_meta(actor):
            await query.edit_message_text("Массовая отправка больше недоступна.")
            state.clear_request_context(context)
            return ConversationHandler.END
        target_ids = [int(user_id) for user_id in pending.get("target_ids", []) if str(user_id).isdigit()]
        sent, skipped = await update_user_data(
            lambda config: queue_manual_reminders(
                config,
                actor=actor,
                target_ids=target_ids,
            )
        )
        state.clear_request_context(context)
        await query.edit_message_text(ui_ok_text(f"Напоминания поставлены в очередь: {sent}. Пропущено: {skipped}."))
        return ConversationHandler.END

    state.clear_request_context(context)
    await query.edit_message_text("Неизвестное или устаревшее подтверждение.")
    return ConversationHandler.END


__all__ = ["product_confirm_cb"]
