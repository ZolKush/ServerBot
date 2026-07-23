"""Manual payment reminder composition and delivery."""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from ...bot.guards import require_admin
from ...bot.ui import html_escape, ui_ok_text, ui_warn_text
from ...storage import UserData, append_audit_entry, update_user_data
from ...users.staff import (
    is_lead_or_owner_meta,
    staff_public_signature,
    staff_title_label,
)
from ..policy import PLAN_MONTHS, PLAN_TOTAL_RUB
from . import state
from .eligibility import is_eligible_paid_subscriber
from .operations import queue_message
from .views import payment_profile_ready, payment_target


def manual_reminder_text(
    meta: dict[str, Any],
    settings: dict[str, Any],
    actor: dict[str, Any],
) -> str:
    end = state.parse_datetime(meta.get("subscription_end_at"))
    target = payment_target(settings, after=end or state.now())
    can_report_payment = bool(
        end and timedelta(0) <= end - state.now() <= timedelta(days=3) and target and payment_profile_ready(settings)
    )
    lines = [
        "✉️ <b>Персональное напоминание об оплате</b>",
        "",
        f"Отправитель: <b>{html_escape(staff_title_label(actor))}</b>",
        "",
        (f"Текущий доступ до: <code>{html_escape(state.datetime_text(meta.get('subscription_end_at')))}</code>"),
        f"Стоимость продления: <b>{PLAN_TOTAL_RUB} ₽ за {PLAN_MONTHS} месяца</b>",
    ]
    if target:
        lines.append(f"Следующий период до: <code>{html_escape(state.datetime_text(target.isoformat()))}</code>")
    if payment_profile_ready(settings):
        lines.extend(
            [
                "",
                f"Банк: <b>{html_escape(str(settings.get('payment_bank')))}</b>",
                (f"Получатель: <b>{html_escape(str(settings.get('payment_recipient')))}</b>"),
                f"Телефон: <code>{html_escape(str(settings.get('payment_phone')))}</code>",
            ]
        )
    if can_report_payment:
        lines.extend(
            [
                "",
                "После перевода нажмите «Я оплатил продление». При вопросах создайте тикет.",
            ]
        )
    else:
        lines.extend(
            [
                "",
                "Кнопка оплаты появится за 3 дня до окончания. При вопросах создайте тикет.",
            ]
        )
    return "\n".join(lines)


def queue_manual_reminders(
    config: UserData,
    *,
    actor: dict[str, Any],
    target_ids: list[int],
) -> tuple[int, int]:
    sent = skipped = 0
    for user_id in sorted(set(target_ids)):
        current = config.authorized_users.get(str(user_id))
        end = state.parse_datetime(current.get("subscription_end_at")) if isinstance(current, dict) else None
        if not isinstance(current, dict) or not is_eligible_paid_subscriber(current) or end is None:
            skipped += 1
            continue
        can_report_payment = bool(
            timedelta(0) <= end - state.now() <= timedelta(days=3)
            and payment_profile_ready(config.product_settings)
            and payment_target(config.product_settings, after=end)
        )
        markup = [[{"text": "🎫 Создать тикет", "callback_data": "menu:ticket"}]]
        if can_report_payment:
            markup.insert(
                0,
                [
                    {
                        "text": "✅ Я оплатил продление",
                        "callback_data": "subscription:renew",
                    }
                ],
            )
        queue_message(
            config,
            recipient_ids=[user_id],
            kind="manual_payment_reminder",
            text=manual_reminder_text(current, config.product_settings, actor),
            reply_markup=markup,
        )
        updated = UserData._normalize_user(
            {
                **current,
                "last_manual_payment_reminder_at": state.now_iso(),
                "last_manual_payment_reminder_by_id": actor.get("user_id"),
                "last_manual_payment_reminder_by_name": staff_public_signature(
                    actor,
                    allow_alias=False,
                ),
            }
        )
        config.authorized_users[str(user_id)] = updated
        append_audit_entry(
            config,
            action="manual_payment_reminder",
            actor_meta=actor,
            target_user_id=user_id,
            details={},
        )
        sent += 1
    return sent, skipped


@require_admin
async def product_manual_reminder_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    actor = state.actor_meta(update)
    if not query or not actor:
        return
    match = re.fullmatch(r"product:remind:(\d+)", query.data or "")
    if not match:
        return
    await query.answer()
    if not is_lead_or_owner_meta(actor):
        await query.edit_message_text("Недостаточно прав.")
        return
    user_id = int(match.group(1))
    sent, _skipped = await update_user_data(
        lambda config: queue_manual_reminders(
            config,
            actor=actor,
            target_ids=[user_id],
        )
    )
    text = ui_ok_text("Напоминание поставлено в очередь.") if sent else ui_warn_text("напоминание отправить нельзя.")
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "⬅️ Назад",
                        callback_data=f"product:manage:{user_id}",
                    )
                ]
            ]
        ),
    )


__all__ = [
    "manual_reminder_text",
    "product_manual_reminder_cb",
    "queue_manual_reminders",
]
