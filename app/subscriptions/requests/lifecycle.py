"""Subscription expiry, reminder, period-rollover, and claim lifecycle."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from telegram.ext import ContextTypes

from ...bot.ui import html_escape
from ...runtime.logging import logger
from ...storage import UserData, update_user_data
from ..policy import PLAN_MONTHS, PLAN_TOTAL_RUB
from . import state
from .eligibility import is_paid_subscriber
from .operations import owner_meta_from_config, queue_message
from .views import payment_profile_ready, payment_target, renewal_markup


def automatic_reminder_text(
    meta: dict[str, Any],
    settings: dict[str, Any],
    reminder_type: str,
) -> str:
    heading = {
        "3d": "Срок оплаченного доступа завершится через 3 дня.",
        "1d": "Срок оплаченного доступа завершится через 1 день.",
        "15m": "‼️ Срок оплаченного доступа завершится менее чем через 15 минут.",
    }[reminder_type]
    target = payment_target(
        settings,
        after=state.parse_datetime(meta.get("subscription_end_at")) or state.now(),
    )
    lines = [
        "🤖 <b>Системное уведомление</b>",
        "",
        heading,
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
    if target and payment_profile_ready(settings):
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
                "Реквизиты или следующий платёжный период пока не настроены. Создайте тикет в поддержку.",
            ]
        )
    return "\n".join(lines)


def _roll_billing_period(
    config: UserData,
    *,
    current_time,
    counters: dict[str, int],
) -> None:
    owner = owner_meta_from_config(config)
    settings = config.product_settings
    current_end = state.parse_datetime(settings.get("current_period_end"))
    next_end = state.parse_datetime(settings.get("next_period_end"))
    if current_end and current_time >= current_end:
        if next_end and next_end > current_end:
            settings["current_period_end"] = next_end.isoformat()
            settings["next_period_end"] = None
            settings["period_setup_reminder_for"] = None
            settings["period_missing_notice_for"] = None
            counters["period_rollover"] += 1
            if owner:
                queue_message(
                    config,
                    recipient_ids=[int(owner.get("user_id") or 0)],
                    kind="billing_period_rollover",
                    text=(
                        "🤖 <b>Платёжный период обновлён</b>\n\n"
                        "Новый текущий период заканчивается: "
                        f"<code>{html_escape(state.datetime_text(next_end.isoformat()))}</code>.\n"
                        "Даты пользователей автоматически не изменялись. "
                        "Задайте следующий период заранее."
                    ),
                )
        elif owner and settings.get("period_missing_notice_for") != current_end.isoformat():
            queue_message(
                config,
                recipient_ids=[int(owner.get("user_id") or 0)],
                kind="billing_period_missing",
                text=("⚠️ Текущий платёжный период завершён, но следующая дата не настроена."),
            )
            settings["period_missing_notice_for"] = current_end.isoformat()
    elif (
        owner
        and current_end
        and not next_end
        and timedelta(0) <= current_end - current_time <= timedelta(days=7)
        and settings.get("period_setup_reminder_for") != current_end.isoformat()
    ):
        queue_message(
            config,
            recipient_ids=[int(owner.get("user_id") or 0)],
            kind="billing_period_setup_reminder",
            text=(
                "🤖 <b>Напоминание руководителю</b>\n\n"
                "Текущий период завершится: "
                f"<code>{html_escape(state.datetime_text(current_end.isoformat()))}</code>.\n"
                "Укажите дату следующего периода. "
                "Пользователи от этого автоматически не продлятся."
            ),
        )
        settings["period_setup_reminder_for"] = current_end.isoformat()
    config.product_settings = settings


def _release_stale_claims(
    config: UserData,
    *,
    current_time,
) -> int:
    released = 0
    for request_id, request in list(config.service_requests.items()):
        if not isinstance(request, dict) or request.get("status") != "awaiting_link":
            continue
        claimed_at = state.parse_datetime(request.get("claimed_at"))
        claimed_by = int(request.get("claimed_by_id", 0) or 0)
        claimed_meta = config.authorized_users.get(str(claimed_by))
        claimed_admin_active = bool(
            isinstance(claimed_meta, dict)
            and claimed_meta.get("role") == "admin"
            and claimed_meta.get("access_state") == "approved"
            and bool(claimed_meta.get("enabled", True))
        )
        if claimed_at is None or not claimed_admin_active or current_time - claimed_at >= state.REQUEST_CLAIM_TIMEOUT:
            updated = dict(request)
            updated.update(
                {
                    "status": str(request.get("resume_status") or "pending"),
                    "claimed_by_id": None,
                    "claimed_at": None,
                    "updated_at": current_time.isoformat(),
                }
            )
            config.service_requests[request_id] = updated
            released += 1
    return released


def _process_subscribers(
    config: UserData,
    *,
    current_time,
) -> tuple[int, int]:
    reminders = expired = 0
    settings = config.product_settings
    for key, current in list(config.authorized_users.items()):
        if not isinstance(current, dict) or not is_paid_subscriber(current):
            continue
        end = state.parse_datetime(current.get("subscription_end_at"))
        if end is None:
            continue
        user_id = int(current.get("user_id", key))
        if current_time >= end:
            updated = UserData._normalize_user(
                {
                    **current,
                    "service_tier": "basic",
                    "is_paid": False,
                    "service_tier_updated_at": current_time.isoformat(),
                    "service_tier_updated_by_id": None,
                    "service_tier_updated_by_name": "Система",
                }
            )
            config.authorized_users[key] = updated
            if current.get("access_state") == "approved" and bool(current.get("enabled", True)):
                queue_message(
                    config,
                    recipient_ids=[user_id],
                    kind="subscription_expired",
                    text=(
                        "🤖 <b>Системное уведомление</b>\n\n"
                        "Срок оплаченного доступа завершён. Уровень в боте "
                        "изменён на базовый. Персональная ссылка сохранена. "
                        "После подтверждения оплаты полный доступ будет восстановлен."
                    ),
                    reply_markup=[
                        [
                            {
                                "text": "💳 Купить подписку",
                                "callback_data": "subscription:buy",
                            }
                        ],
                        [
                            {
                                "text": "🎫 Создать тикет",
                                "callback_data": "menu:ticket",
                            }
                        ],
                    ],
                )
            expired += 1
            continue
        if current.get("access_state") != "approved" or not bool(current.get("enabled", True)):
            continue
        remaining = end - current_time
        reminder_type = (
            "15m"
            if remaining <= timedelta(minutes=15)
            else ("1d" if remaining <= timedelta(days=1) else ("3d" if remaining <= timedelta(days=3) else ""))
        )
        if not reminder_type:
            continue
        reminder_key = f"{end.isoformat()}:{reminder_type}"
        sent_map = dict(current.get("payment_auto_reminders") or {})
        if reminder_key in sent_map:
            continue
        markup = (
            renewal_markup()
            if payment_profile_ready(settings) and payment_target(settings, after=end)
            else [[{"text": "🎫 Создать тикет", "callback_data": "menu:ticket"}]]
        )
        queue_message(
            config,
            recipient_ids=[user_id],
            kind=f"subscription_reminder_{reminder_type}",
            text=automatic_reminder_text(current, settings, reminder_type),
            reply_markup=markup,
        )
        sent_map[reminder_key] = current_time.isoformat()
        config.authorized_users[key] = UserData._normalize_user(
            {
                **current,
                "payment_auto_reminders": sent_map,
                "last_auto_payment_reminder_at": current_time.isoformat(),
                "last_auto_payment_reminder_type": reminder_type,
            }
        )
        reminders += 1
    return reminders, expired


async def subscription_lifecycle_job(
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    current_time = state.now()

    def tick(config: UserData) -> dict[str, int]:
        counters = {
            "reminders": 0,
            "expired": 0,
            "released": 0,
            "period_rollover": 0,
        }
        _roll_billing_period(
            config,
            current_time=current_time,
            counters=counters,
        )
        counters["released"] = _release_stale_claims(
            config,
            current_time=current_time,
        )
        counters["reminders"], counters["expired"] = _process_subscribers(
            config,
            current_time=current_time,
        )
        return counters

    counters = await update_user_data(tick)
    if any(counters.values()):
        logger.info(
            "Subscription lifecycle processed: %s",
            counters,
            extra={"action": "subscription_lifecycle"},
        )


__all__ = ["automatic_reminder_text", "subscription_lifecycle_job"]
