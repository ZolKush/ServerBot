"""Customer payment and renewal reporting handlers."""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from ...bot.guards import require_auth
from ...storage import UserData, update_user_data
from . import state
from .operations import (
    create_request,
    find_active_request,
    owner_meta_from_config,
    queue_message,
)
from .views import payment_profile_ready, payment_target, request_card


def payment_report_notification(
    request: dict[str, Any],
    meta: dict[str, Any],
) -> str:
    return (
        "💰 <b>Пользователь сообщил об оплате</b>\n\n"
        + request_card(request, meta)
        + "\n\nПодтвердить поступление может только руководитель сервиса."
    )


@require_auth
async def payment_reported_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = state.actor_meta(update)
    if not query or not actor:
        return
    match = re.fullmatch(r"subscription:paid:(\d+)", query.data or "")
    if not match:
        return
    await query.answer()
    request_id = int(match.group(1))
    user_id = int(actor.get("user_id") or 0)

    def report(config: UserData) -> str:
        request = config.service_requests.get(str(request_id))
        if not isinstance(request, dict) or int(request.get("user_id", 0) or 0) != user_id:
            return "missing"
        status = str(request.get("status") or "")
        if status == "payment_reported":
            return "already"
        if status != "requisites_sent":
            return "stale"
        updated = dict(request)
        updated.update(
            {
                "status": "payment_reported",
                "payment_reported_at": state.now_iso(),
                "updated_at": state.now_iso(),
            }
        )
        config.service_requests[str(request_id)] = updated
        owner = owner_meta_from_config(config)
        if owner:
            queue_message(
                config,
                recipient_ids=[int(owner.get("user_id") or 0)],
                kind="payment_reported",
                text=payment_report_notification(updated, actor),
                reply_markup=[
                    [
                        {
                            "text": "✅ Подтвердить",
                            "callback_data": f"product:req:confirm:{request_id}",
                        },
                        {
                            "text": "🔎 Не найден",
                            "callback_data": f"product:req:notfound:{request_id}",
                        },
                    ],
                    [{"text": "👤 Профиль", "callback_data": f"users:user:{user_id}"}],
                ],
            )
        return "reported"

    outcome = await update_user_data(report)
    texts = {
        "reported": "✅ Информация об оплате отправлена руководителю сервиса. Ожидайте подтверждения.",
        "already": "Оплата уже ожидает проверки руководителем сервиса.",
        "missing": "Заявка не найдена.",
        "stale": "Эта платёжная кнопка больше неактивна.",
    }
    await query.edit_message_text(
        texts[outcome],
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]]),
    )


@require_auth
async def renewal_reported_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = state.actor_meta(update)
    if not query or not actor:
        return
    await query.answer()
    user_id = int(actor.get("user_id") or 0)
    if actor.get("service_tier") != "subscriber" or not actor.get("is_paid"):
        await query.edit_message_text("Продление недоступно для текущего уровня доступа.")
        return
    actor_end = state.parse_datetime(actor.get("subscription_end_at"))
    if actor_end is None or not (timedelta(0) <= actor_end - state.now() <= timedelta(days=3)):
        await query.edit_message_text("Сообщить о продлении можно начиная за 3 дня до окончания доступа.")
        return

    def create(config: UserData) -> tuple[str, int | None]:
        current = config.authorized_users.get(str(user_id))
        if not isinstance(current, dict) or current.get("service_tier") != "subscriber" or not current.get("is_paid"):
            return "denied", None
        current_end = state.parse_datetime(current.get("subscription_end_at"))
        if current_end is None or not (timedelta(0) <= current_end - state.now() <= timedelta(days=3)):
            return "not_due", None
        existing = find_active_request(config, user_id=user_id, kind="renewal")
        if existing:
            return "exists", int(existing.get("id", 0) or 0)
        target = payment_target(
            config.product_settings,
            after=current_end or state.now(),
        )
        if target is None or not payment_profile_ready(config.product_settings):
            return "not_configured", None
        request = create_request(
            config,
            kind="renewal",
            user_id=user_id,
            status="payment_reported",
            target_end_at=target.isoformat(),
        )
        request_id = int(request["id"])
        owner = owner_meta_from_config(config)
        if owner:
            queue_message(
                config,
                recipient_ids=[int(owner.get("user_id") or 0)],
                kind="renewal_payment_reported",
                text=payment_report_notification(request, current),
                reply_markup=[
                    [
                        {
                            "text": "✅ Подтвердить",
                            "callback_data": f"product:req:confirm:{request_id}",
                        },
                        {
                            "text": "🔎 Не найден",
                            "callback_data": f"product:req:notfound:{request_id}",
                        },
                    ],
                    [{"text": "👤 Профиль", "callback_data": f"users:user:{user_id}"}],
                ],
            )
        return "created", request_id

    outcome, _request_id = await update_user_data(create)
    texts = {
        "created": "✅ Информация о продлении отправлена руководителю сервиса.",
        "exists": "Продление уже ожидает проверки.",
        "not_configured": "Следующий платёжный период или реквизиты ещё не настроены. Создайте тикет.",
        "not_due": "Сообщить о продлении можно начиная за 3 дня до окончания доступа.",
        "denied": "Продление недоступно для текущего уровня доступа.",
    }
    await query.edit_message_text(
        texts[outcome],
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("🎫 Создать тикет", callback_data="menu:ticket")],
                [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
            ]
        ),
    )


__all__ = [
    "payment_report_notification",
    "payment_reported_cb",
    "renewal_reported_cb",
]
