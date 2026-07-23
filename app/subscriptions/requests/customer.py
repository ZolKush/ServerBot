"""Customer-facing trial and subscription purchase entry points."""

from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ...bot.guards import require_auth
from ...bot.ui import html_escape
from ...storage import (
    UserData,
    product_settings_snapshot,
    service_requests_snapshot,
    update_user_data,
)
from ...users.staff import is_billing_exempt_meta
from ..policy import PLAN_MONTHS, PLAN_TOTAL_RUB
from . import state
from .operations import (
    approved_admin_ids,
    create_request,
    find_active_request,
    queue_message,
)
from .views import (
    payment_profile_ready,
    payment_target,
    request_card,
)


@require_auth
async def trial_request_start_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    meta = state.actor_meta(update)
    if not query or not meta:
        return ConversationHandler.END
    await query.answer()
    if meta.get("role") == "admin" or meta.get("service_tier") != "basic":
        await query.edit_message_text("Тестовый доступ предназначен для пользователей с базовым доступом.")
        return ConversationHandler.END
    if meta.get("trial_issued_at"):
        await query.edit_message_text("Тестовый доступ уже выдавался ранее.")
        return ConversationHandler.END
    user_id = int(meta.get("user_id") or 0)
    if any(
        int(item.get("user_id", 0) or 0) == user_id
        and item.get("kind") == "trial"
        and item.get("status") in state.ACTIVE_REQUEST_STATUSES
        for item in service_requests_snapshot().values()
    ):
        await query.edit_message_text("Заявка на тестовый доступ уже ожидает решения.")
        return ConversationHandler.END
    state.clear_request_context(context)
    state.context_data(context)[state.CTX_ACTION] = "trial_comment"
    await query.edit_message_text(
        "🧪 <b>Запрос тестового доступа</b>\n\n"
        "Коротко опишите запрос. Приложение указывать не нужно — используется Happ.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Отмена", callback_data="menu:home")]]),
    )
    return state.PRODUCT_INPUT


@require_auth
async def purchase_show_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    meta = state.actor_meta(update)
    if not query or not meta:
        return
    await query.answer()
    if is_billing_exempt_meta(meta) or meta.get("service_tier") != "basic":
        await query.edit_message_text("Покупка доступна пользователям с базовым доступом.")
        return
    settings = product_settings_snapshot()
    target = payment_target(settings)
    if not payment_profile_ready(settings) or target is None:
        await query.edit_message_text(
            "Покупка через бот временно недоступна: руководитель ещё не настроил "
            "реквизиты или дату периода. Создайте тикет в поддержку.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🎫 Создать тикет", callback_data="menu:ticket")],
                    [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
                ]
            ),
        )
        return
    text = (
        "💳 <b>Покупка подписки</b>\n\n"
        f"• Период: <b>{PLAN_MONTHS} месяца</b>\n"
        f"• Стоимость: <b>{PLAN_TOTAL_RUB} ₽</b>\n"
        f"• Доступ до: <code>{html_escape(state.datetime_text(target.isoformat()))}</code>\n\n"
        "После создания заявки сотрудник отправит реквизиты."
    )
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("✅ Создать заявку", callback_data="subscription:buyconfirm")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="menu:subscription")],
            ]
        ),
    )


@require_auth
async def purchase_create_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = state.actor_meta(update)
    if not query or not actor:
        return
    await query.answer()
    user_id = int(actor.get("user_id") or 0)
    if is_billing_exempt_meta(actor) or actor.get("service_tier") != "basic":
        await query.edit_message_text("Заявка больше недоступна для вашего уровня доступа.")
        return

    def create(config: UserData) -> tuple[str, int | None]:
        current = config.authorized_users.get(str(user_id))
        if not isinstance(current, dict) or is_billing_exempt_meta(current) or current.get("service_tier") != "basic":
            return "denied", None
        existing = find_active_request(config, user_id=user_id, kind="purchase")
        if existing:
            return "exists", int(existing.get("id", 0) or 0)
        target = payment_target(config.product_settings)
        if not payment_profile_ready(config.product_settings) or target is None:
            return "not_configured", None
        request = create_request(
            config,
            kind="purchase",
            user_id=user_id,
            target_end_at=target.isoformat(),
        )
        request_id = int(request["id"])
        queue_message(
            config,
            recipient_ids=approved_admin_ids(config),
            kind="purchase_request",
            text=request_card(request, current),
            reply_markup=[
                [
                    {
                        "text": "💳 Отправить реквизиты",
                        "callback_data": f"product:req:requisites:{request_id}",
                    },
                    {
                        "text": "❌ Отклонить",
                        "callback_data": f"product:req:reject:{request_id}",
                    },
                ],
                [{"text": "👤 Профиль", "callback_data": f"users:user:{user_id}"}],
            ],
        )
        return "created", request_id

    outcome, _request_id = await update_user_data(create)
    texts = {
        "created": "✅ Заявка на покупку создана. Сотрудник отправит реквизиты после проверки.",
        "exists": "Заявка на покупку уже находится в обработке.",
        "not_configured": "Платёжные реквизиты или дата периода пока не настроены.",
        "denied": "Заявка больше недоступна для вашего уровня доступа.",
    }
    await query.edit_message_text(
        texts[outcome],
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]]),
    )


def apply_trial_comment(
    config: UserData,
    *,
    user_id: int,
    comment: str,
) -> tuple[str, int | None]:
    current = config.authorized_users.get(str(user_id))
    if not isinstance(current, dict) or current.get("service_tier") != "basic" or current.get("role") == "admin":
        return "denied", None
    if current.get("trial_issued_at"):
        return "issued", None
    existing = find_active_request(config, user_id=user_id, kind="trial")
    if existing:
        return "exists", int(existing.get("id", 0) or 0)
    request = create_request(config, kind="trial", user_id=user_id, comment=comment)
    request_id = int(request["id"])
    queue_message(
        config,
        recipient_ids=approved_admin_ids(config),
        kind="trial_request",
        text=request_card(request, current),
        reply_markup=[
            [
                {
                    "text": "✅ Одобрить",
                    "callback_data": f"product:req:approve:{request_id}",
                },
                {
                    "text": "❌ Отклонить",
                    "callback_data": f"product:req:reject:{request_id}",
                },
            ],
            [{"text": "👤 Профиль", "callback_data": f"users:user:{user_id}"}],
        ],
    )
    return "created", request_id


__all__ = [
    "apply_trial_comment",
    "purchase_create_cb",
    "purchase_show_cb",
    "trial_request_start_cb",
]
