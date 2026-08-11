"""Staff-facing subscription access management handlers."""

from __future__ import annotations

import re
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ...bot.guards import require_admin
from ...bot.ui import html_escape, ui_ok_text
from ...messaging.review_sync import sync_service_review_messages_for_user
from ...runtime.logging import logger
from ...storage import UserData, append_audit_entry, get_user_meta_copy, update_user_data
from ...users.staff import (
    is_admin_meta,
    is_billing_exempt_meta,
    is_lead_or_owner_meta,
    is_owner_meta,
    staff_public_signature,
)
from . import state
from .operations import cancel_active_requests
from .views import real_user_name, service_tier_label, user_nickname


@require_admin
async def product_manage_user_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    actor = state.actor_meta(update)
    if not query or not actor:
        return
    match = re.fullmatch(r"product:manage:(\d+)", query.data or "")
    if not match:
        return
    await query.answer()
    user_id = int(match.group(1))
    target = get_user_meta_copy(user_id)
    if not target:
        await query.edit_message_text("Пользователь не найден.")
        return
    rows: list[list[InlineKeyboardButton]] = []
    billing_exempt = is_billing_exempt_meta(target)
    if is_lead_or_owner_meta(actor) and not billing_exempt:
        rows.append(
            [
                InlineKeyboardButton(
                    "📅 Изменить дату",
                    callback_data=f"product:input:user_end:{user_id}",
                ),
                InlineKeyboardButton(
                    "🔔 Напомнить",
                    callback_data=f"product:remind:{user_id}",
                ),
            ]
        )
    if is_owner_meta(actor) and not billing_exempt:
        rows.append(
            [
                InlineKeyboardButton(
                    "💰 Подтвердить оплату вручную",
                    callback_data=f"product:input:manualpay:{user_id}",
                )
            ]
        )
        if not is_admin_meta(target):
            rows.append(
                [
                    InlineKeyboardButton(
                        "Базовый",
                        callback_data=f"product:tier:{user_id}:basic",
                    ),
                    InlineKeyboardButton(
                        "Безлимитный",
                        callback_data=f"product:tier:{user_id}:unlimited_trial",
                    ),
                ]
            )
    if is_owner_meta(actor) and is_admin_meta(target) and not is_owner_meta(target):
        rows.append(
            [
                InlineKeyboardButton(
                    "🪪 Изменить должность",
                    callback_data=f"administration:title:{user_id}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                "⬅️ Профиль пользователя",
                callback_data=f"users:user:{user_id}",
            )
        ]
    )
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    payment = (
        "бессрочная — руководитель сервиса"
        if billing_exempt
        else ("подтверждена" if target.get("is_paid") else "не подтверждена")
    )
    end_text = "бессрочно" if billing_exempt else state.datetime_text(target.get("subscription_end_at"))
    text = (
        "⚙️ <b>Управление доступом</b>\n\n"
        f"• Никнейм: <b>{html_escape(user_nickname(target))}</b>\n"
        f"• Имя Telegram: <b>{html_escape(real_user_name(target))}</b>\n"
        f"• ID: <code>{user_id}</code>\n"
        f"• Уровень: <b>{html_escape(service_tier_label(target.get('service_tier')))}</b>\n"
        f"• Оплата: <b>{payment}</b>\n"
        f"• Доступ до: <code>{html_escape(end_text)}</code>"
    )
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


@require_admin
async def product_tier_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = state.actor_meta(update)
    if not query or not actor:
        return
    match = re.fullmatch(
        r"product:tier:(\d+):(basic|unlimited_trial)",
        query.data or "",
    )
    if not match:
        return
    await query.answer()
    if not is_owner_meta(actor):
        await query.edit_message_text("Доступно только руководителю сервиса.")
        return
    user_id = int(match.group(1))
    tier = match.group(2)

    def apply(config: UserData) -> dict[str, Any] | None:
        current = config.authorized_users.get(str(user_id))
        if not isinstance(current, dict) or current.get("role") == "admin":
            return None
        old = str(current.get("service_tier") or "basic")
        updated = dict(current)
        updated.update(
            {
                "service_tier": tier,
                "is_paid": False,
                "subscription_end_at": None,
                "service_tier_updated_at": state.now_iso(),
                "service_tier_updated_by_id": actor.get("user_id"),
                "service_tier_updated_by_name": staff_public_signature(
                    actor,
                    allow_alias=False,
                ),
            }
        )
        updated = UserData._normalize_user(updated)
        config.authorized_users[str(user_id)] = updated
        cancel_active_requests(
            config,
            user_id=user_id,
            reason="service_tier_changed",
        )
        append_audit_entry(
            config,
            action="service_tier_changed",
            actor_meta=actor,
            target_user_id=user_id,
            details={"old": old, "new": tier},
        )
        return updated

    updated = await update_user_data(apply)
    if not updated:
        await query.edit_message_text("Уровень этого пользователя изменить нельзя.")
        return
    await query.edit_message_text(
        ui_ok_text(f"Назначен уровень: {service_tier_label(tier)}"),
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
    try:
        await sync_service_review_messages_for_user(context.bot, user_id)
    except Exception:
        logger.exception("Could not synchronize cancelled service requests user_id=%s", user_id)


__all__ = ["product_manage_user_cb", "product_tier_cb"]
