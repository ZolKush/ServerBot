"""Subscription request cards, payment text, and inline keyboards."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from ...bot.ui import clip_html, html_escape
from ...storage import get_user_meta_copy
from ...users.staff import is_owner_meta, staff_internal_identity
from ..connections import has_connection
from ..policy import PLAN_MONTHS, PLAN_TOTAL_RUB
from . import state


def service_tier_label(value: object) -> str:
    return {
        "basic": "Базовый доступ",
        "subscriber": "Подписчик",
        "unlimited_trial": "Безлимитный тестовый доступ",
    }.get(str(value or ""), "Неизвестный уровень")


def request_kind_label(value: object) -> str:
    return {
        "trial": "Тестовый доступ",
        "purchase": "Покупка подписки",
        "renewal": "Продление подписки",
    }.get(str(value or ""), "Заявка")


def request_status_label(value: object) -> str:
    return {
        "pending": "ожидает решения",
        "claimed": "обрабатывается",
        "awaiting_link": "ожидает ссылку",
        "requisites_sent": "реквизиты отправлены",
        "payment_reported": "пользователь сообщил об оплате",
        "approved": "одобрена",
        "rejected": "отклонена",
        "cancelled": "отменена",
    }.get(str(value or ""), "неизвестно")


def real_user_name(meta: dict[str, Any]) -> str:
    name = " ".join(
        str(part).strip()[:80] for part in (meta.get("first_name"), meta.get("last_name")) if str(part or "").strip()
    )
    return name[:160] or "не указано"


def user_nickname(meta: dict[str, Any]) -> str:
    nickname = str(meta.get("nickname") or "").strip()
    return nickname[:160] or real_user_name(meta)


def request_card(request: dict[str, Any], meta: dict[str, Any]) -> str:
    username = str(meta.get("username") or "").strip().lstrip("@")
    comment = str(request.get("comment") or "").strip()
    lines = [
        f"📥 <b>{html_escape(request_kind_label(request.get('kind')))}</b> · #{request.get('id')}",
        "",
        f"• Статус: <b>{html_escape(request_status_label(request.get('status')))}</b>",
        f"• Никнейм: <b>{html_escape(user_nickname(meta))}</b>",
        f"• Имя Telegram: <b>{html_escape(real_user_name(meta))}</b>",
        f"• Username: <code>{html_escape('@' + username if username else '-')}</code>",
        f"• Telegram ID: <code>{html_escape(str(meta.get('user_id') or request.get('user_id')))}</code>",
        f"• Резервная почта: <code>{html_escape(str(meta.get('contact_email') or '-'))}</code>",
        f"• Допущен: <code>{html_escape(state.datetime_text(meta.get('auth_at')))}</code>",
        f"• Уровень: <b>{html_escape(service_tier_label(meta.get('service_tier')))}</b>",
        f"• Оплата: <b>{'подтверждена' if meta.get('is_paid') else 'не подтверждена'}</b>",
        f"• Тест ранее: <b>{'выдавался' if meta.get('trial_issued_at') else 'не выдавался'}</b>",
        f"• Ссылка: <b>{'назначена' if has_connection(meta) else 'не назначена'}</b>",
    ]
    if request.get("target_end_at"):
        lines.append(f"• Доступ до: <code>{html_escape(state.datetime_text(request.get('target_end_at')))}</code>")
    claimed_by = int(request.get("claimed_by_id", 0) or 0)
    if claimed_by:
        claimed_meta = get_user_meta_copy(claimed_by)
        claimed_identity = staff_internal_identity(claimed_meta) if claimed_meta else f"ID {claimed_by}"
        lines.append(f"• Обрабатывает: <code>{html_escape(claimed_identity)}</code>")
    if comment:
        lines.extend(["", "<b>Комментарий пользователя:</b>", clip_html(comment, limit=1400)])
    return "\n".join(lines)


def request_markup(request: dict[str, Any], actor_meta: dict[str, Any]) -> InlineKeyboardMarkup:
    request_id = int(request.get("id", 0) or 0)
    user_id = int(request.get("user_id", 0) or 0)
    kind = str(request.get("kind") or "")
    status = str(request.get("status") or "")
    rows: list[list[InlineKeyboardButton]] = []
    if kind == "trial" and status in {"pending", "claimed", "awaiting_link"}:
        rows.append(
            [
                InlineKeyboardButton("✅ Одобрить", callback_data=f"product:req:approve:{request_id}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"product:req:reject:{request_id}"),
            ]
        )
    elif kind == "purchase" and status == "pending":
        rows.append(
            [
                InlineKeyboardButton(
                    "💳 Отправить реквизиты",
                    callback_data=f"product:req:requisites:{request_id}",
                ),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"product:req:reject:{request_id}"),
            ]
        )
    elif kind == "purchase" and status == "requisites_sent":
        rows.append([InlineKeyboardButton("❌ Отклонить", callback_data=f"product:req:reject:{request_id}")])
    if kind in {"purchase", "renewal"} and status == "payment_reported" and is_owner_meta(actor_meta):
        rows.append(
            [
                InlineKeyboardButton(
                    "✅ Подтвердить оплату",
                    callback_data=f"product:req:confirm:{request_id}",
                ),
                InlineKeyboardButton(
                    "🔎 Платёж не найден",
                    callback_data=f"product:req:notfound:{request_id}",
                ),
            ]
        )
    elif (
        kind in {"purchase", "renewal"}
        and status == "awaiting_link"
        and is_owner_meta(actor_meta)
        and int(request.get("claimed_by_id", 0) or 0) == int(actor_meta.get("user_id", 0) or 0)
    ):
        rows.append(
            [
                InlineKeyboardButton(
                    "🔗 Продолжить ввод ссылки",
                    callback_data=f"product:req:confirm:{request_id}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("👤 Открыть профиль", callback_data=f"users:user:{user_id}")])
    rows.append([InlineKeyboardButton("⬅️ К заявкам", callback_data="product:requests")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def payment_target(settings: dict[str, Any], *, after: datetime | None = None) -> datetime | None:
    floor = after or state.now()
    next_end = state.parse_datetime(settings.get("next_period_end"))
    if next_end and next_end > floor:
        return next_end
    current_end = state.parse_datetime(settings.get("current_period_end"))
    if current_end and current_end > floor:
        return current_end
    return None


def payment_profile_ready(settings: dict[str, Any]) -> bool:
    return all(str(settings.get(key) or "").strip() for key in ("payment_bank", "payment_recipient", "payment_phone"))


def payment_message(settings: dict[str, Any], request: dict[str, Any]) -> str:
    return (
        "💳 <b>Оплата подписки</b>\n\n"
        f"• Период: <b>{PLAN_MONTHS} месяца</b>\n"
        f"• Стоимость: <b>{PLAN_TOTAL_RUB} ₽</b>\n"
        f"• Доступ до: <code>{html_escape(state.datetime_text(request.get('target_end_at')))}</code>\n\n"
        f"• Банк: <b>{html_escape(str(settings.get('payment_bank') or '-'))}</b>\n"
        f"• Получатель: <b>{html_escape(str(settings.get('payment_recipient') or '-'))}</b>\n"
        f"• Телефон: <code>{html_escape(str(settings.get('payment_phone') or '-'))}</code>\n\n"
        "После перевода нажмите «Я оплатил». Если возникнут вопросы, создайте тикет в поддержку."
    )


def payment_markup(request_id: int) -> list[list[dict[str, str]]]:
    return [
        [{"text": "✅ Я оплатил", "callback_data": f"subscription:paid:{request_id}"}],
        [{"text": "🎫 Создать тикет", "callback_data": "menu:ticket"}],
    ]


def renewal_markup() -> list[list[dict[str, str]]]:
    return [
        [{"text": "✅ Я оплатил продление", "callback_data": "subscription:renew"}],
        [{"text": "🎫 Создать тикет", "callback_data": "menu:ticket"}],
    ]


__all__ = [
    "payment_markup",
    "payment_message",
    "payment_profile_ready",
    "payment_target",
    "real_user_name",
    "renewal_markup",
    "request_card",
    "request_kind_label",
    "request_markup",
    "request_status_label",
    "service_tier_label",
    "user_nickname",
]
