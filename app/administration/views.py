from __future__ import annotations

from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from ..bot.help import help_text_from_settings
from ..bot.ui import html_escape
from ..subscriptions.policy import PLAN_MONTHS, PLAN_TOTAL_RUB
from ..users.staff import (
    STAFF_DISPLAY_TITLE_ALIAS,
    can_edit_help_meta,
    is_lead_or_owner_meta,
    is_owner_meta,
    staff_internal_identity,
    staff_public_signature,
    staff_title_label,
)
from .dates import datetime_text

INPUT_PROMPTS = {
    "alias": ("Введите псевдоним длиной от 2 до 32 символов. Для удаления отправьте один дефис: <code>-</code>"),
    "help": "Введите новый текст инструкции. Разрешён обычный текст длиной до 3500 символов:",
    "support_email": "Введите контактную почту администрации. Для удаления отправьте один дефис: <code>-</code>",
    "payment_bank": "Введите название банка:",
    "payment_recipient": "Введите имя получателя платежа:",
    "payment_phone": "Введите номер телефона для перевода:",
    "period_current": "Введите окончание текущего периода в формате <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>:",
    "period_next": "Введите окончание следующего периода в формате <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>:",
}


def administration_text(meta: dict[str, Any]) -> str:
    mode = (
        "должность и псевдоним" if meta.get("staff_display_mode") == STAFF_DISPLAY_TITLE_ALIAS else "только должность"
    )
    return (
        "⚙️ <b>Администрирование</b>\n\n"
        "<b>Публичное представление сотрудника</b>\n"
        f"• Публичная подпись: <b>{html_escape(staff_public_signature(meta))}</b>\n"
        f"• Должность: <b>{html_escape(staff_title_label(meta))}</b>\n"
        f"• Псевдоним: <b>{html_escape(str(meta.get('staff_alias') or '-'))}</b>\n"
        f"• Режим: <b>{html_escape(mode)}</b>\n\n"
        f"• Внутренняя личность: <code>{html_escape(staff_internal_identity(meta))}</code>"
    )


def administration_markup(meta: dict[str, Any]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton("🏷 Изменить псевдоним", callback_data="administration:input:alias")],
        [
            InlineKeyboardButton("Только должность", callback_data="administration:signature:title"),
            InlineKeyboardButton(
                "Должность + псевдоним",
                callback_data="administration:signature:title_alias",
            ),
        ],
    ]
    if can_edit_help_meta(meta) or is_owner_meta(meta):
        rows.append([InlineKeyboardButton("⚙️ Настройки сервиса", callback_data="administration:settings")])
    if is_lead_or_owner_meta(meta):
        rows.append(
            [
                InlineKeyboardButton("📅 Массовая дата", callback_data="product:input:massdate"),
                InlineKeyboardButton("🔔 Массово напомнить", callback_data="product:input:massremind"),
            ]
        )
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def service_settings_text(settings: dict[str, Any], actor: dict[str, Any]) -> str:
    help_text = help_text_from_settings(settings)
    help_preview = help_text if len(help_text) <= 500 else help_text[:500].rstrip() + "…"
    lines = [
        "⚙️ <b>Администрирование → Настройки сервиса</b>",
        "",
        "<b>Инструкция помощи</b>",
        html_escape(help_preview),
        "",
        f"• Последнее изменение: <code>{html_escape(datetime_text(settings.get('help_updated_at')))}</code>",
        f"• Изменил: <b>{html_escape(str(settings.get('help_updated_by_name') or '-'))}</b>",
    ]
    if is_owner_meta(actor):
        lines.extend(
            [
                "",
                "<b>Контакты и оплата</b>",
                f"• Почта администрации: <code>{html_escape(str(settings.get('support_email') or '-'))}</code>",
                f"• Банк: <b>{html_escape(str(settings.get('payment_bank') or '-'))}</b>",
                f"• Получатель: <b>{html_escape(str(settings.get('payment_recipient') or '-'))}</b>",
                f"• Телефон: <code>{html_escape(str(settings.get('payment_phone') or '-'))}</code>",
                f"• Тариф: <b>{PLAN_TOTAL_RUB} ₽ / {PLAN_MONTHS} месяца</b>",
                "",
                f"• Текущий период до: <code>{html_escape(datetime_text(settings.get('current_period_end')))}</code>",
                f"• Следующий период до: <code>{html_escape(datetime_text(settings.get('next_period_end')))}</code>",
                "",
                "Изменение следующего периода не продлевает пользователей автоматически.",
            ]
        )
    return "\n".join(lines)


def service_settings_markup(actor: dict[str, Any]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if can_edit_help_meta(actor):
        rows.extend(
            [
                [InlineKeyboardButton("📝 Изменить помощь", callback_data="administration:input:help")],
                [InlineKeyboardButton("↩️ Стандартная помощь", callback_data="administration:help:reset")],
            ]
        )
    if is_owner_meta(actor):
        rows.extend(
            [
                [
                    InlineKeyboardButton(
                        "📧 Почта администрации",
                        callback_data="administration:input:support_email",
                    )
                ],
                [
                    InlineKeyboardButton("🏦 Банк", callback_data="administration:input:payment_bank"),
                    InlineKeyboardButton(
                        "👤 Получатель",
                        callback_data="administration:input:payment_recipient",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "📱 Телефон",
                        callback_data="administration:input:payment_phone",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "📅 Текущий период",
                        callback_data="administration:input:period_current",
                    ),
                    InlineKeyboardButton(
                        "⏭ Следующий период",
                        callback_data="administration:input:period_next",
                    ),
                ],
            ]
        )
    rows.extend(
        [
            [InlineKeyboardButton("⬅️ Администрирование", callback_data="administration:show")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )
    return InlineKeyboardMarkup(rows)


def cancel_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="administration:cancel")]])


def confirmation_markup(*, save_label: str = "✅ Подтвердить") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(save_label, callback_data="administration:confirm")],
            [InlineKeyboardButton("❌ Отмена", callback_data="administration:cancel")],
        ]
    )


__all__ = [
    "INPUT_PROMPTS",
    "administration_markup",
    "administration_text",
    "cancel_markup",
    "confirmation_markup",
    "datetime_text",
    "service_settings_markup",
    "service_settings_text",
]
