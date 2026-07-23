from __future__ import annotations

from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ..bot.guards import get_user_id, require_auth
from ..bot.ui import format_dt_human, html_escape, ui_ok_text
from ..storage import UserData, append_audit_entry, get_user_meta_copy, update_user_data
from ..subscriptions.connections import has_connection
from .staff import is_billing_exempt_meta
from .validation import normalize_email

PROFILE_EMAIL_INPUT = 91


def _real_name(meta: dict[str, Any]) -> str:
    value = " ".join(
        str(part).strip() for part in (meta.get("first_name"), meta.get("last_name")) if str(part or "").strip()
    )
    return value or str(meta.get("username") or meta.get("user_id") or "-")


def _tier_label(meta: dict[str, Any]) -> str:
    if is_billing_exempt_meta(meta):
        return "Бессрочный оплаченный доступ — руководитель сервиса"
    return {
        "basic": "Базовый",
        "subscriber": "Подписчик",
        "unlimited_trial": "Безлимитный тестовый доступ",
    }.get(str(meta.get("service_tier") or "basic"), "Базовый")


def personal_profile_text(meta: dict[str, Any]) -> str:
    username = str(meta.get("username") or "").strip().lstrip("@")
    nickname = str(meta.get("nickname") or "").strip()
    billing_exempt = is_billing_exempt_meta(meta)
    payment_text = "бессрочно" if billing_exempt else ("подтверждена" if meta.get("is_paid") else "не подтверждена")
    paid_at_text = "не применяется" if billing_exempt else format_dt_human(meta.get("paid_at"))
    end_text = "бессрочно" if billing_exempt else format_dt_human(meta.get("subscription_end_at"))
    return "\n".join(
        [
            "👤 <b>Личный профиль</b>",
            "",
            f"• Никнейм: <b>{html_escape(nickname or '-')}</b>",
            f"• Имя Telegram: <b>{html_escape(_real_name(meta))}</b>",
            f"• Username: <code>{html_escape('@' + username if username else '-')}</code>",
            f"• Резервная почта: <code>{html_escape(str(meta.get('contact_email') or '-'))}</code>",
            f"• Уровень: <b>{html_escape(_tier_label(meta))}</b>",
            f"• Оплата: <b>{html_escape(payment_text)}</b>",
            f"• Дата оплаты: <code>{html_escape(paid_at_text)}</code>",
            f"• Доступ до: <code>{html_escape(end_text)}</code>",
            f"• Персональная ссылка: <b>{'назначена' if has_connection(meta) else 'не назначена'}</b>",
            f"• Тестовый доступ: <b>{'выдавался' if meta.get('trial_issued_at') else 'не выдавался'}</b>",
        ]
    )


def personal_profile_markup(meta: dict[str, Any]) -> InlineKeyboardMarkup:
    email_label = "📧 Изменить резервную почту" if meta.get("contact_email") else "📧 Указать резервную почту"
    rows = [[InlineKeyboardButton(email_label, callback_data="profile:email:edit")]]
    if meta.get("contact_email"):
        rows.append([InlineKeyboardButton("🗑 Удалить резервную почту", callback_data="profile:email:clear")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def _meta(update: Update) -> dict[str, Any] | None:
    uid = get_user_id(update)
    return get_user_meta_copy(uid) if uid is not None else None


@require_auth
async def personal_profile_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    meta = _meta(update)
    if not query or not meta:
        return
    await query.answer()
    await query.edit_message_text(
        personal_profile_text(meta),
        parse_mode=ParseMode.HTML,
        reply_markup=personal_profile_markup(meta),
    )


@require_auth
async def profile_email_start_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()
    await query.edit_message_text(
        "Введите резервный адрес электронной почты. Он будет виден только администраторам и не подтверждается письмом.",
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⬅️ Назад", callback_data="profile:show")],
                [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
            ]
        ),
    )
    return PROFILE_EMAIL_INPUT


@require_auth
async def profile_email_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    uid = get_user_id(update)
    if not message or uid is None:
        return ConversationHandler.END
    email = normalize_email(message.text)
    if email is None:
        await message.reply_text("Некорректный адрес электронной почты. Проверьте формат и повторите ввод.")
        return PROFILE_EMAIL_INPUT

    def _save(cfg: UserData) -> dict[str, Any] | None:
        current = cfg.authorized_users.get(str(uid))
        if not isinstance(current, dict):
            return None
        old = current.get("contact_email")
        updated = UserData._normalize_user({**current, "contact_email": email})
        cfg.authorized_users[str(uid)] = updated
        append_audit_entry(
            cfg,
            action="contact_email_changed",
            actor_meta=updated,
            target_user_id=uid,
            details={"old": old or "-", "new": email},
        )
        return updated

    updated = await update_user_data(_save)
    if not updated:
        await message.reply_text("Профиль больше не найден.")
        return ConversationHandler.END
    await message.reply_text(ui_ok_text("Резервная почта сохранена"))
    await message.reply_text(
        personal_profile_text(updated),
        parse_mode=ParseMode.HTML,
        reply_markup=personal_profile_markup(updated),
    )
    return ConversationHandler.END


@require_auth
async def profile_email_clear_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    uid = get_user_id(update)
    if not query or uid is None:
        return
    await query.answer()

    def _clear(cfg: UserData) -> dict[str, Any] | None:
        current = cfg.authorized_users.get(str(uid))
        if not isinstance(current, dict):
            return None
        old = current.get("contact_email")
        updated = UserData._normalize_user({**current, "contact_email": None})
        cfg.authorized_users[str(uid)] = updated
        append_audit_entry(
            cfg,
            action="contact_email_cleared",
            actor_meta=updated,
            target_user_id=uid,
            details={"old": old or "-"},
        )
        return updated

    updated = await update_user_data(_clear)
    if not updated:
        await query.edit_message_text("Профиль больше не найден.")
        return
    await query.edit_message_text(
        personal_profile_text(updated) + "\n\n" + ui_ok_text("Резервная почта удалена"),
        parse_mode=ParseMode.HTML,
        reply_markup=personal_profile_markup(updated),
    )


async def profile_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    meta = _meta(update)
    if query and meta:
        await query.answer()
        await query.edit_message_text(
            personal_profile_text(meta),
            parse_mode=ParseMode.HTML,
            reply_markup=personal_profile_markup(meta),
        )
    return ConversationHandler.END


__all__ = [
    "PROFILE_EMAIL_INPUT",
    "personal_profile_cb",
    "personal_profile_markup",
    "personal_profile_text",
    "profile_cancel",
    "profile_email_clear_cb",
    "profile_email_start_cb",
    "profile_email_text",
]
