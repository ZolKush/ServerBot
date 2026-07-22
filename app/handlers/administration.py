from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ..config import TZ
from ..help_content import DEFAULT_HELP_TEXT, help_text_from_settings
from ..service_plan import PLAN_MONTHS, PLAN_TOTAL_RUB
from ..staff import (
    REGULAR_STAFF_TITLES,
    STAFF_DISPLAY_TITLE,
    STAFF_DISPLAY_TITLE_ALIAS,
    STAFF_TITLE_LABELS,
    can_edit_help_meta,
    is_lead_or_owner_meta,
    is_owner_meta,
    normalize_staff_alias,
    staff_internal_identity,
    staff_public_signature,
    staff_title_label,
)
from ..storage import (
    UserData,
    append_audit_entry,
    get_user_meta_copy,
    product_settings_snapshot,
    update_user_data,
)
from ..validation import normalize_email
from .common import get_user_id, html_escape, require_admin, ui_ok_text

ADMINISTRATION_INPUT = 81
ADMINISTRATION_CONFIRM = 82

_CTX_KEY = "administration_flow"
_CTX_ACTION = "action"
_CTX_PENDING = "pending"


def _state(context: ContextTypes.DEFAULT_TYPE) -> dict[str, Any]:
    user_data = context.user_data
    if user_data is None:
        raise RuntimeError("Telegram user_data is unavailable")
    value = user_data.get(_CTX_KEY)
    if not isinstance(value, dict):
        value = {}
        user_data[_CTX_KEY] = value
    return value


def _clear_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    if context.user_data is not None:
        context.user_data.pop(_CTX_KEY, None)


def _actor(update: Update) -> dict[str, Any] | None:
    uid = get_user_id(update)
    return get_user_meta_copy(uid) if uid is not None else None


def _parse_input_dt(value: str) -> datetime | None:
    try:
        parsed = datetime.strptime(value.strip(), "%d.%m.%Y %H:%M")
    except ValueError:
        return None
    return parsed.replace(tzinfo=TZ)


def _parse_dt(value: object) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=TZ)
    return parsed.astimezone(TZ)


def _dt_text(value: object) -> str:
    parsed = _parse_dt(value)
    return parsed.strftime("%d.%m.%Y %H:%M") if parsed else "-"


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
            InlineKeyboardButton("Должность + псевдоним", callback_data="administration:signature:title_alias"),
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
        f"• Последнее изменение: <code>{html_escape(_dt_text(settings.get('help_updated_at')))}</code>",
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
                f"• Текущий период до: <code>{html_escape(_dt_text(settings.get('current_period_end')))}</code>",
                f"• Следующий период до: <code>{html_escape(_dt_text(settings.get('next_period_end')))}</code>",
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
                [InlineKeyboardButton("📧 Почта администрации", callback_data="administration:input:support_email")],
                [
                    InlineKeyboardButton("🏦 Банк", callback_data="administration:input:payment_bank"),
                    InlineKeyboardButton("👤 Получатель", callback_data="administration:input:payment_recipient"),
                ],
                [InlineKeyboardButton("📱 Телефон", callback_data="administration:input:payment_phone")],
                [
                    InlineKeyboardButton("📅 Текущий период", callback_data="administration:input:period_current"),
                    InlineKeyboardButton("⏭ Следующий период", callback_data="administration:input:period_next"),
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


@require_admin
async def administration_show_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = _actor(update)
    if not query or not actor:
        return
    await query.answer()
    await query.edit_message_text(
        administration_text(actor),
        parse_mode=ParseMode.HTML,
        reply_markup=administration_markup(actor),
    )


@require_admin
async def administration_signature_mode_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = _actor(update)
    if not query or not actor:
        return
    match = re.fullmatch(r"(?:administration:signature|staff:mode):(title|title_alias)", query.data or "")
    if not match:
        return
    await query.answer()
    mode = match.group(1)
    if mode == STAFF_DISPLAY_TITLE_ALIAS and not actor.get("staff_alias"):
        await query.edit_message_text("Сначала задайте псевдоним.", reply_markup=administration_markup(actor))
        return
    uid = int(actor.get("user_id") or 0)

    def _change(cfg: UserData) -> dict[str, Any]:
        current = cfg.authorized_users.get(str(uid))
        if not isinstance(current, dict) or current.get("role") != "admin":
            raise ValueError("admin_missing")
        old_mode = str(current.get("staff_display_mode") or STAFF_DISPLAY_TITLE)
        updated = UserData._normalize_user({**current, "staff_display_mode": mode})
        cfg.authorized_users[str(uid)] = updated
        append_audit_entry(
            cfg,
            action="staff_display_mode_changed",
            actor_meta=updated,
            target_user_id=uid,
            details={"old": old_mode, "new": updated.get("staff_display_mode")},
        )
        return updated

    updated = await update_user_data(_change)
    await query.edit_message_text(
        administration_text(updated),
        parse_mode=ParseMode.HTML,
        reply_markup=administration_markup(updated),
    )


@require_admin
async def administration_service_settings_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = _actor(update)
    if not query or not actor:
        return
    await query.answer()
    if not (can_edit_help_meta(actor) or is_owner_meta(actor)):
        await query.edit_message_text("Настройки сервиса недоступны для специалиста поддержки.")
        return
    await query.edit_message_text(
        service_settings_text(product_settings_snapshot(), actor),
        parse_mode=ParseMode.HTML,
        reply_markup=service_settings_markup(actor),
    )


def _normalize_input_action(data: str) -> str | None:
    aliases = {
        "staff:alias": "alias",
        "product:input:setting_bank": "payment_bank",
        "product:input:setting_recipient": "payment_recipient",
        "product:input:setting_phone": "payment_phone",
        "product:input:setting_current": "period_current",
        "product:input:setting_next": "period_next",
    }
    if data in aliases:
        return aliases[data]
    match = re.fullmatch(
        r"administration:input:(alias|help|support_email|payment_bank|payment_recipient|payment_phone|period_current|period_next)",
        data,
    )
    return match.group(1) if match else None


@require_admin
async def administration_input_start_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    actor = _actor(update)
    if not query or not actor:
        return ConversationHandler.END
    action = _normalize_input_action(query.data or "")
    if not action:
        return ConversationHandler.END
    if action == "help" and not can_edit_help_meta(actor):
        await query.answer("Недостаточно прав.", show_alert=True)
        return ConversationHandler.END
    if action not in {"alias", "help"} and not is_owner_meta(actor):
        await query.answer("Доступно только руководителю сервиса.", show_alert=True)
        return ConversationHandler.END
    prompts = {
        "alias": "Введите псевдоним длиной от 2 до 32 символов. Для удаления отправьте один дефис: <code>-</code>",
        "help": "Введите новый текст инструкции. Разрешён обычный текст длиной до 3500 символов:",
        "support_email": "Введите контактную почту администрации. Для удаления отправьте один дефис: <code>-</code>",
        "payment_bank": "Введите название банка:",
        "payment_recipient": "Введите имя получателя платежа:",
        "payment_phone": "Введите номер телефона для перевода:",
        "period_current": "Введите окончание текущего периода в формате <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>:",
        "period_next": "Введите окончание следующего периода в формате <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>:",
    }
    await query.answer()
    _clear_state(context)
    _state(context)[_CTX_ACTION] = action
    await query.edit_message_text(
        prompts[action],
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="administration:cancel")]]),
    )
    return ADMINISTRATION_INPUT


@require_admin
async def administration_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    actor = _actor(update)
    if not message or not actor:
        return ConversationHandler.END
    text = (message.text or "").strip()
    action = str(_state(context).get(_CTX_ACTION) or "")
    if not text:
        await message.reply_text("Пустое значение. Повторите ввод.")
        return ADMINISTRATION_INPUT

    if action == "alias":
        cleaned = " ".join(text.split())
        if cleaned != "-" and not 2 <= len(cleaned) <= 32:
            await message.reply_text("Псевдоним должен содержать от 2 до 32 символов.")
            return ADMINISTRATION_INPUT
        alias = None if cleaned == "-" else normalize_staff_alias(cleaned)
        uid = int(actor.get("user_id") or 0)

        def _set_alias(cfg: UserData) -> dict[str, Any]:
            current = cfg.authorized_users.get(str(uid))
            if not isinstance(current, dict) or current.get("role") != "admin":
                raise ValueError("admin_missing")
            mode = current.get("staff_display_mode") if alias else STAFF_DISPLAY_TITLE
            updated = UserData._normalize_user({**current, "staff_alias": alias, "staff_display_mode": mode})
            cfg.authorized_users[str(uid)] = updated
            append_audit_entry(
                cfg,
                action="staff_alias_changed",
                actor_meta=updated,
                target_user_id=uid,
                details={"old": current.get("staff_alias"), "new": alias},
            )
            return updated

        updated = await update_user_data(_set_alias)
        _clear_state(context)
        await message.reply_text(
            administration_text(updated),
            parse_mode=ParseMode.HTML,
            reply_markup=administration_markup(updated),
        )
        return ConversationHandler.END

    if action == "help":
        if not can_edit_help_meta(actor):
            _clear_state(context)
            await message.reply_text("Недостаточно прав.")
            return ConversationHandler.END
        if len(text) > 3500:
            await message.reply_text("Инструкция слишком длинная. Максимум 3500 символов.")
            return ADMINISTRATION_INPUT
        _state(context)[_CTX_PENDING] = {"kind": "help", "value": text}
        await message.reply_text(
            "📝 <b>Предварительный просмотр инструкции</b>\n\n" + html_escape(text),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("✅ Сохранить", callback_data="administration:confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="administration:cancel")],
                ]
            ),
        )
        return ADMINISTRATION_CONFIRM

    if action == "support_email":
        if not is_owner_meta(actor):
            _clear_state(context)
            await message.reply_text("Доступно только руководителю сервиса.")
            return ConversationHandler.END
        email = None if text == "-" else normalize_email(text)
        if text != "-" and email is None:
            await message.reply_text("Некорректный адрес электронной почты. Повторите ввод.")
            return ADMINISTRATION_INPUT

        def _set_email(cfg: UserData) -> dict[str, Any]:
            old = cfg.product_settings.get("support_email")
            cfg.product_settings["support_email"] = email
            append_audit_entry(
                cfg,
                action="support_email_changed",
                actor_meta=actor,
                details={"old": old or "-", "new": email or "-"},
            )
            return dict(cfg.product_settings)

        settings = await update_user_data(_set_email)
        _clear_state(context)
        await message.reply_text(
            service_settings_text(settings, actor),
            parse_mode=ParseMode.HTML,
            reply_markup=service_settings_markup(actor),
        )
        return ConversationHandler.END

    if action in {"payment_bank", "payment_recipient", "payment_phone"}:
        if not is_owner_meta(actor):
            _clear_state(context)
            await message.reply_text("Доступно только руководителю сервиса.")
            return ConversationHandler.END
        limits = {"payment_bank": 160, "payment_recipient": 160, "payment_phone": 80}
        if len(text) > limits[action]:
            await message.reply_text("Значение слишком длинное.")
            return ADMINISTRATION_INPUT

        def _set_payment(cfg: UserData) -> dict[str, Any]:
            cfg.product_settings[action] = " ".join(text.split())
            append_audit_entry(cfg, action=f"{action}_changed", actor_meta=actor, details={"value": "обновлено"})
            return dict(cfg.product_settings)

        settings = await update_user_data(_set_payment)
        _clear_state(context)
        await message.reply_text(
            service_settings_text(settings, actor),
            parse_mode=ParseMode.HTML,
            reply_markup=service_settings_markup(actor),
        )
        return ConversationHandler.END

    if action in {"period_current", "period_next"}:
        if not is_owner_meta(actor):
            _clear_state(context)
            await message.reply_text("Доступно только руководителю сервиса.")
            return ConversationHandler.END
        target = _parse_input_dt(text)
        if target is None or target <= datetime.now(TZ):
            await message.reply_text("Укажите будущую дату в формате ДД.ММ.ГГГГ ЧЧ:ММ.")
            return ADMINISTRATION_INPUT
        _state(context)[_CTX_PENDING] = {"kind": action, "target_end_at": target.isoformat()}
        await message.reply_text(
            f"Новое значение: <code>{html_escape(_dt_text(target.isoformat()))}</code>\n\nПодтвердите изменение.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="administration:confirm")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="administration:cancel")],
                ]
            ),
        )
        return ADMINISTRATION_CONFIRM

    _clear_state(context)
    await message.reply_text("Сценарий ввода устарел. Начните действие заново.")
    return ConversationHandler.END


@require_admin
async def administration_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    actor = _actor(update)
    if not query or not actor:
        return ConversationHandler.END
    await query.answer()
    pending = _state(context).get(_CTX_PENDING)
    if not isinstance(pending, dict):
        await query.edit_message_text("Подтверждение устарело. Начните действие заново.")
        return ConversationHandler.END
    kind = str(pending.get("kind") or "")
    if kind == "help":
        if not can_edit_help_meta(actor):
            await query.edit_message_text("Недостаточно прав.")
            _clear_state(context)
            return ConversationHandler.END
        value = str(pending.get("value") or "").strip()
        if not value or len(value) > 3500:
            await query.edit_message_text("Текст инструкции потерян или превышает лимит.")
            _clear_state(context)
            return ConversationHandler.END

        def _save_help(cfg: UserData) -> dict[str, Any]:
            old = cfg.product_settings.get("help_text")
            now = datetime.now(TZ).isoformat()
            cfg.product_settings.update(
                {
                    "help_text": value,
                    "help_updated_at": now,
                    "help_updated_by_id": actor.get("user_id"),
                    "help_updated_by_name": staff_public_signature(actor, allow_alias=False),
                }
            )
            append_audit_entry(
                cfg,
                action="help_text_changed",
                actor_meta=actor,
                details={"old_length": len(str(old or "")), "new_length": len(value)},
            )
            return dict(cfg.product_settings)

        settings = await update_user_data(_save_help)
    elif kind in {"period_current", "period_next"}:
        target = _parse_dt(pending.get("target_end_at"))
        if not is_owner_meta(actor) or target is None or target <= datetime.now(TZ):
            await query.edit_message_text("Изменение больше недоступно или дата устарела.")
            _clear_state(context)
            return ConversationHandler.END

        def _save_period(cfg: UserData) -> tuple[str, dict[str, Any]]:
            current = _parse_dt(cfg.product_settings.get("current_period_end"))
            next_end = _parse_dt(cfg.product_settings.get("next_period_end"))
            if kind == "period_current" and next_end and target >= next_end:
                return "order", dict(cfg.product_settings)
            if kind == "period_next" and current is None:
                return "missing_current", dict(cfg.product_settings)
            if kind == "period_next" and current and target <= current:
                return "order", dict(cfg.product_settings)
            key = "current_period_end" if kind == "period_current" else "next_period_end"
            old = cfg.product_settings.get(key)
            cfg.product_settings[key] = target.isoformat()
            cfg.product_settings["period_setup_reminder_for"] = None
            cfg.product_settings["period_missing_notice_for"] = None
            append_audit_entry(
                cfg,
                action=f"{key}_changed",
                actor_meta=actor,
                details={"old": old, "new": target.isoformat()},
            )
            return "updated", dict(cfg.product_settings)

        outcome, settings = await update_user_data(_save_period)
        if outcome == "order":
            await query.edit_message_text("Следующий период должен заканчиваться позже текущего.")
            _clear_state(context)
            return ConversationHandler.END
        if outcome == "missing_current":
            await query.edit_message_text("Сначала укажите дату окончания текущего периода.")
            _clear_state(context)
            return ConversationHandler.END
    else:
        await query.edit_message_text("Неизвестное подтверждение.")
        _clear_state(context)
        return ConversationHandler.END
    _clear_state(context)
    await query.edit_message_text(
        service_settings_text(settings, actor),
        parse_mode=ParseMode.HTML,
        reply_markup=service_settings_markup(actor),
    )
    return ConversationHandler.END


@require_admin
async def administration_help_reset_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = _actor(update)
    if not query or not actor:
        return
    await query.answer()
    if not can_edit_help_meta(actor):
        await query.edit_message_text("Недостаточно прав.")
        return

    def _reset(cfg: UserData) -> dict[str, Any]:
        old = cfg.product_settings.get("help_text")
        cfg.product_settings.update(
            {
                "help_text": None,
                "help_updated_at": datetime.now(TZ).isoformat(),
                "help_updated_by_id": actor.get("user_id"),
                "help_updated_by_name": staff_public_signature(actor, allow_alias=False),
            }
        )
        append_audit_entry(
            cfg,
            action="help_text_reset",
            actor_meta=actor,
            details={"old_length": len(str(old or "")), "default_length": len(DEFAULT_HELP_TEXT)},
        )
        return dict(cfg.product_settings)

    settings = await update_user_data(_reset)
    await query.edit_message_text(
        service_settings_text(settings, actor),
        parse_mode=ParseMode.HTML,
        reply_markup=service_settings_markup(actor),
    )


@require_admin
async def administration_staff_title_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = _actor(update)
    if not query or not actor:
        return
    match = re.fullmatch(r"(?:administration:title|product:titlemenu):(\d+)", query.data or "")
    if not match:
        return
    await query.answer()
    if not is_owner_meta(actor):
        await query.edit_message_text("Доступно только руководителю сервиса.")
        return
    uid = int(match.group(1))
    target = get_user_meta_copy(uid)
    if not target or target.get("role") != "admin" or is_owner_meta(target):
        await query.edit_message_text("Должность этого пользователя изменить нельзя.")
        return
    rows = [
        [InlineKeyboardButton(STAFF_TITLE_LABELS[code], callback_data=f"administration:title:{uid}:{code}")]
        for code in REGULAR_STAFF_TITLES
    ]
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"product:manage:{uid}")])
    await query.edit_message_text(
        f"🪪 <b>Должность сотрудника</b>\n\nТекущая: <b>{html_escape(staff_title_label(target))}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


@require_admin
async def administration_staff_title_apply_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = _actor(update)
    if not query or not actor:
        return
    match = re.fullmatch(r"(?:administration:title|product:title):(\d+):([a-z_]+)", query.data or "")
    if not match:
        return
    await query.answer()
    if not is_owner_meta(actor):
        await query.edit_message_text("Доступно только руководителю сервиса.")
        return
    uid = int(match.group(1))
    title_code = match.group(2)
    if title_code not in REGULAR_STAFF_TITLES:
        await query.edit_message_text("Неизвестная должность.")
        return

    def _apply(cfg: UserData) -> dict[str, Any] | None:
        target = cfg.authorized_users.get(str(uid))
        if not isinstance(target, dict) or target.get("role") != "admin" or is_owner_meta(target):
            return None
        old_title = staff_title_label(target)
        updated = UserData._normalize_user({**target, "staff_title": title_code})
        cfg.authorized_users[str(uid)] = updated
        append_audit_entry(
            cfg,
            action="staff_title_changed",
            actor_meta=actor,
            target_user_id=uid,
            details={"old": old_title, "new": STAFF_TITLE_LABELS[title_code]},
        )
        return updated

    updated = await update_user_data(_apply)
    if not updated:
        await query.edit_message_text("Сотрудник не найден.")
        return
    await query.edit_message_text(
        ui_ok_text(f"Должность изменена: {STAFF_TITLE_LABELS[title_code]}"),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data=f"product:manage:{uid}")]]),
    )


async def administration_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _clear_state(context)
    query = update.callback_query
    actor = _actor(update)
    if query:
        await query.answer()
        if actor:
            await query.edit_message_text(
                administration_text(actor),
                parse_mode=ParseMode.HTML,
                reply_markup=administration_markup(actor),
            )
    return ConversationHandler.END


__all__ = [
    "ADMINISTRATION_CONFIRM",
    "ADMINISTRATION_INPUT",
    "administration_cancel",
    "administration_confirm_cb",
    "administration_help_reset_cb",
    "administration_input_start_cb",
    "administration_service_settings_cb",
    "administration_show_cb",
    "administration_signature_mode_cb",
    "administration_staff_title_apply_cb",
    "administration_staff_title_menu_cb",
    "administration_text_input",
    "administration_markup",
    "administration_text",
    "service_settings_markup",
    "service_settings_text",
]
