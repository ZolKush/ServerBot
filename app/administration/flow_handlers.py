from __future__ import annotations

from datetime import datetime
from typing import Any, cast

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ..bot.guards import get_user_id, require_admin
from ..bot.ui import html_escape
from ..config import TZ
from ..storage import get_user_meta_copy
from ..users.staff import (
    can_edit_help_meta,
    is_owner_meta,
    normalize_staff_alias,
)
from ..users.validation import normalize_email
from .dates import datetime_text, parse_datetime, parse_input_datetime
from .operations import (
    PaymentSetting,
    PeriodKind,
    change_payment_setting,
    change_staff_alias,
    change_support_email,
    save_billing_period,
    save_help_text,
)
from .state import (
    ADMINISTRATION_CONFIRM,
    ADMINISTRATION_INPUT,
    clear_flow_state,
    flow_action,
    normalize_input_action,
    pending_change,
    set_flow_action,
    set_pending_change,
)
from .views import (
    INPUT_PROMPTS,
    administration_markup,
    administration_text,
    cancel_markup,
    confirmation_markup,
    service_settings_markup,
    service_settings_text,
)


def actor_meta(update: Update) -> dict[str, Any] | None:
    user_id = get_user_id(update)
    return get_user_meta_copy(user_id) if user_id is not None else None


@require_admin
async def administration_input_start_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    query = update.callback_query
    actor = actor_meta(update)
    if not query or not actor:
        return ConversationHandler.END
    action = normalize_input_action(query.data or "")
    if not action:
        return ConversationHandler.END
    if action == "help" and not can_edit_help_meta(actor):
        await query.answer("Недостаточно прав.", show_alert=True)
        return ConversationHandler.END
    if action not in {"alias", "help"} and not is_owner_meta(actor):
        await query.answer("Доступно только руководителю сервиса.", show_alert=True)
        return ConversationHandler.END
    await query.answer()
    clear_flow_state(context)
    set_flow_action(context, action)
    await query.edit_message_text(
        INPUT_PROMPTS[action],
        parse_mode=ParseMode.HTML,
        reply_markup=cancel_markup(),
    )
    return ADMINISTRATION_INPUT


@require_admin
async def administration_text_input(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    message = update.effective_message
    actor = actor_meta(update)
    if not message or not actor:
        return ConversationHandler.END
    text = (message.text or "").strip()
    action = flow_action(context)
    if not text:
        await message.reply_text("Пустое значение. Повторите ввод.")
        return ADMINISTRATION_INPUT

    if action == "alias":
        cleaned = " ".join(text.split())
        if cleaned != "-" and not 2 <= len(cleaned) <= 32:
            await message.reply_text("Псевдоним должен содержать от 2 до 32 символов.")
            return ADMINISTRATION_INPUT
        alias = None if cleaned == "-" else normalize_staff_alias(cleaned)
        updated = await change_staff_alias(
            user_id=int(actor.get("user_id") or 0),
            alias=alias,
        )
        clear_flow_state(context)
        await message.reply_text(
            administration_text(updated),
            parse_mode=ParseMode.HTML,
            reply_markup=administration_markup(updated),
        )
        return ConversationHandler.END

    if action == "help":
        if not can_edit_help_meta(actor):
            clear_flow_state(context)
            await message.reply_text("Недостаточно прав.")
            return ConversationHandler.END
        if len(text) > 3500:
            await message.reply_text("Инструкция слишком длинная. Максимум 3500 символов.")
            return ADMINISTRATION_INPUT
        set_pending_change(context, {"kind": "help", "value": text})
        await message.reply_text(
            "📝 <b>Предварительный просмотр инструкции</b>\n\n" + html_escape(text),
            parse_mode=ParseMode.HTML,
            reply_markup=confirmation_markup(save_label="✅ Сохранить"),
        )
        return ADMINISTRATION_CONFIRM

    if action == "support_email":
        if not is_owner_meta(actor):
            clear_flow_state(context)
            await message.reply_text("Доступно только руководителю сервиса.")
            return ConversationHandler.END
        email = None if text == "-" else normalize_email(text)
        if text != "-" and email is None:
            await message.reply_text("Некорректный адрес электронной почты. Повторите ввод.")
            return ADMINISTRATION_INPUT
        settings = await change_support_email(actor=actor, email=email)
        clear_flow_state(context)
        await message.reply_text(
            service_settings_text(settings, actor),
            parse_mode=ParseMode.HTML,
            reply_markup=service_settings_markup(actor),
        )
        return ConversationHandler.END

    if action in {"payment_bank", "payment_recipient", "payment_phone"}:
        if not is_owner_meta(actor):
            clear_flow_state(context)
            await message.reply_text("Доступно только руководителю сервиса.")
            return ConversationHandler.END
        limits = {
            "payment_bank": 160,
            "payment_recipient": 160,
            "payment_phone": 80,
        }
        if len(text) > limits[action]:
            await message.reply_text("Значение слишком длинное.")
            return ADMINISTRATION_INPUT
        settings = await change_payment_setting(
            actor=actor,
            key=cast(PaymentSetting, action),
            value=" ".join(text.split()),
        )
        clear_flow_state(context)
        await message.reply_text(
            service_settings_text(settings, actor),
            parse_mode=ParseMode.HTML,
            reply_markup=service_settings_markup(actor),
        )
        return ConversationHandler.END

    if action in {"period_current", "period_next"}:
        if not is_owner_meta(actor):
            clear_flow_state(context)
            await message.reply_text("Доступно только руководителю сервиса.")
            return ConversationHandler.END
        target = parse_input_datetime(text)
        if target is None or target <= datetime.now(TZ):
            await message.reply_text("Укажите будущую дату в формате ДД.ММ.ГГГГ ЧЧ:ММ.")
            return ADMINISTRATION_INPUT
        set_pending_change(
            context,
            {"kind": action, "target_end_at": target.isoformat()},
        )
        await message.reply_text(
            f"Новое значение: <code>{html_escape(datetime_text(target.isoformat()))}</code>\n\nПодтвердите изменение.",
            parse_mode=ParseMode.HTML,
            reply_markup=confirmation_markup(),
        )
        return ADMINISTRATION_CONFIRM

    clear_flow_state(context)
    await message.reply_text("Сценарий ввода устарел. Начните действие заново.")
    return ConversationHandler.END


@require_admin
async def administration_confirm_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    query = update.callback_query
    actor = actor_meta(update)
    if not query or not actor:
        return ConversationHandler.END
    await query.answer()
    pending = pending_change(context)
    if pending is None:
        await query.edit_message_text("Подтверждение устарело. Начните действие заново.")
        return ConversationHandler.END
    kind = str(pending.get("kind") or "")

    if kind == "help":
        if not can_edit_help_meta(actor):
            await query.edit_message_text("Недостаточно прав.")
            clear_flow_state(context)
            return ConversationHandler.END
        value = str(pending.get("value") or "").strip()
        if not value or len(value) > 3500:
            await query.edit_message_text("Текст инструкции потерян или превышает лимит.")
            clear_flow_state(context)
            return ConversationHandler.END
        settings = await save_help_text(
            actor=actor,
            value=value,
            changed_at=datetime.now(TZ),
        )
    elif kind in {"period_current", "period_next"}:
        target = parse_datetime(pending.get("target_end_at"))
        if not is_owner_meta(actor) or target is None or target <= datetime.now(TZ):
            await query.edit_message_text("Изменение больше недоступно или дата устарела.")
            clear_flow_state(context)
            return ConversationHandler.END
        outcome, settings = await save_billing_period(
            actor=actor,
            kind=cast(PeriodKind, kind),
            target=target,
        )
        if outcome == "order":
            await query.edit_message_text("Следующий период должен заканчиваться позже текущего.")
            clear_flow_state(context)
            return ConversationHandler.END
        if outcome == "missing_current":
            await query.edit_message_text("Сначала укажите дату окончания текущего периода.")
            clear_flow_state(context)
            return ConversationHandler.END
    else:
        await query.edit_message_text("Неизвестное подтверждение.")
        clear_flow_state(context)
        return ConversationHandler.END

    clear_flow_state(context)
    await query.edit_message_text(
        service_settings_text(settings, actor),
        parse_mode=ParseMode.HTML,
        reply_markup=service_settings_markup(actor),
    )
    return ConversationHandler.END


async def administration_cancel(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    clear_flow_state(context)
    query = update.callback_query
    actor = actor_meta(update)
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
    "administration_cancel",
    "administration_confirm_cb",
    "administration_input_start_cb",
    "administration_text_input",
]
