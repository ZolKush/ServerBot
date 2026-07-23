"""Entry and immediate-announcement steps of the maintenance conversation."""

from __future__ import annotations

import re
from datetime import datetime

from telegram import Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import ContextTypes, ConversationHandler

from ..bot.guards import get_user_id, require_maintenance, staff_title
from ..bot.ui import html_escape
from ..config import TZ, logger
from ..storage import get_active_maintenance, get_scheduled_maintenance
from .calendar import maint_mode_kb, parse_hhmm, schedule_calendar_kb, scope_kb, urgency_kb
from .notifications import make_maintenance_notice_event
from .operations import start_maintenance
from .policy import MAINT_SCOPE_ALL, hhmm_to_minutes, normalize_scope, scope_label
from .records import build_maintenance_record
from .state import (
    STATE_MAINT_DURATION,
    STATE_MAINT_MODE,
    STATE_MAINT_SCHEDULE_DATE,
    STATE_MAINT_SCOPE,
    STATE_MAINT_URGENCY,
    clear_maintenance_context,
    maintenance_context,
)
from .views import (
    format_maintenance,
    maintenance_control_keyboard,
    maintenance_delivery_status,
    maintenance_menu_text,
    maintenance_panel_text,
    scheduled_control_keyboard,
    scheduled_panel_text,
)


@require_maintenance
async def maint_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    active = get_active_maintenance()
    if active and str(active.get("id") or ""):
        clear_maintenance_context(context)
        message = update.effective_message
        if query and message:
            await query.answer()
            await query.edit_message_text(
                maintenance_panel_text(active),
                parse_mode=ParseMode.HTML,
                reply_markup=maintenance_control_keyboard(str(active["id"])),
            )
        elif message:
            await message.reply_text(
                maintenance_panel_text(active),
                parse_mode=ParseMode.HTML,
                reply_markup=maintenance_control_keyboard(str(active["id"])),
            )
        return ConversationHandler.END

    scheduled = get_scheduled_maintenance()
    message = update.effective_message
    if scheduled and str(scheduled.get("id") or ""):
        text = scheduled_panel_text(scheduled)
        keyboard = scheduled_control_keyboard(str(scheduled["id"]))
        if query and message:
            await query.answer()
            await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        elif message:
            await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        return STATE_MAINT_MODE
    if query and message:
        await query.answer()
        await query.edit_message_text(
            maintenance_menu_text(scheduled),
            parse_mode=ParseMode.HTML,
            reply_markup=maint_mode_kb(),
        )
    elif message:
        await message.reply_text(
            maintenance_menu_text(scheduled),
            parse_mode=ParseMode.HTML,
            reply_markup=maint_mode_kb(),
        )
    return STATE_MAINT_MODE


@require_maintenance
async def maint_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()
    match = re.fullmatch(r"maint:mode:(announce|schedule)", query.data or "")
    if not match:
        return ConversationHandler.END
    data = maintenance_context(context)
    data["maint_mode"] = match.group(1)
    current_schedule = get_scheduled_maintenance() or {}
    data["maint_base_scheduled_id"] = str(current_schedule.get("id") or "")
    await query.edit_message_text("Выберите область техработ:", reply_markup=scope_kb())
    return STATE_MAINT_SCOPE


@require_maintenance
async def maint_scope(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()
    match = re.fullmatch(r"maint:scope:([a-z0-9_-]{1,12})", query.data or "")
    if not match:
        return ConversationHandler.END
    data = maintenance_context(context)
    scope = normalize_scope(match.group(1))
    data["maint_scope"] = scope
    if query.message and query.message.chat:
        data["maint_panel_chat_id"] = query.message.chat.id
        data["maint_panel_msg_id"] = query.message.message_id
    if str(data.get("maint_mode", "announce")) == "schedule":
        today = datetime.now(TZ).date()
        await query.edit_message_text(
            f"Область: {html_escape(scope_label(scope))}\n\nВыберите дату техработ:",
            parse_mode=ParseMode.HTML,
            reply_markup=schedule_calendar_kb(today.year, today.month, today=today),
        )
        return STATE_MAINT_SCHEDULE_DATE
    await query.edit_message_text(
        f"Область: {html_escape(scope_label(scope))}\n\nВыберите тип работ:",
        parse_mode=ParseMode.HTML,
        reply_markup=urgency_kb(),
    )
    return STATE_MAINT_URGENCY


@require_maintenance
async def maint_urgency(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()
    if query.data not in ("maint:urgency:urgent", "maint:urgency:planned"):
        return ConversationHandler.END
    maintenance_context(context)["maint_urgency"] = "urgent" if query.data.endswith("urgent") else "planned"
    await query.edit_message_text("Введите ожидаемое время простоя в формате ЧЧ:ММ (например, 1:35):")
    return STATE_MAINT_DURATION


@require_maintenance
async def maint_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    parsed = parse_hhmm((message.text if message else "") or "")
    if not parsed:
        if message:
            await message.reply_text("Некорректно. Введите ЧЧ:ММ, например 0:45 или 2:00:")
        return STATE_MAINT_DURATION

    hours, minutes = parsed
    data = maintenance_context(context)
    scope = normalize_scope(str(data.get("maint_scope", MAINT_SCOPE_ALL)))
    urgency = str(data.get("maint_urgency", "planned"))
    author = staff_title(update)
    author_id = get_user_id(update)
    maintenance = build_maintenance_record(scope, urgency, hours, minutes, author_id, author)
    maintenance_id = str(maintenance.get("id") or "")
    event, users_count, admins_count = make_maintenance_notice_event(
        author_id=author_id,
        text=format_maintenance(scope, urgency, hours, minutes, author),
        kind="maintenance_started",
    )
    expected_schedule_id = str(data.get("maint_base_scheduled_id") or "")

    if not await start_maintenance(
        maintenance,
        expected_schedule_id=expected_schedule_id,
        notice_event=event,
    ):
        if message:
            await message.reply_text("Другой администратор уже запустил техработы. Откройте /maint заново.")
        clear_maintenance_context(context)
        return ConversationHandler.END
    logger.info(
        "Maintenance started by user_id=%s scope=%s urgency=%s duration_min=%s",
        author_id,
        scope,
        urgency,
        hhmm_to_minutes(hours, minutes),
    )

    panel_text = f"{maintenance_panel_text(maintenance)}\n\n{maintenance_delivery_status(users_count, admins_count)}"
    panel_chat_id = data.get("maint_panel_chat_id")
    panel_message_id = data.get("maint_panel_msg_id")
    if panel_chat_id and panel_message_id:
        try:
            await context.bot.edit_message_text(
                chat_id=panel_chat_id,
                message_id=panel_message_id,
                text=panel_text,
                parse_mode=ParseMode.HTML,
                reply_markup=maintenance_control_keyboard(maintenance_id),
            )
            clear_maintenance_context(context)
            return ConversationHandler.END
        except BadRequest as exc:
            if "message is not modified" in str(exc).lower():
                clear_maintenance_context(context)
                return ConversationHandler.END
            logger.warning("Не удалось обновить панель техработ (%s), отправляю новое сообщение", exc)
        except Exception as exc:
            logger.warning("Не удалось обновить панель техработ (%s), отправляю новое сообщение", exc)

    if message:
        await message.reply_text(
            panel_text,
            parse_mode=ParseMode.HTML,
            reply_markup=maintenance_control_keyboard(maintenance_id),
        )
    clear_maintenance_context(context)
    return ConversationHandler.END


__all__ = ["maint_duration", "maint_mode", "maint_scope", "maint_start", "maint_urgency"]
