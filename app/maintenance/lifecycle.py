"""Handlers for extending and ending active maintenance."""

from __future__ import annotations

import re
from datetime import datetime

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ..bot.guards import get_user_id, require_admin, staff_title
from ..config import TZ, logger
from ..storage import get_active_maintenance
from .calendar import parse_hhmm
from .operations import end_maintenance, extend_maintenance
from .policy import hhmm_to_minutes
from .state import STATE_MAINT_EXTEND, clear_maintenance_context, maintenance_context
from .views import (
    maintenance_control_keyboard,
    maintenance_delivery_status,
    maintenance_end_confirm_keyboard,
    maintenance_panel_text,
)


@require_admin
async def maint_extend_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()
    match = re.fullmatch(r"maint:extend:([0-9a-f]+)", query.data or "")
    if not match:
        clear_maintenance_context(context)
        return ConversationHandler.END
    maintenance_id = match.group(1)
    maintenance = get_active_maintenance()
    if not maintenance or str(maintenance.get("id")) != maintenance_id:
        await query.edit_message_text("Техработы не активны или уже завершены.")
        clear_maintenance_context(context)
        return ConversationHandler.END
    maintenance_context(context)["maint_extend_id"] = maintenance_id
    await query.edit_message_text(
        "⏳ Продление техработ.\nВведите новое время простоя в формате ЧЧ:ММ (например, 1:35):"
    )
    return STATE_MAINT_EXTEND


@require_admin
async def maint_extend_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    parsed = parse_hhmm((message.text if message else "") or "")
    if not parsed:
        if message:
            await message.reply_text("Некорректно. Введите ЧЧ:ММ, например 0:45 или 2:00:")
        return STATE_MAINT_EXTEND

    data = maintenance_context(context)
    maintenance_id = data.get("maint_extend_id")
    maintenance = get_active_maintenance()
    if not maintenance or str(maintenance.get("id")) != str(maintenance_id):
        if message:
            await message.reply_text("Техработы не активны или уже завершены.")
        clear_maintenance_context(context)
        return ConversationHandler.END

    hours, minutes = parsed
    duration_min = hhmm_to_minutes(hours, minutes)
    try:
        maintenance, users_count, admins_count = await extend_maintenance(
            maintenance_id,
            duration_min=duration_min,
            hours=hours,
            minutes=minutes,
            author=staff_title(update),
            author_id=get_user_id(update),
        )
    except RuntimeError:
        if message:
            await message.reply_text("Техработы не активны или уже завершены.")
        clear_maintenance_context(context)
        return ConversationHandler.END

    logger.info(
        "Maintenance extended by user_id=%s duration_min=%s maint_id=%s",
        get_user_id(update),
        duration_min,
        maintenance_id,
    )
    panel_text = f"{maintenance_panel_text(maintenance)}\n\n{maintenance_delivery_status(users_count, admins_count)}"
    data.pop("maint_extend_id", None)
    if message:
        await message.reply_text(
            panel_text,
            parse_mode=ParseMode.HTML,
            reply_markup=maintenance_control_keyboard(str(maintenance_id)),
        )
    clear_maintenance_context(context)
    return ConversationHandler.END


@require_admin
async def maint_end_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    await query.answer()
    match = re.fullmatch(r"maint:end:([0-9a-f]+)", query.data or "")
    if not match:
        return
    maintenance_id = match.group(1)
    maintenance, users_count, admins_count = await end_maintenance(
        maintenance_id,
        author=staff_title(update),
        author_id=get_user_id(update),
        ended_at=datetime.now(TZ),
    )
    if not isinstance(maintenance, dict):
        await query.edit_message_text("Техработы не активны или уже завершены.")
        return

    logger.info("Maintenance ended by user_id=%s maint_id=%s", get_user_id(update), maintenance_id)
    await query.edit_message_text(
        "✅ Техработы завершены.\n\n" + maintenance_delivery_status(users_count, admins_count)
    )


@require_admin
async def maint_end_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    await query.answer()
    match = re.fullmatch(r"maint:endconfirm:([0-9a-f]+)", query.data or "")
    if not match:
        return
    maintenance_id = match.group(1)
    maintenance = get_active_maintenance()
    if not maintenance or str(maintenance.get("id")) != maintenance_id:
        await query.edit_message_text("Техработы не активны или уже завершены.")
        return
    text = maintenance_panel_text(maintenance) + "\n\n<b>Подтвердить завершение?</b>"
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=maintenance_end_confirm_keyboard(maintenance_id),
    )


@require_admin
async def maint_cancel_end_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    await query.answer()
    match = re.fullmatch(r"maint:cancelend:([0-9a-f]+)", query.data or "")
    if not match:
        return
    maintenance_id = match.group(1)
    maintenance = get_active_maintenance()
    if not maintenance or str(maintenance.get("id")) != maintenance_id:
        await query.edit_message_text("Техработы не активны или уже завершены.")
        return
    await query.edit_message_text(
        maintenance_panel_text(maintenance),
        parse_mode=ParseMode.HTML,
        reply_markup=maintenance_control_keyboard(maintenance_id),
    )


__all__ = [
    "maint_cancel_end_cb",
    "maint_end_cb",
    "maint_end_confirm_cb",
    "maint_extend_cb",
    "maint_extend_duration",
]
