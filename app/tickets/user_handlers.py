"""User-facing ticket creation conversation handlers."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ..bot.guards import authorized_ids, display_name, get_user_id, is_admin, require_auth
from ..bot.help import render_support_contact
from ..bot.menu import show_main_menu
from ..bot.ui import ui_error_text, ui_ok_text, ui_warn_text
from ..config import TZ, logger
from ..messaging.message_cleanup import record_navigation_result
from ..storage import ImportantData, get_user_open_tickets, product_settings_snapshot
from .dashboard_handlers import _show_ticket_dashboard
from .notifications import _queue_admin_full_notifications, _queue_user_notification
from .operations import TicketFlowError, create_ticket
from .routes import (
    MAX_TICKET_SUBJECT_LEN,
    MAX_TICKET_TEXT_LEN,
    TICKET_CONFIRM,
    TICKET_SUBJECT,
    TICKET_TEXT,
    TICKET_URGENCY,
)
from .views import (
    _format_ticket_for_user,
    _ticket_preview_text,
    _ticket_user_kb,
    ticket_confirm_kb,
    ticket_input_kb,
    ticket_urgency_kb,
)
from .workflow import _clear_ticket_ctx, _extract_message_payload, ticket_context_data


@require_auth
async def ticket_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    uid = get_user_id(update)
    if uid is None:
        return ConversationHandler.END

    if is_admin(update):
        if query:
            await query.answer()
        _clear_ticket_ctx(context)
        return await _show_ticket_dashboard(update, context)

    open_tickets = get_user_open_tickets(uid)
    if open_tickets:
        ticket = open_tickets[0]
        message = update.effective_message
        text = _format_ticket_for_user(
            ticket,
            event_line=ui_warn_text(
                "У вас уже есть открытый тикет. Новый можно создать после его закрытия.",
            ),
        )
        keyboard = _ticket_user_kb(ticket, uid)
        if query and message:
            await query.answer()
            result = await query.edit_message_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
        elif message:
            result = await message.reply_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
        else:
            return ConversationHandler.END
        await record_navigation_result(update, result)
        return ConversationHandler.END

    _clear_ticket_ctx(context)
    message = update.effective_message
    prompt = "<b>Тикет > Тема</b>\n\nВведите тему тикета (кратко).\nДля отмены: /cancel" + render_support_contact(
        product_settings_snapshot()
    )
    if query and message:
        await query.answer()
        result = await query.edit_message_text(
            prompt,
            parse_mode=ParseMode.HTML,
            reply_markup=ticket_input_kb(),
        )
    elif message:
        result = await message.reply_text(
            prompt,
            parse_mode=ParseMode.HTML,
            reply_markup=ticket_input_kb(),
        )
    else:
        return ConversationHandler.END
    await record_navigation_result(update, result)
    return TICKET_SUBJECT


@require_auth
async def ticket_subject(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    subject = str(message.text or "").strip() if message else ""
    if len(subject) < 3:
        if message:
            await message.reply_text("Тема слишком короткая. Введите минимум 3 символа.")
        return TICKET_SUBJECT
    if len(subject) > MAX_TICKET_SUBJECT_LEN:
        if message:
            await message.reply_text(
                f"Тема слишком длинная. Максимум {MAX_TICKET_SUBJECT_LEN} символов.",
            )
        return TICKET_SUBJECT

    data = ticket_context_data(context)
    data["ticket_subject"] = subject
    edit_field = data.pop("ticket_edit_field", None)
    if edit_field == "subject" and data.get("ticket_text"):
        if message:
            result = await message.reply_text(
                _ticket_preview_text(context),
                parse_mode=ParseMode.HTML,
                reply_markup=ticket_confirm_kb(),
            )
            await record_navigation_result(update, result)
        return TICKET_CONFIRM
    if message:
        result = await message.reply_text("Срочность:", reply_markup=ticket_urgency_kb())
        await record_navigation_result(update, result)
    return TICKET_URGENCY


@require_auth
async def ticket_urgency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()
    if query.data not in ("ticket:p1", "ticket:p2", "ticket:p3"):
        return ConversationHandler.END
    ticket_context_data(context)["ticket_urgency"] = query.data.split(":")[1]
    await query.edit_message_text(
        "<b>Тикет > Описание</b>\n\nОпишите проблему (лучше одним сообщением). Для отмены: /cancel",
        parse_mode=ParseMode.HTML,
        reply_markup=ticket_input_kb(),
    )
    return TICKET_TEXT


@require_auth
async def ticket_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    text, attachment = _extract_message_payload(message)
    if len(text) > MAX_TICKET_TEXT_LEN:
        if message:
            await message.reply_text(
                f"Описание слишком длинное. Максимум {MAX_TICKET_TEXT_LEN} символов.",
            )
        return TICKET_TEXT
    if not attachment and len(text) < 10:
        if message:
            await message.reply_text(
                "Описание слишком короткое. Дайте больше деталей (>= 10 символов) "
                "или приложите фото/файл с пояснением.",
            )
        return TICKET_TEXT
    if attachment and not text:
        text = "(вложение)"

    data = ticket_context_data(context)
    data["ticket_text"] = text
    if attachment:
        data["ticket_attachment"] = attachment
    else:
        data.pop("ticket_attachment", None)
    data.pop("ticket_edit_field", None)
    if message:
        result = await message.reply_text(
            _ticket_preview_text(context),
            parse_mode=ParseMode.HTML,
            reply_markup=ticket_confirm_kb(),
        )
        await record_navigation_result(update, result)
    return TICKET_CONFIRM


@require_auth
async def ticket_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()
    data = ticket_context_data(context)
    if query.data == "ticket:cancel":
        _clear_ticket_ctx(context)
        await show_main_menu(
            update,
            text=ui_warn_text("Создание тикета отменено.") + "\n\nВыберите раздел:",
        )
        return ConversationHandler.END
    if query.data == "ticket:edit_subj":
        data["ticket_edit_field"] = "subject"
        await query.edit_message_text(
            "<b>Тикет > Тема</b>\n\nВведите новую тему:",
            parse_mode=ParseMode.HTML,
            reply_markup=ticket_input_kb(),
        )
        return TICKET_SUBJECT
    if query.data == "ticket:edit_text":
        data["ticket_edit_field"] = "text"
        await query.edit_message_text(
            "<b>Тикет > Описание</b>\n\nВведите новое описание:",
            parse_mode=ParseMode.HTML,
            reply_markup=ticket_input_kb(),
        )
        return TICKET_TEXT
    if query.data != "ticket:send":
        return ConversationHandler.END

    in_progress = data.get("ticket_send_in_progress")
    if in_progress:
        try:
            started = datetime.fromisoformat(str(in_progress))
            if started.tzinfo is None:
                started = started.replace(tzinfo=TZ)
            elapsed = (datetime.now(TZ) - started.astimezone(TZ)).total_seconds()
            if 0 <= elapsed < 120:
                await query.edit_message_text(
                    ui_warn_text("тикет уже отправляется, подождите..."),
                )
                return ConversationHandler.END
        except (TypeError, ValueError):
            pass
        data.pop("ticket_send_in_progress", None)
    data["ticket_send_in_progress"] = datetime.now(TZ).isoformat()

    try:
        uid = get_user_id(update)
        author_name = display_name(update)
        author_username = getattr(update.effective_user, "username", None)
        subject = str(data.get("ticket_subject", "-"))
        urgency = str(data.get("ticket_urgency", "p3")).lower()
        text = str(data.get("ticket_text", "-"))
        admin_ids = authorized_ids(role_filter="admin")
        if uid is None:
            _clear_ticket_ctx(context)
            await query.edit_message_text(
                ui_error_text("не удалось определить пользователя."),
            )
            return ConversationHandler.END
        if not admin_ids:
            _clear_ticket_ctx(context)
            await query.edit_message_text(
                ui_error_text("нет авторизованных администраторов."),
            )
            return ConversationHandler.END

        raw_attachment = data.get("ticket_attachment")
        attachment = raw_attachment if isinstance(raw_attachment, dict) else None

        def queue_created(config: ImportantData, created: dict[str, Any]) -> None:
            _queue_admin_full_notifications(
                config,
                created,
                admin_ids,
                event_line="🆕 <b>Новый тикет ожидает исполнителя</b>",
                kind="ticket_created_admin",
            )
            _queue_user_notification(
                config,
                created,
                event_line="✅ <b>Тикет создан и отправлен администраторам</b>",
                kind="ticket_created_user",
                include_attachment=False,
            )

        try:
            ticket = await create_ticket(
                user_id=uid,
                user_name=author_name,
                user_username=author_username,
                subject=subject,
                urgency=urgency,
                text=text,
                attachment=attachment,
                outbox_builder=queue_created,
            )
        except TicketFlowError as error:
            _clear_ticket_ctx(context)
            warning = (
                "у вас уже есть незакрытый тикет."
                if str(error) == "open_ticket_exists"
                else "создание тикета отменено."
            )
            await query.edit_message_text(ui_warn_text(warning))
            return ConversationHandler.END

        ticket_id = int(ticket["id"])
        logger.info(
            "Ticket created ticket_id=%s user_id=%s urgency=%s subject=%s",
            ticket_id,
            uid,
            urgency,
            subject,
        )
        _clear_ticket_ctx(context)
        await query.edit_message_text(
            ui_ok_text(f"Тикет #{ticket_id} создан. Ожидайте ответа администратора."),
        )
        return ConversationHandler.END
    finally:
        data.pop("ticket_send_in_progress", None)


__all__ = [
    "ticket_confirm",
    "ticket_start",
    "ticket_subject",
    "ticket_text",
    "ticket_urgency",
]
