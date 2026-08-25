"""User and staff ticket reply handlers."""

from __future__ import annotations

from typing import Any

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ..bot.guards import display_name, get_user_id, require_admin, require_auth, staff_signature
from ..config import logger
from ..storage import ImportantData, get_ticket_copy
from .history import _append_ticket_message, _now_iso
from .notifications import _queue_admin_full_notifications, _queue_user_notification
from .operations import (
    TicketFlowError,
    _safe_int,
    _ticket_can_user_reply,
    _ticket_is_assignee,
    _ticket_update,
)
from .routes import MAX_TICKET_TEXT_LEN, TICKET_ADMIN_REPLY_TEXT, TICKET_USER_REPLY_TEXT
from .views import (
    SEP,
    _format_ticket_for_admin,
    _format_ticket_for_user,
    _ticket_admin_kb,
    _ticket_user_kb,
    ticket_input_kb,
)
from .workflow import (
    _clear_ticket_ctx,
    _extract_message_payload,
    _parse_ticket_callback_id,
    ticket_context_data,
)


@require_admin
async def ticket_admin_reply_start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    ticket_id = _parse_ticket_callback_id(query.data, "adminreply")
    admin_id = get_user_id(update)
    ticket = get_ticket_copy(ticket_id) if ticket_id else None
    if not ticket or admin_id is None:
        logger.warning(
            "Ticket admin reply start failed ticket_id=%s admin_id=%s reason=ticket_not_found",
            ticket_id,
            admin_id,
        )
        await query.answer("Тикет не найден.", show_alert=True)
        return ConversationHandler.END
    if str(ticket.get("status", "open")) == "closed":
        logger.warning(
            "Ticket admin reply start denied ticket_id=%s admin_id=%s reason=ticket_closed",
            ticket_id,
            admin_id,
        )
        await query.answer("Тикет уже закрыт.", show_alert=True)
        return ConversationHandler.END
    if not _ticket_is_assignee(ticket, admin_id):
        logger.warning(
            "Ticket admin reply denied ticket_id=%s admin_id=%s reason=not_assignee",
            ticket_id,
            admin_id,
        )
        await query.answer(
            "Ответить может только исполнитель тикета.",
            show_alert=True,
        )
        return ConversationHandler.END
    await query.answer()
    data = ticket_context_data(context)
    data["ticket_reply_ticket_id"] = ticket_id
    data["ticket_reply_role"] = "admin"
    await query.edit_message_text(
        f"🎫 <b>Тикет #{ticket_id} — ответ</b>\n{SEP}\nВведите ответ пользователю:",
        parse_mode=ParseMode.HTML,
        reply_markup=ticket_input_kb(),
    )
    return TICKET_ADMIN_REPLY_TEXT


@require_auth
async def ticket_user_reply_start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    ticket_id = _parse_ticket_callback_id(query.data, "userreply")
    uid = get_user_id(update)
    ticket = get_ticket_copy(ticket_id) if ticket_id else None
    if not ticket or uid is None:
        logger.warning(
            "Ticket user reply start failed ticket_id=%s user_id=%s reason=ticket_not_found",
            ticket_id,
            uid,
        )
        await query.answer("Тикет не найден.", show_alert=True)
        return ConversationHandler.END
    if not _ticket_can_user_reply(ticket, uid):
        logger.warning(
            "Ticket user reply denied ticket_id=%s user_id=%s reason=reply_not_allowed assignee_id=%s status=%s",
            ticket_id,
            uid,
            ticket.get("assignee_id"),
            ticket.get("status"),
        )
        await query.answer(
            "Сейчас ответить на этот тикет нельзя.",
            show_alert=True,
        )
        return ConversationHandler.END
    await query.answer()
    data = ticket_context_data(context)
    data["ticket_reply_ticket_id"] = ticket_id
    data["ticket_reply_role"] = "user"
    await query.edit_message_text(
        f"🎫 <b>Мой тикет #{ticket_id} — ответ</b>\n{SEP}\nВведите ответ администратору:",
        parse_mode=ParseMode.HTML,
        reply_markup=ticket_input_kb(),
    )
    return TICKET_USER_REPLY_TEXT


@require_admin
async def ticket_admin_reply_text(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    message = update.effective_message
    text, attachment = _extract_message_payload(message)
    ticket_id = _safe_int(ticket_context_data(context).get("ticket_reply_ticket_id"))
    admin_id = get_user_id(update)
    if not ticket_id or admin_id is None:
        return ConversationHandler.END
    if not attachment and not text:
        if message:
            await message.reply_text(
                "Пустой ответ. Введите текст или приложите фото/файл.",
            )
        return TICKET_ADMIN_REPLY_TEXT
    if len(text) > MAX_TICKET_TEXT_LEN:
        if message:
            await message.reply_text(
                f"Ответ слишком длинный. Максимум {MAX_TICKET_TEXT_LEN} символов.",
            )
        return TICKET_ADMIN_REPLY_TEXT
    if attachment and not text:
        text = "(вложение)"

    admin_name = staff_signature(update)

    def reply(ticket: dict[str, Any]) -> dict[str, Any]:
        if str(ticket.get("status", "open")) == "closed":
            raise TicketFlowError("ticket_closed")
        if not _ticket_is_assignee(ticket, admin_id):
            raise TicketFlowError("not_assignee")
        updated = _append_ticket_message(
            ticket,
            sender_role="admin",
            sender_id=admin_id,
            sender_name=admin_name,
            text=text,
            kind="reply",
            attachment=attachment,
        )
        updated["status"] = "in_progress"
        updated["user_reply_allowed"] = True
        return dict(updated)

    def queue_reply(config: ImportantData, updated: dict[str, Any]) -> None:
        _queue_user_notification(
            config,
            updated,
            event_line="💬 <b>Администратор ответил на ваш тикет</b>",
            kind="ticket_admin_reply",
        )

    try:
        ticket = await _ticket_update(ticket_id, reply, queue_reply)
    except TicketFlowError as error:
        logger.warning(
            "Ticket admin reply failed ticket_id=%s admin_id=%s reason=%s",
            ticket_id,
            admin_id,
            error,
        )
        if message:
            await message.reply_text(
                "Не удалось отправить ответ: тикет закрыт или закреплён за другим администратором.",
            )
        _clear_ticket_ctx(context)
        return ConversationHandler.END

    logger.info(
        "Ticket admin reply ticket_id=%s admin_id=%s user_id=%s text_len=%s",
        ticket_id,
        admin_id,
        ticket.get("user_id"),
        len(text),
    )
    if message:
        await message.reply_text(
            _format_ticket_for_admin(
                ticket,
                admin_id,
                event_line="✅ <b>Ответ отправлен пользователю</b>",
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=_ticket_admin_kb(ticket, admin_id),
        )
    _clear_ticket_ctx(context)
    return ConversationHandler.END


@require_auth
async def ticket_user_reply_text(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    message = update.effective_message
    text, attachment = _extract_message_payload(message)
    ticket_id = _safe_int(ticket_context_data(context).get("ticket_reply_ticket_id"))
    uid = get_user_id(update)
    if not ticket_id or uid is None:
        return ConversationHandler.END
    if not attachment and not text:
        if message:
            await message.reply_text(
                "Пустой ответ. Введите текст или приложите фото/файл.",
            )
        return TICKET_USER_REPLY_TEXT
    if len(text) > MAX_TICKET_TEXT_LEN:
        if message:
            await message.reply_text(
                f"Ответ слишком длинный. Максимум {MAX_TICKET_TEXT_LEN} символов.",
            )
        return TICKET_USER_REPLY_TEXT
    if attachment and not text:
        text = "(вложение)"

    user_name = display_name(update)

    def reply(ticket: dict[str, Any]) -> dict[str, Any]:
        if not _ticket_can_user_reply(ticket, uid):
            raise TicketFlowError("user_reply_not_allowed")
        updated = _append_ticket_message(
            ticket,
            sender_role="user",
            sender_id=uid,
            sender_name=user_name,
            text=text,
            kind="reply",
            attachment=attachment,
        )
        updated["updated_at"] = _now_iso()
        return dict(updated)

    def queue_reply(config: ImportantData, updated: dict[str, Any]) -> None:
        assignee_id = _safe_int(updated.get("assignee_id"))
        if assignee_id:
            _queue_admin_full_notifications(
                config,
                updated,
                [assignee_id],
                event_line="💬 <b>Пользователь ответил по тикету</b>",
                kind="ticket_user_reply",
            )

    try:
        ticket = await _ticket_update(ticket_id, reply, queue_reply)
    except TicketFlowError as error:
        logger.warning(
            "Ticket user reply failed ticket_id=%s user_id=%s reason=%s",
            ticket_id,
            uid,
            error,
        )
        if message:
            await message.reply_text("Сейчас ответить на этот тикет нельзя.")
        _clear_ticket_ctx(context)
        return ConversationHandler.END

    logger.info(
        "Ticket user reply ticket_id=%s user_id=%s assignee_id=%s text_len=%s",
        ticket_id,
        uid,
        ticket.get("assignee_id"),
        len(text),
    )
    if message:
        await message.reply_text(
            _format_ticket_for_user(
                ticket,
                event_line="✅ <b>Ваш ответ отправлен администратору</b>",
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=_ticket_user_kb(ticket, uid),
        )
    _clear_ticket_ctx(context)
    return ConversationHandler.END


__all__ = [
    "ticket_admin_reply_start",
    "ticket_admin_reply_text",
    "ticket_user_reply_start",
    "ticket_user_reply_text",
]
