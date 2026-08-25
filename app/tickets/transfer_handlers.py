"""Staff ticket-transfer selection and atomic reassignment."""

from __future__ import annotations

from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, ConversationHandler

from ..bot.guards import authorized_ids, get_user_id, require_admin, staff_signature
from ..bot.ui import SEP, html_escape, safe_edit_or_reply, ui_ok_text
from ..config import logger
from ..storage import ImportantData, get_admin_name_by_id, get_ticket_copy, get_user_meta_copy
from ..users.staff import staff_internal_identity
from .history import _append_ticket_message, _now_iso
from .notifications import (
    MAX_TRANSFER_ATTACHMENTS,
    _queue_admin_full_notifications,
    _queue_ticket_text,
    _queue_user_notification,
)
from .operations import TicketFlowError, _ticket_is_assignee, _ticket_update


@require_admin
async def ticket_transfer_init_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    del context
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    admin_id = get_user_id(update)
    if admin_id is None:
        await query.answer()
        return ConversationHandler.END

    parts = (query.data or "").split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        await query.answer()
        return ConversationHandler.END
    ticket_id = int(parts[2])

    ticket = get_ticket_copy(ticket_id)
    if not ticket:
        await query.answer("Тикет не найден.", show_alert=True)
        return ConversationHandler.END
    if str(ticket.get("status", "open")) == "closed":
        await query.answer("Тикет уже закрыт.", show_alert=True)
        return ConversationHandler.END
    if not _ticket_is_assignee(ticket, admin_id):
        await query.answer("Передать можно только свой тикет.", show_alert=True)
        return ConversationHandler.END

    other_admins = [uid for uid in authorized_ids(role_filter="admin") if uid != admin_id]
    if not other_admins:
        await query.answer("Нет других администраторов.", show_alert=True)
        return ConversationHandler.END

    await query.answer()
    rows: list[list[InlineKeyboardButton]] = []
    for other_admin_id in other_admins:
        admin_meta = get_user_meta_copy(other_admin_id)
        name = (
            staff_internal_identity(admin_meta)
            if admin_meta
            else (get_admin_name_by_id(other_admin_id) or str(other_admin_id))
        )
        rows.append(
            [
                InlineKeyboardButton(
                    f"👤 {name}"[:60],
                    callback_data=f"ticket:transfer_to:{ticket_id}:{other_admin_id}",
                ),
            ],
        )
    rows.append(
        [
            InlineKeyboardButton(
                "⬅️ Назад",
                callback_data=f"ticket:open:{ticket_id}",
            ),
        ],
    )
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])

    await safe_edit_or_reply(
        update.effective_message,
        f"🔄 <b>Тикет #{ticket_id} — передача</b>\n{SEP}\nВыберите администратора для передачи тикета:",
        reply_markup=InlineKeyboardMarkup(rows),
    )
    return ConversationHandler.END


@require_admin
async def ticket_transfer_to_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    del context
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    admin_id = get_user_id(update)
    if admin_id is None:
        await query.answer()
        return ConversationHandler.END

    parts = (query.data or "").split(":")
    if len(parts) != 4 or not parts[2].isdigit() or not parts[3].isdigit():
        await query.answer()
        return ConversationHandler.END
    ticket_id = int(parts[2])
    new_admin_id = int(parts[3])
    if not ticket_id or not new_admin_id or new_admin_id == admin_id:
        await query.answer()
        return ConversationHandler.END

    active_admin_ids = authorized_ids(role_filter="admin")
    new_admin_name = get_admin_name_by_id(new_admin_id)
    if not new_admin_name or new_admin_id not in active_admin_ids:
        await query.answer("Администратор не найден.", show_alert=True)
        return ConversationHandler.END
    admin_name = staff_signature(update)

    def transfer(ticket: dict[str, Any]) -> dict[str, Any]:
        if str(ticket.get("status", "open")) == "closed":
            raise TicketFlowError("ticket_closed")
        if not _ticket_is_assignee(ticket, admin_id):
            raise TicketFlowError("not_assignee")
        if get_admin_name_by_id(new_admin_id) is None:
            raise TicketFlowError("target_inactive")
        updated = dict(ticket)
        updated["assignee_id"] = new_admin_id
        updated["assignee_name"] = new_admin_name
        updated["assignee_signature_version"] = 1
        updated["updated_at"] = _now_iso()
        return dict(
            _append_ticket_message(
                updated,
                sender_role="admin",
                sender_id=admin_id,
                sender_name=admin_name,
                text=f"Тикет передан администратору {new_admin_name}",
                kind="transfer",
            ),
        )

    def queue_transfer(config: ImportantData, updated: dict[str, Any]) -> None:
        _queue_admin_full_notifications(
            config,
            updated,
            [new_admin_id],
            event_line="🔄 <b>Тикет передан вам</b>",
            kind="ticket_transferred_assignee",
            attachment_limit=MAX_TRANSFER_ATTACHMENTS,
        )
        _queue_user_notification(
            config,
            updated,
            event_line=f"🔄 <b>Новый исполнитель:</b> {html_escape(new_admin_name)}",
            kind="ticket_transferred_user",
            include_attachment=False,
        )
        for other_id in [uid for uid in active_admin_ids if uid not in {admin_id, new_admin_id}]:
            _queue_ticket_text(
                config,
                uid=other_id,
                text=f"🔄 Тикет #{ticket_id} передан: {html_escape(new_admin_name)}",
                markup=None,
                kind="ticket_transferred_admin",
            )

    try:
        await _ticket_update(ticket_id, transfer, queue_transfer)
    except TicketFlowError as error:
        code = str(error)
        logger.warning(
            "Ticket transfer failed ticket_id=%s admin_id=%s reason=%s",
            ticket_id,
            admin_id,
            code,
        )
        if code == "ticket_closed":
            await query.answer("Тикет уже закрыт.", show_alert=True)
        elif code == "ticket_not_found":
            await query.answer("Тикет не найден.", show_alert=True)
        elif code == "target_inactive":
            await query.answer(
                "Новый исполнитель уже вышел из бота.",
                show_alert=True,
            )
        else:
            await query.answer(
                "Передать можно только свой тикет.",
                show_alert=True,
            )
        return ConversationHandler.END

    await query.answer()
    logger.info(
        "Ticket transferred ticket_id=%s from_admin=%s to_admin=%s",
        ticket_id,
        admin_id,
        new_admin_id,
    )
    await safe_edit_or_reply(
        update.effective_message,
        ui_ok_text(
            f"Тикет #{ticket_id} передан: {html_escape(new_admin_name)}",
        ),
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "⬅️ К панели",
                        callback_data="ticket:list",
                    ),
                ],
            ],
        ),
    )
    return ConversationHandler.END


__all__ = ["ticket_transfer_init_cb", "ticket_transfer_to_cb"]
