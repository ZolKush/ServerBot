"""Staff assignment and closure actions for tickets."""

from __future__ import annotations

from typing import Any

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..bot.guards import authorized_ids, get_user_id, require_admin, staff_signature
from ..bot.ui import html_escape
from ..config import logger
from ..storage import ImportantData
from .history import _append_ticket_message, _now_iso
from .notifications import _queue_ticket_text, _queue_user_notification
from .operations import TicketFlowError, _safe_int, _ticket_is_assignee, _ticket_update
from .views import _format_ticket_for_admin, _ticket_admin_kb
from .workflow import _parse_ticket_callback_id


@require_admin
async def ticket_take_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    del context
    query = update.callback_query
    if not query:
        return
    ticket_id = _parse_ticket_callback_id(query.data, "take")
    admin_id = get_user_id(update)
    if not ticket_id or admin_id is None:
        await query.answer()
        return
    admin_name = staff_signature(update)
    active_admins = authorized_ids(role_filter="admin")

    def assign(ticket: dict[str, Any]) -> dict[str, Any]:
        if str(ticket.get("status", "open")) == "closed":
            raise TicketFlowError("ticket_closed")
        assignee_id = _safe_int(ticket.get("assignee_id"))
        if assignee_id and assignee_id != admin_id and assignee_id in active_admins:
            raise TicketFlowError("ticket_taken")
        if assignee_id == admin_id:
            raise TicketFlowError("already_assigned")
        updated = dict(ticket)
        was_orphaned = bool(assignee_id and assignee_id not in active_admins)
        updated["assignee_id"] = admin_id
        updated["assignee_name"] = admin_name
        updated["assignee_signature_version"] = 1
        updated["status"] = "in_progress"
        updated["updated_at"] = _now_iso()
        if was_orphaned:
            updated = dict(
                _append_ticket_message(
                    updated,
                    sender_role="admin",
                    sender_id=admin_id,
                    sender_name=admin_name,
                    text="Тикет переназначен после выхода предыдущего исполнителя",
                    kind="reclaim",
                ),
            )
        return updated

    def queue_assignment(config: ImportantData, updated: dict[str, Any]) -> None:
        for other_id in [uid for uid in active_admins if uid != admin_id]:
            _queue_ticket_text(
                config,
                uid=other_id,
                text=f"🫳 Тикет #{ticket_id} взят в работу: {html_escape(admin_name)}",
                markup=None,
                kind="ticket_assigned_admin",
            )
        _queue_user_notification(
            config,
            updated,
            event_line=f"👤 <b>Обращение принял:</b> {html_escape(admin_name)}",
            kind="ticket_assigned_user",
            include_attachment=False,
        )

    try:
        ticket = await _ticket_update(ticket_id, assign, queue_assignment)
    except TicketFlowError as error:
        code = str(error)
        logger.warning(
            "Ticket take denied ticket_id=%s admin_id=%s reason=%s",
            ticket_id,
            admin_id,
            code,
        )
        if code == "ticket_taken":
            await query.answer(
                "Тикет уже взят другим администратором.",
                show_alert=True,
            )
        elif code == "already_assigned":
            await query.answer("Этот тикет уже назначен вам.", show_alert=True)
        elif code == "ticket_not_found":
            await query.answer("Тикет не найден.", show_alert=True)
        else:
            await query.answer("Тикет уже закрыт.", show_alert=True)
        return

    await query.answer()
    logger.info(
        "Ticket assigned ticket_id=%s admin_id=%s user_id=%s",
        ticket_id,
        admin_id,
        ticket.get("user_id"),
    )
    await query.edit_message_text(
        _format_ticket_for_admin(
            ticket,
            admin_id,
            event_line="🫳 <b>Вы взяли тикет в работу</b>",
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=_ticket_admin_kb(ticket, admin_id),
    )


@require_admin
async def ticket_close_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    del context
    query = update.callback_query
    if not query:
        return
    ticket_id = _parse_ticket_callback_id(query.data, "close")
    admin_id = get_user_id(update)
    if not ticket_id or admin_id is None:
        await query.answer()
        return
    admin_name = staff_signature(update)
    other_admin_ids = [uid for uid in authorized_ids(role_filter="admin") if uid != admin_id]

    def close(ticket: dict[str, Any]) -> dict[str, Any]:
        if str(ticket.get("status", "open")) == "closed":
            raise TicketFlowError("ticket_closed")
        if not _ticket_is_assignee(ticket, admin_id):
            raise TicketFlowError("not_assignee")
        updated = dict(ticket)
        updated["status"] = "closed"
        updated["closed_at"] = _now_iso()
        updated["closed_by_id"] = admin_id
        updated["closed_by_name"] = admin_name
        updated["closed_by_signature_version"] = 1
        updated["user_reply_allowed"] = False
        updated["updated_at"] = _now_iso()
        return updated

    def queue_close(config: ImportantData, updated: dict[str, Any]) -> None:
        _queue_user_notification(
            config,
            updated,
            event_line=f"✅ <b>Тикет закрыт:</b> {html_escape(admin_name)}",
            kind="ticket_closed_user",
            include_attachment=False,
        )
        for other_id in other_admin_ids:
            _queue_ticket_text(
                config,
                uid=other_id,
                text=f"✅ Тикет #{ticket_id} закрыт: {html_escape(admin_name)}",
                markup=None,
                kind="ticket_closed_admin",
            )

    try:
        ticket = await _ticket_update(ticket_id, close, queue_close)
    except TicketFlowError as error:
        logger.warning(
            "Ticket close failed ticket_id=%s admin_id=%s reason=%s",
            ticket_id,
            admin_id,
            error,
        )
        if str(error) == "ticket_closed":
            await query.answer("Тикет уже закрыт.", show_alert=True)
        elif str(error) == "ticket_not_found":
            await query.answer("Тикет не найден.", show_alert=True)
        else:
            await query.answer(
                "Закрыть тикет может только его исполнитель.",
                show_alert=True,
            )
        return

    await query.answer()
    logger.info(
        "Ticket closed ticket_id=%s admin_id=%s user_id=%s",
        ticket_id,
        admin_id,
        ticket.get("user_id"),
    )
    await query.edit_message_text(
        _format_ticket_for_admin(
            ticket,
            admin_id,
            event_line="✅ <b>Вы закрыли тикет</b>",
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=_ticket_admin_kb(ticket, admin_id),
    )


__all__ = ["ticket_close_cb", "ticket_take_cb"]
