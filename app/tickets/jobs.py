"""Scheduled maintenance of ticket assignment state."""

from __future__ import annotations

from telegram.ext import ContextTypes

from ..bot.guards import authorized_ids
from ..config import logger
from ..storage import ImportantData, UpdateAborted, update_important_data
from .history import _append_ticket_message
from .notifications import _queue_admin_full_notifications, _queue_user_notification
from .operations import _safe_int


async def release_orphaned_tickets(
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    del context
    active_admin_ids = authorized_ids(role_filter="admin")
    active_set = set(active_admin_ids)

    def release(config: ImportantData) -> int:
        tickets = dict(config.tickets or {})
        released = 0
        for key, raw in list(tickets.items()):
            if not isinstance(raw, dict) or str(raw.get("status", "open")) == "closed":
                continue
            assignee_id = _safe_int(raw.get("assignee_id"))
            if not assignee_id or assignee_id in active_set:
                continue
            updated = dict(raw)
            updated["assignee_id"] = None
            updated["assignee_name"] = None
            updated["assignee_signature_version"] = None
            updated["status"] = "open"
            updated["user_reply_allowed"] = False
            updated = dict(
                _append_ticket_message(
                    updated,
                    sender_role="admin",
                    sender_id=None,
                    sender_name="Система",
                    text=("Предыдущий исполнитель недоступен; тикет возвращён в общую очередь"),
                    kind="assignee_released",
                ),
            )
            tickets[key] = updated
            _queue_user_notification(
                config,
                updated,
                event_line=("⚠️ <b>Исполнитель временно недоступен; тикет возвращён в очередь</b>"),
                kind="ticket_orphaned_user",
                include_attachment=False,
            )
            _queue_admin_full_notifications(
                config,
                updated,
                active_admin_ids,
                event_line=("⚠️ <b>Тикет освобождён: предыдущий исполнитель недоступен</b>"),
                kind="ticket_orphaned_admin",
                attachment_limit=0,
            )
            released += 1
        config.tickets = tickets
        if not released:
            raise UpdateAborted()
        return released

    try:
        released = await update_important_data(release)
    except UpdateAborted:
        return
    if released:
        logger.warning(
            "Released orphaned tickets: %s",
            released,
            extra={"action": "ticket_orphan_release"},
        )


__all__ = ["release_orphaned_tickets"]
