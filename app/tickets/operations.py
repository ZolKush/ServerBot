"""Atomic ticket persistence operations and workflow invariants."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from ..storage import ImportantData, UpdateAborted, update_important_data
from .history import _now_iso
from .models import Ticket

TicketData = dict[str, Any]
TicketMutator = Callable[[TicketData], TicketData]
TicketOutboxBuilder = Callable[[ImportantData, TicketData], None]


class TicketFlowError(RuntimeError):
    pass


def _safe_int(value: object) -> int:
    try:
        return int(cast(Any, value or 0))
    except (TypeError, ValueError):
        return 0


def _ticket_can_user_reply(ticket: Ticket | TicketData, uid: int) -> bool:
    return (
        _safe_int(ticket.get("user_id")) == uid
        and str(ticket.get("status", "open")) != "closed"
        and bool(ticket.get("assignee_id"))
        and bool(ticket.get("user_reply_allowed", False))
    )


def _ticket_is_assignee(ticket: Ticket | TicketData, uid: int) -> bool:
    return _safe_int(ticket.get("assignee_id")) == uid


def _build_ticket_record(
    ticket_id: int,
    *,
    user_id: int,
    user_name: str,
    user_username: str | None,
    subject: str,
    urgency: str,
    text: str,
    attachment: dict[str, Any] | None = None,
) -> TicketData:
    now = _now_iso()
    initial_message: TicketData = {
        "ts": now,
        "sender_role": "user",
        "sender_id": user_id,
        "sender_name": user_name,
        "text": text,
        "kind": "initial",
    }
    if attachment:
        initial_message["attachment"] = dict(attachment)
    return {
        "id": ticket_id,
        "status": "open",
        "subject": subject,
        "urgency": urgency,
        "user_id": user_id,
        "user_name": user_name,
        "user_username": user_username or None,
        "assignee_id": None,
        "assignee_name": None,
        "assignee_signature_version": None,
        "created_at": now,
        "updated_at": now,
        "closed_at": None,
        "closed_by_id": None,
        "closed_by_name": None,
        "user_reply_allowed": False,
        "messages": [initial_message],
    }


async def _ticket_update(
    ticket_id: int,
    mutator: TicketMutator,
    outbox_builder: TicketOutboxBuilder | None = None,
) -> TicketData:
    flow_state: dict[str, str] = {}

    def apply_update(config: ImportantData) -> TicketData:
        tickets = dict(config.tickets or {})
        raw = tickets.get(str(ticket_id))
        if not isinstance(raw, dict):
            flow_state["error"] = "ticket_not_found"
            raise UpdateAborted()
        try:
            updated = mutator(dict(raw))
        except TicketFlowError as exc:
            flow_state["error"] = str(exc) or "ticket_error"
            raise UpdateAborted() from exc
        tickets[str(ticket_id)] = dict(updated)
        config.tickets = tickets
        if outbox_builder is not None:
            outbox_builder(config, dict(updated))
        return dict(updated)

    try:
        return await update_important_data(apply_update)
    except UpdateAborted:
        raise TicketFlowError(flow_state.get("error", "ticket_error")) from None


async def create_ticket(
    *,
    user_id: int,
    user_name: str,
    user_username: str | None,
    subject: str,
    urgency: str,
    text: str,
    attachment: dict[str, Any] | None,
    outbox_builder: TicketOutboxBuilder,
) -> TicketData:
    conflict = False

    def create(config: ImportantData) -> TicketData:
        nonlocal conflict
        tickets = dict(config.tickets or {})
        for raw_ticket in tickets.values():
            if not isinstance(raw_ticket, dict):
                continue
            if _safe_int(raw_ticket.get("user_id")) == user_id and str(raw_ticket.get("status", "open")) != "closed":
                conflict = True
                raise UpdateAborted()
        config.tickets_seq = max(int(config.tickets_seq or 0), 0) + 1
        created = _build_ticket_record(
            config.tickets_seq,
            user_id=user_id,
            user_name=user_name,
            user_username=user_username,
            subject=subject,
            urgency=urgency,
            text=text,
            attachment=attachment,
        )
        tickets[str(config.tickets_seq)] = created
        config.tickets = tickets
        outbox_builder(config, created)
        return created

    try:
        return await update_important_data(create)
    except UpdateAborted:
        reason = "open_ticket_exists" if conflict else "creation_aborted"
        raise TicketFlowError(reason) from None


__all__ = ["TicketFlowError", "create_ticket"]
