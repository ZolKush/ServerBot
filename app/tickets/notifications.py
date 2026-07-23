"""Transactional ticket notification and attachment outbox builders."""

from __future__ import annotations

from typing import Any

from telegram import InlineKeyboardMarkup

from ..messaging.outbox import message_payload
from ..storage import ImportantData, enqueue_important_outbox, make_outbox_event
from .history import _last_attachment, _ticket_messages
from .views import (
    _format_ticket_for_admin,
    _format_ticket_for_user,
    _ticket_admin_kb,
    _ticket_user_kb,
)

MAX_TRANSFER_ATTACHMENTS = 3


def _markup_descriptor(markup: InlineKeyboardMarkup | None) -> list[list[dict[str, str]]]:
    if markup is None:
        return []
    rows: list[list[dict[str, str]]] = []
    for row in markup.inline_keyboard:
        encoded: list[dict[str, str]] = []
        for button in row:
            if button.callback_data:
                encoded.append(
                    {"text": button.text, "callback_data": str(button.callback_data)},
                )
            elif button.url:
                encoded.append({"text": button.text, "url": str(button.url)})
        if encoded:
            rows.append(encoded)
    return rows


def _queue_ticket_text(
    config: ImportantData,
    *,
    uid: int,
    text: str,
    markup: InlineKeyboardMarkup | None,
    kind: str,
) -> None:
    enqueue_important_outbox(
        config,
        make_outbox_event(
            kind=kind,
            recipient_ids=[uid],
            payload=message_payload(text, reply_markup=_markup_descriptor(markup)),
        ),
    )


def _queue_ticket_attachment(
    config: ImportantData,
    *,
    uid: int,
    attachment: dict[str, Any] | None,
    kind: str,
) -> None:
    if not isinstance(attachment, dict):
        return
    attachment_type = str(attachment.get("type") or "")
    file_id = str(attachment.get("file_id") or "")
    if attachment_type not in {"photo", "document"} or not file_id:
        return
    enqueue_important_outbox(
        config,
        make_outbox_event(
            kind=kind,
            recipient_ids=[uid],
            payload={"method": f"send_{attachment_type}", "file_id": file_id},
        ),
    )


def _queue_admin_full_notifications(
    config: ImportantData,
    ticket: dict[str, Any],
    admin_ids: list[int],
    *,
    event_line: str,
    kind: str,
    attachment_limit: int = 1,
) -> None:
    attachments = [
        dict(item["attachment"]) for item in _ticket_messages(ticket) if isinstance(item.get("attachment"), dict)
    ][-max(0, attachment_limit) :]
    for admin_id in admin_ids:
        _queue_ticket_text(
            config,
            uid=admin_id,
            text=_format_ticket_for_admin(ticket, admin_id, event_line=event_line),
            markup=_ticket_admin_kb(ticket, admin_id),
            kind=kind,
        )
        for attachment in attachments:
            _queue_ticket_attachment(
                config,
                uid=admin_id,
                attachment=attachment,
                kind=f"{kind}_attachment",
            )


def _queue_user_notification(
    config: ImportantData,
    ticket: dict[str, Any],
    *,
    event_line: str,
    kind: str,
    include_attachment: bool = True,
) -> None:
    try:
        uid = int(ticket.get("user_id", 0) or 0)
    except (TypeError, ValueError):
        return
    if not uid:
        return
    _queue_ticket_text(
        config,
        uid=uid,
        text=_format_ticket_for_user(ticket, event_line=event_line),
        markup=_ticket_user_kb(ticket, uid),
        kind=kind,
    )
    if include_attachment:
        attachment = _last_attachment(ticket)
        _queue_ticket_attachment(
            config,
            uid=uid,
            attachment=dict(attachment) if attachment else None,
            kind=f"{kind}_attachment",
        )


__all__ = ["MAX_TRANSFER_ATTACHMENTS"]
