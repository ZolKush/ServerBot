"""Telegram input extraction and transient ticket-flow context."""

from __future__ import annotations

from typing import Any

from telegram import Message
from telegram.ext import ContextTypes


def ticket_context_data(context: ContextTypes.DEFAULT_TYPE) -> dict[str, Any]:
    data = context.user_data
    if data is None:
        raise RuntimeError("Telegram user_data is unavailable")
    return data


def _clear_ticket_ctx(context: ContextTypes.DEFAULT_TYPE) -> None:
    data = ticket_context_data(context)
    for key in (
        "ticket_subject",
        "ticket_urgency",
        "ticket_text",
        "ticket_attachment",
        "ticket_send_in_progress",
        "ticket_edit_field",
        "ticket_reply_ticket_id",
        "ticket_reply_role",
    ):
        data.pop(key, None)


def _extract_message_payload(message: Message | None) -> tuple[str, dict[str, Any] | None]:
    if message is None:
        return "", None
    text = (message.text or message.caption or "").strip()
    attachment: dict[str, Any] | None = None
    if message.photo:
        photo = message.photo[-1]
        attachment = {
            "type": "photo",
            "file_id": photo.file_id,
            "file_unique_id": getattr(photo, "file_unique_id", None),
        }
    elif message.document:
        attachment = {
            "type": "document",
            "file_id": message.document.file_id,
            "file_unique_id": getattr(message.document, "file_unique_id", None),
            "filename": message.document.file_name,
            "mime_type": message.document.mime_type,
            "file_size": message.document.file_size,
        }
    return text, attachment


def _parse_ticket_callback_id(data: str | None, action: str) -> int:
    parts = (data or "").split(":")
    if len(parts) != 3 or parts[0] != "ticket" or parts[1] != action or not parts[2].isdigit():
        return 0
    return int(parts[2])


__all__ = []
