"""Typed ticket records used by domain operations and persistence adapters."""

from __future__ import annotations

from typing import Any, Literal, TypedDict

TicketStatus = Literal["open", "in_progress", "closed"]
TicketUrgency = Literal["p1", "p2", "p3"]
SenderRole = Literal["user", "admin"]
AttachmentType = Literal["photo", "document"]


class Attachment(TypedDict, total=False):
    type: AttachmentType
    file_id: str
    file_unique_id: str | None
    filename: str | None
    mime_type: str | None
    file_size: int | None


class TicketMessage(TypedDict, total=False):
    ts: str
    sender_role: SenderRole
    sender_id: int | None
    sender_name: str
    text: str
    kind: str
    attachment: Attachment
    sender_signature_version: int


class Ticket(TypedDict, total=False):
    id: int
    status: TicketStatus
    subject: str
    urgency: TicketUrgency
    user_id: int
    user_name: str
    user_username: str | None
    assignee_id: int | None
    assignee_name: str | None
    assignee_signature_version: int | None
    created_at: str
    updated_at: str
    closed_at: str | None
    closed_by_id: int | None
    closed_by_name: str | None
    closed_by_signature_version: int | None
    user_reply_allowed: bool
    messages: list[TicketMessage]


class TicketKeys:
    ID = "id"
    STATUS = "status"
    SUBJECT = "subject"
    URGENCY = "urgency"
    USER_ID = "user_id"
    USER_NAME = "user_name"
    USER_USERNAME = "user_username"
    ASSIGNEE_ID = "assignee_id"
    ASSIGNEE_NAME = "assignee_name"
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"
    CLOSED_AT = "closed_at"
    CLOSED_BY_ID = "closed_by_id"
    CLOSED_BY_NAME = "closed_by_name"
    USER_REPLY_ALLOWED = "user_reply_allowed"
    MESSAGES = "messages"


def coerce_ticket(raw: dict[str, Any] | None) -> Ticket:
    if not isinstance(raw, dict):
        return Ticket()
    return raw  # type: ignore[return-value]
