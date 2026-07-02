from __future__ import annotations

from typing import Any, Literal, TypedDict

TicketStatus = Literal["open", "in_progress", "closed"]
TicketUrgency = Literal["p1", "p2", "p3"]
SenderRole = Literal["user", "admin"]
AttachmentType = Literal["photo", "document"]
MaintScope = str
MaintUrgency = Literal["urgent", "planned"]


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
    created_at: str
    updated_at: str
    closed_at: str | None
    closed_by_id: int | None
    closed_by_name: str | None
    user_reply_allowed: bool
    messages: list[TicketMessage]


class Maintenance(TypedDict, total=False):
    id: str
    active: bool
    scope: MaintScope
    urgency: MaintUrgency
    duration_min: int
    started_at: str
    expected_end: str
    author_id: int | None
    author_name: str
    updated_at: str


class ScheduledMaintenance(TypedDict, total=False):
    id: str
    scope: MaintScope
    urgency: MaintUrgency
    duration_min: int
    scheduled_start: str
    scheduled_end: str
    author_id: int | None
    author_name: str
    created_at: str
    updated_at: str
    notified_thresholds: list[int]
    notified_start: bool


class UserMeta(TypedDict, total=False):
    user_id: int
    role: Literal["user", "admin"]
    enabled: bool
    is_paid: bool
    nickname: str | None
    username: str | None
    first_name: str | None
    last_name: str | None
    auth_at: str | None
    subscription_text: str | None
    subscription_updated_at: str | None
    subscription_updated_by_id: int | None
    subscription_updated_by_name: str | None


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


class MaintenanceKeys:
    ID = "id"
    ACTIVE = "active"
    SCOPE = "scope"
    URGENCY = "urgency"
    DURATION_MIN = "duration_min"
    STARTED_AT = "started_at"
    EXPECTED_END = "expected_end"
    AUTHOR_ID = "author_id"
    AUTHOR_NAME = "author_name"
    UPDATED_AT = "updated_at"


class ScheduledMaintenanceKeys:
    ID = "id"
    SCOPE = "scope"
    URGENCY = "urgency"
    DURATION_MIN = "duration_min"
    SCHEDULED_START = "scheduled_start"
    SCHEDULED_END = "scheduled_end"
    AUTHOR_ID = "author_id"
    AUTHOR_NAME = "author_name"
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"
    NOTIFIED_THRESHOLDS = "notified_thresholds"
    NOTIFIED_START = "notified_start"


def coerce_ticket(raw: dict[str, Any] | None) -> Ticket:
    if not isinstance(raw, dict):
        return Ticket()
    return raw  # type: ignore[return-value]


def coerce_maintenance(raw: dict[str, Any] | None) -> Maintenance:
    if not isinstance(raw, dict):
        return Maintenance()
    return raw  # type: ignore[return-value]


def coerce_scheduled_maintenance(raw: dict[str, Any] | None) -> ScheduledMaintenance:
    if not isinstance(raw, dict):
        return ScheduledMaintenance()
    return raw  # type: ignore[return-value]


__all__ = [
    "Attachment",
    "AttachmentType",
    "Maintenance",
    "MaintenanceKeys",
    "MaintScope",
    "MaintUrgency",
    "ScheduledMaintenance",
    "ScheduledMaintenanceKeys",
    "SenderRole",
    "Ticket",
    "TicketKeys",
    "TicketMessage",
    "TicketStatus",
    "TicketUrgency",
    "UserMeta",
    "coerce_maintenance",
    "coerce_scheduled_maintenance",
    "coerce_ticket",
]
