"""Pure aggregate operations shared by features and the storage façade."""

from __future__ import annotations

import copy
import uuid
from datetime import datetime, timezone
from typing import Any

from ..users.staff import staff_public_signature
from .aggregates import ImportantData, UserData
from .normalization import normalize_outbox


def make_outbox_event(
    *,
    kind: str,
    recipient_ids: list[int] | tuple[int, ...] | set[int],
    payload: dict[str, Any],
    event_id: str | None = None,
    allow_blocked_delivery: bool = False,
    completion: dict[str, Any] | None = None,
) -> dict[str, Any]:
    valid_ids: set[int] = set()
    for raw_uid in recipient_ids:
        try:
            uid = int(raw_uid)
        except (TypeError, ValueError):
            continue
        if uid > 0:
            valid_ids.add(uid)
    if not valid_ids:
        raise ValueError("outbox event requires at least one recipient")
    normalized_id = (event_id or uuid.uuid4().hex).strip()
    now = datetime.now(timezone.utc).isoformat()
    return {
        "id": normalized_id,
        "kind": str(kind or "message")[:100],
        "created_at": now,
        "payload": copy.deepcopy(payload),
        "allow_blocked_delivery": bool(allow_blocked_delivery),
        "completion": copy.deepcopy(completion) if isinstance(completion, dict) else {},
        "recipients": {
            str(uid): {
                "status": "pending",
                "attempts": 0,
                "part_index": 0,
                "next_attempt_at": now,
                "last_error": "",
                "delivered_at": "",
                "delivered_chat_id": None,
                "delivered_message_id": None,
                "dead_lettered_at": "",
            }
            for uid in sorted(valid_ids)
        },
    }


def enqueue_user_outbox(cfg: UserData, event: dict[str, Any]) -> dict[str, Any]:
    return _enqueue_outbox(cfg.outbox, event, "user")


def enqueue_important_outbox(cfg: ImportantData, event: dict[str, Any]) -> dict[str, Any]:
    return _enqueue_outbox(cfg.outbox, event, "important")


def _enqueue_outbox(
    outbox: dict[str, dict[str, Any]],
    event: dict[str, Any],
    label: str,
) -> dict[str, Any]:
    normalized = normalize_outbox({str(event.get("id") or ""): event})
    if not normalized:
        raise ValueError(f"invalid {label} outbox event")
    event_id, clean = next(iter(normalized.items()))
    if event_id in outbox:
        raise ValueError(f"duplicate {label} outbox event id: {event_id}")
    outbox[event_id] = clean
    return copy.deepcopy(clean)


def suppress_user_outbox_recipient(
    cfg: UserData,
    user_id: int,
    *,
    keep_event_id: str | None = None,
) -> int:
    uid_text = str(int(user_id))
    removed = 0
    for event_id, event in list(cfg.outbox.items()):
        if event_id == keep_event_id or not isinstance(event, dict):
            continue
        recipients = event.get("recipients")
        if not isinstance(recipients, dict) or uid_text not in recipients:
            continue
        recipients.pop(uid_text, None)
        removed += 1
        if not recipients:
            cfg.outbox.pop(event_id, None)
    return removed


def next_service_request_id(cfg: UserData) -> int:
    cfg.request_seq = max(0, int(cfg.request_seq or 0)) + 1
    return cfg.request_seq


def append_audit_entry(
    cfg: UserData,
    *,
    action: str,
    actor_meta: dict[str, Any] | None,
    target_user_id: int | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    safe_details = copy.deepcopy(details) if isinstance(details, dict) else {}
    for key in tuple(safe_details):
        lowered = str(key).lower()
        if any(secret in lowered for secret in ("password", "token", "connection", "url")):
            safe_details[key] = "<скрыто>"
    actor_id = (actor_meta or {}).get("user_id")
    real_name = " ".join(
        str(part).strip()
        for part in ((actor_meta or {}).get("first_name"), (actor_meta or {}).get("last_name"))
        if str(part or "").strip()
    )
    username = str((actor_meta or {}).get("username") or "").strip().lstrip("@")
    internal_name = (
        f"{real_name} (@{username})"
        if real_name and username
        else (real_name or (f"@{username}" if username else "система"))
    )
    internal = f"{internal_name}, ID {actor_id}" if actor_id not in (None, "") else internal_name
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "action": str(action or "unknown")[:100],
        "actor_id": actor_id,
        "actor_public": staff_public_signature(actor_meta) if actor_meta else "Система",
        "actor_internal": internal[:240],
        "target_user_id": int(target_user_id) if target_user_id is not None else None,
        "details": safe_details,
    }
    cfg.audit_log = [*list(cfg.audit_log or []), entry][-2000:]
    return copy.deepcopy(entry)


__all__ = [
    "append_audit_entry",
    "enqueue_important_outbox",
    "enqueue_user_outbox",
    "make_outbox_event",
    "next_service_request_id",
    "suppress_user_outbox_recipient",
]
