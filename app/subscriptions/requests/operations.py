"""Atomic mutations for subscription and trial requests."""

from __future__ import annotations

import contextlib
from typing import Any

from ...bot.ui import html_escape
from ...messaging.outbox import message_payload
from ...storage import (
    UserData,
    append_audit_entry,
    enqueue_user_outbox,
    make_outbox_event,
    next_service_request_id,
)
from ...users.staff import (
    is_admin_meta,
    is_billing_exempt_meta,
    is_owner_meta,
    staff_public_signature,
)
from ..connections import CONNECTION_URL_KEY, connection_outbox_payload
from . import state


def approved_admin_ids(config: UserData) -> list[int]:
    result: list[int] = []
    for key, meta in config.authorized_users.items():
        if not isinstance(meta, dict) or meta.get("role") != "admin":
            continue
        if meta.get("access_state") != "approved" or not bool(meta.get("enabled", True)):
            continue
        with contextlib.suppress(TypeError, ValueError):
            result.append(int(meta.get("user_id", key)))
    return sorted(set(result))


def owner_meta_from_config(config: UserData) -> dict[str, Any] | None:
    for meta in config.authorized_users.values():
        if isinstance(meta, dict) and is_owner_meta(meta):
            return meta
    return None


def queue_message(
    config: UserData,
    *,
    recipient_ids: list[int],
    kind: str,
    text: str,
    reply_markup: list[list[dict[str, str]]] | None = None,
) -> None:
    if not recipient_ids:
        return
    enqueue_user_outbox(
        config,
        make_outbox_event(
            kind=kind,
            recipient_ids=recipient_ids,
            payload=message_payload(text, reply_markup=reply_markup),
        ),
    )


def find_active_request(
    config: UserData,
    *,
    user_id: int,
    kind: str,
) -> dict[str, Any] | None:
    for request in config.service_requests.values():
        if not isinstance(request, dict):
            continue
        if int(request.get("user_id", 0) or 0) != user_id or request.get("kind") != kind:
            continue
        if request.get("status") in state.ACTIVE_REQUEST_STATUSES:
            return request
    return None


def create_request(
    config: UserData,
    *,
    kind: str,
    user_id: int,
    status: str = "pending",
    comment: str | None = None,
    target_end_at: str | None = None,
) -> dict[str, Any]:
    request_id = next_service_request_id(config)
    timestamp = state.now_iso()
    request = {
        "id": request_id,
        "kind": kind,
        "status": status,
        "user_id": user_id,
        "created_at": timestamp,
        "updated_at": timestamp,
        "comment": comment,
        "target_end_at": target_end_at,
        "claimed_by_id": None,
        "claimed_at": None,
        "reviewed_by_id": None,
        "reviewed_at": None,
        "payment_reported_at": timestamp if status == "payment_reported" else None,
    }
    config.service_requests[str(request_id)] = request
    return request


def cancel_active_requests(
    config: UserData,
    *,
    user_id: int,
    reason: str,
    exclude_request_id: int | None = None,
    kinds: set[str] | None = None,
) -> int:
    cancelled = 0
    for key, request in list(config.service_requests.items()):
        if not isinstance(request, dict):
            continue
        if int(request.get("user_id", 0) or 0) != user_id or request.get("status") not in state.ACTIVE_REQUEST_STATUSES:
            continue
        if kinds is not None and str(request.get("kind") or "") not in kinds:
            continue
        if exclude_request_id is not None and int(request.get("id", 0) or 0) == exclude_request_id:
            continue
        updated = dict(request)
        updated.update(
            {
                "status": "cancelled",
                "decision_reason": reason,
                "updated_at": state.now_iso(),
                "claimed_by_id": None,
                "claimed_at": None,
            }
        )
        config.service_requests[key] = updated
        cancelled += 1
    return cancelled


def finalize_trial(
    config: UserData,
    request: dict[str, Any],
    actor: dict[str, Any],
    connection_url: str | None,
) -> dict[str, Any]:
    if not is_admin_meta(actor):
        raise ValueError("admin_required")
    user_id = int(request.get("user_id", 0) or 0)
    current = config.authorized_users.get(str(user_id))
    if not isinstance(current, dict):
        raise ValueError("user_missing")
    if current.get("role") == "admin" or current.get("service_tier") != "basic":
        raise ValueError("tier_changed")
    if current.get("trial_issued_at"):
        raise ValueError("already_issued")
    updated = dict(current)
    if connection_url:
        updated[CONNECTION_URL_KEY] = connection_url
        updated["subscription_updated_at"] = state.now_iso()
        updated["subscription_updated_by_id"] = actor.get("user_id")
        updated["subscription_updated_by_name"] = staff_public_signature(actor)
    updated["trial_issued_at"] = state.now_iso()
    updated["trial_issued_by_id"] = actor.get("user_id")
    updated["trial_issued_by_name"] = staff_public_signature(actor)
    updated = UserData._normalize_user(updated)
    config.authorized_users[str(user_id)] = updated
    finished = dict(request)
    finished.update(
        {
            "status": "approved",
            "reviewed_by_id": actor.get("user_id"),
            "reviewed_at": state.now_iso(),
            "updated_at": state.now_iso(),
            "claimed_by_id": None,
            "claimed_at": None,
        }
    )
    config.service_requests[str(request["id"])] = finished
    queue_message(
        config,
        recipient_ids=[user_id],
        kind="trial_approved",
        text=(
            "🧪 <b>Тестовый доступ одобрен</b>\n\n"
            "Для вашей учётной записи подготовлена персональная ссылка подключения. "
            "Тест не меняет базовый уровень доступа в боте."
        ),
    )
    enqueue_user_outbox(
        config,
        make_outbox_event(
            kind="trial_connection",
            recipient_ids=[user_id],
            payload=connection_outbox_payload(updated),
        ),
    )
    append_audit_entry(
        config,
        action="trial_approved",
        actor_meta=actor,
        target_user_id=user_id,
        details={"request_id": request.get("id")},
    )
    return updated


def finalize_payment(
    config: UserData,
    request: dict[str, Any],
    actor: dict[str, Any],
    *,
    connection_url: str | None = None,
) -> dict[str, Any]:
    if not is_owner_meta(actor):
        raise ValueError("owner_required")
    user_id = int(request.get("user_id", 0) or 0)
    current = config.authorized_users.get(str(user_id))
    if not isinstance(current, dict):
        raise ValueError("user_missing")
    if is_billing_exempt_meta(current):
        raise ValueError("billing_exempt")
    target = state.parse_datetime(request.get("target_end_at"))
    if target is None or target <= state.now():
        raise ValueError("invalid_target")
    updated = dict(current)
    if connection_url:
        updated[CONNECTION_URL_KEY] = connection_url
        updated["subscription_updated_at"] = state.now_iso()
        updated["subscription_updated_by_id"] = actor.get("user_id")
        updated["subscription_updated_by_name"] = staff_public_signature(actor)
    if not str(updated.get(CONNECTION_URL_KEY) or "").strip():
        raise ValueError("connection_missing")
    timestamp = state.now_iso()
    updated.update(
        {
            "service_tier": "subscriber",
            "is_paid": True,
            "paid_at": timestamp,
            "payment_confirmed_by_id": actor.get("user_id"),
            "payment_confirmed_by_name": staff_public_signature(actor, allow_alias=False),
            "subscription_end_at": target.isoformat(),
            "payment_auto_reminders": {},
            "service_tier_updated_at": timestamp,
            "service_tier_updated_by_id": actor.get("user_id"),
            "service_tier_updated_by_name": staff_public_signature(actor, allow_alias=False),
        }
    )
    updated = UserData._normalize_user(updated)
    config.authorized_users[str(user_id)] = updated
    finished = dict(request)
    finished.update(
        {
            "status": "approved",
            "reviewed_by_id": actor.get("user_id"),
            "reviewed_at": timestamp,
            "updated_at": timestamp,
            "claimed_by_id": None,
            "claimed_at": None,
        }
    )
    config.service_requests[str(request["id"])] = finished
    cancel_active_requests(
        config,
        user_id=user_id,
        reason="payment_activated",
        exclude_request_id=int(request.get("id", 0) or 0),
    )
    queue_message(
        config,
        recipient_ids=[user_id],
        kind="payment_approved",
        text=(
            "✅ <b>Доступ к сервису активирован</b>\n\n"
            "Оплата подтверждена. Для вашей учётной записи открыт полный доступ к функциям сервиса.\n\n"
            f"Доступ оплачен до: <code>{html_escape(state.datetime_text(target.isoformat()))}</code>"
        ),
    )
    if connection_url:
        enqueue_user_outbox(
            config,
            make_outbox_event(
                kind="payment_connection",
                recipient_ids=[user_id],
                payload=connection_outbox_payload(updated),
            ),
        )
    append_audit_entry(
        config,
        action="payment_confirmed",
        actor_meta=actor,
        target_user_id=user_id,
        details={"request_id": request.get("id"), "target_end_at": target.isoformat()},
    )
    return updated


__all__ = [
    "approved_admin_ids",
    "cancel_active_requests",
    "create_request",
    "finalize_payment",
    "finalize_trial",
    "find_active_request",
    "owner_meta_from_config",
    "queue_message",
]
