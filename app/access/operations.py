from __future__ import annotations

import contextlib
from datetime import datetime
from typing import Any, Literal

from ..config import ACCESS_REQUEST_COOLDOWN_SEC, TZ
from ..storage import (
    UserData,
    append_audit_entry,
    enqueue_user_outbox,
    mutate_user_meta,
    suppress_user_outbox_recipient,
    update_user_data,
)
from ..users.staff import STAFF_TITLE_OWNER, STAFF_TITLE_SUPPORT

AccessReviewAction = Literal["approve", "reject", "block"]


async def authorize_admin(
    *,
    user_id: int,
    username: str | None,
    first_name: str | None,
    last_name: str | None,
    authenticated_at: datetime,
) -> int:
    def _authorize(data: UserData) -> int:
        existing = data.authorized_users.get(str(user_id))
        current = dict(existing) if isinstance(existing, dict) else {}
        current.update(
            {
                "user_id": user_id,
                "role": "admin",
                "access_state": "approved",
                "enabled": True,
                "username": username,
                "first_name": first_name,
                "last_name": last_name,
                "auth_at": authenticated_at.isoformat(),
                "is_paid": bool(current.get("is_paid", False)),
                "service_tier": current.get("service_tier") or "subscriber",
                "admin_level": current.get("admin_level") or "admin",
                "staff_title": current.get("staff_title") or STAFF_TITLE_SUPPORT,
                "logged_out_at": None,
            }
        )
        data.authorized_users[str(user_id)] = UserData._normalize_user(current)
        return sum(
            1
            for candidate in data.authorized_users.values()
            if isinstance(candidate, dict) and candidate.get("access_state") == "pending"
        )

    return await update_user_data(_authorize)


async def submit_access_request(
    *,
    user_id: int,
    username: str | None,
    first_name: str | None,
    last_name: str | None,
    requested_at: datetime,
    notification_event: dict[str, Any] | None,
) -> str:
    def _apply(data: UserData) -> str:
        existing = data.authorized_users.get(str(user_id))
        current = dict(existing) if isinstance(existing, dict) else {}
        state = str(current.get("access_state") or "")
        if state == "approved":
            return "approved"
        if current.get("role") == "admin":
            return "admin"
        if state == "blocked":
            return "blocked"
        if state == "pending":
            return "pending"
        previous = str(current.get("access_requested_at") or "")
        if previous:
            with contextlib.suppress(ValueError):
                previous_dt = datetime.fromisoformat(previous)
                if previous_dt.tzinfo is None:
                    previous_dt = previous_dt.replace(tzinfo=TZ)
                if (requested_at - previous_dt.astimezone(TZ)).total_seconds() < ACCESS_REQUEST_COOLDOWN_SEC:
                    return "cooldown"
        current.update(
            {
                "user_id": user_id,
                "role": "user",
                # A request after /logout must retain paid/trial service data.
                "service_tier": current.get("service_tier") or "basic",
                "access_state": "pending",
                "enabled": False,
                "username": username,
                "first_name": first_name,
                "last_name": last_name,
                "access_requested_at": requested_at.isoformat(),
            }
        )
        data.authorized_users[str(user_id)] = UserData._normalize_user(current)
        if notification_event:
            enqueue_user_outbox(data, notification_event)
        return "created"

    return await update_user_data(_apply)


async def review_access(
    *,
    actor_id: int,
    actor_name: str,
    target_user_id: int,
    action: AccessReviewAction,
    reviewed_at: datetime,
    notification_event: dict[str, Any],
) -> tuple[str, dict[str, Any] | None]:
    def _apply(data: UserData) -> tuple[str, dict[str, Any] | None]:
        existing = data.authorized_users.get(str(target_user_id))
        if not isinstance(existing, dict):
            return "missing", None
        current = dict(existing)
        if current.get("role") == "admin":
            return "admin", current
        state = str(current.get("access_state") or "")
        desired = {"approve": "approved", "reject": "rejected", "block": "blocked"}[action]
        if state == desired:
            return "already", current
        if action in {"approve", "reject"} and state != "pending":
            return "stale", current
        current.update(
            {
                "access_state": desired,
                "enabled": desired == "approved",
                "access_reviewed_at": reviewed_at.isoformat(),
                "access_reviewed_by_id": actor_id,
                "access_reviewed_by_name": actor_name,
                "auth_at": reviewed_at.isoformat() if desired == "approved" else current.get("auth_at"),
                "blocked_at": reviewed_at.isoformat() if desired == "blocked" else None,
                "blocked_by_id": actor_id if desired == "blocked" else None,
                "blocked_by_name": actor_name if desired == "blocked" else None,
            }
        )
        data.authorized_users[str(target_user_id)] = UserData._normalize_user(current)
        append_audit_entry(
            data,
            action=f"access_{desired}",
            actor_meta=data.authorized_users.get(str(actor_id)),
            target_user_id=target_user_id,
            details={},
        )
        enqueue_user_outbox(data, notification_event)
        if desired == "blocked":
            suppress_user_outbox_recipient(
                data,
                target_user_id,
                keep_event_id=str(notification_event["id"]),
            )
        return "updated", current

    return await update_user_data(_apply)


async def claim_service_owner(*, user_id: int, claimed_at: datetime) -> str:
    def _claim(data: UserData) -> str:
        if any(
            isinstance(meta, dict) and meta.get("role") == "admin" and meta.get("admin_level") == "owner"
            for meta in data.authorized_users.values()
        ):
            return "exists"
        latest = data.authorized_users.get(str(user_id))
        if not isinstance(latest, dict) or latest.get("role") != "admin":
            return "denied"
        updated = UserData._normalize_user(
            {
                **latest,
                "admin_level": "owner",
                "staff_title": STAFF_TITLE_OWNER,
                "service_tier": "subscriber",
                "is_paid": True,
                "subscription_end_at": None,
                "payment_auto_reminders": {},
            }
        )
        data.authorized_users[str(user_id)] = updated
        for request_id, request in list(data.service_requests.items()):
            if (
                isinstance(request, dict)
                and int(request.get("user_id", 0) or 0) == user_id
                and request.get("kind") in {"purchase", "renewal"}
                and request.get("status")
                in {"pending", "claimed", "awaiting_link", "requisites_sent", "payment_reported"}
            ):
                data.service_requests[request_id] = {
                    **request,
                    "status": "cancelled",
                    "updated_at": claimed_at.isoformat(),
                    "decision_reason": "service_manager_billing_exempt",
                    "claimed_by_id": None,
                    "claimed_at": None,
                }
        append_audit_entry(
            data,
            action="owner_claimed",
            actor_meta=updated,
            target_user_id=user_id,
            details={"staff_title": "Руководитель сервиса"},
        )
        return "claimed"

    return await update_user_data(_claim)


async def logout_user(*, user_id: int, logged_out_at: datetime) -> dict[str, Any] | None:
    return await mutate_user_meta(
        user_id,
        lambda meta: {
            **meta,
            "access_state": "logged_out",
            "enabled": False,
            "logged_out_at": logged_out_at.isoformat(),
        },
    )


__all__ = [
    "AccessReviewAction",
    "authorize_admin",
    "claim_service_owner",
    "logout_user",
    "review_access",
    "submit_access_request",
]
