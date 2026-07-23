"""Atomic storage mutations used by user-administration handlers."""

from __future__ import annotations

from typing import Any, Literal

from ...messaging.outbox import message_payload
from ...storage import (
    UserData,
    append_audit_entry,
    enqueue_user_outbox,
    make_outbox_event,
    suppress_user_outbox_recipient,
    update_user_data,
)
from ...subscriptions.connections import (
    CONNECTION_UPDATED_AT_KEY,
    CONNECTION_UPDATED_BY_ID_KEY,
    CONNECTION_UPDATED_BY_NAME_KEY,
    CONNECTION_URL_KEY,
    connection_outbox_payload,
)

AccessUpdateOutcome = Literal["missing", "admin", "invalid", "already", "updated"]


async def queue_broadcast(
    event: dict[str, Any],
    *,
    sender_id: int | None,
    recipient_count: int,
) -> None:
    def apply(data: UserData) -> None:
        enqueue_user_outbox(data, event)
        append_audit_entry(
            data,
            action="broadcast_queued",
            actor_meta=data.authorized_users.get(str(sender_id)),
            details={"recipient_count": recipient_count},
        )

    await update_user_data(apply)


async def queue_direct_message(
    event: dict[str, Any],
    *,
    actor_id: int | None,
    target_user_id: int,
) -> None:
    def apply(data: UserData) -> None:
        enqueue_user_outbox(data, event)
        append_audit_entry(
            data,
            action="direct_message_queued",
            actor_meta=data.authorized_users.get(str(actor_id)),
            target_user_id=target_user_id,
            details={},
        )

    await update_user_data(apply)


async def update_access_state(
    *,
    target_user_id: int,
    actor_id: int | None,
    actor_name: str,
    changed_at: str,
    desired_state: str | None,
) -> tuple[AccessUpdateOutcome, dict[str, Any] | None]:
    def apply(data: UserData) -> tuple[AccessUpdateOutcome, dict[str, Any] | None]:
        current = data.authorized_users.get(str(target_user_id))
        if not isinstance(current, dict):
            return "missing", None
        current = dict(current)
        if current.get("role") == "admin":
            return "admin", current

        old_state = str(current.get("access_state") or ("approved" if current.get("enabled", True) else "blocked"))
        new_state = desired_state or ("blocked" if old_state == "approved" else "approved")
        if new_state not in {"approved", "blocked"}:
            return "invalid", current
        if old_state == new_state:
            return "already", current

        current.update(
            {
                "access_state": new_state,
                "enabled": new_state == "approved",
                "access_reviewed_at": changed_at,
                "access_reviewed_by_id": actor_id,
                "access_reviewed_by_name": actor_name,
                "blocked_at": changed_at if new_state == "blocked" else None,
                "blocked_by_id": actor_id if new_state == "blocked" else None,
                "blocked_by_name": actor_name if new_state == "blocked" else None,
                "blocked_reason": "manual_admin_action" if new_state == "blocked" else None,
            }
        )
        updated = UserData._normalize_user(current)
        data.authorized_users[str(target_user_id)] = updated
        append_audit_entry(
            data,
            action="access_blocked" if new_state == "blocked" else "access_approved",
            actor_meta=data.authorized_users.get(str(actor_id)),
            target_user_id=target_user_id,
            details={},
        )
        notification = (
            "🚫 Доступ к боту отключён\n\nВы отключены от данного бота по решению администрации."
            if new_state == "blocked"
            else "✅ Доступ к боту одобрен. Используйте /menu."
        )
        notification_event = make_outbox_event(
            kind=f"access_{new_state}",
            recipient_ids=[target_user_id],
            payload=message_payload(notification, parse_mode=None),
            allow_blocked_delivery=new_state == "blocked",
        )
        enqueue_user_outbox(data, notification_event)
        if new_state == "blocked":
            suppress_user_outbox_recipient(
                data,
                target_user_id,
                keep_event_id=str(notification_event["id"]),
            )
        return "updated", updated

    return await update_user_data(apply)


async def update_nickname(
    *,
    target_user_id: int,
    nickname: str,
    actor_id: int | None,
) -> dict[str, Any] | None:
    def apply(data: UserData) -> dict[str, Any] | None:
        current = data.authorized_users.get(str(target_user_id))
        if not isinstance(current, dict):
            return None
        updated = UserData._normalize_user({**current, "nickname": nickname})
        data.authorized_users[str(target_user_id)] = updated
        append_audit_entry(
            data,
            action="nickname_changed",
            actor_meta=data.authorized_users.get(str(actor_id)),
            target_user_id=target_user_id,
            details={},
        )
        return updated

    return await update_user_data(apply)


async def assign_connection(
    *,
    target_user_id: int,
    connection_url: str,
    delivery_mode: str,
    actor_id: int | None,
    actor_name: str,
    changed_at: str,
) -> dict[str, Any] | None:
    def apply(data: UserData) -> dict[str, Any] | None:
        current = data.authorized_users.get(str(target_user_id))
        if not isinstance(current, dict):
            return None
        updated = dict(current)
        updated[CONNECTION_URL_KEY] = connection_url
        updated[CONNECTION_UPDATED_AT_KEY] = changed_at
        updated[CONNECTION_UPDATED_BY_ID_KEY] = actor_id
        updated[CONNECTION_UPDATED_BY_NAME_KEY] = actor_name
        updated = UserData._normalize_user(updated)
        data.authorized_users[str(target_user_id)] = updated
        append_audit_entry(
            data,
            action="connection_assigned",
            actor_meta=data.authorized_users.get(str(actor_id)),
            target_user_id=target_user_id,
            details={"delivery_mode": delivery_mode},
        )
        if delivery_mode != "assign":
            event = make_outbox_event(
                kind="subscription_assigned",
                recipient_ids=[target_user_id],
                payload=connection_outbox_payload(
                    updated,
                    title=(
                        "🔗 <b>Ссылка подключения готова</b>\n\n"
                        "Для вашей учётной записи назначена персональная ссылка подключения.\n"
                        "Откройте её, чтобы посмотреть инструкцию, или скопируйте ссылку и добавьте её в Happ."
                    ),
                    filename_prefix=f"connection_{target_user_id}",
                ),
            )
            enqueue_user_outbox(data, event)
        return updated

    return await update_user_data(apply)
