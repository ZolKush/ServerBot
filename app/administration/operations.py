from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from ..bot.help import DEFAULT_HELP_TEXT
from ..storage import UserData, append_audit_entry, update_user_data
from ..users.staff import (
    STAFF_DISPLAY_TITLE,
    STAFF_TITLE_LABELS,
    is_owner_meta,
    staff_public_signature,
    staff_title_label,
)
from .dates import parse_datetime

PaymentSetting = Literal["payment_bank", "payment_recipient", "payment_phone"]
PeriodKind = Literal["period_current", "period_next"]


async def change_staff_display_mode(*, user_id: int, mode: str) -> dict[str, Any]:
    def _change(data: UserData) -> dict[str, Any]:
        current = data.authorized_users.get(str(user_id))
        if not isinstance(current, dict) or current.get("role") != "admin":
            raise ValueError("admin_missing")
        old_mode = str(current.get("staff_display_mode") or STAFF_DISPLAY_TITLE)
        updated = UserData._normalize_user({**current, "staff_display_mode": mode})
        data.authorized_users[str(user_id)] = updated
        append_audit_entry(
            data,
            action="staff_display_mode_changed",
            actor_meta=updated,
            target_user_id=user_id,
            details={"old": old_mode, "new": updated.get("staff_display_mode")},
        )
        return updated

    return await update_user_data(_change)


async def change_staff_alias(*, user_id: int, alias: str | None) -> dict[str, Any]:
    def _change(data: UserData) -> dict[str, Any]:
        current = data.authorized_users.get(str(user_id))
        if not isinstance(current, dict) or current.get("role") != "admin":
            raise ValueError("admin_missing")
        mode = current.get("staff_display_mode") if alias else STAFF_DISPLAY_TITLE
        updated = UserData._normalize_user(
            {
                **current,
                "staff_alias": alias,
                "staff_display_mode": mode,
            }
        )
        data.authorized_users[str(user_id)] = updated
        append_audit_entry(
            data,
            action="staff_alias_changed",
            actor_meta=updated,
            target_user_id=user_id,
            details={"old": current.get("staff_alias"), "new": alias},
        )
        return updated

    return await update_user_data(_change)


async def change_support_email(
    *,
    actor: dict[str, Any],
    email: str | None,
) -> dict[str, Any]:
    def _change(data: UserData) -> dict[str, Any]:
        old = data.product_settings.get("support_email")
        data.product_settings["support_email"] = email
        append_audit_entry(
            data,
            action="support_email_changed",
            actor_meta=actor,
            details={"old": old or "-", "new": email or "-"},
        )
        return dict(data.product_settings)

    return await update_user_data(_change)


async def change_payment_setting(
    *,
    actor: dict[str, Any],
    key: PaymentSetting,
    value: str,
) -> dict[str, Any]:
    def _change(data: UserData) -> dict[str, Any]:
        data.product_settings[key] = value
        append_audit_entry(
            data,
            action=f"{key}_changed",
            actor_meta=actor,
            details={"value": "обновлено"},
        )
        return dict(data.product_settings)

    return await update_user_data(_change)


async def save_help_text(
    *,
    actor: dict[str, Any],
    value: str,
    changed_at: datetime,
) -> dict[str, Any]:
    def _save(data: UserData) -> dict[str, Any]:
        old = data.product_settings.get("help_text")
        data.product_settings.update(
            {
                "help_text": value,
                "help_updated_at": changed_at.isoformat(),
                "help_updated_by_id": actor.get("user_id"),
                "help_updated_by_name": staff_public_signature(actor, allow_alias=False),
            }
        )
        append_audit_entry(
            data,
            action="help_text_changed",
            actor_meta=actor,
            details={
                "old_length": len(str(old or "")),
                "new_length": len(value),
            },
        )
        return dict(data.product_settings)

    return await update_user_data(_save)


async def reset_help_text(
    *,
    actor: dict[str, Any],
    changed_at: datetime,
) -> dict[str, Any]:
    def _reset(data: UserData) -> dict[str, Any]:
        old = data.product_settings.get("help_text")
        data.product_settings.update(
            {
                "help_text": None,
                "help_updated_at": changed_at.isoformat(),
                "help_updated_by_id": actor.get("user_id"),
                "help_updated_by_name": staff_public_signature(actor, allow_alias=False),
            }
        )
        append_audit_entry(
            data,
            action="help_text_reset",
            actor_meta=actor,
            details={
                "old_length": len(str(old or "")),
                "default_length": len(DEFAULT_HELP_TEXT),
            },
        )
        return dict(data.product_settings)

    return await update_user_data(_reset)


async def save_billing_period(
    *,
    actor: dict[str, Any],
    kind: PeriodKind,
    target: datetime,
) -> tuple[str, dict[str, Any]]:
    def _save(data: UserData) -> tuple[str, dict[str, Any]]:
        current = parse_datetime(data.product_settings.get("current_period_end"))
        next_end = parse_datetime(data.product_settings.get("next_period_end"))
        if kind == "period_current" and next_end and target >= next_end:
            return "order", dict(data.product_settings)
        if kind == "period_next" and current is None:
            return "missing_current", dict(data.product_settings)
        if kind == "period_next" and current and target <= current:
            return "order", dict(data.product_settings)
        key = "current_period_end" if kind == "period_current" else "next_period_end"
        old = data.product_settings.get(key)
        data.product_settings[key] = target.isoformat()
        data.product_settings["period_setup_reminder_for"] = None
        data.product_settings["period_missing_notice_for"] = None
        append_audit_entry(
            data,
            action=f"{key}_changed",
            actor_meta=actor,
            details={"old": old, "new": target.isoformat()},
        )
        return "updated", dict(data.product_settings)

    return await update_user_data(_save)


async def change_staff_title(
    *,
    actor: dict[str, Any],
    target_user_id: int,
    title_code: str,
) -> dict[str, Any] | None:
    def _change(data: UserData) -> dict[str, Any] | None:
        target = data.authorized_users.get(str(target_user_id))
        if not isinstance(target, dict) or target.get("role") != "admin" or is_owner_meta(target):
            return None
        old_title = staff_title_label(target)
        updated = UserData._normalize_user({**target, "staff_title": title_code})
        data.authorized_users[str(target_user_id)] = updated
        append_audit_entry(
            data,
            action="staff_title_changed",
            actor_meta=actor,
            target_user_id=target_user_id,
            details={"old": old_title, "new": STAFF_TITLE_LABELS[title_code]},
        )
        return updated

    return await update_user_data(_change)


__all__ = [
    "PaymentSetting",
    "PeriodKind",
    "change_payment_setting",
    "change_staff_alias",
    "change_staff_display_mode",
    "change_staff_title",
    "change_support_email",
    "reset_help_text",
    "save_billing_period",
    "save_help_text",
]
