"""Typed user, access, staff, and subscription-facing metadata."""

from __future__ import annotations

from typing import Literal, TypedDict

AccessState = Literal["pending", "approved", "blocked", "logged_out", "rejected"]
ServiceTier = Literal["basic", "subscriber", "unlimited_trial"]
AdminLevel = Literal["admin", "owner", "none"]


class UserMeta(TypedDict, total=False):
    user_id: int
    role: Literal["user", "admin"]
    enabled: bool
    access_state: AccessState
    is_paid: bool
    service_tier: ServiceTier
    admin_level: AdminLevel
    staff_title: str | None
    staff_alias: str | None
    staff_display_mode: str
    nickname: str | None
    contact_email: str | None
    username: str | None
    first_name: str | None
    last_name: str | None
    auth_at: str | None
    connection_url: str | None
    subscription_end_at: str | None
    paid_at: str | None
    trial_issued_at: str | None
    last_auto_payment_reminder_at: str | None
    last_auto_payment_reminder_type: str | None
    last_manual_payment_reminder_at: str | None
    subscription_updated_at: str | None
    subscription_updated_by_id: int | None
    subscription_updated_by_name: str | None
    access_requested_at: str | None
    access_reviewed_at: str | None
    access_reviewed_by_id: int | None
    access_reviewed_by_name: str | None
    blocked_at: str | None
    blocked_by_id: int | None
    blocked_by_name: str | None
    blocked_reason: str | None
    logged_out_at: str | None
