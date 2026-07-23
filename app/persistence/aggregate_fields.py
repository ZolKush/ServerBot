"""Field ownership for aggregate projections over split domain stores."""

PROFILE_FIELDS = frozenset(
    {
        "user_id",
        "nickname",
        "contact_email",
        "username",
        "first_name",
        "last_name",
    }
)
ACCESS_FIELDS = frozenset(
    {
        "role",
        "access_state",
        "admin_level",
        "staff_title",
        "staff_alias",
        "staff_display_mode",
        "auth_at",
        "access_requested_at",
        "access_reviewed_at",
        "access_reviewed_by_id",
        "access_reviewed_by_name",
        "blocked_at",
        "blocked_by_id",
        "blocked_by_name",
        "blocked_reason",
        "logged_out_at",
    }
)
SUBSCRIPTION_FIELDS = frozenset(
    {
        "service_tier",
        "is_paid",
        "connection_url",
        "subscription_updated_at",
        "subscription_updated_by_id",
        "subscription_updated_by_name",
        "paid_at",
        "payment_confirmed_by_id",
        "payment_confirmed_by_name",
        "subscription_end_at",
        "trial_issued_at",
        "trial_issued_by_id",
        "trial_issued_by_name",
        "last_auto_payment_reminder_at",
        "last_auto_payment_reminder_type",
        "last_manual_payment_reminder_at",
        "last_manual_payment_reminder_by_id",
        "last_manual_payment_reminder_by_name",
        "service_tier_updated_at",
        "service_tier_updated_by_id",
        "service_tier_updated_by_name",
        "payment_auto_reminders",
    }
)
BILLING_FIELDS = frozenset(
    {
        "payment_bank",
        "payment_recipient",
        "payment_phone",
        "current_period_end",
        "next_period_end",
        "period_setup_reminder_for",
        "period_missing_notice_for",
    }
)
HELP_FIELDS = frozenset(
    {
        "help_text",
        "help_updated_at",
        "help_updated_by_id",
        "help_updated_by_name",
        "support_email",
    }
)
DERIVED_USER_FIELDS = frozenset({"enabled"})
KNOWN_USER_FIELDS = PROFILE_FIELDS | ACCESS_FIELDS | SUBSCRIPTION_FIELDS | DERIVED_USER_FIELDS

OUTBOX_ORIGIN_FIELD = "origin"
OUTBOX_ORIGINS = frozenset({"user", "important"})

__all__ = [
    "ACCESS_FIELDS",
    "BILLING_FIELDS",
    "DERIVED_USER_FIELDS",
    "HELP_FIELDS",
    "KNOWN_USER_FIELDS",
    "OUTBOX_ORIGINS",
    "OUTBOX_ORIGIN_FIELD",
    "PROFILE_FIELDS",
    "SUBSCRIPTION_FIELDS",
]
