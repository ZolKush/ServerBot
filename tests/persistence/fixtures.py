from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any


def write_v4_source(root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    root.mkdir(parents=True)
    user = synthetic_user_data()
    important = synthetic_important_data()
    _write_json(root / "user_data.json", user)
    _write_json(root / "important_data.json", important)
    (root / "ptb_persistence").write_bytes(b"\x80synthetic-pickle-bytes\x00")
    (root / "important_data.fail2ban_state.local.json").write_text(
        '{"updated_at":"old","server_key":"local"}\n',
        encoding="utf-8",
    )
    return user, important


def synthetic_user_data() -> dict[str, Any]:
    user_meta = {
        "user_id": 42,
        "nickname": "Example",
        "contact_email": "user@example.com",
        "username": "example_user",
        "first_name": "Example",
        "last_name": "User",
        "role": "user",
        "access_state": "approved",
        "enabled": True,
        "admin_level": "none",
        "staff_title": None,
        "staff_alias": None,
        "staff_display_mode": "title",
        "auth_at": "2026-01-01T00:00:00+00:00",
        "access_requested_at": None,
        "access_reviewed_at": None,
        "access_reviewed_by_id": None,
        "access_reviewed_by_name": None,
        "blocked_at": None,
        "blocked_by_id": None,
        "blocked_by_name": None,
        "blocked_reason": None,
        "logged_out_at": None,
        "service_tier": "subscriber",
        "is_paid": True,
        "connection_url": "https://example.com/connect/42",
        "subscription_updated_at": "2026-01-01T00:00:00+00:00",
        "subscription_updated_by_id": 1,
        "subscription_updated_by_name": "Manager",
        "paid_at": "2026-01-01T00:00:00+00:00",
        "payment_confirmed_by_id": 1,
        "payment_confirmed_by_name": "Manager",
        "subscription_end_at": "2026-02-01T00:00:00+00:00",
        "trial_issued_at": None,
        "trial_issued_by_id": None,
        "trial_issued_by_name": None,
        "last_auto_payment_reminder_at": None,
        "last_auto_payment_reminder_type": None,
        "last_manual_payment_reminder_at": None,
        "last_manual_payment_reminder_by_id": None,
        "last_manual_payment_reminder_by_name": None,
        "service_tier_updated_at": None,
        "service_tier_updated_by_id": None,
        "service_tier_updated_by_name": None,
        "payment_auto_reminders": {"2026-02-01:15m": "2026-01-31T23:45:00+00:00"},
    }
    return {
        "schema_version": 4,
        "authorized_users": {"42": user_meta},
        "outbox": {"shared": _outbox_event("shared", "user")},
        "request_seq": 3,
        "service_requests": {
            "3": {
                "id": 3,
                "user_id": 9001,
                "kind": "purchase",
                "status": "approved",
                "created_at": "2026-01-01T00:00:00+00:00",
            }
        },
        "product_settings": {
            "payment_bank": "Example Bank",
            "payment_recipient": "Example Recipient",
            "payment_phone": "+10000000000",
            "current_period_end": "2026-02-01T00:00:00+00:00",
            "next_period_end": "2026-03-01T00:00:00+00:00",
            "period_setup_reminder_for": None,
            "period_missing_notice_for": None,
            "help_text": "Help",
            "help_updated_at": "2026-01-01T00:00:00+00:00",
            "help_updated_by_id": 1,
            "help_updated_by_name": "Manager",
            "support_email": "support@example.com",
        },
        "audit_log": [
            {
                "ts": "2026-01-01T00:00:00+00:00",
                "action": "seed",
                "actor_id": 1,
                "target_user_id": 9001,
            }
        ],
    }


def synthetic_important_data() -> dict[str, Any]:
    return {
        "schema_version": 4,
        "tickets_seq": 7,
        "tickets": {
            "7": {
                "id": 7,
                "status": "open",
                "subject": "Historical ticket",
                "urgency": "p2",
                "user_id": 9001,
                "user_name": "Historical User",
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
                "messages": [
                    {
                        "ts": "2026-01-01T00:00:00+00:00",
                        "sender_role": "user",
                        "sender_id": 9001,
                        "sender_name": "Historical User",
                        "text": "Message",
                        "kind": "text",
                    }
                ],
            }
        },
        "maintenance": {"active": True, "scope": "all"},
        "scheduled_maintenance": {},
        "dns_status": {"main": {"ok": True}},
        "daily_node_status": {"main": {"cpu": 12.5}},
        "outbox": {
            "shared": _outbox_event("shared", "important"),
            "important-only": _outbox_event("important-only", "important"),
        },
        "fail2ban_cursors": {"main": {"offset": 123, "inode": 456}},
        "tls_certificates": {"example.com:443": {"status": "ok"}},
        "docker_status": {
            "main": {
                "updated_at": "2026-01-01T00:00:00+00:00",
                "containers": [["api", True, "Up (healthy)", "2"]],
            }
        },
    }


def clone_payload(value: dict[str, Any]) -> dict[str, Any]:
    return copy.deepcopy(value)


def _outbox_event(event_id: str, origin: str) -> dict[str, Any]:
    return {
        "id": event_id,
        "kind": origin,
        "created_at": "2026-01-01T00:00:00+00:00",
        "payload": {"text": origin},
        "recipients": {
            "1": {
                "status": "pending",
                "attempts": 0,
                "part_index": 0,
                "next_attempt_at": "",
                "last_error": "",
                "delivered_at": "",
            }
        },
    }


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
