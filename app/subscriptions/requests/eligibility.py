"""Eligibility predicates used across subscription request workflows."""

from __future__ import annotations

from typing import Any

from ...users.staff import is_billing_exempt_meta


def is_paid_subscriber(meta: dict[str, Any]) -> bool:
    return bool(not is_billing_exempt_meta(meta) and meta.get("service_tier") == "subscriber" and meta.get("is_paid"))


def is_eligible_paid_subscriber(meta: dict[str, Any]) -> bool:
    return bool(is_paid_subscriber(meta) and meta.get("access_state") == "approved" and bool(meta.get("enabled", True)))


def parse_id_list(raw: str) -> set[int] | None:
    values = [part.strip() for part in raw.split(",") if part.strip()]
    if not values or any(not value.isdigit() for value in values):
        return None
    return {int(value) for value in values if int(value) > 0}


__all__ = [
    "is_eligible_paid_subscriber",
    "is_paid_subscriber",
    "parse_id_list",
]
