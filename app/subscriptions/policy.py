"""Subscription plan constants and policy helpers."""

from collections.abc import Mapping
from typing import Any

PLAN_MONTHS = 3
PLAN_MONTHLY_RUB = 100
PLAN_TOTAL_RUB = PLAN_MONTHS * PLAN_MONTHLY_RUB
MAX_PRICE_RUB = 1_000_000
DEFAULT_TRIAL_DURATION_HOURS = 24
MAX_CUSTOM_TRIAL_DURATION_HOURS = 24 * 365
MIN_CUSTOM_TRIAL_DURATION_HOURS = 1


def parse_price(value: object) -> int | None:
    """Accept a positive amount in whole rubles without rounding input."""
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    text = str(value).strip()
    if not text.isascii() or not text.isdigit() or len(text) > len(str(MAX_PRICE_RUB)):
        return None
    amount = int(text)
    return amount if 1 <= amount <= MAX_PRICE_RUB else None


def standard_price(settings: Mapping[str, Any]) -> int:
    """Use the deployed plan price until an owner configures a new amount."""
    return parse_price(settings.get("standard_price_rub")) or PLAN_TOTAL_RUB


__all__ = [
    "DEFAULT_TRIAL_DURATION_HOURS",
    "MAX_CUSTOM_TRIAL_DURATION_HOURS",
    "MAX_PRICE_RUB",
    "MIN_CUSTOM_TRIAL_DURATION_HOURS",
    "PLAN_MONTHLY_RUB",
    "PLAN_MONTHS",
    "PLAN_TOTAL_RUB",
    "parse_price",
    "standard_price",
]
