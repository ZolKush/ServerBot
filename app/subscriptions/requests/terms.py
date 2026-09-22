"""Validation and atomic edits of the terms of a single purchase request."""

from __future__ import annotations

from calendar import monthrange
from typing import Any

from ...storage import UserData, append_audit_entry
from ...users.staff import is_admin_meta, is_billing_exempt_meta
from ..policy import parse_price
from . import state

MAX_PERIOD_MONTHS = 36


def terms_version(request: dict[str, Any]) -> dict[str, Any]:
    return {key: request.get(key) for key in ("updated_at", "amount_rub", "period_months", "target_end_at")}


def parse_terms(text: str) -> dict[str, Any] | None:
    """Accept rubles | months [| exact local deadline], bounded before date arithmetic."""
    parts = [part.strip() for part in text.split("|")]
    if len(parts) not in {2, 3}:
        return None
    amount = parse_price(parts[0])
    try:
        months = int(parts[1])
    except (ValueError, OverflowError):
        return None
    if amount is None or not 1 <= months <= MAX_PERIOD_MONTHS:
        return None
    now = state.now()
    if len(parts) == 3:
        target = state.parse_input_datetime(parts[2])
    else:
        year, month = divmod(now.year * 12 + now.month - 1 + months, 12)
        month += 1
        target = now.replace(year=year, month=month, day=min(now.day, monthrange(year, month)[1]))
    if target is None or target <= now:
        return None
    return {"amount_rub": amount, "period_months": months, "target_end_at": target.isoformat()}


def update_request_terms(
    config: UserData,
    *,
    request_id: int,
    actor: dict[str, Any],
    expected: dict[str, Any],
    terms: dict[str, Any],
) -> str:
    # Reload the actor as well as the request at the point of publication.
    current_actor = config.authorized_users.get(str(actor.get("user_id")))
    if (
        not current_actor
        or not is_admin_meta(current_actor)
        or current_actor.get("access_state") != "approved"
        or not current_actor.get("enabled", True)
    ):
        return "forbidden"
    request = config.service_requests.get(str(request_id))
    if not isinstance(request, dict) or request.get("kind") != "purchase" or request.get("status") != "pending":
        return "stale"
    if terms_version(request) != expected:
        return "changed"
    user_id = int(request.get("user_id", 0) or 0)
    current = config.authorized_users.get(str(user_id))
    if not current or current.get("service_tier") != "basic" or is_billing_exempt_meta(current):
        return "stale"
    amount = parse_price(terms.get("amount_rub"))
    months = terms.get("period_months")
    target = state.parse_datetime(terms.get("target_end_at"))
    if (
        amount is None
        or type(months) is not int
        or not 1 <= months <= MAX_PERIOD_MONTHS
        or target is None
        or target <= state.now()
    ):
        return "invalid"
    new_terms = {"amount_rub": amount, "period_months": months, "target_end_at": target.isoformat()}
    config.service_requests[str(request_id)] = {
        **request,
        **new_terms,
        "custom_terms": True,
        "updated_at": state.now_iso(),
    }
    append_audit_entry(
        config,
        action="purchase_terms_changed",
        actor_meta=current_actor,
        target_user_id=user_id,
        details={"request_id": request_id, "old": expected, "new": new_terms},
    )
    return "updated"
