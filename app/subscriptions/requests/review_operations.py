"""Atomic staff decisions for active subscription requests."""

from __future__ import annotations

from typing import Any

from ...storage import UserData, append_audit_entry
from ...users.staff import is_billing_exempt_meta, is_owner_meta
from ..connections import has_connection
from . import state
from .operations import (
    cancel_active_requests,
    finalize_payment,
    finalize_trial,
    queue_message,
)
from .views import payment_markup, payment_message, payment_profile_ready, payment_target

RequestResult = tuple[str, dict[str, Any] | None]


def approve_trial(
    config: UserData,
    *,
    request_id: int,
    actor: dict[str, Any],
) -> RequestResult:
    request = config.service_requests.get(str(request_id))
    if not isinstance(request, dict) or request.get("kind") != "trial":
        return "missing", None
    if request.get("status") not in {"pending", "claimed", "awaiting_link"}:
        return "stale", request
    if request.get("claimed_by_id") not in (None, actor.get("user_id")):
        return "claimed", request
    user_id = int(request.get("user_id", 0) or 0)
    current = config.authorized_users.get(str(user_id))
    if not isinstance(current, dict):
        return "missing", request
    if current.get("role") == "admin" or current.get("service_tier") != "basic":
        cancel_active_requests(
            config,
            user_id=user_id,
            reason="service_tier_changed",
            kinds={"trial"},
        )
        return "tier_changed", request
    if current.get("trial_issued_at"):
        return "already_issued", request
    if has_connection(current):
        finalize_trial(config, request, actor, None)
        return "completed", request
    updated = dict(request)
    updated.update(
        {
            "status": "awaiting_link",
            "resume_status": "pending",
            "claimed_by_id": actor.get("user_id"),
            "claimed_at": state.now_iso(),
            "updated_at": state.now_iso(),
        }
    )
    config.service_requests[str(request_id)] = updated
    return "need_link", updated


def send_requisites(
    config: UserData,
    *,
    request_id: int,
    actor: dict[str, Any],
) -> RequestResult:
    request = config.service_requests.get(str(request_id))
    if not isinstance(request, dict) or request.get("kind") != "purchase":
        return "missing", None
    if request.get("status") == "requisites_sent":
        return "already_sent", request
    if request.get("status") != "pending":
        return "stale", request
    user_id = int(request.get("user_id", 0) or 0)
    current = config.authorized_users.get(str(user_id))
    if not isinstance(current, dict):
        return "missing", request
    if is_billing_exempt_meta(current) or current.get("service_tier") != "basic":
        cancel_active_requests(
            config,
            user_id=user_id,
            reason="service_tier_changed",
            kinds={"purchase"},
        )
        return "tier_changed", request
    target = state.parse_datetime(request.get("target_end_at"))
    if target is None or target <= state.now():
        target = payment_target(config.product_settings)
    if target is None or target <= state.now() or not payment_profile_ready(config.product_settings):
        return "not_configured", request
    updated = dict(request)
    updated.update(
        {
            "status": "requisites_sent",
            "target_end_at": target.isoformat(),
            "updated_at": state.now_iso(),
            "reviewed_by_id": actor.get("user_id"),
            "reviewed_at": state.now_iso(),
        }
    )
    config.service_requests[str(request_id)] = updated
    queue_message(
        config,
        recipient_ids=[int(updated.get("user_id", 0) or 0)],
        kind="payment_requisites",
        text=payment_message(config.product_settings, updated),
        reply_markup=payment_markup(request_id),
    )
    return "sent", updated


def reject_request(
    config: UserData,
    *,
    request_id: int,
    actor: dict[str, Any],
) -> str:
    request = config.service_requests.get(str(request_id))
    if not isinstance(request, dict) or request.get("status") not in state.ACTIVE_REQUEST_STATUSES:
        return "stale"
    if request.get("status") == "awaiting_link" and request.get("claimed_by_id") not in (
        None,
        actor.get("user_id"),
    ):
        return "claimed"
    if request.get("status") == "payment_reported" and not is_owner_meta(actor):
        return "owner_only"
    updated = dict(request)
    updated.update(
        {
            "status": "rejected",
            "updated_at": state.now_iso(),
            "reviewed_at": state.now_iso(),
            "reviewed_by_id": actor.get("user_id"),
            "claimed_by_id": None,
            "claimed_at": None,
        }
    )
    config.service_requests[str(request_id)] = updated
    text = {
        "trial": "❌ Запрос тестового доступа отклонён.",
        "purchase": ("❌ Заявка на покупку подписки отклонена. При необходимости создайте тикет в поддержку."),
        "renewal": ("❌ Запрос на продление подписки отклонён. При необходимости создайте тикет в поддержку."),
    }.get(str(request.get("kind") or ""), "❌ Заявка отклонена.")
    user_id = int(request.get("user_id", 0) or 0)
    queue_message(
        config,
        recipient_ids=[user_id],
        kind="service_request_rejected",
        text=text,
    )
    append_audit_entry(
        config,
        action="service_request_rejected",
        actor_meta=actor,
        target_user_id=user_id,
        details={"request_id": request_id, "kind": request.get("kind")},
    )
    return "rejected"


def reset_unconfirmed_payment(
    config: UserData,
    *,
    request_id: int,
    actor: dict[str, Any],
) -> str:
    request = config.service_requests.get(str(request_id))
    if not isinstance(request, dict) or request.get("status") != "payment_reported":
        return "stale"
    updated = dict(request)
    updated.update(
        {
            "status": "requisites_sent",
            "updated_at": state.now_iso(),
            "payment_reported_at": None,
            "reviewed_by_id": actor.get("user_id"),
            "reviewed_at": state.now_iso(),
        }
    )
    config.service_requests[str(request_id)] = updated
    queue_message(
        config,
        recipient_ids=[int(request.get("user_id", 0) or 0)],
        kind="payment_not_found",
        text=(
            "🔎 <b>Платёж пока не найден</b>\n\n"
            "Проверьте реквизиты и статус перевода. После поступления платежа "
            "нажмите кнопку ещё раз. Если возникли вопросы, создайте тикет."
        ),
        reply_markup=payment_markup(request_id),
    )
    return "reset"


def confirm_payment(
    config: UserData,
    *,
    request_id: int,
    actor: dict[str, Any],
) -> RequestResult:
    request = config.service_requests.get(str(request_id))
    if not isinstance(request, dict) or request.get("kind") not in {"purchase", "renewal"}:
        return "missing", None
    if request.get("status") == "awaiting_link":
        if int(request.get("claimed_by_id", 0) or 0) != int(actor.get("user_id", 0) or 0):
            return "claimed", request
        refreshed = dict(request)
        refreshed.update({"claimed_at": state.now_iso(), "updated_at": state.now_iso()})
        config.service_requests[str(request_id)] = refreshed
        return "need_link", refreshed
    if request.get("status") != "payment_reported":
        return "stale", request
    user_id = int(request.get("user_id", 0) or 0)
    current = config.authorized_users.get(str(user_id))
    if not isinstance(current, dict):
        return "missing", request
    target = state.parse_datetime(request.get("target_end_at"))
    if target is None or target <= state.now():
        return "invalid_target", request
    if has_connection(current):
        finalize_payment(config, request, actor)
        return "completed", request
    updated = dict(request)
    updated.update(
        {
            "status": "awaiting_link",
            "resume_status": "payment_reported",
            "claimed_by_id": actor.get("user_id"),
            "claimed_at": state.now_iso(),
            "updated_at": state.now_iso(),
        }
    )
    config.service_requests[str(request_id)] = updated
    return "need_link", updated


__all__ = [
    "approve_trial",
    "confirm_payment",
    "reject_request",
    "reset_unconfirmed_payment",
    "send_requisites",
]
