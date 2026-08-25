from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from telegram.ext import ConversationHandler

from app import storage
from app.config import TZ
from app.persistence.normalization import normalize_product_settings
from app.subscriptions.policy import PLAN_TOTAL_RUB
from app.subscriptions.requests import customer as product_customer
from app.subscriptions.requests import input_processing as product_input
from app.subscriptions.requests import operations as product_operations
from app.subscriptions.requests import payment_reports as product_payment_reports
from app.subscriptions.requests import review_handlers as product_review
from app.subscriptions.requests import review_operations as product_review_operations
from app.subscriptions.requests import state as product_state
from app.users.staff import STAFF_TITLE_SUPPORT
from tests.product_support import _admin, _callback_update, _user


def test_trial_is_one_time_keeps_basic_tier_and_sends_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=TZ)
    monkeypatch.setattr(product_state, "now", lambda: now)
    cfg = storage.UserData(
        authorized_users={"1": _admin(1), "42": _user(42)},
    )
    outcome, request_id = product_customer.apply_trial_comment(
        cfg,
        user_id=42,
        comment="Хочу проверить подключение",
    )
    request = cfg.service_requests[str(request_id)]

    updated = product_operations.finalize_trial(
        cfg,
        request,
        cfg.authorized_users["1"],
        "https://connect.test/trial",
    )

    assert outcome == "created"
    assert updated["service_tier"] == "basic"
    assert updated["trial_issued_at"] == now.isoformat()
    assert updated["trial_end_at"] == (now + timedelta(hours=24)).isoformat()
    assert updated["trial_duration_hours"] == 24
    assert updated["connection_url"] == "https://connect.test/trial"
    assert cfg.service_requests[str(request_id)]["status"] == "approved"
    assert {event["kind"] for event in cfg.outbox.values()} == {
        "trial_request",
        "trial_approved",
        "trial_connection",
    }
    assert (
        product_customer.apply_trial_comment(
            cfg,
            user_id=42,
            comment="Ещё раз",
        )[0]
        == "issued"
    )


@pytest.mark.asyncio
async def test_trial_callback_flow_claims_link_and_completes_request(
    isolated_storage: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=TZ)
    monkeypatch.setattr(product_state, "now", lambda: now)

    def _seed(cfg: storage.UserData) -> int:
        cfg.authorized_users = {"1": _admin(1), "42": _user(42)}
        request = product_operations.create_request(
            cfg,
            kind="trial",
            user_id=42,
            comment="Проверка подключения",
        )
        return int(request["id"])

    request_id = await storage.update_user_data(_seed)
    update, context = _callback_update(1, f"product:req:approve:{request_id}")

    state = await product_review.product_request_action_cb(update, context)

    assert state == product_state.PRODUCT_INPUT
    claimed = storage.service_requests_snapshot()[str(request_id)]
    assert claimed["status"] == "awaiting_link"
    assert claimed["claimed_by_id"] == 1

    update.callback_query = None
    update.effective_message.text = "https://connect.test/trial-callback"
    finished_state = await product_input.product_text_input(update, context)

    assert finished_state == ConversationHandler.END
    current = storage.get_user_meta_copy(42)
    assert current is not None
    assert current["service_tier"] == "basic"
    assert current["trial_issued_at"] == now.isoformat()
    assert current["trial_end_at"] == (now + timedelta(hours=24)).isoformat()
    assert current["trial_duration_hours"] == 24
    assert current["connection_url"] == "https://connect.test/trial-callback"
    assert storage.service_requests_snapshot()[str(request_id)]["status"] == "approved"
    assert {event["kind"] for _, event in storage.outbox_snapshot()} == {
        "trial_approved",
        "trial_connection",
    }


def test_payment_activation_promotes_user_and_delivers_new_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=TZ)
    monkeypatch.setattr(product_state, "now", lambda: now)
    cfg = storage.UserData(authorized_users={"1": _admin(1, admin_level="owner"), "42": _user(42)})
    request = product_operations.create_request(
        cfg,
        kind="purchase",
        user_id=42,
        status="payment_reported",
        target_end_at=(now + timedelta(days=90)).isoformat(),
    )

    updated = product_operations.finalize_payment(
        cfg,
        request,
        cfg.authorized_users["1"],
        connection_url="https://connect.test/paid",
    )

    assert updated["service_tier"] == "subscriber"
    assert updated["is_paid"] is True
    assert updated["connection_url"] == "https://connect.test/paid"
    assert updated["subscription_end_at"] == (now + timedelta(days=90)).isoformat()
    assert cfg.service_requests[str(request["id"])]["status"] == "approved"
    assert {event["kind"] for event in cfg.outbox.values()} == {"payment_approved", "payment_connection"}


@pytest.mark.asyncio
async def test_purchase_callbacks_send_requisites_and_require_owner_confirmation(
    isolated_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=TZ)
    monkeypatch.setattr(product_state, "now", lambda: now)

    def _seed(cfg: storage.UserData) -> int:
        cfg.authorized_users = {
            "1": _admin(1, admin_level="owner"),
            "2": _admin(2),
            "42": _user(42),
        }
        cfg.product_settings = normalize_product_settings(
            {
                "payment_message": (
                    "Переведите {amount} ₽ за {months} мес.\nДоступ до {access_until}\n<обычный текст>"
                ),
                "current_period_end": (now + timedelta(days=90)).isoformat(),
            }
        )
        request = product_operations.create_request(
            cfg,
            kind="purchase",
            user_id=42,
            target_end_at=(now + timedelta(days=90)).isoformat(),
        )
        return int(request["id"])

    request_id = await storage.update_user_data(_seed)

    admin_update, context = _callback_update(2, f"product:req:requisites:{request_id}")
    assert await product_review.product_request_action_cb(admin_update, context) == ConversationHandler.END
    assert storage.service_requests_snapshot()[str(request_id)]["status"] == "requisites_sent"
    assert {event["kind"] for _, event in storage.outbox_snapshot()} == {"payment_requisites"}
    requisites = next(event for _, event in storage.outbox_snapshot() if event["kind"] == "payment_requisites")
    assert requisites["payload"]["parse_mode"] == ""
    assert requisites["payload"]["text"] == (
        f"Переведите {PLAN_TOTAL_RUB} ₽ за 3 мес.\n"
        f"Доступ до {product_state.datetime_text((now + timedelta(days=90)).isoformat())}\n"
        "<обычный текст>"
    )

    user_update, user_context = _callback_update(42, f"subscription:paid:{request_id}")
    await product_payment_reports.payment_reported_cb(user_update, user_context)
    assert storage.service_requests_snapshot()[str(request_id)]["status"] == "payment_reported"
    assert {event["kind"] for _, event in storage.outbox_snapshot()} == {
        "payment_requisites",
        "payment_reported",
    }

    owner_update, owner_context = _callback_update(1, f"product:req:confirm:{request_id}")
    assert await product_review.product_request_action_cb(owner_update, owner_context) == product_state.PRODUCT_INPUT
    assert storage.service_requests_snapshot()[str(request_id)]["status"] == "awaiting_link"

    # Потерянный Telegram-диалог можно восстановить из карточки занятой заявки.
    reopened_update, reopened_context = _callback_update(1, f"product:req:confirm:{request_id}")
    assert (
        await product_review.product_request_action_cb(reopened_update, reopened_context) == product_state.PRODUCT_INPUT
    )
    reopened_update.callback_query = None
    reopened_update.effective_message.text = "https://connect.test/paid-callback"
    assert await product_input.product_text_input(reopened_update, reopened_context) == ConversationHandler.END

    current = storage.get_user_meta_copy(42)
    assert current is not None
    assert current["service_tier"] == "subscriber"
    assert current["is_paid"] is True
    assert current["subscription_end_at"] == (now + timedelta(days=90)).isoformat()
    assert storage.service_requests_snapshot()[str(request_id)]["status"] == "approved"
    assert {event["kind"] for _, event in storage.outbox_snapshot()} == {
        "payment_requisites",
        "payment_reported",
        "payment_approved",
        "payment_connection",
    }


def test_regular_admin_cannot_finalize_payment(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=TZ)
    monkeypatch.setattr(product_state, "now", lambda: now)
    cfg = storage.UserData(authorized_users={"1": _admin(1), "42": _user(42)})
    request = product_operations.create_request(
        cfg,
        kind="purchase",
        user_id=42,
        status="payment_reported",
        target_end_at=(now + timedelta(days=90)).isoformat(),
    )

    with pytest.raises(ValueError, match="owner_required"):
        product_operations.finalize_payment(
            cfg,
            request,
            cfg.authorized_users["1"],
            connection_url="https://connect.test/paid",
        )


def test_support_can_use_standard_trial_but_only_owner_can_change_duration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=TZ)
    monkeypatch.setattr(product_state, "now", lambda: now)
    support = _admin(2)
    owner = _admin(1, admin_level="owner")

    support_cfg = storage.UserData(
        authorized_users={"1": owner, "2": support, "42": _user(42)},
    )
    support_request = product_operations.create_request(support_cfg, kind="trial", user_id=42)
    forbidden, _request = product_review_operations.approve_trial(
        support_cfg,
        request_id=int(support_request["id"]),
        actor=support,
        duration_hours=48,
    )
    standard, claimed = product_review_operations.approve_trial(
        support_cfg,
        request_id=int(support_request["id"]),
        actor=support,
    )

    assert forbidden == "duration_forbidden"
    with pytest.raises(ValueError, match="duration_forbidden"):
        product_operations.finalize_trial(
            support_cfg,
            {**support_request, "trial_duration_hours": 48},
            support,
            "https://connect.test/forged-custom",
        )
    assert standard == "need_link"
    assert claimed is not None
    assert claimed["trial_duration_hours"] == 24
    assert claimed["target_end_at"] == (now + timedelta(hours=24)).isoformat()

    owner_cfg = storage.UserData(
        authorized_users={
            "1": owner,
            "43": _user(43, connection_url="https://connect.test/custom"),
        }
    )
    owner_request = product_operations.create_request(owner_cfg, kind="trial", user_id=43)
    custom, claimed_custom = product_review_operations.approve_trial(
        owner_cfg,
        request_id=int(owner_request["id"]),
        actor=owner,
        duration_hours=72,
    )

    assert custom == "need_link"
    assert claimed_custom is not None
    assert owner_cfg.authorized_users["43"]["connection_url"] == "https://connect.test/custom"
    with pytest.raises(ValueError, match="connection_missing"):
        product_operations.finalize_trial(owner_cfg, claimed_custom, owner, None)
    product_operations.finalize_trial(
        owner_cfg,
        claimed_custom,
        owner,
        "https://connect.test/custom-72h",
    )
    assert owner_cfg.authorized_users["43"]["trial_end_at"] == (now + timedelta(hours=72)).isoformat()
    assert owner_cfg.authorized_users["43"]["trial_duration_hours"] == 72
    assert owner_cfg.authorized_users["43"]["connection_url"] == "https://connect.test/custom-72h"


def test_payment_decision_operations_enforce_owner_server_side(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=TZ)
    monkeypatch.setattr(product_state, "now", lambda: now)
    support = _admin(2)
    cfg = storage.UserData(
        authorized_users={"2": support, "42": _user(42)},
    )
    request = product_operations.create_request(
        cfg,
        kind="purchase",
        user_id=42,
        status="payment_reported",
        target_end_at=(now + timedelta(days=90)).isoformat(),
    )

    assert (
        product_review_operations.confirm_payment(
            cfg,
            request_id=int(request["id"]),
            actor=support,
        )[0]
        == "owner_only"
    )
    assert (
        product_review_operations.reset_unconfirmed_payment(
            cfg,
            request_id=int(request["id"]),
            actor=support,
        )
        == "owner_only"
    )
    assert cfg.service_requests[str(request["id"])]["status"] == "payment_reported"


def test_support_can_reject_pending_trial_or_purchase() -> None:
    support = _admin(2)
    cfg = storage.UserData(
        authorized_users={"2": support, "42": _user(42), "43": _user(43)},
    )
    trial = product_operations.create_request(cfg, kind="trial", user_id=42)
    purchase = product_operations.create_request(cfg, kind="purchase", user_id=43)

    assert (
        product_review_operations.reject_request(
            cfg,
            request_id=int(trial["id"]),
            actor=support,
        )
        == "rejected"
    )
    assert (
        product_review_operations.reject_request(
            cfg,
            request_id=int(purchase["id"]),
            actor=support,
        )
        == "rejected"
    )


def test_owner_can_confirm_payment_for_regular_staff_without_changing_staff_role(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=TZ)
    monkeypatch.setattr(product_state, "now", lambda: now)
    cfg = storage.UserData(
        authorized_users={
            "1": _admin(1, admin_level="owner"),
            "2": _admin(
                2,
                service_tier="basic",
                is_paid=False,
                connection_url="https://connect.test/staff",
            ),
        }
    )
    request = product_operations.create_request(
        cfg,
        kind="purchase",
        user_id=2,
        status="payment_reported",
        target_end_at=(now + timedelta(days=90)).isoformat(),
    )

    updated = product_operations.finalize_payment(
        cfg,
        request,
        cfg.authorized_users["1"],
    )

    assert updated["role"] == "admin"
    assert updated["admin_level"] == "admin"
    assert updated["staff_title"] == STAFF_TITLE_SUPPORT
    assert updated["service_tier"] == "subscriber"
    assert updated["is_paid"] is True
    assert updated["subscription_end_at"] == (now + timedelta(days=90)).isoformat()
