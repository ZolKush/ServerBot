from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from telegram.ext import ConversationHandler

from app import storage
from app.config import TZ
from app.subscriptions.connections import _dashboard_markup, _dashboard_text
from app.subscriptions.requests import confirmation as product_confirmation
from app.subscriptions.requests import input_processing as product_input
from app.subscriptions.requests import input_start as product_input_start
from app.subscriptions.requests import operations as product_operations
from app.subscriptions.requests import reminders as product_reminders
from app.subscriptions.requests import state as product_state
from app.subscriptions.requests import views as product_views
from app.users.staff import STAFF_TITLE_LEAD
from tests.product_support import _admin, _callback_names, _callback_update, _user


@pytest.mark.asyncio
async def test_manual_payment_conversation_accepts_regular_staff_account(
    isolated_storage: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=TZ)
    monkeypatch.setattr(product_state, "now", lambda: now)

    def _seed(cfg: storage.UserData) -> None:
        cfg.authorized_users = {
            "1": _admin(1, admin_level="owner"),
            "2": _admin(
                2,
                service_tier="basic",
                is_paid=False,
                connection_url="https://connect.test/staff",
            ),
        }

    await storage.update_user_data(_seed)
    start, context = _callback_update(1, "product:input:manualpay:2")
    assert await product_input_start.product_input_start_cb(start, context) == product_state.PRODUCT_INPUT

    start.callback_query = None
    start.effective_message.text = "20.10.2026 18:00"
    assert await product_input.product_text_input(start, context) == product_state.PRODUCT_CONFIRM

    confirm, _unused = _callback_update(1, "product:confirm:apply")
    assert await product_confirmation.product_confirm_cb(confirm, context) == ConversationHandler.END

    staff = storage.get_user_meta_copy(2)
    assert staff is not None
    assert staff["role"] == "admin"
    assert staff["is_paid"] is True
    assert staff["subscription_end_at"] == datetime(2026, 10, 20, 18, 0, tzinfo=TZ).isoformat()


def test_staff_subscription_dashboard_supports_payment_but_owner_is_exempt() -> None:
    staff = _admin(2, service_tier="basic", is_paid=False)
    staff_callbacks = _callback_names(_dashboard_markup(staff))
    owner = _admin(1, admin_level="owner", service_tier="basic", is_paid=False)
    owner_callbacks = _callback_names(_dashboard_markup(owner))

    assert "subscription:buy" in staff_callbacks
    assert "subscription:trial" not in staff_callbacks
    assert "subscription:buy" not in owner_callbacks
    assert "Бессрочный оплаченный доступ — руководитель сервиса" in _dashboard_text(owner)
    assert "Доступ до: <code>бессрочно</code>" in _dashboard_text(owner)


def test_payment_card_uses_nickname_as_primary_identity_and_includes_backup_email() -> None:
    cfg = storage.UserData(authorized_users={"42": _user(42, nickname="Panel User", contact_email="u@example.com")})
    request = product_operations.create_request(
        cfg,
        kind="purchase",
        user_id=42,
    )

    text = product_views.request_card(request, cfg.authorized_users["42"])

    assert "Никнейм: <b>Panel User</b>" in text
    assert "Имя Telegram: <b>User 42</b>" in text
    assert "Резервная почта: <code>u@example.com</code>" in text


def test_payment_cancels_incompatible_trial_request(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=TZ)
    monkeypatch.setattr(product_state, "now", lambda: now)
    cfg = storage.UserData(authorized_users={"1": _admin(1, admin_level="owner"), "42": _user(42)})
    trial = product_operations.create_request(
        cfg,
        kind="trial",
        user_id=42,
        comment="Тест",
    )
    purchase = product_operations.create_request(
        cfg,
        kind="purchase",
        user_id=42,
        status="payment_reported",
        target_end_at=(now + timedelta(days=90)).isoformat(),
    )

    product_operations.finalize_payment(
        cfg,
        purchase,
        cfg.authorized_users["1"],
        connection_url="https://connect.test/paid",
    )

    assert cfg.service_requests[str(trial["id"])]["status"] == "cancelled"
    assert cfg.authorized_users["42"]["connection_url"] == "https://connect.test/paid"
    with pytest.raises(ValueError, match="tier_changed"):
        product_operations.finalize_trial(
            cfg,
            trial,
            cfg.authorized_users["1"],
            "https://connect.test/stale-trial",
        )
    assert cfg.authorized_users["42"]["connection_url"] == "https://connect.test/paid"


def test_manual_reminder_only_targets_active_paid_subscribers(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=TZ)
    monkeypatch.setattr(product_state, "now", lambda: now)
    cfg = storage.UserData(
        authorized_users={
            "1": _admin(1, staff_title=STAFF_TITLE_LEAD),
            "10": _user(
                10,
                service_tier="subscriber",
                is_paid=True,
                subscription_end_at=(now + timedelta(days=2)).isoformat(),
            ),
            "11": _user(11),
            "12": _user(12, service_tier="unlimited_trial"),
            "13": _user(
                13,
                access_state="logged_out",
                enabled=False,
                service_tier="subscriber",
                is_paid=True,
                subscription_end_at=(now + timedelta(days=2)).isoformat(),
            ),
        },
        product_settings={
            "payment_bank": "Банк",
            "payment_recipient": "Получатель",
            "payment_phone": "+70000000000",
            "current_period_end": (now + timedelta(days=90)).isoformat(),
        },
    )

    sent, skipped = product_reminders.queue_manual_reminders(
        cfg,
        actor=cfg.authorized_users["1"],
        target_ids=[10, 11, 12, 13],
    )

    assert (sent, skipped) == (1, 3)
    assert cfg.authorized_users["10"]["last_manual_payment_reminder_at"] == now.isoformat()
    payload = next(iter(cfg.outbox.values()))["payload"]
    assert "Ведущий инженер сопровождения" in payload["text"]
    assert "Admin 1" not in payload["text"]
