from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from app import storage
from app.bot.menu import main_menu_inline_kb_for_meta
from app.config import TZ
from app.persistence.normalization import normalize_product_settings
from app.subscriptions.connections import (
    _dashboard_markup,
    connection_outbox_payload,
    is_valid_connection_url,
    trial_access_expired,
)
from app.subscriptions.requests import flow_cleanup as product_flow_cleanup
from app.subscriptions.requests import lifecycle as product_lifecycle
from app.subscriptions.requests import state as product_state
from app.users.views import format_user_card
from tests.product_support import _admin, _callback_names, _callback_update, _user


@pytest.mark.asyncio
async def test_lifecycle_sends_each_reminder_once_and_expires_access(
    isolated_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    now_box = [datetime(2026, 7, 15, 12, 0, tzinfo=TZ)]
    monkeypatch.setattr(product_state, "now", lambda: now_box[0])
    end = now_box[0] + timedelta(days=2)

    def _seed(cfg: storage.UserData) -> None:
        cfg.authorized_users = {
            "1": _admin(1, admin_level="owner"),
            "10": _user(
                10,
                service_tier="subscriber",
                is_paid=True,
                connection_url="https://connect.test/10",
                subscription_end_at=end.isoformat(),
            ),
            "11": _user(
                11,
                access_state="logged_out",
                enabled=False,
                service_tier="subscriber",
                is_paid=True,
                connection_url="https://connect.test/11",
                subscription_end_at=(now_box[0] - timedelta(minutes=1)).isoformat(),
            ),
            "12": _user(12, service_tier="unlimited_trial"),
        }
        cfg.product_settings = normalize_product_settings(
            {
                "payment_bank": "Банк",
                "payment_recipient": "Получатель",
                "payment_phone": "+70000000000",
                "current_period_end": end.isoformat(),
                "next_period_end": (end + timedelta(days=90)).isoformat(),
            }
        )

    await storage.update_user_data(_seed)
    context = SimpleNamespace()

    await product_lifecycle.subscription_lifecycle_job(context)
    first_events = storage.outbox_snapshot()
    await product_lifecycle.subscription_lifecycle_job(context)

    user10 = storage.get_user_meta_copy(10)
    user11 = storage.get_user_meta_copy(11)
    assert user10["last_auto_payment_reminder_type"] == "3d"
    assert user11["service_tier"] == "basic"
    assert user11["is_paid"] is False
    assert user11["connection_url"] == "https://connect.test/11"
    assert len(storage.outbox_snapshot()) == len(first_events)
    assert not any(
        event["kind"] == "subscription_expired" and "11" in event["recipients"]
        for _, event in storage.outbox_snapshot()
    )

    now_box[0] = end - timedelta(hours=12)
    await product_lifecycle.subscription_lifecycle_job(context)
    assert storage.get_user_meta_copy(10)["last_auto_payment_reminder_type"] == "1d"

    now_box[0] = end - timedelta(minutes=10)
    await product_lifecycle.subscription_lifecycle_job(context)
    assert storage.get_user_meta_copy(10)["last_auto_payment_reminder_type"] == "15m"
    fifteen_minute_events = [
        event for _, event in storage.outbox_snapshot() if event["kind"] == "subscription_reminder_15m"
    ]
    assert len(fifteen_minute_events) == 1
    assert "Банк" in fifteen_minute_events[0]["payload"]["text"]
    assert "Получатель" in fifteen_minute_events[0]["payload"]["text"]
    assert "+70000000000" in fifteen_minute_events[0]["payload"]["text"]
    await product_lifecycle.subscription_lifecycle_job(context)
    assert len([event for _, event in storage.outbox_snapshot() if event["kind"] == "subscription_reminder_15m"]) == 1

    now_box[0] = end + timedelta(minutes=1)
    await product_lifecycle.subscription_lifecycle_job(context)
    expired = storage.get_user_meta_copy(10)
    assert expired["service_tier"] == "basic"
    assert expired["is_paid"] is False
    assert expired["connection_url"] == "https://connect.test/10"
    assert storage.product_settings_snapshot()["current_period_end"] == (end + timedelta(days=90)).isoformat()
    assert storage.product_settings_snapshot()["next_period_end"] is None


@pytest.mark.asyncio
async def test_staff_subscription_expiry_preserves_administrator_role(
    isolated_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=TZ)
    monkeypatch.setattr(product_state, "now", lambda: now)

    def _seed(cfg: storage.UserData) -> None:
        cfg.authorized_users = {
            "1": _admin(1, admin_level="owner"),
            "2": _admin(
                2,
                service_tier="subscriber",
                is_paid=True,
                connection_url="https://connect.test/staff",
                subscription_end_at=(now - timedelta(minutes=1)).isoformat(),
            ),
        }

    await storage.update_user_data(_seed)
    await product_lifecycle.subscription_lifecycle_job(SimpleNamespace())

    staff = storage.get_user_meta_copy(2)
    assert staff is not None
    assert staff["role"] == "admin"
    assert staff["admin_level"] == "admin"
    assert staff["service_tier"] == "basic"
    assert staff["is_paid"] is False
    assert staff["connection_url"] == "https://connect.test/staff"
    assert any(
        event["kind"] == "subscription_expired" and "2" in event["recipients"] for _, event in storage.outbox_snapshot()
    )


@pytest.mark.asyncio
async def test_trial_link_is_removed_at_deadline_and_notification_is_sent_once(
    isolated_storage: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=TZ)
    monkeypatch.setattr(product_state, "now", lambda: now)

    def _seed(cfg: storage.UserData) -> None:
        cfg.authorized_users = {
            "42": _user(
                42,
                connection_url="https://connect.test/trial",
                trial_issued_at=(now - timedelta(hours=24)).isoformat(),
                trial_end_at=(now - timedelta(seconds=1)).isoformat(),
                trial_duration_hours=24,
            )
        }

    await storage.update_user_data(_seed)
    before = storage.get_user_meta_copy(42)
    assert before is not None
    assert trial_access_expired(before, at=now)
    assert "subscription:connection" not in _callback_names(_dashboard_markup(before))

    await product_lifecycle.subscription_lifecycle_job(SimpleNamespace())
    await product_lifecycle.subscription_lifecycle_job(SimpleNamespace())

    expired = storage.get_user_meta_copy(42)
    assert expired is not None
    assert expired["connection_url"] is None
    assert expired["trial_end_at"] == (now - timedelta(seconds=1)).isoformat()
    events = [event for _, event in storage.outbox_snapshot() if event["kind"] == "trial_expired"]
    assert len(events) == 1
    assert "42" in events[0]["recipients"]


@pytest.mark.asyncio
async def test_stale_connection_claim_is_released(isolated_storage: None, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=TZ)
    monkeypatch.setattr(product_state, "now", lambda: now)

    def _seed(cfg: storage.UserData) -> None:
        cfg.authorized_users = {"1": _admin(1), "42": _user(42)}
        cfg.service_requests = {
            "1": {
                "id": 1,
                "kind": "trial",
                "status": "awaiting_link",
                "user_id": 42,
                "resume_status": "pending",
                "claimed_by_id": 1,
                "claimed_at": (now - product_state.REQUEST_CLAIM_TIMEOUT - timedelta(seconds=1)).isoformat(),
            }
        }
        cfg.request_seq = 1

    await storage.update_user_data(_seed)
    await product_lifecycle.subscription_lifecycle_job(SimpleNamespace())

    request = storage.service_requests_snapshot()["1"]
    assert request["status"] == "pending"
    assert request["claimed_by_id"] is None
    assert request["claimed_at"] is None


@pytest.mark.asyncio
async def test_navigation_releases_current_connection_claim(isolated_storage: None) -> None:
    def _seed(cfg: storage.UserData) -> None:
        cfg.authorized_users = {"1": _admin(1), "42": _user(42)}
        cfg.service_requests = {
            "1": {
                "id": 1,
                "kind": "trial",
                "status": "awaiting_link",
                "user_id": 42,
                "resume_status": "pending",
                "claimed_by_id": 1,
                "claimed_at": datetime.now(TZ).isoformat(),
            }
        }
        cfg.request_seq = 1

    await storage.update_user_data(_seed)
    update, context = _callback_update(1, "menu:users")
    context.user_data.update(
        {
            "product_input_action": "request_link",
            "product_request_id": 1,
            "product_target_uid": 42,
        }
    )

    await product_flow_cleanup.abandon_product_flow(update, context)

    request = storage.service_requests_snapshot()["1"]
    assert request["status"] == "pending"
    assert request["claimed_by_id"] is None
    assert request["claimed_at"] is None
    assert context.user_data == {}


def test_menu_hides_server_status_from_basic_users() -> None:
    basic = _callback_names(main_menu_inline_kb_for_meta(_user(1)))
    subscriber = _callback_names(main_menu_inline_kb_for_meta(_user(2, service_tier="subscriber")))
    unlimited = _callback_names(main_menu_inline_kb_for_meta(_user(3, service_tier="unlimited_trial")))

    assert "menu:status" not in basic
    assert "menu:subscription" in basic
    assert "menu:ticket" in basic
    assert "menu:status" in subscriber
    assert "menu:status" in unlimited


def test_connection_payload_uses_link_terminology_and_url_button() -> None:
    payload = connection_outbox_payload(_user(42, connection_url="https://connect.test/42"))

    assert "персональная ссылка подключения" in payload["text"].lower()
    assert payload["reply_markup"][0][0]["url"] == "https://connect.test/42"
    assert "конфиг" not in payload["text"].lower()


@pytest.mark.parametrize(
    "value",
    ["", "ftp://connect.test/u", "https://", "https://connect.test/bad value", "https://connect.test:bad/u"],
)
def test_invalid_connection_urls_are_rejected(value: str) -> None:
    assert not is_valid_connection_url(value)


def test_valid_connection_url_is_accepted() -> None:
    assert is_valid_connection_url("https://connect.test/path?token=abc")


def test_admin_user_card_stays_within_telegram_limit_for_malformed_data() -> None:
    meta = _admin(
        1,
        nickname="&" * 10_000,
        first_name="&" * 10_000,
        last_name="&" * 10_000,
        staff_alias="&" * 10_000,
        subscription_end_at="&" * 10_000,
    )

    assert len(format_user_card(meta)) < 4096
