from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.error import TimedOut

from app import storage
from app.config import TZ
from app.messaging import outbox
from app.subscriptions.requests import lifecycle, operations, state
from tests.product_support import _admin, _user


@pytest.fixture
def clock(monkeypatch):
    current = [datetime(2026, 9, 19, 12, tzinfo=TZ)]
    monkeypatch.setattr(state, "now", lambda: current[0])
    return current


async def _seed_subscriber(end):
    def apply(cfg):
        cfg.authorized_users = {
            "1": _admin(1, admin_level="owner"),
            "42": _user(
                42,
                service_tier="subscriber",
                is_paid=True,
                subscription_end_at=end.isoformat(),
                connection_url="https://connect.test/paid",
            ),
        }

    await storage.update_user_data(apply)


async def _renew(end):
    def apply(cfg):
        request = operations.create_request(
            cfg, kind="renewal", user_id=42, status="payment_reported", target_end_at=end.isoformat()
        )
        return operations.finalize_payment(cfg, request, cfg.authorized_users["1"])

    return await storage.update_user_data(apply)


def _expiry_events():
    return [event for _, event in storage.outbox_snapshot() if event["kind"] == "subscription_expired"]


@pytest.mark.asyncio
async def test_expiry_queues_explicit_basic_notice_once_at_exact_deadline(isolated_storage, clock):
    await _seed_subscriber(clock[0])

    # Reporting a transfer alone does not confirm the next paid period.
    await storage.update_user_data(
        lambda cfg: operations.create_request(
            cfg,
            kind="renewal",
            user_id=42,
            status="payment_reported",
            target_end_at=(clock[0] + timedelta(days=30)).isoformat(),
        )
    )
    await lifecycle.subscription_lifecycle_job(SimpleNamespace())
    await lifecycle.subscription_lifecycle_job(SimpleNamespace())

    meta = storage.get_user_meta_copy(42)
    assert meta["service_tier"] == "basic"
    assert meta["is_paid"] is False
    assert meta["enabled"] is True
    assert meta["access_state"] == "approved"
    assert meta["connection_url"] == "https://connect.test/paid"
    events = _expiry_events()
    assert len(events) == 1
    assert set(events[0]["recipients"]) == {"42"}
    assert events[0]["recipients"]["42"]["status"] == "pending"
    assert "Доступ понижен до базового" in events[0]["payload"]["text"]
    assert "оплата следующего периода не подтверждена" in events[0]["payload"]["text"]
    assert {button["callback_data"] for row in events[0]["payload"]["reply_markup"] for button in row} == {
        "subscription:buy",
        "menu:ticket",
    }


@pytest.mark.asyncio
async def test_confirmed_renewal_does_not_expire_at_previous_deadline(isolated_storage, clock):
    end = clock[0] + timedelta(minutes=1)
    await _seed_subscriber(end)
    await _renew(end + timedelta(days=30))

    clock[0] = end
    await lifecycle.subscription_lifecycle_job(SimpleNamespace())

    meta = storage.get_user_meta_copy(42)
    assert meta["service_tier"] == "subscriber"
    assert meta["is_paid"] is True
    assert not _expiry_events()


@pytest.mark.asyncio
async def test_notice_and_downgrade_roll_back_together_on_queue_failure(isolated_storage, clock, monkeypatch):
    await _seed_subscriber(clock[0])
    queue_message = lifecycle.queue_message

    def fail_queue(*args, **kwargs):
        raise OSError("disk")

    monkeypatch.setattr(lifecycle, "queue_message", fail_queue)

    with pytest.raises(OSError, match="disk"):
        await lifecycle.subscription_lifecycle_job(SimpleNamespace())

    assert storage.get_user_meta_copy(42)["service_tier"] == "subscriber"
    assert not _expiry_events()
    monkeypatch.setattr(lifecycle, "queue_message", queue_message)
    await lifecycle.subscription_lifecycle_job(SimpleNamespace())
    assert storage.get_user_meta_copy(42)["service_tier"] == "basic"
    assert len(_expiry_events()) == 1


@pytest.mark.asyncio
async def test_owner_and_unlimited_access_do_not_expire(isolated_storage, clock):
    past_end = (clock[0] - timedelta(days=1)).isoformat()
    await storage.update_user_data(
        lambda cfg: cfg.authorized_users.update(
            {
                "1": _admin(1, admin_level="owner", subscription_end_at=past_end),
                "42": _user(42, service_tier="unlimited_trial", subscription_end_at=past_end),
            }
        )
    )
    owner_before = storage.get_user_meta_copy(1)

    await lifecycle.subscription_lifecycle_job(SimpleNamespace())

    assert storage.get_user_meta_copy(1) == owner_before
    assert storage.get_user_meta_copy(42)["service_tier"] == "unlimited_trial"
    assert not _expiry_events()


@pytest.mark.asyncio
@pytest.mark.parametrize("delivery_status", ["pending", "dead_letter"])
async def test_renewal_withdraws_only_its_undelivered_expiry_notice(isolated_storage, clock, delivery_status):
    await _seed_subscriber(clock[0])
    await lifecycle.subscription_lifecycle_job(SimpleNamespace())
    event_id = _expiry_events()[0]["id"]

    def seed_other_notices(cfg):
        cfg.outbox[event_id]["recipients"]["42"]["status"] = delivery_status
        operations.queue_message(cfg, recipient_ids=[43], kind="subscription_expired", text="Other expiry")
        operations.queue_message(cfg, recipient_ids=[42], kind="trial_expired", text="Other notification")

    await storage.update_user_data(seed_other_notices)
    await _renew(clock[0] + timedelta(days=30))

    assert storage.get_user_meta_copy(42)["service_tier"] == "subscriber"
    assert storage.get_outbox_event("user", event_id) is None
    assert len(_expiry_events()) == 1
    assert set(_expiry_events()[0]["recipients"]) == {"43"}
    assert any(event["kind"] == "trial_expired" for _, event in storage.outbox_snapshot())
    assert any(event["kind"] == "payment_approved" for _, event in storage.outbox_snapshot())


@pytest.mark.asyncio
@pytest.mark.parametrize("delivery_status", ["delivered", "terminal"])
async def test_renewal_preserves_finished_expiry_delivery_states(isolated_storage, clock, delivery_status):
    await _seed_subscriber(clock[0])
    await lifecycle.subscription_lifecycle_job(SimpleNamespace())
    event_id = _expiry_events()[0]["id"]
    await storage.update_user_data(lambda cfg: cfg.outbox[event_id]["recipients"]["42"].update(status=delivery_status))

    await _renew(clock[0] + timedelta(days=30))

    assert storage.get_outbox_event("user", event_id)["recipients"]["42"]["status"] == delivery_status


@pytest.mark.asyncio
async def test_failed_payment_transaction_preserves_queued_expiry_notice(isolated_storage, clock, monkeypatch):
    await _seed_subscriber(clock[0])
    await lifecycle.subscription_lifecycle_job(SimpleNamespace())
    event_id = _expiry_events()[0]["id"]

    def fail_queue(*args, **kwargs):
        raise OSError("disk")

    monkeypatch.setattr(operations, "queue_message", fail_queue)
    with pytest.raises(OSError, match="disk"):
        await _renew(clock[0] + timedelta(days=30))

    assert storage.get_user_meta_copy(42)["service_tier"] == "basic"
    assert storage.get_outbox_event("user", event_id)["recipients"]["42"]["status"] == "pending"
    assert not any(event["kind"] == "payment_approved" for _, event in storage.outbox_snapshot())


@pytest.mark.asyncio
async def test_outbox_discards_expiry_snapshot_after_payment_during_another_delivery(isolated_storage, clock):
    await _seed_subscriber(clock[0])
    await storage.update_user_data(
        lambda cfg: operations.queue_message(cfg, recipient_ids=[42], kind="other", text="Other notification")
    )
    await lifecycle.subscription_lifecycle_job(SimpleNamespace())

    async def confirm_payment(**kwargs):
        await _renew(clock[0] + timedelta(days=30))

    bot = SimpleNamespace(send_message=AsyncMock(side_effect=confirm_payment))
    await outbox.process_outbox(bot)

    bot.send_message.assert_awaited_once()
    assert bot.send_message.await_args.kwargs["text"] == "Other notification"
    assert not _expiry_events()
    assert storage.get_user_meta_copy(42)["service_tier"] == "subscriber"


@pytest.mark.asyncio
async def test_expiry_notice_remains_retryable_after_telegram_timeout(isolated_storage, clock):
    await _seed_subscriber(clock[0])
    await lifecycle.subscription_lifecycle_job(SimpleNamespace())
    event_id = _expiry_events()[0]["id"]
    bot = SimpleNamespace(send_message=AsyncMock(side_effect=TimedOut()))

    await outbox.process_outbox(bot)
    await lifecycle.subscription_lifecycle_job(SimpleNamespace())

    assert storage.get_user_meta_copy(42)["service_tier"] == "basic"
    assert len(_expiry_events()) == 1
    assert _expiry_events()[0]["id"] == event_id
    assert _expiry_events()[0]["recipients"]["42"]["status"] == "pending"
    assert _expiry_events()[0]["recipients"]["42"]["attempts"] == 1
    await storage.update_user_data(lambda cfg: cfg.outbox[event_id]["recipients"]["42"].update(next_attempt_at=""))
    bot.send_message = AsyncMock()
    await outbox.process_outbox(bot)
    bot.send_message.assert_awaited_once()
    assert not _expiry_events()
