from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from app import storage
from app.config import TZ
from app.subscriptions.connections import trial_access_expired
from app.subscriptions.requests import lifecycle, operations, state
from tests.product_support import _admin, _user


@pytest.mark.asyncio
async def test_paid_link_survives_expiry_after_a_previous_trial(isolated_storage, monkeypatch):
    now = datetime(2026, 9, 7, 12, tzinfo=TZ)
    monkeypatch.setattr(state, "now", lambda: now)
    user = _user(
        42,
        connection_url="https://connect.test/paid",
        trial_issued_at=(now - timedelta(days=40)).isoformat(),
        trial_end_at=(now - timedelta(days=39)).isoformat(),
        paid_at=(now - timedelta(days=30)).isoformat(),
        subscription_end_at=(now - timedelta(seconds=1)).isoformat(),
        service_tier="subscriber",
        is_paid=True,
    )
    await storage.update_user_data(lambda cfg: cfg.authorized_users.update({"42": user}))
    await lifecycle.subscription_lifecycle_job(SimpleNamespace())
    expired = storage.get_user_meta_copy(42)
    assert expired["service_tier"] == "basic"
    assert expired["connection_url"] == "https://connect.test/paid"
    assert not trial_access_expired(expired, at=now)
    assert all(event["kind"] != "trial_expired" for _, event in storage.outbox_snapshot())


def test_trial_issued_after_an_old_payment_still_expires():
    now = datetime.now(TZ)
    meta = _user(
        42,
        connection_url="https://connect.test/trial",
        paid_at=(now - timedelta(days=30)).isoformat(),
        trial_issued_at=(now - timedelta(days=2)).isoformat(),
        trial_end_at=(now - timedelta(days=1)).isoformat(),
    )
    assert trial_access_expired(meta, at=now)


def test_trial_finalization_preserves_deadline_shown_to_the_operator(monkeypatch):
    now = datetime(2026, 9, 7, 12, tzinfo=TZ)
    monkeypatch.setattr(state, "now", lambda: now)
    cfg = storage.UserData(authorized_users={"1": _admin(1), "42": _user(42)})
    target = now + timedelta(hours=23, minutes=55)
    request = operations.create_request(cfg, kind="trial", user_id=42, target_end_at=target.isoformat())
    result = operations.finalize_trial(cfg, request, cfg.authorized_users["1"], "https://connect.test/trial")
    assert result["trial_end_at"] == target.isoformat()


def test_payment_rejects_invalid_stored_link_before_mutation(monkeypatch):
    now = datetime.now(TZ)
    monkeypatch.setattr(state, "now", lambda: now)
    cfg = storage.UserData(
        authorized_users={"1": _admin(1, admin_level="owner"), "42": _user(42, connection_url="broken")}
    )
    request = operations.create_request(
        cfg, kind="purchase", user_id=42, target_end_at=(now + timedelta(days=30)).isoformat()
    )
    with pytest.raises(ValueError, match="connection_missing"):
        operations.finalize_payment(cfg, request, cfg.authorized_users["1"])
    assert not cfg.authorized_users["42"]["is_paid"]
