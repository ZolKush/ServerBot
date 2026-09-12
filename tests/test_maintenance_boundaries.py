from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from app import storage
from app.config import TZ
from app.maintenance import calendar, jobs, operations, policy, records
from app.messaging.outbox import message_payload


def test_unknown_maintenance_scope_never_expands_to_all_servers():
    assert policy.normalize_scope("removed") == "removed"
    assert policy.scope_label("removed") != policy.scope_label("all")


@pytest.mark.asyncio
async def test_schedule_for_removed_server_is_not_activated(isolated_storage, monkeypatch):
    now = datetime.now(TZ)
    scheduled = dict(
        records.build_scheduled_maintenance_record(
            "all", now - timedelta(minutes=1), now + timedelta(hours=1), 1, "Admin"
        )
    )
    scheduled["scope"] = "removed"
    await storage.set_scheduled_maintenance_record(scheduled)
    monkeypatch.setattr(jobs, "maintenance_manager_ids", lambda: [1])
    await jobs.maint_schedule_tick(SimpleNamespace())
    assert storage.get_active_maintenance() is None
    assert storage.get_scheduled_maintenance() is None
    assert any(event["kind"] == "maintenance_schedule_invalid" for _, event in storage.outbox_snapshot())


@pytest.mark.asyncio
@pytest.mark.parametrize("transition", ["cancel", "expire", "activate", "invalid"])
async def test_obsolete_schedule_warnings_are_removed_atomically(isolated_storage, transition):
    now = datetime.now(TZ)
    scheduled = dict(records.build_scheduled_maintenance_record("all", now, now + timedelta(hours=1), 1, "Admin"))
    await storage.set_scheduled_maintenance_record(scheduled)
    warning = storage.make_outbox_event(
        kind="maintenance_schedule_warning", recipient_ids=[1], payload=message_payload("Soon")
    )
    await operations.mark_schedule_thresholds(
        scheduled, updated_notified=[15], due=[15], updated_at=now, notice_event=warning
    )
    assert any(event["id"] == warning["id"] for _, event in storage.outbox_snapshot())
    if transition == "cancel":
        await operations.cancel_scheduled_maintenance(scheduled["id"], actor_id=1)
    elif transition == "expire":
        await operations.expire_schedule(scheduled, None)
    elif transition == "activate":
        await operations.activate_scheduled_maintenance(scheduled, None)
    else:
        await operations.clear_invalid_schedule(scheduled["id"])
    assert not any(event["id"] == warning["id"] for _, event in storage.outbox_snapshot())


@pytest.mark.parametrize(("year", "month"), [(2026, 99), (9999, 12), (0, 0)])
def test_calendar_safely_bounds_untrusted_navigation(year, month):
    today = datetime(2026, 9, 10).date()
    keyboard = calendar.schedule_calendar_kb(year, month, today=today)
    assert keyboard.inline_keyboard[0][0].text == "Сентябрь 2026"
