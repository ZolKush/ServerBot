"""Atomic maintenance transition tests against split storage v1."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from app.config import TZ
from app.maintenance.operations import (
    activate_scheduled_maintenance,
    end_maintenance,
    extend_maintenance,
    schedule_maintenance,
    start_maintenance,
)
from app.maintenance.records import build_maintenance_record, build_scheduled_maintenance_record
from app.storage import get_active_maintenance, get_scheduled_maintenance


@pytest.mark.asyncio
async def test_start_extend_and_end_are_atomic(isolated_storage: None) -> None:
    now = datetime.now(TZ)
    scheduled = build_scheduled_maintenance_record(
        "all",
        now + timedelta(hours=2),
        now + timedelta(hours=3),
        1,
        "Admin",
    )
    assert await schedule_maintenance(scheduled)

    active = build_maintenance_record("all", "urgent", 1, 0, 1, "Admin")
    assert await start_maintenance(
        active,
        expected_schedule_id=str(scheduled["id"]),
        notice_event=None,
    )
    assert get_scheduled_maintenance() is None
    assert get_active_maintenance()["id"] == active["id"]

    updated, users_count, admins_count = await extend_maintenance(
        active["id"],
        duration_min=90,
        hours=1,
        minutes=30,
        author="Admin",
        author_id=1,
    )
    assert updated["duration_min"] == 90
    assert (users_count, admins_count) == (0, 0)

    ended, users_count, admins_count = await end_maintenance(
        str(active["id"]),
        author="Admin",
        author_id=1,
        ended_at=datetime.now(TZ),
    )
    assert ended and ended["id"] == active["id"]
    assert (users_count, admins_count) == (0, 0)
    assert get_active_maintenance() is None


@pytest.mark.asyncio
async def test_immediate_start_rejects_a_stale_schedule(isolated_storage: None) -> None:
    now = datetime.now(TZ)
    scheduled = build_scheduled_maintenance_record(
        "all",
        now + timedelta(hours=2),
        now + timedelta(hours=3),
        1,
        "Admin",
    )
    assert await schedule_maintenance(scheduled)
    active = build_maintenance_record("all", "planned", 1, 0, 1, "Admin")

    assert not await start_maintenance(
        active,
        expected_schedule_id="stale-id",
        notice_event=None,
    )
    assert get_active_maintenance() is None
    assert get_scheduled_maintenance()["id"] == scheduled["id"]


@pytest.mark.asyncio
async def test_scheduled_activation_moves_the_record(isolated_storage: None) -> None:
    now = datetime.now(TZ)
    scheduled = build_scheduled_maintenance_record(
        "all",
        now - timedelta(minutes=5),
        now + timedelta(minutes=25),
        1,
        "Admin",
    )
    assert await schedule_maintenance(scheduled)

    active = await activate_scheduled_maintenance(scheduled, None)

    assert active and active["id"] == scheduled["id"]
    assert active["expected_end"] == scheduled["scheduled_end"]
    assert get_scheduled_maintenance() is None
