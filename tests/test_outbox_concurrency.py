from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from telegram.error import NetworkError, RetryAfter

from app import storage
from app.messaging import outbox, outbox_redrive, review_sync, telegram_rate
from app.messaging.outbox_state import recipient_mutation


@pytest.fixture(autouse=True)
def reset_flood_gate(monkeypatch):
    monkeypatch.setattr(telegram_rate, "_FLOOD_UNTIL", 0.0)
    monkeypatch.setattr(telegram_rate, "_FLOOD_LOCK", None)


async def enqueue(*recipients: int) -> dict:
    event = storage.make_outbox_event(
        kind="notice", recipient_ids=list(recipients), payload=outbox.message_payload("notice")
    )
    await storage.update_user_data(lambda cfg: storage.enqueue_user_outbox(cfg, event))
    return event


@pytest.mark.asyncio
async def test_removed_recipient_is_not_sent_from_stale_snapshot(isolated_storage):
    await enqueue(42, 43)
    sent = []

    async def send_message(**kwargs):
        sent.append(kwargs["chat_id"])
        if kwargs["chat_id"] == 42:
            await storage.update_user_data(lambda cfg: storage.suppress_user_outbox_recipient(cfg, 43))

    await outbox.process_outbox(SimpleNamespace(send_message=send_message))
    assert sent == [42]


@pytest.mark.asyncio
async def test_flood_wait_defers_the_job_without_sleeping(isolated_storage):
    await enqueue(42, 43)
    sent = []

    async def send_message(**kwargs):
        sent.append(kwargs["chat_id"])
        raise RetryAfter(3600)

    bot = SimpleNamespace(send_message=send_message)
    assert await asyncio.wait_for(outbox.process_outbox(bot), timeout=2) == 1
    assert await asyncio.wait_for(outbox.process_outbox(bot), timeout=2) == 0
    assert sent == [42]
    states = storage.outbox_snapshot()[0][1]["recipients"]
    assert states["42"]["next_attempt_at"]
    assert states["43"]["attempts"] == 0


@pytest.mark.asyncio
async def test_flood_wait_rechecks_an_extended_deadline(monkeypatch):
    clock = [0.0]
    sleeps = []
    monkeypatch.setattr(telegram_rate.asyncio, "get_running_loop", lambda: SimpleNamespace(time=lambda: clock[0]))
    monkeypatch.setattr(telegram_rate, "_FLOOD_UNTIL", 5.0)

    async def sleep(delay):
        sleeps.append(delay)
        clock[0] += delay
        if len(sleeps) == 1:
            await telegram_rate.extend_flood_gate(5)

    monkeypatch.setattr(telegram_rate.asyncio, "sleep", sleep)
    await telegram_rate.wait_flood_gate()
    assert sleeps == [5.0, 5.0]


@pytest.mark.parametrize("value", [float("inf"), float("nan"), "Infinity"])
def test_invalid_flood_delay_has_finite_fallback(value):
    assert telegram_rate.retry_after_seconds(value) == 1.0


@pytest.mark.asyncio
async def test_old_redriven_event_gets_a_fresh_retry_budget(isolated_storage):
    event = await enqueue(42)

    def make_old(current):
        current["created_at"] = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
        current["recipients"]["42"]["status"] = "dead_letter"
        return current

    await storage.mutate_outbox_event("user", event["id"], make_old)
    assert await outbox.redrive_outbox_dead_letters("user", event["id"])
    storage.initialize_storage(storage.storage_data_dir())

    async def send_message(**kwargs):
        raise NetworkError("offline")

    await outbox.process_outbox(SimpleNamespace(send_message=send_message))
    assert storage.outbox_snapshot()[0][1]["recipients"]["42"]["status"] == "pending"


def test_late_failure_does_not_resurrect_a_terminal_recipient():
    event = {"recipients": {"42": {"status": "terminal", "attempts": 0}}}
    updated = recipient_mutation(42, status="pending", attempts=1)(event)
    assert updated["recipients"]["42"]["status"] == "terminal"


def test_offline_redrive_refuses_the_running_instance(isolated_storage, monkeypatch, tmp_path):
    from app import config
    from app.runtime.lock import ALREADY_RUNNING_EXIT_CODE, SingleInstanceLock

    path = tmp_path / "instance.lock"
    monkeypatch.setattr(config, "INSTANCE_LOCK_PATH", path)
    monkeypatch.setattr(config, "DATA_DIR", storage.storage_data_dir())
    revision = storage.storage_revision()
    with SingleInstanceLock(path):
        result = outbox_redrive.main(
            ["--data-dir", str(storage.storage_data_dir()), "--source", "user", "--event-id", "absent"]
        )
    assert result == ALREADY_RUNNING_EXIT_CODE
    assert storage.storage_revision() == revision


def test_offline_redrive_rejects_storage_owned_by_another_configuration(isolated_storage, monkeypatch, tmp_path):
    from app import config

    monkeypatch.setattr(config, "DATA_DIR", storage.storage_data_dir())
    unrelated = tmp_path / "other-data"
    revision = storage.storage_revision()
    result = outbox_redrive.main(["--data-dir", str(unrelated), "--source", "user", "--event-id", "absent"])
    assert result == 2
    assert not unrelated.exists()
    assert storage.storage_revision() == revision


@pytest.mark.asyncio
async def test_cancelled_review_batch_does_not_create_unstarted_coroutines():
    started = asyncio.Event()
    calls = []

    async def work():
        started.set()
        await asyncio.Event().wait()

    def factory():
        calls.append(1)
        return work()

    task = asyncio.create_task(review_sync._run_bounded([factory] * 5, limit=1))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert calls == [1]
