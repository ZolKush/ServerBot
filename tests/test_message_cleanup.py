from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.constants import ChatType
from telegram.ext import ExtBot

from app.main import build_app
from app.messaging.message_cleanup import MessageTracker, TrackingExtBot


async def _record(
    tracker: MessageTracker,
    chat_id: int,
    message_id: int,
    date: datetime,
    *,
    chat_type: str = ChatType.PRIVATE,
) -> None:
    await tracker.record_message(
        chat_id=chat_id,
        message_id=message_id,
        message_date=date,
        chat_type=chat_type,
    )


@pytest.mark.asyncio
async def test_cleanup_deletes_messages_older_than_day_but_keeps_latest() -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
    tracker = MessageTracker(enabled=True, retention=timedelta(hours=24))
    tracker.bind({})
    await _record(tracker, 42, 1, now - timedelta(hours=30))
    await _record(tracker, 42, 2, now - timedelta(hours=25))
    await _record(tracker, 42, 3, now - timedelta(hours=25))
    await _record(tracker, 43, 10, now - timedelta(days=10))
    bot = SimpleNamespace(delete_messages=AsyncMock(return_value=True))

    stats = await tracker.cleanup(bot, now=now)

    bot.delete_messages.assert_awaited_once_with(chat_id=42, message_ids=[1, 2])
    assert stats.deleted == 2
    assert stats.expired == 0
    assert await tracker.snapshot() == {42: [3], 43: [10]}


@pytest.mark.asyncio
async def test_cleanup_drops_undeletable_registry_entries_after_telegram_limit() -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
    tracker = MessageTracker(enabled=True, retention=timedelta(hours=24))
    tracker.bind({})
    await _record(tracker, 42, 1, now - timedelta(hours=50))
    await _record(tracker, 42, 2, now - timedelta(hours=49))
    bot = SimpleNamespace(delete_messages=AsyncMock(return_value=True))

    stats = await tracker.cleanup(bot, now=now)

    bot.delete_messages.assert_not_awaited()
    assert stats.expired == 1
    assert await tracker.snapshot() == {42: [2]}


@pytest.mark.asyncio
async def test_failed_cleanup_is_retried_and_registry_survives_restart() -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
    bot_data: dict[str, object] = {}
    tracker = MessageTracker(enabled=True, retention=timedelta(hours=24))
    tracker.bind(bot_data)
    await _record(tracker, 42, 1, now - timedelta(hours=25))
    await _record(tracker, 42, 2, now - timedelta(hours=1))
    bot = SimpleNamespace(delete_messages=AsyncMock(return_value=False))

    stats = await tracker.cleanup(bot, now=now)
    restored_data = copy.deepcopy(bot_data)
    restored = MessageTracker(enabled=True, retention=timedelta(hours=24))
    restored.bind(restored_data)

    assert stats.failed == 1
    assert await restored.snapshot() == {42: [1, 2]}


@pytest.mark.asyncio
async def test_tracker_ignores_non_private_messages() -> None:
    tracker = MessageTracker(enabled=True, retention=timedelta(hours=24))
    tracker.bind({})

    await _record(
        tracker,
        -100123,
        1,
        datetime.now(timezone.utc),
        chat_type=ChatType.SUPERGROUP,
    )

    assert await tracker.snapshot() == {}


@pytest.mark.asyncio
async def test_tracking_bot_records_sent_messages_and_forgets_deleted_ones(monkeypatch) -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
    tracker = MessageTracker(enabled=True, retention=timedelta(hours=24))
    tracker.bind({})

    async def fake_post(self, endpoint, data=None, **kwargs):
        _ = self, kwargs
        if endpoint == "sendMessage":
            return {
                "message_id": 77,
                "date": int(now.timestamp()),
                "chat": {"id": 42, "type": "private"},
            }
        return True

    monkeypatch.setattr(ExtBot, "_post", fake_post)
    bot = TrackingExtBot(
        "123456:TEST_TOKEN_NOT_USED_BY_TESTS_ABCDEFGHIJKLMNOPQRSTUVWXYZ",
        message_tracker=tracker,
    )

    await bot._post("sendMessage", {"chat_id": 42, "text": "hello"})
    assert await tracker.snapshot() == {42: [77]}

    await bot._post("deleteMessage", {"chat_id": 42, "message_id": 77})
    assert await tracker.snapshot() == {}


@pytest.mark.asyncio
async def test_startup_hook_initializes_and_checks_persisted_registry() -> None:
    application = build_app()

    assert application.post_init is not None
    await application.post_init(application)

    assert "maintbot_message_registry_v1" in application.bot_data
