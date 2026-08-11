from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.constants import ChatType

from app.main import build_app
from app.messaging.message_cleanup import (
    MessageTracker,
    TrackingExtBot,
    record_navigation_message,
    record_navigation_result,
)


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
    assert stats.expired == 1
    assert await tracker.snapshot() == {42: [3]}


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
    assert stats.expired == 2
    assert await tracker.snapshot() == {}


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
async def test_tracking_bot_only_records_explicit_navigation_messages() -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
    tracker = MessageTracker(enabled=True, retention=timedelta(hours=24))
    tracker.bind({})
    bot = TrackingExtBot(
        "123456:TEST_TOKEN_NOT_USED_BY_TESTS_ABCDEFGHIJKLMNOPQRSTUVWXYZ",
        message_tracker=tracker,
    )
    message = SimpleNamespace(
        message_id=77,
        date=now,
        chat=SimpleNamespace(id=42, type=ChatType.PRIVATE),
    )

    assert await tracker.snapshot() == {}
    await record_navigation_message(bot, message)
    assert await tracker.snapshot() == {42: [77]}
    await tracker.forget_messages(42, [77])
    assert await tracker.snapshot() == {}


@pytest.mark.asyncio
async def test_navigation_edit_result_uses_callback_message_coordinates() -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
    tracker = MessageTracker(enabled=True, retention=timedelta(hours=24))
    tracker.bind({})
    bot = SimpleNamespace(message_tracker=tracker)
    callback_message = SimpleNamespace(
        message_id=78,
        date=now,
        chat=SimpleNamespace(id=42, type=ChatType.PRIVATE),
    )
    update = SimpleNamespace(
        callback_query=SimpleNamespace(message=callback_message),
        get_bot=lambda: bot,
    )

    await record_navigation_result(update, True)

    assert await tracker.snapshot() == {42: [78]}


@pytest.mark.asyncio
async def test_startup_cleanup_removes_latest_navigation_panel_too() -> None:
    now = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
    tracker = MessageTracker(enabled=True, retention=timedelta(hours=24))
    tracker.bind({})
    await _record(tracker, 42, 1, now - timedelta(hours=1))
    bot = SimpleNamespace(delete_messages=AsyncMock(return_value=True))

    stats = await tracker.cleanup(bot, now=now, startup=True)

    bot.delete_messages.assert_awaited_once_with(chat_id=42, message_ids=[1])
    assert stats.deleted == 1
    assert await tracker.snapshot() == {}


def test_legacy_registry_is_discarded_without_deletion() -> None:
    bot_data = {
        "maintbot_message_registry_v1": {
            "version": 1,
            "chats": {"42": {"messages": {"9": 1_700_000_000}}},
        }
    }
    tracker = MessageTracker(enabled=True, retention=timedelta(hours=24))

    tracker.bind(bot_data)

    assert "maintbot_message_registry_v1" not in bot_data
    assert bot_data["maintbot_navigation_registry_v2"] == {"version": 2, "chats": {}}


@pytest.mark.asyncio
async def test_startup_hook_initializes_and_checks_persisted_registry() -> None:
    application = build_app()

    assert application.post_init is not None
    await application.post_init(application)

    assert "maintbot_navigation_registry_v2" in application.bot_data
