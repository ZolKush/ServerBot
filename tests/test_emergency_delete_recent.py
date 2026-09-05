from __future__ import annotations

from types import SimpleNamespace

import pytest
from telegram.error import BadRequest

from app import storage
from app.messaging.emergency_delete_recent import (
    EmergencyDeleteStats,
    cancel_pending_broadcasts,
    known_recipient_ids,
    purge_chat,
)
from app.messaging.outbox import message_payload


class _DeletingBot:
    def __init__(
        self,
        *,
        marker_id: int,
        rejected_id: int | None = None,
        rejection: str = "message to delete not found",
    ) -> None:
        self.marker_id = marker_id
        self.rejected_id = rejected_id
        self.rejection = rejection
        self.deleted: list[list[int]] = []

    async def send_message(self, **_kwargs):
        return SimpleNamespace(message_id=self.marker_id)

    async def delete_messages(self, **kwargs):
        message_ids = list(kwargs["message_ids"])
        self.deleted.append(message_ids)
        if self.rejected_id in message_ids:
            raise BadRequest(self.rejection)
        return True


def test_known_recipient_ids_includes_disabled_accounts_and_deduplicates() -> None:
    assert known_recipient_ids(
        {
            "10": {"user_id": 10, "enabled": True},
            "20": {"user_id": 20, "enabled": False},
            "alias": {"user_id": "10"},
            "bad": {"user_id": "not-an-id"},
        }
    ) == [10, 20]


@pytest.mark.asyncio
async def test_purge_chat_uses_marker_as_upper_bound() -> None:
    bot = _DeletingBot(marker_id=105)
    stats = EmergencyDeleteStats()

    await purge_chat(bot, chat_id=42, scan_depth=5, fallback_upper=100, anchor=None, stats=stats)

    assert bot.deleted == [[101, 102, 103, 104, 105]]
    assert stats.chats_marked == 1
    assert stats.ids_accepted == 5


@pytest.mark.asyncio
async def test_purge_chat_splits_rejected_batches_and_continues() -> None:
    bot = _DeletingBot(marker_id=5, rejected_id=3)
    stats = EmergencyDeleteStats()

    await purge_chat(bot, chat_id=42, scan_depth=5, fallback_upper=100, anchor=None, stats=stats)

    assert [3] in bot.deleted
    assert stats.ids_accepted == 4
    assert stats.ids_rejected == 1


@pytest.mark.asyncio
async def test_cancel_pending_broadcasts_preserves_other_outbox_events(isolated_storage: None) -> None:
    broadcast = storage.make_outbox_event(
        kind="admin_broadcast",
        recipient_ids=[10],
        payload=message_payload("leaked"),
    )
    reminder = storage.make_outbox_event(
        kind="reminder",
        recipient_ids=[20],
        payload=message_payload("keep"),
    )
    await storage.update_user_data(lambda data: storage.enqueue_user_outbox(data, broadcast))
    await storage.update_user_data(lambda data: storage.enqueue_user_outbox(data, reminder))

    assert await cancel_pending_broadcasts() == 1

    remaining = storage.outbox_snapshot()
    assert len(remaining) == 1
    assert remaining[0][1]["kind"] == "reminder"
