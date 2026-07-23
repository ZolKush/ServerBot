from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from telegram.error import Forbidden, NetworkError, RetryAfter

from app import storage
from app.messaging import outbox
from app.persistence.backend import SplitJsonBackend
from app.persistence.errors import SchemaError
from app.persistence.normalization import normalize_outbox


@pytest.mark.asyncio
async def test_split_runtime_rejects_multiple_service_owners(isolated_storage: None) -> None:
    def seed(cfg: storage.UserData) -> None:
        cfg.authorized_users.update(
            {
                "1": storage.UserData._normalize_user(
                    {
                        "user_id": 1,
                        "role": "admin",
                        "access_state": "approved",
                        "admin_level": "owner",
                    }
                ),
                "2": storage.UserData._normalize_user(
                    {
                        "user_id": 2,
                        "role": "admin",
                        "access_state": "approved",
                        "admin_level": "owner",
                    }
                ),
            }
        )

    with pytest.raises(SchemaError, match="multiple service owners"):
        await storage.update_user_data(seed)


@pytest.mark.asyncio
async def test_user_update_is_persisted_in_separate_domain_stores(isolated_storage: None) -> None:
    user = storage.UserData._normalize_user(
        {
            "user_id": 42,
            "role": "user",
            "access_state": "approved",
            "nickname": "Tester",
            "service_tier": "subscriber",
            "is_paid": True,
        }
    )
    await storage.update_user_data(lambda cfg: cfg.authorized_users.update({"42": user}))

    snapshot = SplitJsonBackend(storage.storage_data_dir()).inspect()

    assert snapshot.data("users.profiles")["42"]["nickname"] == "Tester"
    assert snapshot.data("access.grants")["42"]["access_state"] == "approved"
    assert snapshot.data("subscriptions.accounts")["42"]["service_tier"] == "subscriber"


def test_malformed_outbox_state_is_normalized_without_crash() -> None:
    normalized = normalize_outbox(
        {
            "event": {
                "id": "event",
                "payload": {"method": "send_message", "text": "hello"},
                "recipients": {"42": {"attempts": "bad", "part_index": "bad"}, "-1": {}},
            }
        }
    )

    assert normalized["event"]["recipients"] == {
        "42": {
            "status": "pending",
            "attempts": 0,
            "part_index": 0,
            "next_attempt_at": "",
            "last_error": "",
            "delivered_at": "",
        }
    }


@pytest.mark.asyncio
async def test_concurrent_user_updates_are_not_lost(isolated_storage: None) -> None:
    def increment(cfg: storage.UserData) -> None:
        current = dict(cfg.authorized_users.get("42") or {"user_id": 42, "access_state": "approved"})
        current["nickname"] = str(int(current.get("nickname") or 0) + 1)
        cfg.authorized_users["42"] = storage.UserData._normalize_user(current)

    await asyncio.gather(*(storage.update_user_data(increment) for _ in range(25)))

    assert storage.get_user_meta_copy(42)["nickname"] == "25"
    on_disk = SplitJsonBackend(storage.storage_data_dir()).inspect()
    assert on_disk.data("users.profiles")["42"]["nickname"] == "25"


class _FlakyBot:
    def __init__(self) -> None:
        self.calls = 0

    async def send_message(self, **kwargs) -> None:
        self.calls += 1
        if self.calls == 1:
            raise NetworkError("temporary test failure")


class _PartiallyReachableBot:
    async def send_message(self, **kwargs) -> None:
        if kwargs["chat_id"] == 43:
            raise Forbidden("recipient blocked the bot")


class _RecordingBot:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    async def send_message(self, **kwargs) -> None:
        self.messages.append(kwargs)


@pytest.mark.asyncio
async def test_fail2ban_cursor_advances_only_after_delivery(isolated_storage: None) -> None:
    event = storage.make_outbox_event(
        kind="fail2ban_daily",
        recipient_ids=[42],
        payload=outbox.message_payload("test"),
    )
    event["completion"] = {
        "type": "fail2ban_cursor",
        "server_key": "local",
        "cursor": {"offset": 123, "updated_at": "2026-01-01T00:00:00+00:00"},
    }

    def seed(cfg: storage.ImportantData) -> None:
        storage.enqueue_important_outbox(cfg, event)

    await storage.update_important_data(seed)
    bot = _FlakyBot()

    await outbox.process_outbox(bot)
    assert storage.get_fail2ban_cursor("local") is None
    assert storage.outbox_snapshot()

    event_id = event["id"]

    def make_due(current: dict) -> dict:
        current["recipients"]["42"]["next_attempt_at"] = ""
        return current

    await storage.mutate_outbox_event("important", event_id, make_due)
    await outbox.process_outbox(bot)

    assert storage.get_fail2ban_cursor("local")["offset"] == 123
    assert storage.outbox_snapshot() == []


@pytest.mark.asyncio
async def test_fail2ban_terminal_admin_does_not_duplicate_digest_for_reachable_admin(isolated_storage: None) -> None:
    event = storage.make_outbox_event(
        kind="fail2ban_daily",
        recipient_ids=[42, 43],
        payload=outbox.message_payload("test"),
    )
    event["completion"] = {
        "type": "fail2ban_cursor",
        "server_key": "local",
        "cursor": {"offset": 456},
    }
    await storage.update_important_data(lambda cfg: storage.enqueue_important_outbox(cfg, event))

    await outbox.process_outbox(_PartiallyReachableBot())

    assert storage.get_fail2ban_cursor("local")["offset"] == 456
    assert storage.outbox_snapshot() == []


@pytest.mark.asyncio
async def test_blocked_recipient_gets_only_explicit_final_notification(isolated_storage: None) -> None:
    normal = storage.make_outbox_event(
        kind="queued_before_block",
        recipient_ids=[42],
        payload=outbox.message_payload("must not be delivered"),
    )
    final = storage.make_outbox_event(
        kind="access_blocked",
        recipient_ids=[42],
        payload=outbox.message_payload("final block notice"),
        allow_blocked_delivery=True,
    )

    def _seed(cfg: storage.UserData) -> None:
        cfg.authorized_users["42"] = storage.UserData._normalize_user(
            {"user_id": 42, "role": "user", "access_state": "blocked"}
        )
        storage.enqueue_user_outbox(cfg, normal)
        storage.enqueue_user_outbox(cfg, final)

    await storage.update_user_data(_seed)
    bot = _RecordingBot()

    processed = await outbox.process_outbox(bot)

    assert processed == 2
    assert [message["text"] for message in bot.messages] == ["final block notice"]
    assert storage.outbox_snapshot() == []


def test_blocking_suppresses_only_target_recipient_and_preserves_final_event() -> None:
    cfg = storage.UserData()
    shared = storage.make_outbox_event(
        kind="shared",
        recipient_ids=[42, 43],
        payload=outbox.message_payload("shared"),
        event_id="shared",
    )
    final = storage.make_outbox_event(
        kind="access_blocked",
        recipient_ids=[42],
        payload=outbox.message_payload("final"),
        event_id="final",
        allow_blocked_delivery=True,
    )
    storage.enqueue_user_outbox(cfg, shared)
    storage.enqueue_user_outbox(cfg, final)

    removed = storage.suppress_user_outbox_recipient(cfg, 42, keep_event_id="final")

    assert removed == 1
    assert set(cfg.outbox) == {"shared", "final"}
    assert set(cfg.outbox["shared"]["recipients"]) == {"43"}
    assert cfg.outbox["final"]["allow_blocked_delivery"] is True


def test_retry_after_timedelta_is_supported() -> None:
    exc = RetryAfter(timedelta(seconds=2))
    assert outbox._retry_after_seconds(exc) == 2.0
