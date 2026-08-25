from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from telegram.error import NetworkError

from app import storage
from app.messaging import outbox, review_sync
from app.messaging.review_sync import review_completion


class _ReviewBot:
    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.edited: list[dict] = []

    async def send_message(self, **kwargs):
        self.sent.append(kwargs)
        return SimpleNamespace(chat_id=kwargs["chat_id"], message_id=100 + len(self.sent))

    async def edit_message_text(self, **kwargs):
        self.edited.append(kwargs)
        return True


class _UnavailableBot:
    async def send_message(self, **_kwargs) -> None:
        raise NetworkError("temporary test failure")


@pytest.mark.asyncio
async def test_delivered_review_card_registration_retries_without_second_send(
    isolated_storage: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation = "2026-08-11T12:00:00+00:00"
    event = storage.make_outbox_event(
        kind="access_request",
        recipient_ids=[1],
        payload=outbox.message_payload("review card"),
        completion=review_completion(scope="access", target_id=42, generation=generation),
    )

    def seed(cfg: storage.UserData) -> None:
        cfg.authorized_users["1"] = storage.UserData._normalize_user(
            {"user_id": 1, "role": "admin", "access_state": "approved", "admin_level": "admin"}
        )
        cfg.authorized_users["42"] = storage.UserData._normalize_user(
            {"user_id": 42, "role": "user", "access_state": "pending", "access_requested_at": generation}
        )
        storage.enqueue_user_outbox(cfg, event)

    await storage.update_user_data(seed)
    bot = _ReviewBot()
    original = review_sync.record_review_delivery

    async def fail_registration(*_args, **_kwargs) -> None:
        raise OSError("simulated storage outage after Telegram delivery")

    monkeypatch.setattr(review_sync, "record_review_delivery", fail_registration)
    await outbox.process_outbox(bot)

    pending = storage.outbox_snapshot()[0][1]
    state = pending["recipients"]["1"]
    assert state["status"] == "delivered_pending_registration"
    assert state["delivered_chat_id"] == 1
    assert len(bot.sent) == 1

    await storage.mutate_outbox_event(
        "user",
        event["id"],
        lambda current: current["recipients"]["1"].update(next_attempt_at="") or current,
    )
    monkeypatch.setattr(review_sync, "record_review_delivery", original)
    await outbox.process_outbox(bot)

    assert len(bot.sent) == 1
    assert storage.outbox_snapshot() == []
    assert storage.get_user_meta_copy(42)["review_messages"]["1"][0]["message_id"] == 101


@pytest.mark.asyncio
async def test_exhausted_transient_delivery_is_retained_for_explicit_redrive(isolated_storage: None) -> None:
    event = storage.make_outbox_event(
        kind="important_notice",
        recipient_ids=[42],
        payload=outbox.message_payload("must not be lost"),
    )
    event["created_at"] = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
    await storage.update_user_data(lambda cfg: storage.enqueue_user_outbox(cfg, event))

    await outbox.process_outbox(_UnavailableBot())

    retained = storage.outbox_snapshot()[0][1]
    assert retained["recipients"]["42"]["status"] == "dead_letter"
    assert retained["recipients"]["42"]["dead_lettered_at"]

    assert await outbox.redrive_outbox_dead_letters("user", event["id"]) is True
    redriven = storage.outbox_snapshot()[0][1]["recipients"]["42"]
    assert redriven["status"] == "pending"
    assert redriven["attempts"] == 0


@pytest.mark.asyncio
async def test_redrive_does_not_resend_an_already_delivered_review_card(
    isolated_storage: None,
) -> None:
    event = storage.make_outbox_event(
        kind="review_card",
        recipient_ids=[1],
        payload={"owner_user_id": 42, "text": "review"},
    )
    state = event["recipients"]["1"]
    state.update(
        status="dead_letter",
        delivered_chat_id=1,
        delivered_message_id=101,
        dead_lettered_at=datetime.now(timezone.utc).isoformat(),
    )
    await storage.update_user_data(lambda cfg: storage.enqueue_user_outbox(cfg, event))

    assert await outbox.redrive_outbox_dead_letters("user", event["id"]) is True
    redriven = storage.outbox_snapshot()[0][1]["recipients"]["1"]
    assert redriven["status"] == "delivered_pending_registration"
    assert redriven["delivered_chat_id"] == 1
    assert redriven["delivered_message_id"] == 101
