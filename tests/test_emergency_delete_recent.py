from __future__ import annotations

import json
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from telegram import Bot
from telegram.error import BadRequest, Forbidden, RetryAfter, TimedOut
from telegram.request import BaseRequest

from app import storage
from app.messaging.outbox import message_payload
from tools._emergency_delete import RangeDeleter, cancel_pending_broadcasts, known_recipient_ids


class FakeBot:
    """Sparse per-chat IDs: accepting a batch doesn't imply any deletion."""

    def __init__(self, messages=None, rejected=None, failures=None):
        self.messages = set(messages or [])
        self.rejected = set(rejected or [])
        self.failures = dict(failures or {})
        self.calls = []
        self.deleted = set()

    async def delete_messages(self, *, chat_id, message_ids):
        self.calls.append((chat_id, message_ids))
        failures = self.failures.get(chat_id, [])
        if failures:
            raise failures.pop(0)
        if any((chat_id, mid) in self.rejected for mid in message_ids):
            raise BadRequest("Message can't be deleted for everyone")
        for mid in message_ids:
            ref = (chat_id, mid)
            if ref in self.messages:
                self.messages.remove(ref)
                self.deleted.add(ref)
        return True


def engine(bot, **kwargs):
    events = []
    sleep = AsyncMock()
    runner = RangeDeleter(bot, lambda event, **fields: events.append((event, fields)), sleep=sleep, **kwargs)
    return runner, events, sleep


def test_recipients_include_disabled_deduplicate_and_handle_null_id():
    assert known_recipient_ids(
        {
            "10": {"user_id": 10},
            "20": {"user_id": 20, "enabled": False},
            "alias": {"user_id": "10"},
            "30": {"user_id": None},
            "bad": {"user_id": "invalid"},
            "-1": {},
            "0": {},
            "bool": {"user_id": True},
            "float": {"user_id": 1.2},
        }
    ) == [10, 20, 30]


@pytest.mark.asyncio
async def test_explicit_shared_range_reaches_sparse_broadcast_ids_without_markers():
    bot = FakeBot(messages=[(10, 8301), (20, 8310), (10, 8601), (30, 8302)])
    runner, _, _ = engine(bot)
    results = await runner.run([20, 10, 20], 8300, 8600)
    assert bot.deleted == {(10, 8301), (20, 8310)}
    assert bot.messages == {(10, 8601), (30, 8302)}
    assert len(bot.calls) == 8
    assert all(1 <= len(ids) <= 100 and min(ids) >= 8300 and max(ids) <= 8600 for _, ids in bot.calls)
    assert [result.accepted_ids for result in results] == [301, 301]
    assert all(result.complete for result in results)


@pytest.mark.asyncio
async def test_missing_ids_are_not_reported_as_confirmed_deletions():
    bot = FakeBot()
    runner, events, _ = engine(bot)
    (result,) = await runner.run([10], 1, 2)
    assert result.accepted_ids == 2
    assert bot.deleted == set()
    assert all(event != "DELETED" for event, _ in events)


@pytest.mark.asyncio
async def test_bad_message_does_not_prevent_older_or_newer_deletions():
    bot = FakeBot(messages=[(10, 1), (10, 5)], rejected=[(10, 3)])
    runner, _, _ = engine(bot)
    (result,) = await runner.run([10], 1, 5)
    assert bot.deleted == {(10, 1), (10, 5)}
    assert result.accepted_ids == 4
    assert result.rejected_ids == 1
    assert not result.complete


@pytest.mark.asyncio
async def test_timeout_retries_the_same_batch_and_then_succeeds():
    runner, events, sleep = engine(FakeBot(failures={10: [TimedOut(), TimedOut()]}))
    (result,) = await runner.run([10], 1, 2)
    assert result.complete
    assert len(runner.bot.calls) == 3
    assert sleep.await_args_list[0].args == (1.0,)
    assert sum(event == "RETRY" for event, _ in events) == 2


@pytest.mark.asyncio
async def test_exhausted_retries_preserve_unknown_range_and_continue_next_chat():
    bot = FakeBot(messages=[(20, 1)], failures={10: [TimedOut()] * 3})
    runner, events, _ = engine(bot, attempts=3)
    first, second = await runner.run([10, 20], 1, 2)
    assert first.unresolved_ids == 2 and not first.complete
    assert second.complete and bot.deleted == {(20, 1)}
    assert len(bot.calls) == 4
    assert any(event == "UNRESOLVED" and fields["min_id"] == 1 for event, fields in events)


@pytest.mark.asyncio
async def test_timeout_does_not_skip_remaining_batches_of_chat():
    bot = FakeBot(messages=[(10, 1)], failures={10: [TimedOut()]})
    runner, _, _ = engine(bot, attempts=1, batch_size=2)
    (result,) = await runner.run([10], 1, 4)
    assert result.unresolved_ids == 2
    assert result.accepted_ids == 2
    assert bot.deleted == {(10, 1)}


@pytest.mark.asyncio
@pytest.mark.parametrize("delay", [2, timedelta(seconds=2)])
async def test_flood_wait_handles_integer_and_timedelta(delay):
    runner, _, sleep = engine(FakeBot(failures={10: [RetryAfter(delay)]}))
    (result,) = await runner.run([10], 1, 1)
    assert result.complete
    assert sleep.await_args_list[0].args == (3.0,)


@pytest.mark.asyncio
async def test_flood_wait_is_bounded_by_attempts_and_respected_before_next_chat():
    runner, events, sleep = engine(FakeBot(failures={10: [RetryAfter(61)]}), attempts=1)
    first, second = await runner.run([10, 20], 1, 1)
    assert first.unresolved_ids == 1 and second.complete
    assert [call.args[0] for call in sleep.await_args_list[:3]] == [30.0, 30.0, 2.0]
    assert sum(event == "WAIT" for event, _ in events) == 3


@pytest.mark.asyncio
async def test_forbidden_is_not_retried_and_other_chats_continue():
    bot = FakeBot(failures={10: [Forbidden("blocked")]}, messages=[(20, 1)])
    runner, _, _ = engine(bot, batch_size=1)
    first, second = await runner.run([10, 20], 1, 3)
    assert first.forbidden and first.unresolved_ids == 3
    assert second.complete
    assert len([chat for chat, _ in bot.calls if chat == 10]) == 1


@pytest.mark.asyncio
async def test_false_response_is_unresolved():
    bot = AsyncMock()
    bot.delete_messages.return_value = False
    runner, _, _ = engine(bot)
    (result,) = await runner.run([10], 1, 1)
    assert result.accepted_ids == 0 and result.unresolved_ids == 1


@pytest.mark.asyncio
async def test_cancel_pending_broadcasts_preserves_other_events(isolated_storage):
    for kind in ("admin_broadcast", "reminder"):
        event = storage.make_outbox_event(kind=kind, recipient_ids=[10], payload=message_payload("test"))
        await storage.update_user_data(lambda data, event=event: storage.enqueue_user_outbox(data, event))
    assert await cancel_pending_broadcasts() == 1
    assert [event["kind"] for _, event in storage.outbox_snapshot()] == ["reminder"]


@pytest.mark.asyncio
async def test_real_ptb_client_serializes_ids_and_translates_api_errors():
    class Transport(BaseRequest):
        read_timeout = 60

        def __init__(self):
            self.requests = []

        async def initialize(self):
            pass

        async def shutdown(self):
            pass

        async def do_request(self, url, method, request_data=None, **kwargs):
            name = url.rsplit("/", 1)[-1]
            params = request_data.parameters if request_data else {}
            self.requests.append((name, params))
            if name == "getMe":
                payload = {"ok": True, "result": {"id": 123456, "is_bot": True, "first_name": "Test"}}
                return 200, json.dumps(payload).encode()
            assert name == "deleteMessages" and method == "POST"
            if 11 in params["message_ids"]:
                return 400, b'{"ok": false, "description": "Bad Request: message cannot be deleted"}'
            return 200, b'{"ok": true, "result": true}'

    request = Transport()
    async with Bot("123456:LOCAL_TEST_ONLY", request=request, get_updates_request=Transport()) as bot:
        runner, _, _ = engine(bot)
        (result,) = await runner.run([123], 10, 12)
    assert result.accepted_ids == 2 and result.rejected_ids == 1
    deletions = [params for name, params in request.requests if name == "deleteMessages"]
    assert deletions[0] == {"chat_id": 123, "message_ids": [10, 11, 12]}
    assert {"chat_id": 123, "message_ids": [10]} in deletions
    assert {"chat_id": 123, "message_ids": [12]} in deletions
