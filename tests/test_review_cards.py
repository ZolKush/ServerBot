from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.constants import ChatType
from telegram.error import TimedOut

from app import storage
from app.messaging import review_sync
from app.messaging.message_cleanup import MessageTracker
from app.messaging.review_navigation import (
    keeps_review_card_reference,
    retire_review_card_for_navigation,
)
from app.messaging.review_sync import (
    refresh_service_review_message,
    sync_access_review_messages,
    sync_service_review_messages,
    sync_service_review_messages_for_user,
)
from app.subscriptions.requests import state as request_state
from app.subscriptions.requests.admin_listing import product_request_view_cb
from app.subscriptions.requests.confirmation import product_confirm_cb
from app.subscriptions.requests.review_handlers import product_request_action_cb
from tests.product_support import _admin, _callback_update, _user


class _ReviewBot:
    def __init__(self, *, fail_once: bool = False) -> None:
        self.fail_once = fail_once
        self.edited: list[dict[str, object]] = []

    async def edit_message_text(self, **kwargs: object) -> bool:
        self.edited.append(kwargs)
        if self.fail_once and len(self.edited) == 1:
            raise TimedOut("temporary review edit timeout")
        return True


def _request(
    request_id: int,
    *,
    user_id: int = 42,
    message_id: int | None = None,
) -> dict[str, object]:
    request: dict[str, object] = {
        "id": request_id,
        "kind": "trial",
        "status": "pending",
        "user_id": user_id,
        "comment": "Проверка",
        "created_at": f"2026-08-11T12:00:0{request_id}+00:00",
        "updated_at": f"2026-08-11T12:00:0{request_id}+00:00",
    }
    if message_id is not None:
        request["review_messages"] = {
            "1": [
                {
                    "chat_id": 1,
                    "message_id": message_id,
                    "generation": request["created_at"],
                }
            ]
        }
    return request


@pytest.mark.parametrize(
    "callback_data",
    [
        "access:approve:42",
        "access:reject:42",
        "product:req:view:7",
        "product:req:approve24:7",
        "product:req:confirm:7",
    ],
)
def test_review_actions_keep_the_current_card_reference(callback_data: str) -> None:
    assert keeps_review_card_reference(callback_data)


@pytest.mark.asyncio
async def test_navigation_retires_every_reference_for_reused_message(
    isolated_storage: None,
) -> None:
    generation = "2026-08-11T12:00:01+00:00"

    def seed(cfg: storage.UserData) -> None:
        cfg.authorized_users = {
            "1": _admin(1),
            "42": _user(
                42,
                review_messages={
                    "1": [
                        {
                            "chat_id": 1,
                            "message_id": 700,
                            "generation": generation,
                        }
                    ]
                },
            ),
        }
        cfg.service_requests["1"] = _request(1, message_id=700)

    await storage.update_user_data(seed)
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=1),
        callback_query=SimpleNamespace(
            data="menu:home",
            message=SimpleNamespace(chat_id=1, message_id=700),
        ),
    )

    removed = await retire_review_card_for_navigation(update)

    assert removed == 2
    assert not storage.get_user_meta_copy(42)["review_messages"]
    assert not storage.service_requests_snapshot()["1"]["review_messages"]
    bot = _ReviewBot()
    await sync_access_review_messages(bot, 42)
    await sync_service_review_messages(bot, 1)
    assert bot.edited == []


@pytest.mark.asyncio
async def test_manual_request_view_is_registered_for_future_synchronization(
    isolated_storage: None,
) -> None:
    await storage.update_user_data(
        lambda cfg: (
            cfg.authorized_users.update({"1": _admin(1), "42": _user(42)}),
            cfg.service_requests.update({"7": _request(7)}),
        )
    )
    update, context = _callback_update(1, "product:req:view:7")
    update.callback_query.message.chat_id = 1
    update.callback_query.message.message_id = 707
    context.bot = _ReviewBot()

    await product_request_view_cb(update, context)

    refs = storage.service_requests_snapshot()["7"]["review_messages"]
    assert refs["1"][0]["chat_id"] == 1
    assert refs["1"][0]["message_id"] == 707
    assert context.bot.edited[-1]["message_id"] == 707


@pytest.mark.asyncio
async def test_retryable_review_edit_is_retried_immediately(
    isolated_storage: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await storage.update_user_data(
        lambda cfg: (
            cfg.authorized_users.update({"1": _admin(1), "42": _user(42)}),
            cfg.service_requests.update({"7": _request(7)}),
        )
    )
    sleep = AsyncMock()
    monkeypatch.setattr(review_sync.asyncio, "sleep", sleep)
    bot = _ReviewBot(fail_once=True)

    await refresh_service_review_message(bot, 7, 1, 1, 707)

    assert len(bot.edited) == 2
    sleep.assert_awaited_once()


@pytest.mark.asyncio
async def test_review_card_becomes_untracked_before_link_prompt_sync(
    isolated_storage: None,
) -> None:
    def seed(cfg: storage.UserData) -> None:
        cfg.authorized_users.update({"1": _admin(1), "42": _user(42)})
        cfg.service_requests["7"] = _request(7, message_id=707)

    await storage.update_user_data(seed)
    update, context = _callback_update(1, "product:req:approve24:7")
    update.callback_query.message.chat_id = 1
    update.callback_query.message.message_id = 707
    context.bot = _ReviewBot()

    await product_request_action_cb(update, context)

    request = storage.service_requests_snapshot()["7"]
    assert request["status"] == "awaiting_link"
    assert not request["review_messages"]
    assert context.bot.edited == []


@pytest.mark.asyncio
async def test_owner_duration_chooser_stays_synchronized_and_is_registered_for_cleanup(
    isolated_storage: None,
) -> None:
    def seed(cfg: storage.UserData) -> None:
        cfg.authorized_users.update(
            {
                "1": _admin(1, admin_level="owner"),
                "42": _user(42),
            }
        )
        cfg.service_requests["7"] = _request(7, message_id=707)

    await storage.update_user_data(seed)
    tracker = MessageTracker(enabled=True, retention=timedelta(hours=24))
    tracker.bind({})
    update, context = _callback_update(1, "product:req:approve:7")
    update.callback_query.message.chat_id = 1
    update.callback_query.message.message_id = 707
    update.callback_query.message.chat = SimpleNamespace(id=1, type=ChatType.PRIVATE)
    update.callback_query.message.date = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
    update.get_bot = lambda: SimpleNamespace(message_tracker=tracker)

    await product_request_action_cb(update, context)

    refs = storage.service_requests_snapshot()["7"]["review_messages"]
    assert refs["1"][0]["message_id"] == 707
    assert await tracker.snapshot() == {1: [707]}


@pytest.mark.asyncio
async def test_user_wide_sync_refreshes_cards_of_related_requests(
    isolated_storage: None,
) -> None:
    def seed(cfg: storage.UserData) -> None:
        cfg.authorized_users.update({"1": _admin(1), "42": _user(42)})
        cfg.service_requests.update(
            {
                "1": _request(1, message_id=701),
                "2": _request(2, message_id=702),
            }
        )

    await storage.update_user_data(seed)
    bot = _ReviewBot()

    await sync_service_review_messages_for_user(bot, 42)

    assert [call["message_id"] for call in bot.edited] == [701, 702]


@pytest.mark.asyncio
async def test_manual_payment_syncs_cards_cancelled_for_the_same_user(
    isolated_storage: None,
) -> None:
    target_end = "2027-08-11T12:00:00+03:00"

    def seed(cfg: storage.UserData) -> None:
        cfg.authorized_users.update(
            {
                "1": _admin(1, admin_level="owner"),
                "42": _user(42, connection_url="https://connect.test/paid"),
            }
        )
        cfg.service_requests["1"] = _request(1, message_id=701)

    await storage.update_user_data(seed)
    update, context = _callback_update(1, "product:confirm:apply")
    context.user_data = {
        request_state.CTX_PENDING: {
            "kind": "manualpay",
            "target_uid": 42,
            "target_end_at": target_end,
        }
    }
    context.bot = _ReviewBot()

    await product_confirm_cb(update, context)

    assert storage.service_requests_snapshot()["1"]["status"] == "cancelled"
    assert any(call["message_id"] == 701 for call in context.bot.edited)
