"""Staff browsing of active and completed subscription requests."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from telegram import InlineKeyboardMarkup
from telegram.constants import ChatType

from app import storage
from app.messaging.review_navigation import retire_review_card_for_navigation
from app.messaging.review_refs import register_review_reference, review_completion
from app.messaging.review_sync import sync_service_review_messages
from app.subscriptions.requests import admin_listing, operations, state
from tests.product_support import _admin, _callback_names, _callback_update, _user


async def _seed_requests(statuses: list[str], *, kind: str = "purchase") -> None:
    def seed(config: storage.UserData) -> None:
        config.authorized_users = {"1": _admin(1, admin_level="owner"), "42": _user(42)}
        for status in statuses:
            operations.create_request(config, kind=kind, user_id=42, status=status)

    await storage.update_user_data(seed)


def _request_ids(markup: InlineKeyboardMarkup) -> list[int]:
    return [
        int(str(button.callback_data).split(":")[3])
        for row in markup.inline_keyboard
        for button in row
        if str(button.callback_data).startswith("product:req:view:")
    ]


@pytest.mark.asyncio
async def test_staff_can_switch_between_active_requests_and_archive(isolated_storage: None) -> None:
    statuses = ["pending", "claimed", "awaiting_link", "requisites_sent", "payment_reported"]
    await _seed_requests([*statuses, "approved", "rejected", "cancelled"])
    update, context = _callback_update(1, "product:requests")

    await admin_listing.product_requests_cb(update, context)

    markup = update.callback_query.edit_message_text.call_args.kwargs["reply_markup"]
    assert _request_ids(markup) == [5, 4, 3, 2, 1]
    assert "product:requests:archive:0" in _callback_names(markup)
    update.callback_query.data = "product:requests:archive:0"

    await admin_listing.product_requests_cb(update, context)

    call = update.callback_query.edit_message_text.call_args
    assert "Архив заявок" in call.args[0]
    assert "Завершённых заявок: <b>3</b>" in call.args[0]
    assert _request_ids(call.kwargs["reply_markup"]) == [8, 7, 6]
    assert "product:requests" in _callback_names(call.kwargs["reply_markup"])


@pytest.mark.asyncio
@pytest.mark.parametrize(("section", "status"), [("active", "pending"), ("archive", "approved")])
async def test_request_pages_reach_all_records_and_clamp_stale_page(
    isolated_storage: None, section: str, status: str
) -> None:
    await _seed_requests([status] * 43)
    update, context = _callback_update(1)
    seen = []
    for page in range(5):
        update.callback_query.data = f"product:requests:{section}:{page}"
        await admin_listing.product_requests_cb(update, context)
        call = update.callback_query.edit_message_text.call_args
        markup = call.kwargs["reply_markup"]
        seen.extend(_request_ids(markup))
        assert f"Страница {page + 1} из 5" in call.args[0]
        assert len(_request_ids(markup)) <= admin_listing.REQUESTS_PAGE_SIZE
        if page < 4:
            assert f"product:requests:{section}:{page + 1}" in _callback_names(markup)
        if page > 0:
            assert f"product:requests:{section}:{page - 1}" in _callback_names(markup)

    assert seen == list(range(43, 0, -1))
    update.callback_query.data = f"product:requests:{section}:999"
    await admin_listing.product_requests_cb(update, context)
    call = update.callback_query.edit_message_text.call_args
    assert "Страница 5 из 5" in call.args[0]
    assert _request_ids(call.kwargs["reply_markup"]) == [3, 2, 1]


@pytest.mark.asyncio
async def test_empty_archive_still_has_active_requests_navigation(isolated_storage: None) -> None:
    await _seed_requests([])
    update, context = _callback_update(1, "product:requests:archive:999")

    await admin_listing.product_requests_cb(update, context)

    call = update.callback_query.edit_message_text.call_args
    assert "Архив заявок пуст." in call.args[0]
    assert _callback_names(call.kwargs["reply_markup"]) == {"product:requests", "menu:home"}


@pytest.mark.asyncio
@pytest.mark.parametrize("status", sorted(state.ARCHIVED_REQUEST_STATUSES))
@pytest.mark.parametrize("kind", ["trial", "purchase", "renewal"])
async def test_archive_card_is_read_only_and_returns_to_origin_page(
    isolated_storage: None, status: str, kind: str
) -> None:
    await _seed_requests([status], kind=kind)
    before = storage.service_requests_snapshot()
    update, context = _callback_update(1, "product:req:view:1:archive:2")
    update.callback_query.message.chat_id = 1
    update.callback_query.message.message_id = 10
    context.bot = AsyncMock()

    await admin_listing.product_request_view_cb(update, context)

    call = update.callback_query.edit_message_text.call_args
    assert "#1" in call.args[0]
    assert _callback_names(call.kwargs["reply_markup"]) == {
        "users:user:42",
        "product:requests:archive:2",
        "menu:home",
    }
    context.bot.edit_message_text.assert_not_awaited()
    assert storage.service_requests_snapshot() == before
    assert storage.outbox_snapshot() == []


@pytest.mark.asyncio
async def test_old_callback_for_completed_request_returns_to_archive(isolated_storage: None) -> None:
    await _seed_requests(["rejected"])
    update, context = _callback_update(1, "product:req:view:1")

    await admin_listing.product_request_view_cb(update, context)

    markup = update.callback_query.edit_message_text.call_args.kwargs["reply_markup"]
    assert "product:requests:archive:0" in _callback_names(markup)


@pytest.mark.asyncio
async def test_active_card_still_registers_for_review_sync(isolated_storage: None) -> None:
    await _seed_requests(["pending"])
    update, context = _callback_update(1, "product:req:view:1")
    update.callback_query.message.chat_id = 1
    update.callback_query.message.message_id = 10
    context.bot = AsyncMock()

    await admin_listing.product_request_view_cb(update, context)

    markup = update.callback_query.edit_message_text.call_args.kwargs["reply_markup"]
    assert "product:req:requisites:1" in _callback_names(markup)
    request = storage.service_requests_snapshot()["1"]
    assert request["review_messages"]["1"] == [{"chat_id": 1, "message_id": 10, "generation": request["created_at"]}]
    context.bot.edit_message_text.assert_awaited_once()


@pytest.mark.asyncio
async def test_navigation_to_archive_retires_previous_active_card(isolated_storage: None) -> None:
    await _seed_requests(["pending", "approved"])
    update, context = _callback_update(1, "product:requests:archive:0")
    message = update.callback_query.message
    message.chat_id = 1
    message.message_id = 10
    await register_review_reference(
        review_completion(
            scope="service", target_id=1, generation=storage.service_requests_snapshot()["1"]["created_at"]
        ),
        1,
        message,
    )

    assert await retire_review_card_for_navigation(update) == 1
    await admin_listing.product_requests_cb(update, context)
    update.callback_query.data = "product:req:view:2:archive:0"
    await retire_review_card_for_navigation(update)
    await admin_listing.product_request_view_cb(update, context)
    bot = AsyncMock()
    await sync_service_review_messages(bot, 1)

    bot.edit_message_text.assert_not_awaited()
    assert all(not request["review_messages"] for request in storage.service_requests_snapshot().values())


@pytest.mark.asyncio
@pytest.mark.parametrize("request_id", [1, 999])
async def test_archive_callback_cannot_open_active_or_missing_request(isolated_storage: None, request_id: int) -> None:
    await _seed_requests(["pending"])
    update, context = _callback_update(1, f"product:req:view:{request_id}:archive:0")

    await admin_listing.product_request_view_cb(update, context)

    update.callback_query.edit_message_text.assert_not_awaited()
    assert update.callback_query.answer.call_args.kwargs["show_alert"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("callback", ["product:requests:archive:0", "product:req:view:1:archive:0"])
@pytest.mark.parametrize("unauthorized", ["user", "blocked_admin", "group"])
async def test_request_archive_requires_an_enabled_admin_in_private_chat(
    isolated_storage: None, callback: str, unauthorized: str
) -> None:
    await _seed_requests(["approved"])
    if unauthorized == "blocked_admin":
        await storage.update_user_data(
            lambda config: config.authorized_users["1"].update(access_state="blocked", enabled=False)
        )
    update, context = _callback_update(42 if unauthorized == "user" else 1, callback)
    if unauthorized == "group":
        update.effective_chat.type = ChatType.GROUP
    handler = admin_listing.product_request_view_cb if "req:view" in callback else admin_listing.product_requests_cb

    await handler(update, context)

    update.callback_query.edit_message_text.assert_not_awaited()
