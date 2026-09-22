from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.constants import ChatType
from telegram.ext import ConversationHandler

from app import storage
from app.config import TZ
from app.users.admin.detail_handlers import users_user_menu
from app.users.states import ADMIN_PICK, ADMIN_USER_MENU
from app.users.views import user_card_kb
from tests.product_support import _admin, _callback_names, _callback_update, _user


async def _seed_user(**overrides: object) -> None:
    await storage.update_user_data(
        lambda cfg: cfg.authorized_users.update({"1": _admin(1), "42": _user(42, **overrides)})
    )


def _view_update() -> tuple[SimpleNamespace, SimpleNamespace]:
    update, context = _callback_update(1, "users:subview:42")
    context.bot = SimpleNamespace(send_message=AsyncMock(), send_document=AsyncMock())
    return update, context


@pytest.mark.asyncio
async def test_admin_can_view_link_without_sending_to_user_or_changing_account(isolated_storage) -> None:
    url = "https://connect.test/user?token=a&key=b"
    await _seed_user(connection_url=url)
    before = storage.get_user_meta_copy(42)
    assert "users:subview:42" in _callback_names(user_card_kb(42))
    update, context = _view_update()

    result = await users_user_menu(update, context)

    assert result == ADMIN_USER_MENU
    payload = context.bot.send_message.await_args.kwargs
    assert payload["chat_id"] == 1
    assert "https://connect.test/user?token=a&amp;key=b" in payload["text"]
    assert "<code>42</code>" in payload["text"]
    assert payload["reply_markup"].inline_keyboard[0][0].url == url
    context.bot.send_message.assert_awaited_once()
    context.bot.send_document.assert_not_awaited()
    assert context.user_data["selected_uid"] == 42
    assert "users:user:42" in _callback_names(update.callback_query.edit_message_text.await_args.kwargs["reply_markup"])
    assert storage.get_user_meta_copy(42) == before
    assert storage.outbox_snapshot() == []


@pytest.mark.asyncio
async def test_long_link_is_shown_as_complete_file_to_requesting_admin(isolated_storage) -> None:
    url = "https://connect.test/" + "a" * 4000
    await _seed_user(connection_url=url)
    update, context = _view_update()

    await users_user_menu(update, context)

    payload = context.bot.send_document.await_args.kwargs
    assert payload["chat_id"] == 1
    assert payload["document"].input_file_content == url.encode("utf-8")
    assert payload["document"].filename == "connection_42.txt"
    assert storage.outbox_snapshot() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("expired", [False, True])
async def test_absent_and_expired_trial_links_are_not_disclosed(isolated_storage, expired: bool) -> None:
    now = datetime.now(TZ)
    overrides = (
        {
            "connection_url": "https://connect.test/expired",
            "trial_issued_at": (now - timedelta(days=2)).isoformat(),
            "trial_end_at": (now - timedelta(days=1)).isoformat(),
        }
        if expired
        else {}
    )
    await _seed_user(**overrides)
    update, context = _view_update()

    result = await users_user_menu(update, context)

    assert result == ADMIN_USER_MENU
    text = update.callback_query.edit_message_text.await_args.args[0]
    assert ("завершён" if expired else "не назначена") in text
    context.bot.send_message.assert_not_awaited()
    context.bot.send_document.assert_not_awaited()


@pytest.mark.asyncio
async def test_link_view_reloads_account_after_callback_acknowledgement(isolated_storage) -> None:
    await _seed_user(connection_url="https://connect.test/old")
    update, context = _view_update()

    async def remove_link() -> None:
        await storage.update_user_data(lambda cfg: cfg.authorized_users["42"].update(connection_url=None))

    update.callback_query.answer.side_effect = remove_link

    await users_user_menu(update, context)

    context.bot.send_message.assert_not_awaited()
    assert "не назначена" in update.callback_query.edit_message_text.await_args.args[0]


@pytest.mark.asyncio
async def test_link_view_handles_removed_user(isolated_storage) -> None:
    await storage.update_user_data(lambda cfg: cfg.authorized_users.update({"1": _admin(1)}))
    update, context = _view_update()

    result = await users_user_menu(update, context)

    assert result == ADMIN_PICK
    context.bot.send_message.assert_not_awaited()
    assert "пользователь не найден" in update.callback_query.edit_message_text.await_args.args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("access", ["user", "disabled_admin", "group"])
async def test_link_view_requires_enabled_admin_in_private_chat(isolated_storage, access: str) -> None:
    await _seed_user(connection_url="https://connect.test/private")
    if access != "group":
        requester = _user(1) if access == "user" else _admin(1, access_state="blocked", enabled=False)
        await storage.update_user_data(lambda cfg: cfg.authorized_users.update({"1": requester}))
    update, context = _view_update()
    if access == "group":
        update.effective_chat.type = ChatType.GROUP

    result = await users_user_menu(update, context)

    assert result == ConversationHandler.END
    context.bot.send_message.assert_not_awaited()
    context.bot.send_document.assert_not_awaited()
