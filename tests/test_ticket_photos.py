from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram import Chat, Message, PhotoSize, Update, User
from telegram.constants import ChatType
from telegram.ext import ConversationHandler

from app import storage
from app.bot.flow_routes import build_ticket_flow
from app.messaging.outbox import process_outbox
from app.persistence.backend import SplitJsonBackend
from app.tickets import reply_handlers, user_handlers
from app.tickets.operations import _build_ticket_record
from app.tickets.routes import TICKET_ADMIN_REPLY_TEXT, TICKET_CONFIRM, TICKET_TEXT, TICKET_USER_REPLY_TEXT
from tests.product_support import _admin, _callback_update, _user

_PHOTO = {"type": "photo", "file_id": "photo-large", "file_unique_id": "unique-large"}


def _photo_update(
    uid: int = 42,
    *,
    caption: str | None = None,
    chat_type: str = ChatType.PRIVATE,
) -> Update:
    return Update(
        update_id=1,
        message=Message(
            message_id=1,
            date=datetime.now(timezone.utc),
            chat=Chat(id=uid, type=chat_type),
            from_user=User(id=uid, first_name="Tester", is_bot=False),
            photo=[
                PhotoSize(file_id="photo-small", file_unique_id="unique-small", width=90, height=90),
                PhotoSize(file_id="photo-large", file_unique_id="unique-large", width=1280, height=720),
            ],
            caption=caption,
        ),
    )


async def _seed_people() -> None:
    await storage.update_user_data(lambda cfg: cfg.authorized_users.update({"42": _user(42), "1": _admin(1)}))


async def _assert_photo_delivered(recipient_id: int) -> None:
    bot = SimpleNamespace(
        send_message=AsyncMock(return_value=None),
        send_photo=AsyncMock(return_value=None),
        send_document=AsyncMock(return_value=None),
    )

    await process_outbox(bot)

    bot.send_photo.assert_awaited_once_with(
        chat_id=recipient_id,
        photo="photo-large",
        caption=None,
        parse_mode=None,
        reply_markup=None,
    )
    bot.send_document.assert_not_awaited()
    assert storage.outbox_snapshot() == []


@pytest.mark.parametrize("state", [TICKET_TEXT, TICKET_USER_REPLY_TEXT, TICKET_ADMIN_REPLY_TEXT])
@pytest.mark.parametrize("chat_type", [ChatType.PRIVATE, ChatType.GROUP])
def test_ticket_input_routes_accept_photos_only_in_private_chats(state: int, chat_type: str) -> None:
    flow = build_ticket_flow()
    update = _photo_update(chat_type=chat_type)

    accepted = any(handler.check_update(update) for handler in flow.states[state])

    assert accepted is (chat_type == ChatType.PRIVATE)


@pytest.mark.asyncio
@pytest.mark.parametrize("caption", [None, "Скриншот ошибки"])
async def test_ticket_creation_saves_and_delivers_photo(
    isolated_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    caption: str | None,
) -> None:
    await _seed_people()
    monkeypatch.setattr(Message, "reply_text", AsyncMock(return_value=None))
    context = SimpleNamespace(user_data={"ticket_subject": "Ошибка подключения", "ticket_urgency": "p2"})
    photo_update = _photo_update(caption=caption)
    photo_update.set_bot(AsyncMock())

    state = await user_handlers.ticket_text(photo_update, context)

    assert state == TICKET_CONFIRM
    assert context.user_data["ticket_attachment"] == _PHOTO
    update, _ = _callback_update(42, "ticket:send")
    assert await user_handlers.ticket_confirm(update, context) == ConversationHandler.END

    persisted = SplitJsonBackend(storage.storage_data_dir()).inspect()
    messages = persisted.data("support.ticket_messages")["1"]
    assert len(messages) == 1
    assert messages[0]["attachment"] == _PHOTO
    assert messages[0]["text"] == (caption or "(вложение)")
    assert messages[0]["sender_role"] == "user"
    assert "ticket_attachment" not in context.user_data
    await _assert_photo_delivered(1)


@pytest.mark.asyncio
@pytest.mark.parametrize("sender_role", ["user", "admin"])
@pytest.mark.parametrize("caption", [None, "Скриншот ошибки"])
async def test_ticket_replies_save_and_deliver_photos_from_both_sides(
    isolated_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    sender_role: str,
    caption: str | None,
) -> None:
    await _seed_people()
    monkeypatch.setattr(Message, "reply_text", AsyncMock(return_value=None))
    ticket = _build_ticket_record(
        1,
        user_id=42,
        user_name="User 42",
        user_username=None,
        subject="Ошибка подключения",
        urgency="p2",
        text="Первое описание проблемы",
    )
    ticket.update(assignee_id=1, assignee_name="Поддержка", user_reply_allowed=True)

    def seed(cfg: storage.ImportantData) -> None:
        cfg.tickets_seq = 1
        cfg.tickets["1"] = ticket

    await storage.update_important_data(seed)
    sender_id, recipient_id = (42, 1) if sender_role == "user" else (1, 42)
    handler = reply_handlers.ticket_user_reply_text if sender_role == "user" else reply_handlers.ticket_admin_reply_text
    context = SimpleNamespace(user_data={"ticket_reply_ticket_id": 1, "ticket_reply_role": sender_role})

    state = await handler(_photo_update(sender_id, caption=caption), context)

    assert state == ConversationHandler.END
    persisted = SplitJsonBackend(storage.storage_data_dir()).inspect()
    messages = persisted.data("support.ticket_messages")["1"]
    assert len(messages) == 2
    assert messages[-1]["attachment"] == _PHOTO
    assert messages[-1]["text"] == (caption or "(вложение)")
    assert messages[-1]["sender_role"] == sender_role
    assert messages[-1]["sender_id"] == sender_id
    assert "ticket_reply_ticket_id" not in context.user_data
    await _assert_photo_delivered(recipient_id)
