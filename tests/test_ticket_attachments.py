from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.error import NetworkError
from telegram.ext import ConversationHandler

from app import storage
from app.bot.text_limits import utf16_length
from app.messaging.outbox import process_outbox
from app.tickets.dashboard_handlers import ticket_open_cb
from app.tickets.history import _append_ticket_message
from app.tickets.notifications import queue_ticket_attachments
from app.tickets.operations import _build_ticket_record
from app.tickets.user_handlers import ticket_start
from app.tickets.views import _ticket_admin_kb, _ticket_user_kb
from tests.product_support import _admin, _callback_names, _callback_update, _user


async def _seed_ticket(*, closed: bool = False) -> dict:
    await storage.update_user_data(
        lambda config: config.authorized_users.update({"1": _admin(1), "42": _user(42), "43": _user(43)})
    )
    ticket = _build_ticket_record(
        7,
        user_id=42,
        user_name="User",
        user_username=None,
        subject="Screenshot report",
        urgency="p3",
        text="Original screenshot",
        attachment={"type": "photo", "file_id": "initial-photo"},
    )
    for index in range(8):
        ticket = _append_ticket_message(
            ticket,
            sender_role="admin" if index % 2 else "user",
            sender_id=1 if index % 2 else 42,
            sender_name="Private staff identity" if index % 2 else "User",
            text=f"Reply {index} " + "📸" * 1000,
            kind="reply",
            attachment={
                "type": "document" if index == 7 else "photo",
                "file_id": f"reply-{index}",
            },
        )
    ticket.update(assignee_id=1, assignee_name="Support", user_reply_allowed=not closed)
    if closed:
        ticket.update(status="closed", closed_at=ticket["updated_at"])
    await storage.set_ticket_record(7, ticket)
    return ticket


@pytest.mark.asyncio
@pytest.mark.parametrize("viewer_id", [1, 42])
@pytest.mark.parametrize("closed", [False, True])
async def test_open_ticket_replays_both_sides_including_older_attachments(
    isolated_storage: None, viewer_id: int, closed: bool
) -> None:
    ticket = await _seed_ticket(closed=closed)
    update, context = _callback_update(viewer_id, "ticket:open:7")
    update.effective_message.edit_text = AsyncMock(return_value=update.effective_message)

    assert await ticket_open_cb(update, context) == ConversationHandler.END

    events = [event for _, event in storage.outbox_snapshot()]
    assert len(events) == 9
    assert [event["payload"]["file_id"] for event in events] == ["initial-photo", *[f"reply-{i}" for i in range(8)]]
    assert all(set(event["recipients"]) == {str(viewer_id)} for event in events)
    assert all(utf16_length(event["payload"]["caption"]) <= 1024 for event in events)
    assert all("Private staff identity" not in event["payload"]["caption"] for event in events)
    assert "Пользователь" in events[0]["payload"]["caption"]
    assert "Администратор" in events[-1]["payload"]["caption"]
    assert "Reply 7" in events[-1]["payload"]["caption"]
    assert "ticket:open:7" in _callback_names(_ticket_user_kb(ticket, 42))
    assert "ticket:open:7" in _callback_names(_ticket_admin_kb(ticket, 1))
    if closed:
        callbacks = _callback_names(update.effective_message.edit_text.call_args.kwargs["reply_markup"])
        assert not any(
            name.startswith(("ticket:adminreply:", "ticket:userreply:", "ticket:take:")) for name in callbacks
        )

    bot = SimpleNamespace(send_photo=AsyncMock(), send_document=AsyncMock())
    assert await process_outbox(bot) == 9
    assert [call.kwargs["photo"] for call in bot.send_photo.call_args_list] == [
        "initial-photo",
        *[f"reply-{i}" for i in range(7)],
    ]
    assert bot.send_document.call_args.kwargs["document"] == "reply-7"
    assert storage.get_ticket_copy(7) == ticket


@pytest.mark.asyncio
async def test_user_menu_replays_existing_ticket_attachments(isolated_storage: None) -> None:
    await _seed_ticket()
    update, context = _callback_update(42, "menu:ticket")

    assert await ticket_start(update, context) == ConversationHandler.END

    assert len(storage.outbox_snapshot()) == 9
    assert all(set(event["recipients"]) == {"42"} for _, event in storage.outbox_snapshot())


@pytest.mark.asyncio
@pytest.mark.parametrize("viewer_id", [43, 99])
async def test_other_users_cannot_open_ticket_or_replay_attachments(isolated_storage: None, viewer_id: int) -> None:
    await _seed_ticket()
    update, context = _callback_update(viewer_id, "ticket:open:7")
    update.effective_message.edit_text = AsyncMock()

    assert await ticket_open_cb(update, context) == ConversationHandler.END
    await queue_ticket_attachments(7, viewer_id)

    update.effective_message.edit_text.assert_not_awaited()
    assert not storage.outbox_snapshot()


@pytest.mark.asyncio
@pytest.mark.parametrize("viewer_id", [1, 42])
async def test_replay_rechecks_access_after_showing_ticket(isolated_storage: None, viewer_id: int) -> None:
    await _seed_ticket()
    update, context = _callback_update(viewer_id, "ticket:open:7")

    async def block_while_showing(*_args, **_kwargs):
        await storage.upsert_user_meta(viewer_id, _user(viewer_id, access_state="blocked"))
        return update.effective_message

    update.effective_message.edit_text = AsyncMock(side_effect=block_while_showing)
    await ticket_open_cb(update, context)

    assert not storage.outbox_snapshot()


@pytest.mark.asyncio
async def test_uncertain_card_edit_does_not_enqueue_replay(isolated_storage: None) -> None:
    await _seed_ticket()
    update, context = _callback_update(1, "ticket:open:7")
    update.effective_message.edit_text = AsyncMock(side_effect=NetworkError("connection lost"))

    with pytest.raises(NetworkError):
        await ticket_open_cb(update, context)

    assert not storage.outbox_snapshot()


@pytest.mark.asyncio
async def test_replay_skips_missing_ticket_and_invalid_attachments(isolated_storage: None) -> None:
    ticket = await _seed_ticket()
    ticket["messages"] = [
        {"attachment": {"type": "photo", "file_id": ""}},
        {"attachment": {"type": "video", "file_id": "unsupported"}},
        {"text": "Text only"},
    ]
    await storage.set_ticket_record(7, ticket)

    await queue_ticket_attachments(404, 1)
    await queue_ticket_attachments(7, 1)

    assert not storage.outbox_snapshot()
