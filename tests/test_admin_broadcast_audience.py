from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app import storage
from app.users.admin import broadcast_handlers
from app.users.states import ADMIN_ALL_MSG_CONFIRM, ADMIN_ALL_MSG_TEXT, ADMIN_PICK


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("audience", "label", "title", "recipient_ids"),
    [
        (
            "admins",
            "только активные администраторы",
            "Массовая рассылка только администраторам",
            {"2"},
        ),
        ("all", "все активные пользователи", "Массовая рассылка", {"2", "3"}),
    ],
)
async def test_broadcast_audience_is_clear_in_preview_payload_and_confirmation(
    isolated_storage,
    monkeypatch,
    audience,
    label,
    title,
    recipient_ids,
):
    def seed(data):
        for user_id, role, enabled in [(1, "admin", True), (2, "admin", True), (3, "user", True), (4, "admin", False)]:
            data.authorized_users[str(user_id)] = storage.UserData._normalize_user(
                {"user_id": user_id, "role": role, "access_state": "approved" if enabled else "blocked"}
            )

    await storage.update_user_data(seed)
    monkeypatch.setattr(broadcast_handlers, "record_navigation_result", AsyncMock())
    message = SimpleNamespace(text="План <A> & B", reply_text=AsyncMock())
    query = SimpleNamespace(data=f"users:allmsg:{audience}", answer=AsyncMock(), edit_message_text=AsyncMock())
    update = SimpleNamespace(
        effective_user=SimpleNamespace(id=1),
        effective_chat=SimpleNamespace(type="private"),
        effective_message=message,
        callback_query=query,
    )
    context = SimpleNamespace(user_data={})

    assert await broadcast_handlers.users_all_menu(update, context) == ADMIN_ALL_MSG_TEXT
    assert label in query.edit_message_text.await_args.args[0]

    assert await broadcast_handlers.users_all_msg_text(update, context) == ADMIN_ALL_MSG_CONFIRM
    preview = message.reply_text.await_args.args[0]
    assert label in preview
    assert f"Текущее количество: <b>{len(recipient_ids)}</b>" in preview

    query.data = "users:allsend"
    assert await broadcast_handlers.users_all_msg_confirm(update, context) == ADMIN_PICK
    confirmation = query.edit_message_text.await_args.args[0]
    assert label in confirmation
    assert f"для {len(recipient_ids)} получателей" in confirmation
    assert "(кроме вас)" in confirmation

    events = storage.outbox_snapshot()
    assert len(events) == 1
    _, event = events[0]
    assert event["kind"] == "admin_broadcast"
    assert set(event["recipients"]) == recipient_ids
    assert event["payload"]["text"].startswith(f"📣 <b>{title}</b>\n\n")
    assert "План &lt;A&gt; &amp; B" in event["payload"]["text"]
    assert broadcast_handlers.BROADCAST_TEXT_KEY not in context.user_data
    assert broadcast_handlers.BROADCAST_AUDIENCE_KEY not in context.user_data
