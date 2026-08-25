from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.constants import ChatType

from app import storage
from app.users.admin.broadcast_handlers import (
    users_all_menu,
    users_all_msg_confirm,
    users_all_msg_text,
)
from app.users.states import ADMIN_ALL_MSG_CONFIRM, ADMIN_ALL_MSG_TEXT, ADMIN_PICK
from tests.product_support import _admin, _callback_update, _user


def _text_update(user_id: int, text: str) -> SimpleNamespace:
    message = SimpleNamespace(text=text, reply_text=AsyncMock())
    return SimpleNamespace(
        effective_user=SimpleNamespace(id=user_id, username=f"user{user_id}", first_name="Admin"),
        effective_chat=SimpleNamespace(id=user_id, type=ChatType.PRIVATE),
        effective_message=message,
        callback_query=None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("audience", "expected_recipients"),
    (("admins", {2}), ("all", {2, 3})),
)
async def test_broadcast_audience_is_rechecked_and_persisted(
    isolated_storage: None,
    audience: str,
    expected_recipients: set[int],
) -> None:
    def _seed(data: storage.UserData) -> None:
        data.authorized_users = {
            "1": _admin(1),
            "2": _admin(2),
            "3": _user(3),
            "4": _admin(4, access_state="logged_out", enabled=False),
        }

    await storage.update_user_data(_seed)
    start_update, context = _callback_update(1, f"users:allmsg:{audience}")
    assert await users_all_menu(start_update, context) == ADMIN_ALL_MSG_TEXT

    text_update = _text_update(1, "Проверочная рассылка")
    assert await users_all_msg_text(text_update, context) == ADMIN_ALL_MSG_CONFIRM

    confirm_update, _ = _callback_update(1, "users:allsend")
    assert await users_all_msg_confirm(confirm_update, context) == ADMIN_PICK

    events = [event for _, event in storage.outbox_snapshot() if event["kind"] == "admin_broadcast"]
    assert len(events) == 1
    assert {int(value) for value in events[0]["recipients"]} == expected_recipients
    audit = storage.audit_log_snapshot()[-1]
    assert audit["action"] == "broadcast_queued"
    assert audit["details"] == {
        "recipient_count": len(expected_recipients),
        "audience": audience,
    }
