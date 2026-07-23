from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

from telegram.constants import ChatType

from app import storage
from app.users.staff import STAFF_TITLE_SUPPORT


def _user(uid: int, **overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "user_id": uid,
        "role": "user",
        "access_state": "approved",
        "enabled": True,
        "first_name": f"User {uid}",
        "service_tier": "basic",
    }
    value.update(overrides)
    return storage.UserData._normalize_user(value)


def _admin(uid: int, **overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "user_id": uid,
        "role": "admin",
        "access_state": "approved",
        "enabled": True,
        "first_name": f"Admin {uid}",
        "admin_level": "admin",
        "staff_title": STAFF_TITLE_SUPPORT,
    }
    value.update(overrides)
    return storage.UserData._normalize_user(value)


def _callback_update(uid: int, data: str = "access:request") -> tuple[SimpleNamespace, SimpleNamespace]:
    message = SimpleNamespace(
        text="",
        reply_text=AsyncMock(),
        delete=AsyncMock(),
    )
    query = SimpleNamespace(
        data=data,
        message=message,
        answer=AsyncMock(),
        edit_message_text=AsyncMock(),
    )
    update = SimpleNamespace(
        effective_user=SimpleNamespace(
            id=uid,
            username=f"user{uid}",
            first_name=f"User {uid}",
            last_name=None,
        ),
        effective_chat=SimpleNamespace(id=uid, type=ChatType.PRIVATE),
        effective_message=message,
        callback_query=query,
    )
    return update, SimpleNamespace(user_data={})


def _callback_names(markup: object) -> set[str]:
    return {
        str(button.callback_data)
        for row in markup.inline_keyboard
        for button in row
        if button.callback_data is not None
    }
