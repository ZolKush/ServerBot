from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.constants import ChatType
from telegram.ext import ConversationHandler

from app.bot import guards
from app.maintenance.flow import maint_duration, maint_mode, maint_scope, maint_start, maint_urgency
from app.maintenance.lifecycle import (
    maint_cancel_end_cb,
    maint_end_cb,
    maint_end_confirm_cb,
    maint_extend_cb,
    maint_extend_duration,
)
from app.maintenance.scheduling import (
    maint_cal_day,
    maint_cal_nav,
    maint_cal_noop,
    maint_sched_cancel_back_cb,
    maint_sched_cancel_cb,
    maint_sched_cancel_confirm_cb,
    maint_schedule_range,
)
from app.users.staff import STAFF_TITLE_MAINTAINER, STAFF_TITLE_SUPPORT

MAINTENANCE_HANDLERS = (
    maint_start,
    maint_mode,
    maint_scope,
    maint_urgency,
    maint_duration,
    maint_extend_cb,
    maint_extend_duration,
    maint_end_cb,
    maint_end_confirm_cb,
    maint_cancel_end_cb,
    maint_cal_noop,
    maint_cal_nav,
    maint_cal_day,
    maint_schedule_range,
    maint_sched_cancel_cb,
    maint_sched_cancel_back_cb,
    maint_sched_cancel_confirm_cb,
)


def _admin_meta(title: str) -> dict[str, object]:
    return {
        "user_id": 1,
        "role": "admin",
        "admin_level": "admin",
        "staff_title": title,
        "access_state": "approved",
        "enabled": True,
    }


def _update() -> SimpleNamespace:
    return SimpleNamespace(
        effective_chat=SimpleNamespace(type=ChatType.PRIVATE),
        effective_user=SimpleNamespace(id=1),
        effective_message=SimpleNamespace(reply_text=AsyncMock()),
    )


def _set_meta(monkeypatch: pytest.MonkeyPatch, meta: dict[str, object]) -> None:
    monkeypatch.setattr(guards, "get_user_meta_copy", lambda _uid: meta)
    monkeypatch.setattr(guards, "get_user_meta", lambda _uid: meta)


@pytest.mark.asyncio
@pytest.mark.parametrize("handler", MAINTENANCE_HANDLERS)
async def test_support_specialist_cannot_invoke_maintenance_handlers(monkeypatch, handler) -> None:
    _set_meta(monkeypatch, _admin_meta(STAFF_TITLE_SUPPORT))
    update = _update()

    result = await handler(update, SimpleNamespace(user_data={}))

    assert result == ConversationHandler.END
    update.effective_message.reply_text.assert_awaited_once_with(
        "Техработами могут управлять инженер сопровождения, ведущий инженер сопровождения или руководитель сервиса."
    )


@pytest.mark.asyncio
async def test_maintenance_guard_allows_maintenance_engineer(monkeypatch) -> None:
    _set_meta(monkeypatch, _admin_meta(STAFF_TITLE_MAINTAINER))
    update = _update()
    allowed = AsyncMock(return_value="allowed")
    guarded = guards.require_maintenance(allowed)

    result = await guarded(update, SimpleNamespace())

    assert result == "allowed"
    allowed.assert_awaited_once()
    update.effective_message.reply_text.assert_not_awaited()


def test_maintenance_management_reminders_exclude_support(monkeypatch) -> None:
    metas = {
        1: _admin_meta(STAFF_TITLE_SUPPORT),
        2: {**_admin_meta(STAFF_TITLE_MAINTAINER), "user_id": 2},
    }
    monkeypatch.setattr(guards, "authorized_ids", lambda **_kwargs: [1, 2])
    monkeypatch.setattr(guards, "get_user_meta", metas.get)

    assert guards.maintenance_manager_ids() == [2]
