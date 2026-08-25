from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from app.bot import errors


@pytest.mark.asyncio
async def test_error_notification_outage_is_coalesced_and_does_not_expose_message(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "TOKEN-MUST-NOT-REACH-LOGS-OR-TELEGRAM"
    calls: list[dict] = []

    class UnavailableBot:
        async def send_message(self, **kwargs) -> None:
            calls.append(kwargs)
            raise OSError("Telegram unavailable")

    monkeypatch.setattr(errors, "authorized_ids", lambda **_kwargs: [1, 2])
    monkeypatch.setattr(errors, "_LAST_ERROR_NOTIFY_AT", 0.0)
    monkeypatch.setattr(errors, "_ERROR_NOTIFY_LOCK", None)
    context = SimpleNamespace(error=RuntimeError(secret), bot=UnavailableBot())
    update = SimpleNamespace(callback_query=None, effective_user=None, effective_chat=None)
    caplog.set_level(logging.ERROR, logger="maint-bot")

    await errors.on_error(update, context)
    await errors.on_error(update, context)

    assert len(calls) == 2
    assert secret not in caplog.text
    assert all(secret not in call["text"] for call in calls)
