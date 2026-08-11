from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.ext import ConversationHandler, MessageHandler

from app.bot.application import build_application
from app.bot.easter_eggs import ranepa_easter_egg


@pytest.mark.asyncio
async def test_ranepa_easter_egg_replies_with_exact_text() -> None:
    message = SimpleNamespace(reply_text=AsyncMock())
    update = SimpleNamespace(effective_message=message)

    await ranepa_easter_egg(update, SimpleNamespace())

    message.reply_text.assert_awaited_once_with("сосать")


def test_easter_egg_route_precedes_every_active_conversation() -> None:
    handlers = build_application().handlers[0]
    easter_index = next(
        index
        for index, handler in enumerate(handlers)
        if isinstance(handler, MessageHandler) and handler.callback is ranepa_easter_egg
    )
    conversation_indexes = [index for index, handler in enumerate(handlers) if isinstance(handler, ConversationHandler)]

    assert conversation_indexes
    assert easter_index < min(conversation_indexes)
