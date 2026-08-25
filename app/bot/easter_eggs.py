"""Small exact-match private-chat easter eggs."""

from __future__ import annotations

import re

from telegram import Update
from telegram.ext import ContextTypes, filters

RANEPA_TEXT = (
    filters.ChatType.PRIVATE
    & filters.TEXT
    & ~filters.COMMAND
    & filters.Regex(re.compile(r"^\s*РАНХиГС\s*$", flags=re.IGNORECASE))
)


async def ranepa_easter_egg(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    message = update.effective_message
    if message:
        await message.reply_text("сосать")


__all__ = ["RANEPA_TEXT", "ranepa_easter_egg"]
