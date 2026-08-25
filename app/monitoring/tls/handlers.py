"""Admin-only callback for the persisted TLS report."""

from __future__ import annotations

import re

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ...bot.guards import require_admin
from ...bot.ui import ui_error_text
from ...config import SERVER_KEY_PATTERN
from ..status.cache import tls_views
from ..status.common import get_server_target
from .views import format_tls_report, tls_report_keyboard


def _server_key(data: str) -> str | None:
    match = re.fullmatch(rf"tls:list:({SERVER_KEY_PATTERN})", data or "")
    return match.group(1) if match else None


@require_admin
async def tls_report_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer()
    server_key = _server_key(query.data or "")
    server = get_server_target(server_key)
    if not server:
        await query.edit_message_text(ui_error_text("сервер не найден."))
        return
    await query.edit_message_text(
        format_tls_report(server.label, tls_views(server.key, admin_mode=True)),
        parse_mode=ParseMode.HTML,
        reply_markup=tls_report_keyboard(server.key),
    )


__all__ = ["tls_report_cb"]
