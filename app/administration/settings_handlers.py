from __future__ import annotations

from typing import Any

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..bot.guards import get_user_id, require_admin
from ..storage import get_user_meta_copy, product_settings_snapshot
from ..users.staff import can_edit_help_meta, is_owner_meta
from .views import service_settings_markup, service_settings_text


def actor_meta(update: Update) -> dict[str, Any] | None:
    user_id = get_user_id(update)
    return get_user_meta_copy(user_id) if user_id is not None else None


@require_admin
async def administration_service_settings_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    actor = actor_meta(update)
    if not query or not actor:
        return
    await query.answer()
    if not (can_edit_help_meta(actor) or is_owner_meta(actor)):
        await query.edit_message_text("Настройки сервиса недоступны для специалиста поддержки.")
        return
    await query.edit_message_text(
        service_settings_text(product_settings_snapshot(), actor),
        parse_mode=ParseMode.HTML,
        reply_markup=service_settings_markup(actor),
    )


__all__ = [
    "administration_service_settings_cb",
]
