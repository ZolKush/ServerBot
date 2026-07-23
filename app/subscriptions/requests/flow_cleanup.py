"""Cancellation and navigation cleanup for the product conversation."""

from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, ConversationHandler

from ...bot.menu import main_menu_inline_kb, show_main_menu
from ...storage import UserData, update_user_data
from . import state


async def abandon_product_flow(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    """Release a claimed request before an unfinished input flow is discarded."""

    actor = state.actor_meta(update)
    request_id = int(state.context_data(context).get(state.CTX_REQUEST_ID, 0) or 0)
    try:
        if not request_id or not actor:
            return

        def release(config: UserData) -> None:
            request = config.service_requests.get(str(request_id))
            if not isinstance(request, dict) or request.get("status") != "awaiting_link":
                return
            if int(request.get("claimed_by_id", 0) or 0) != int(actor.get("user_id", 0) or 0):
                return
            updated = dict(request)
            updated.update(
                {
                    "status": str(request.get("resume_status") or "pending"),
                    "claimed_by_id": None,
                    "claimed_at": None,
                    "updated_at": state.now_iso(),
                }
            )
            config.service_requests[str(request_id)] = updated

        await update_user_data(release)
    finally:
        state.clear_request_context(context)


async def product_cancel(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    query = update.callback_query
    return_to_menu = bool(query and query.data == "menu:home")
    await abandon_product_flow(update, context)
    if return_to_menu:
        await show_main_menu(update)
    elif query:
        await query.answer()
        await query.edit_message_text(
            "Действие отменено.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]]),
        )
    elif update.effective_message:
        await update.effective_message.reply_text(
            "Действие отменено.",
            reply_markup=main_menu_inline_kb(update),
        )
    return ConversationHandler.END


__all__ = ["abandon_product_flow", "product_cancel"]
