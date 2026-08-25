"""Cancellation and navigation cleanup for the product conversation."""

from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, ConversationHandler

from ...bot.menu import main_menu_inline_kb, show_main_menu
from ...config import logger
from ...messaging.message_cleanup import record_navigation_result
from ...messaging.review_sync import sync_service_review_messages
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

        def release(config: UserData) -> bool:
            request = config.service_requests.get(str(request_id))
            if not isinstance(request, dict) or request.get("status") != "awaiting_link":
                return False
            if int(request.get("claimed_by_id", 0) or 0) != int(actor.get("user_id", 0) or 0):
                return False
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
            return True

        released = await update_user_data(release)
        bot = getattr(context, "bot", None)
        if released and bot is not None:
            try:
                await sync_service_review_messages(bot, request_id)
            except Exception:
                logger.exception(
                    "Could not synchronize request cards after flow cancellation request_id=%s",
                    request_id,
                    extra={"action": "review_card_sync", "request_id": request_id},
                )
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
        result = await update.effective_message.reply_text(
            "Действие отменено.",
            reply_markup=main_menu_inline_kb(update),
        )
        await record_navigation_result(update, result)
    return ConversationHandler.END


__all__ = ["abandon_product_flow", "product_cancel"]
