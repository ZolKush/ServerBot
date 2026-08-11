"""Feature-neutral transient context cleanup and cancel actions."""

from __future__ import annotations

from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from .guards import require_auth
from .menu import show_main_menu


def clear_transient_user_context(context: ContextTypes.DEFAULT_TYPE) -> None:
    user_data = context.user_data
    if user_data is None:
        return
    transient_keys = {
        "selected_uid",
        "subscription_delivery_mode",
        "users_all_broadcast_audience",
        "users_all_broadcast_text",
    }
    for key in tuple(user_data.keys()):
        if key.startswith(("ticket_", "maint_", "product_", "administration_", "profile_")) or key in transient_keys:
            user_data.pop(key, None)


@require_auth
async def cancel_to_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_transient_user_context(context)
    await show_main_menu(update)
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_transient_user_context(context)
    message = update.effective_message
    if message:
        await message.reply_text("Действие отменено.")
    return ConversationHandler.END
