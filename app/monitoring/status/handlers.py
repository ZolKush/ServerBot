"""Telegram callbacks for status, DNS, UFW and TLS screens."""

from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ...bot.guards import require_admin, require_subscriber
from ...bot.ui import ui_error_text, ui_info_text
from ...config import SERVERS, logger
from ...storage import set_dns_status_cache
from ..tls.service import refresh_tls_certificates
from .cache import invalidate_status_cache
from .collectors import build_status_snapshot_and_server
from .common import exc_brief, first_server_key, get_server_target
from .dns import build_dns_status_payload_live
from .keyboards import (
    parse_dns_refresh_callback,
    parse_tls_refresh_callback,
    parse_ufw_callback,
    resolve_server_key,
    status_actions_keyboard,
    status_pick_keyboard,
    status_pick_text,
    ufw_actions_keyboard,
)
from .presenter import build_status_message
from .views import format_ufw_message


@require_subscriber
async def cmd_health(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    message = update.effective_message
    if not message:
        return
    if query:
        await query.answer()
    if not SERVERS:
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]])
        text = ui_error_text("Сервер не настроен.")
        if query:
            await query.edit_message_text(text, reply_markup=keyboard)
        else:
            await message.reply_text(text, reply_markup=keyboard)
        return
    if len(SERVERS) > 1:
        if query:
            await query.edit_message_text(
                status_pick_text(),
                parse_mode=ParseMode.HTML,
                reply_markup=status_pick_keyboard(),
            )
        else:
            await message.reply_text(
                status_pick_text(),
                parse_mode=ParseMode.HTML,
                reply_markup=status_pick_keyboard(),
            )
        return
    text, markup = await build_status_message(update, server_key=first_server_key())
    if query:
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
        )
    else:
        await message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
        )


@require_subscriber
async def status_pick_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer()
    await query.edit_message_text(
        status_pick_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=status_pick_keyboard(),
    )


@require_subscriber
async def status_show_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer()
    server_key = resolve_server_key(query.data or "", r"status:show")
    if not get_server_target(server_key):
        await query.edit_message_text(
            ui_error_text("сервер не найден."),
            reply_markup=status_pick_keyboard(),
        )
        return
    text, markup = await build_status_message(update, server_key=server_key)
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
    )


@require_admin
async def status_ufw_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer()
    server_key = parse_ufw_callback(query.data or "")
    if not server_key or not get_server_target(server_key):
        await query.edit_message_text(
            ui_error_text("сервер не найден."),
            reply_markup=status_pick_keyboard(),
        )
        return
    snapshot, server = await build_status_snapshot_and_server(update, server_key)
    if not snapshot or not server:
        await query.edit_message_text(
            ui_error_text("сервер не найден."),
            reply_markup=status_pick_keyboard(),
        )
        return
    await query.edit_message_text(
        format_ufw_message(snapshot),
        parse_mode=ParseMode.HTML,
        reply_markup=ufw_actions_keyboard(server.key),
    )


@require_subscriber
async def status_dns_refresh_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer("Обновляю DNS...")
    server_key = parse_dns_refresh_callback(query.data or "")
    server = get_server_target(server_key) if server_key else None
    if not server:
        await query.edit_message_text(
            ui_error_text("сервер не найден."),
            reply_markup=status_pick_keyboard(),
        )
        return
    payload = await build_dns_status_payload_live(server)
    await set_dns_status_cache(server.key, payload)
    invalidate_status_cache(server.key)
    logger.info(
        "DNS status refreshed manually for server=%s ok=%s bad=%s unknown=%s total=%s",
        server.key,
        payload.get("ok"),
        payload.get("bad"),
        payload.get("unknown"),
        payload.get("total"),
    )
    text, markup = await build_status_message(update, server_key=server.key)
    await query.edit_message_text(
        text + "\n\n" + ui_info_text("DNS статус обновлён в реальном времени."),
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
    )


@require_admin
async def status_tls_refresh_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    if not query:
        return
    server_key = parse_tls_refresh_callback(query.data or "")
    if not server_key or not get_server_target(server_key):
        await query.answer("Сервер не найден.", show_alert=True)
        return
    await query.answer("Проверяю TLS-сертификаты...")
    try:
        await refresh_tls_certificates()
    except Exception as exc:
        logger.exception("Manual TLS certificate refresh failed server=%s", server_key)
        await query.edit_message_text(
            ui_error_text(f"не удалось обновить TLS-сертификаты: {exc_brief(exc)}"),
            reply_markup=status_actions_keyboard(True, server_key),
        )
        return
    invalidate_status_cache(server_key)
    text, markup = await build_status_message(update, server_key=server_key)
    await query.edit_message_text(
        text + "\n\n" + ui_info_text("TLS-сертификаты обновлены в реальном времени."),
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
    )


@require_subscriber
async def dns_back_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer()
    server_key = resolve_server_key(query.data or "", r"dns:back")
    if not server_key:
        server_key = first_server_key()
    text, markup = await build_status_message(update, server_key=server_key)
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
    )


__all__ = [
    "cmd_health",
    "dns_back_cb",
    "status_dns_refresh_cb",
    "status_pick_cb",
    "status_show_cb",
    "status_tls_refresh_cb",
    "status_ufw_cb",
]
