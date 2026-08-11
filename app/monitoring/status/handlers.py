"""Telegram callbacks for status, DNS, UFW and TLS screens."""

from __future__ import annotations

import asyncio
import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ...bot.guards import require_admin, require_subscriber
from ...bot.ui import html_escape, ui_error_text, ui_info_text
from ...config import SERVERS, logger
from ...messaging.message_cleanup import record_navigation_result
from ...storage import set_dns_status_cache
from ..remnawave import get_metrics_snapshot
from ..tls.views import format_tls_report, tls_report_keyboard
from .cache import invalidate_status_cache, tls_views
from .collectors import build_status_snapshot_and_server
from .common import first_server_key, get_server_target
from .dns import build_dns_status_payload_live
from .keyboards import (
    parse_dns_refresh_callback,
    parse_refresh_callback,
    parse_ufw_callback,
    resolve_server_key,
    status_pick_keyboard,
    status_pick_text,
    ufw_actions_keyboard,
)
from .presenter import build_status_message
from .source_policy import server_uses_metrics
from .views import format_ufw_message

_REFRESH_LOCKS: dict[str, asyncio.Lock] = {}


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
            result = await query.edit_message_text(text, reply_markup=keyboard)
        else:
            result = await message.reply_text(text, reply_markup=keyboard)
        await record_navigation_result(update, result)
        return
    if len(SERVERS) > 1:
        if query:
            result = await query.edit_message_text(
                status_pick_text(),
                parse_mode=ParseMode.HTML,
                reply_markup=status_pick_keyboard(),
            )
        else:
            result = await message.reply_text(
                status_pick_text(),
                parse_mode=ParseMode.HTML,
                reply_markup=status_pick_keyboard(),
            )
        await record_navigation_result(update, result)
        return
    text, markup = await build_status_message(update, server_key=first_server_key())
    if query:
        result = await query.edit_message_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
        )
    else:
        result = await message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
        )
    await record_navigation_result(update, result)


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


async def _refresh_status_screen(update: Update, *, server_key: str) -> None:
    query = update.callback_query
    if not query:
        return
    server = get_server_target(server_key)
    if not server:
        await query.edit_message_text(
            ui_error_text("сервер не найден."),
            reply_markup=status_pick_keyboard(),
        )
        return
    lock = _REFRESH_LOCKS.setdefault(server.key, asyncio.Lock())
    if lock.locked():
        await query.answer("Обновление уже выполняется.", show_alert=False)
        return
    await query.answer("Обновляю метрики и DNS...")
    started = time.monotonic()
    async with lock:
        errors: list[str] = []
        dns_task = asyncio.create_task(build_dns_status_payload_live(server))
        metrics_task = (
            asyncio.create_task(get_metrics_snapshot(force_refresh=True)) if server_uses_metrics(server) else None
        )
        try:
            dns_result = await dns_task
        except Exception as exc:
            errors.append(f"DNS: {exc.__class__.__name__}")
            logger.warning(
                "Manual status refresh DNS failed server=%s error=%s",
                server.key,
                exc,
                extra={"action": "status_refresh_dns_failed", "source": "manual", "server_key": server.key},
            )
        else:
            await set_dns_status_cache(server.key, dns_result)
        if metrics_task is not None:
            try:
                metrics_result = await metrics_task
            except Exception as exc:
                errors.append(f"метрики: {exc.__class__.__name__}")
            else:
                if not metrics_result.ok:
                    errors.append(f"метрики: {html_escape(metrics_result.error or 'ошибка')}")
        invalidate_status_cache(server.key)
        text, markup = await build_status_message(update, server_key=server.key)
        note = ui_error_text("; ".join(errors)) if errors else ui_info_text("Метрики и DNS обновлены.")
        await query.edit_message_text(
            text + "\n\n" + note,
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
        )
    logger.info(
        "Status refreshed source=manual server=%s duration_ms=%s errors=%s",
        server.key,
        round((time.monotonic() - started) * 1000),
        len(errors),
        extra={
            "action": "status_refresh",
            "source": "manual",
            "server_key": server.key,
            "duration_ms": round((time.monotonic() - started) * 1000),
        },
    )


@require_subscriber
async def status_refresh_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    server_key = parse_refresh_callback(query.data or "")
    if not server_key:
        await query.answer("Сервер не найден.", show_alert=True)
        return
    await _refresh_status_screen(update, server_key=server_key)


@require_subscriber
async def status_dns_refresh_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Compatibility for an old keyboard; performs the new unified refresh."""
    query = update.callback_query
    if not query:
        return
    server_key = parse_dns_refresh_callback(query.data or "")
    if not server_key:
        await query.answer("Сервер не найден.", show_alert=True)
        return
    await _refresh_status_screen(update, server_key=server_key)


@require_admin
async def status_tls_refresh_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    if not query:
        return
    server_key = resolve_server_key(query.data or "", r"status:tlsrefresh")
    server = get_server_target(server_key)
    if not server:
        await query.answer("Сервер не найден.", show_alert=True)
        return
    await query.answer()
    await query.edit_message_text(
        format_tls_report(server.label, tls_views(server.key, admin_mode=True)),
        parse_mode=ParseMode.HTML,
        reply_markup=tls_report_keyboard(server.key),
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
    "status_refresh_cb",
    "status_pick_cb",
    "status_show_cb",
    "status_tls_refresh_cb",
    "status_ufw_cb",
]
