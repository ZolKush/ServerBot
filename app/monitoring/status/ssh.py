"""Disk/UFW collection and guarded SSH fallback callbacks."""

from __future__ import annotations

import asyncio
import re
from datetime import datetime
from typing import cast

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ...bot.guards import require_admin
from ...bot.ui import html_escape, ui_error_text, ui_info_text
from ...config import SERVER_KEY_PATTERN, TZ, ServerTarget, logger
from ...monitoring.remnawave import get_metrics_snapshot
from ...monitoring.remote.status import remote_status_bundle
from ...monitoring.system.metrics import disk_root
from ...monitoring.system.ufw import ufw_summary_for_admin
from ...storage import set_daily_node_status_cache
from .cache import invalidate_status_cache, ssh_refresh_lock
from .common import exc_brief, get_server_target
from .keyboards import confirmation_keyboard, status_pick_keyboard
from .presenter import build_status_message
from .source_policy import node_metrics_problem, server_uses_metrics


async def collect_disk_ufw_uncached(server: ServerTarget, *, admin_mode: bool) -> dict[str, object]:
    if server.mode == "ssh":
        try:
            result = await remote_status_bundle(
                server.ssh_target,
                server.monitor_containers,
                admin_mode=True,
                include_docker=False,
            )
        except Exception as exc:
            logger.warning(
                "SSH disk/UFW refresh failed server=%s error=%s",
                server.key,
                exc_brief(exc),
                extra={"action": "ssh_status_fallback_failed", "source": "ssh", "server_key": server.key},
            )
            return {"ok": False, "error": exc_brief(exc), "updated_at": datetime.now(TZ).isoformat()}
        if not result.ok:
            return {
                "ok": False,
                "error": result.error or "SSH недоступен",
                "updated_at": datetime.now(TZ).isoformat(),
            }
        _, _, remote_disk, _, ufw_state, allow, deny, reject = result.values()
        if str(remote_disk).strip().lower() in {"", "н/д"}:
            return {
                "ok": False,
                "error": "SSH ответ получен, но чтение диска завершилось ошибкой",
                "updated_at": datetime.now(TZ).isoformat(),
            }
        return {
            "ok": True,
            "disk_raw": str(remote_disk),
            "ufw_state": str(ufw_state),
            "ufw_allow": list(allow),
            "ufw_deny": list(deny),
            "ufw_reject": list(reject),
            "updated_at": datetime.now(TZ).isoformat(),
        }

    local_disk, ufw_data = await asyncio.gather(
        disk_root(),
        ufw_summary_for_admin(),
        return_exceptions=True,
    )
    if isinstance(local_disk, Exception):
        return {"ok": False, "error": exc_brief(local_disk), "updated_at": datetime.now(TZ).isoformat()}
    if str(local_disk).strip().lower() in {"", "н/д"}:
        return {
            "ok": False,
            "error": "чтение локального диска завершилось ошибкой",
            "updated_at": datetime.now(TZ).isoformat(),
        }
    if isinstance(ufw_data, Exception):
        return {"ok": False, "error": exc_brief(ufw_data), "updated_at": datetime.now(TZ).isoformat()}
    ufw_state, allow, deny, reject = cast(tuple[str, list[str], list[str], list[str]], ufw_data)
    return {
        "ok": True,
        "disk_raw": str(local_disk),
        "ufw_state": str(ufw_state),
        "ufw_allow": list(allow),
        "ufw_deny": list(deny),
        "ufw_reject": list(reject),
        "updated_at": datetime.now(TZ).isoformat(),
    }


async def collect_disk_ufw(server: ServerTarget, *, admin_mode: bool) -> dict[str, object]:
    async with ssh_refresh_lock(server.key):
        return await collect_disk_ufw_uncached(server, admin_mode=admin_mode)


async def primary_monitoring_failed(server: ServerTarget, *, force_refresh: bool) -> bool:
    """Return true only for a technical/incomplete primary metrics response."""
    if server.mode != "ssh" or not server_uses_metrics(server):
        return False
    metrics = await get_metrics_snapshot(force_refresh=force_refresh)
    return bool(metrics.error or node_metrics_problem(metrics.get(server.remnawave_uuid)))


def _callback_server_key(data: str, action: str) -> str | None:
    match = re.fullmatch(rf"status:{action}:({SERVER_KEY_PATTERN})", data or "")
    return match.group(1) if match else None


async def _show_current_status(update: Update, server_key: str, note: str) -> None:
    query = update.callback_query
    if not query:
        return
    invalidate_status_cache(server_key)
    text, markup = await build_status_message(update, server_key=server_key)
    await query.edit_message_text(
        text + "\n\n" + ui_info_text(note),
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
    )


async def _fallback_confirmation(update: Update, *, action: str) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer("Проверяю основной мониторинг...")
    server_key = _callback_server_key(query.data or "", action)
    server = get_server_target(server_key)
    if not server:
        await query.edit_message_text(ui_error_text("сервер не найден."), reply_markup=status_pick_keyboard())
        return
    if not await primary_monitoring_failed(server, force_refresh=True):
        await _show_current_status(update, server.key, "Основной мониторинг снова доступен; SSH не запускался.")
        return
    text = (
        "⚠️ <b>Подтверждение</b>\n\n"
        f"Основной мониторинг ноды <b>{html_escape(server.label)}</b> недоступен или вернул неполные данные. "
        "Будет выполнено SSH-подключение для проверки disk/UFW.\n\nПродолжить?"
    )
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=confirmation_keyboard(server.key, action),
    )


async def _fallback_confirm(update: Update, *, action: str) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer("Повторно проверяю мониторинг...")
    server_key = _callback_server_key(query.data or "", f"{action}:confirm")
    server = get_server_target(server_key)
    if not server:
        await query.edit_message_text(ui_error_text("сервер не найден."), reply_markup=status_pick_keyboard())
        return
    if not await primary_monitoring_failed(server, force_refresh=True):
        await _show_current_status(update, server.key, "Основной мониторинг восстановился; SSH не запускался.")
        return
    payload = await collect_disk_ufw(server, admin_mode=True)
    if payload.get("ok"):
        await set_daily_node_status_cache(server.key, payload)
    invalidate_status_cache(server.key)
    text, markup = await build_status_message(update, server_key=server.key)
    note = (
        ui_info_text("Disk/UFW проверены через аварийный SSH fallback.")
        if payload.get("ok")
        else ui_error_text(f"SSH ошибка: {html_escape(str(payload.get('error', 'н/д')))}")
    )
    await query.edit_message_text(text + "\n\n" + note, parse_mode=ParseMode.HTML, reply_markup=markup)


@require_admin
async def status_ssh_fallback_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _fallback_confirmation(update, action="sshfallback")


@require_admin
async def status_ssh_fallback_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _fallback_confirm(update, action="sshfallback")


# Old callbacks remain safe while messages from the previous release still exist.
@require_admin
async def status_ssh_refresh_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _fallback_confirmation(update, action="sshrefresh")


@require_admin
async def status_ssh_refresh_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _fallback_confirm(update, action="sshrefresh")


@require_admin
async def status_ssh_diag_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _fallback_confirmation(update, action="sshdiag")


@require_admin
async def status_ssh_diag_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _fallback_confirm(update, action="sshdiag")


__all__ = [
    "collect_disk_ufw",
    "primary_monitoring_failed",
    "status_ssh_diag_cb",
    "status_ssh_diag_confirm_cb",
    "status_ssh_fallback_cb",
    "status_ssh_fallback_confirm_cb",
    "status_ssh_refresh_cb",
    "status_ssh_refresh_confirm_cb",
]
