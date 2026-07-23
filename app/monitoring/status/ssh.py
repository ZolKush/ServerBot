"""On-demand and scheduled disk/UFW collection plus SSH diagnostics."""

from __future__ import annotations

import asyncio
import re
from datetime import datetime
from typing import cast

from telegram import Message, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ...bot.guards import require_admin
from ...bot.ui import html_escape, ui_error_text, ui_info_text
from ...config import SERVER_KEY_PATTERN, TZ, ServerTarget, logger
from ...monitoring.docker.local import docker_containers
from ...monitoring.remote.status import remote_status_bundle
from ...monitoring.system.metrics import check_uptime, disk_root, meminfo
from ...monitoring.system.ufw import ufw_summary_for_admin
from ...storage import set_daily_node_status_cache
from .cache import invalidate_status_cache, ssh_refresh_lock
from .common import exc_brief, get_server_target
from .diagnostics import format_diagnostic_report
from .keyboards import (
    confirmation_keyboard,
    status_pick_keyboard,
)
from .presenter import build_status_message


async def collect_disk_ufw_uncached(
    server: ServerTarget,
    *,
    admin_mode: bool,
) -> dict[str, object]:
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
                "SSH refresh failed for server=%s: %s",
                server.key,
                exc_brief(exc),
            )
            return {
                "ok": False,
                "error": exc_brief(exc),
                "updated_at": datetime.now(TZ).isoformat(),
            }
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

    try:
        local_disk, ufw_data = await asyncio.gather(
            disk_root(),
            ufw_summary_for_admin(),
            return_exceptions=True,
        )
    except Exception as exc:
        return {
            "ok": False,
            "error": exc_brief(exc),
            "updated_at": datetime.now(TZ).isoformat(),
        }
    if isinstance(local_disk, Exception):
        return {
            "ok": False,
            "error": exc_brief(local_disk),
            "updated_at": datetime.now(TZ).isoformat(),
        }
    if str(local_disk).strip().lower() in {"", "н/д"}:
        return {
            "ok": False,
            "error": "чтение локального диска завершилось ошибкой",
            "updated_at": datetime.now(TZ).isoformat(),
        }
    if isinstance(ufw_data, Exception):
        return {
            "ok": False,
            "error": exc_brief(ufw_data),
            "updated_at": datetime.now(TZ).isoformat(),
        }
    ufw_state, allow, deny, reject = cast(
        tuple[str, list[str], list[str], list[str]],
        ufw_data,
    )
    return {
        "ok": True,
        "disk_raw": str(local_disk),
        "ufw_state": str(ufw_state),
        "ufw_allow": list(allow),
        "ufw_deny": list(deny),
        "ufw_reject": list(reject),
        "updated_at": datetime.now(TZ).isoformat(),
    }


async def collect_disk_ufw(
    server: ServerTarget,
    *,
    admin_mode: bool,
) -> dict[str, object]:
    async with ssh_refresh_lock(server.key):
        return await collect_disk_ufw_uncached(server, admin_mode=admin_mode)


async def full_diagnostic(server: ServerTarget) -> dict[str, object]:
    if server.mode == "ssh":
        try:
            result = await remote_status_bundle(
                server.ssh_target,
                server.monitor_containers,
                admin_mode=True,
            )
        except Exception as exc:
            return {"ok": False, "error": exc_brief(exc)}
        if not result.ok:
            return {"ok": False, "error": result.error or "SSH недоступен"}
        remote_uptime, remote_memory, remote_disk, remote_containers, remote_ufw_state, _, _, _ = result.values()
        return {
            "ok": True,
            "uptime": str(remote_uptime),
            "memory": str(remote_memory),
            "disk_raw": str(remote_disk),
            "ufw_state": str(remote_ufw_state),
            "containers": [
                (name, bool(is_up), str(status), str(restarts)) for name, is_up, status, restarts in remote_containers
            ],
        }

    local_uptime, local_memory, local_disk, local_containers, ufw_data = await asyncio.gather(
        check_uptime(),
        meminfo(),
        disk_root(),
        docker_containers(server.monitor_containers),
        ufw_summary_for_admin(),
        return_exceptions=True,
    )
    uptime_value = "н/д" if isinstance(local_uptime, Exception) else str(local_uptime)
    memory_value = "н/д" if isinstance(local_memory, Exception) else str(local_memory)
    disk_value = "н/д" if isinstance(local_disk, Exception) else str(local_disk)
    if isinstance(local_containers, BaseException):
        container_values = [
            (name, False, f"ошибка: {exc_brief(local_containers)}", "-") for name in server.monitor_containers
        ]
    else:
        container_values = list(local_containers)
    ufw_state = "н/д" if isinstance(ufw_data, Exception) or not isinstance(ufw_data, tuple) else str(ufw_data[0])
    return {
        "ok": True,
        "uptime": uptime_value,
        "memory": memory_value,
        "disk_raw": disk_value,
        "ufw_state": ufw_state,
        "containers": [
            (name, bool(is_up), str(status), str(restarts)) for name, is_up, status, restarts in container_values
        ],
    }


def _callback_server_key(data: str, action: str) -> str | None:
    match = re.fullmatch(
        rf"status:{action}:({SERVER_KEY_PATTERN})",
        data or "",
    )
    return match.group(1) if match else None


async def _confirmation_screen(
    update: Update,
    *,
    action: str,
    description: str,
    visibility: str,
) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer()
    server_key = _callback_server_key(query.data or "", action)
    server = get_server_target(server_key)
    if not server:
        await query.edit_message_text(
            ui_error_text("сервер не найден."),
            reply_markup=status_pick_keyboard(),
        )
        return
    text = (
        "⚠️ <b>Подтверждение</b>\n\n"
        f"Будет выполнено SSH-подключение к ноде <b>{html_escape(server.label)}</b>"
        f"{description}\n\n{visibility}\n\nПродолжить?"
    )
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=confirmation_keyboard(server.key, action),
    )


@require_admin
async def status_ssh_refresh_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    await _confirmation_screen(
        update,
        action="sshrefresh",
        description=" для обновления disk/UFW.",
        visibility="Результат сохранится в общий кэш и будет виден всем пользователям.",
    )


@require_admin
async def status_ssh_diag_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    await _confirmation_screen(
        update,
        action="sshdiag",
        description=".",
        visibility="Результат будет показан <b>только вам</b> и не сохранится в общий кэш.",
    )


@require_admin
async def status_ssh_refresh_confirm_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer("Запускаю SSH...")
    server_key = _callback_server_key(query.data or "", "sshrefresh:confirm")
    server = get_server_target(server_key)
    if not server:
        await query.edit_message_text(
            ui_error_text("сервер не найден."),
            reply_markup=status_pick_keyboard(),
        )
        return
    payload = await collect_disk_ufw(server, admin_mode=True)
    if payload.get("ok"):
        await set_daily_node_status_cache(server.key, payload)
        invalidate_status_cache(server.key)
    text, markup = await build_status_message(update, server_key=server.key)
    note = (
        ui_info_text("Disk/UFW обновлены через SSH.")
        if payload.get("ok")
        else ui_error_text(f"SSH ошибка: {html_escape(str(payload.get('error', 'н/д')))}")
    )
    await query.edit_message_text(
        text + "\n\n" + note,
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
    )


@require_admin
async def status_ssh_diag_confirm_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer("Запускаю SSH...")
    server_key = _callback_server_key(query.data or "", "sshdiag:confirm")
    server = get_server_target(server_key)
    if not server:
        await query.edit_message_text(
            ui_error_text("сервер не найден."),
            reply_markup=status_pick_keyboard(),
        )
        return
    report = format_diagnostic_report(server, await full_diagnostic(server))
    text, markup = await build_status_message(update, server_key=server.key)
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
    )
    if isinstance(query.message, Message):
        await query.message.reply_text(report, parse_mode=ParseMode.HTML)


__all__ = [
    "collect_disk_ufw",
    "status_ssh_diag_cb",
    "status_ssh_diag_confirm_cb",
    "status_ssh_refresh_cb",
    "status_ssh_refresh_confirm_cb",
]
