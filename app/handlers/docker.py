import re
from typing import List, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..services.docker_service import docker_inspect_summary, docker_logs_tail, is_valid_container_name
from ..services.remote_service import remote_docker_inspect_summary, remote_docker_logs_tail
from .common import breadcrumbs, clip_text, html_escape, require_admin, ui_error_text, wrap_as_codeblock_html
from .status import build_status_message, get_server_target


def _is_server_container_allowed(server_key: str, name: str) -> bool:
    srv = get_server_target(server_key)
    if not srv:
        return False
    return bool(is_valid_container_name(name) and name in srv.monitor_containers)


def _docker_list_kb(server_key: str) -> InlineKeyboardMarkup:
    srv = get_server_target(server_key)
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    containers = srv.monitor_containers if srv else []
    for nm in containers:
        if not is_valid_container_name(nm):
            continue
        row.append(InlineKeyboardButton(nm[:32], callback_data=f"docker:show:{server_key}:{nm}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Назад к статусу", callback_data=f"docker:back:{server_key}")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def _docker_item_kb(server_key: str, name: str, tail: int = 120) -> InlineKeyboardMarkup:
    tail = int(tail)
    tail = 120 if tail < 120 else (600 if tail > 600 else tail)
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔎 Inspect", callback_data=f"docker:inspect:{server_key}:{name}"),
                InlineKeyboardButton(f"📜 Logs tail {tail}", callback_data=f"docker:logs:{server_key}:{name}:{tail}"),
            ],
            [InlineKeyboardButton("⬅️ К списку", callback_data=f"docker:list:{server_key}")],
            [InlineKeyboardButton("⬅️ К статусу", callback_data=f"docker:back:{server_key}")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


def _parse_server_and_name(data: str, action: str) -> Optional[tuple[str, str]]:
    m = re.fullmatch(rf"docker:{action}:([a-z0-9_-]{{1,12}}):([a-zA-Z0-9_.\-]{{1,64}})", data or "")
    if not m:
        return None
    return m.group(1), m.group(2)


@require_admin
async def docker_list_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"docker:list:([a-z0-9_-]{1,12})", q.data or "")
    server_key = m.group(1) if m else None
    srv = get_server_target(server_key)
    if not srv:
        await q.edit_message_text(ui_error_text("сервер не найден."))
        return
    await q.edit_message_text(
        f"<b>{html_escape(breadcrumbs('Статус', srv.label, 'Docker'))}</b>\n\nВыберите контейнер:",
        parse_mode=ParseMode.HTML,
        reply_markup=_docker_list_kb(srv.key),
    )


@require_admin
async def docker_back_to_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"docker:back:([a-z0-9_-]{1,12})", q.data or "")
    server_key = m.group(1) if m else None
    text, markup = await build_status_message(update, server_key=server_key)
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


@require_admin
async def docker_show(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    parsed = _parse_server_and_name(q.data or "", "show")
    if not parsed:
        await q.edit_message_text(ui_error_text("некорректный запрос."))
        return
    server_key, name = parsed
    srv = get_server_target(server_key)
    if not srv:
        await q.edit_message_text(ui_error_text("сервер не найден."))
        return
    if not _is_server_container_allowed(server_key, name):
        await q.edit_message_text(ui_error_text("контейнер недоступен."), reply_markup=_docker_list_kb(server_key))
        return
    await q.edit_message_text(
        f"<b>{html_escape(breadcrumbs('Статус', srv.label, 'Docker', name))}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=_docker_item_kb(server_key, name),
    )


@require_admin
async def docker_inspect(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    parsed = _parse_server_and_name(q.data or "", "inspect")
    if not parsed:
        return
    server_key, name = parsed
    srv = get_server_target(server_key)
    if not srv:
        await q.edit_message_text(ui_error_text("сервер не найден."))
        return
    if not _is_server_container_allowed(server_key, name):
        await q.edit_message_text(ui_error_text("контейнер недоступен."), reply_markup=_docker_list_kb(server_key))
        return
    if srv.mode == "ssh":
        summary = await remote_docker_inspect_summary(srv.ssh_target, name)
    else:
        summary = await docker_inspect_summary(name)
    payload = (
        f"<b>{html_escape(breadcrumbs('Статус', srv.label, 'Docker', name, 'Inspect'))}</b>\n\n"
        + wrap_as_codeblock_html(clip_text(summary))
    )
    await q.edit_message_text(payload, parse_mode=ParseMode.HTML, reply_markup=_docker_item_kb(server_key, name))


@require_admin
async def docker_logs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"docker:logs:([a-z0-9_-]{1,12}):([a-zA-Z0-9_.\-]{1,64}):(\d{1,3})", q.data or "")
    if not m:
        return
    server_key, name, tail_s = m.group(1), m.group(2), m.group(3)
    tail = int(tail_s)
    tail = 120 if tail < 120 else (600 if tail > 600 else tail)

    srv = get_server_target(server_key)
    if not srv:
        await q.edit_message_text(ui_error_text("сервер не найден."))
        return
    if not _is_server_container_allowed(server_key, name):
        await q.edit_message_text(ui_error_text("контейнер недоступен."), reply_markup=_docker_list_kb(server_key))
        return

    if srv.mode == "ssh":
        log_text = await remote_docker_logs_tail(srv.ssh_target, name, tail)
    else:
        log_text = await docker_logs_tail(name, tail)

    next_tail = 600 if tail >= 600 else tail + 120
    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔎 Inspect", callback_data=f"docker:inspect:{server_key}:{name}"),
                InlineKeyboardButton("📜 Ещё", callback_data=f"docker:logs:{server_key}:{name}:{next_tail}"),
            ],
            [InlineKeyboardButton("⬅️ К списку", callback_data=f"docker:list:{server_key}")],
            [InlineKeyboardButton("⬅️ К статусу", callback_data=f"docker:back:{server_key}")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )
    payload = (
        f"<b>{html_escape(breadcrumbs('Статус', srv.label, 'Docker', name, 'Logs'))}</b>\n"
        f"<code>tail={tail}</code>\n\n"
        + wrap_as_codeblock_html(clip_text(log_text))
    )
    await q.edit_message_text(payload, parse_mode=ParseMode.HTML, reply_markup=kb)
