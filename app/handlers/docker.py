import re
from typing import List

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..config import MONITOR_CONTAINERS
from ..services.docker_service import (
    docker_inspect_summary,
    docker_logs_tail,
    is_allowed_container,
)
from .common import clip_text, html_escape, require_admin, wrap_as_codeblock_html
from .status import build_status_message


def docker_list_kb() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for nm in MONITOR_CONTAINERS:
        if not is_allowed_container(nm):
            continue
        row.append(InlineKeyboardButton(nm[:32], callback_data=f"docker:show:{nm}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Назад к статусу", callback_data="docker:back")])
    return InlineKeyboardMarkup(rows)


def docker_item_kb(name: str, tail: int = 120) -> InlineKeyboardMarkup:
    tail = int(tail)
    tail = 120 if tail < 120 else (600 if tail > 600 else tail)
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔎 Inspect", callback_data=f"docker:inspect:{name}"),
                InlineKeyboardButton(f"📜 Logs tail {tail}", callback_data=f"docker:logs:{name}:{tail}"),
            ],
            [InlineKeyboardButton("⬅️ К списку", callback_data="docker:list")],
            [InlineKeyboardButton("⬅️ К статусу", callback_data="docker:back")],
        ]
    )


@require_admin
async def docker_list_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    await q.edit_message_text(
        "<b>Docker: контейнеры</b>\nВыберите контейнер:",
        parse_mode=ParseMode.HTML,
        reply_markup=docker_list_kb(),
    )


@require_admin
async def docker_back_to_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    text, markup = await build_status_message(update)
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


@require_admin
async def docker_show(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"docker:show:([a-zA-Z0-9_.\-]{1,64})", q.data or "")
    if not m:
        await q.edit_message_text("Некорректный запрос.", reply_markup=docker_list_kb())
        return
    name = m.group(1)
    if not is_allowed_container(name):
        await q.edit_message_text("Контейнер недоступен.", reply_markup=docker_list_kb())
        return
    await q.edit_message_text(
        f"<b>Docker:</b> <code>{html_escape(name)}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=docker_item_kb(name),
    )


@require_admin
async def docker_inspect(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"docker:inspect:([a-zA-Z0-9_.\-]{1,64})", q.data or "")
    if not m:
        return
    name = m.group(1)
    if not is_allowed_container(name):
        await q.edit_message_text("Контейнер недоступен.", reply_markup=docker_list_kb())
        return
    summary = await docker_inspect_summary(name)
    payload = "<b>Inspect</b>\n" + wrap_as_codeblock_html(clip_text(summary))
    await q.edit_message_text(payload, parse_mode=ParseMode.HTML, reply_markup=docker_item_kb(name))


@require_admin
async def docker_logs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"docker:logs:([a-zA-Z0-9_.\-]{1,64}):(\d{1,3})", q.data or "")
    if not m:
        return
    name = m.group(1)
    tail = int(m.group(2))
    tail = 120 if tail < 120 else (600 if tail > 600 else tail)
    if not is_allowed_container(name):
        await q.edit_message_text("Контейнер недоступен.", reply_markup=docker_list_kb())
        return

    log_text = await docker_logs_tail(name, tail)

    next_tail = 600 if tail >= 600 else tail + 120
    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔎 Inspect", callback_data=f"docker:inspect:{name}"),
                InlineKeyboardButton("📜 Ещё", callback_data=f"docker:logs:{name}:{next_tail}"),
            ],
            [InlineKeyboardButton("⬅️ К списку", callback_data="docker:list")],
            [InlineKeyboardButton("⬅️ К статусу", callback_data="docker:back")],
        ]
    )
    payload = f"<b>Logs</b> (<code>{html_escape(name)}</code>, tail {tail})\n" + wrap_as_codeblock_html(
        clip_text(log_text)
    )
    await q.edit_message_text(payload, parse_mode=ParseMode.HTML, reply_markup=kb)
