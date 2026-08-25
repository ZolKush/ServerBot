import re

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ...bot.guards import require_admin
from ...bot.ui import breadcrumbs, html_escape, ui_error_text, wrap_as_codeblock_html
from ...config import SERVER_KEY_PATTERN
from ...storage import get_docker_status_cache
from ..remote.docker import remote_docker_inspect_summary, remote_docker_logs_tail
from ..status.cache import docker_views_from_cache
from ..status.common import format_iso_short, get_server_target
from ..status.presenter import build_status_message
from .local import docker_inspect_summary, docker_logs_tail
from .models import is_valid_container_name
from .presentation import format_docker_report

DOCKER_LOGS_TAIL_MIN = 120
DOCKER_LOGS_TAIL_MAX = 600
DOCKER_LOGS_TAIL_STEP = 120


def _is_server_container_allowed(server_key: str, name: str) -> bool:
    srv = get_server_target(server_key)
    if not srv:
        return False
    return bool(is_valid_container_name(name) and name in srv.monitor_containers)


def _filtered_containers(server_key: str) -> list[str]:
    srv = get_server_target(server_key)
    containers = srv.monitor_containers if srv else []
    return [nm for nm in containers if is_valid_container_name(nm)]


def _container_token(server_key: str, name: str) -> str:
    """Короткий токен контейнера для callback_data.

    Имя контейнера может быть до 64 символов и пробить лимит Telegram
    в 64 байта на callback_data, поэтому в кнопки идёт индекс (i0, i1, …)
    в списке monitor_containers.
    """
    containers = _filtered_containers(server_key)
    try:
        return f"i{containers.index(name)}"
    except ValueError:
        return name


def _resolve_container_token(server_key: str, token: str) -> str | None:
    """Принимает индексный токен (i0, i1, …) или легаси-имя из старых клавиатур."""
    if re.fullmatch(r"i\d{1,3}", token or ""):
        containers = _filtered_containers(server_key)
        idx = int(token[1:])
        return containers[idx] if 0 <= idx < len(containers) else None
    return token or None


def _docker_list_kb(server_key: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for idx, nm in enumerate(_filtered_containers(server_key)):
        row.append(InlineKeyboardButton(nm[:32], callback_data=f"docker:show:{server_key}:i{idx}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ К статусу", callback_data=f"docker:back:{server_key}")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def _docker_report(server_key: str, server_label: str) -> str:
    server = get_server_target(server_key)
    items = docker_views_from_cache(server) if server else []
    cached_status = get_docker_status_cache(server_key) or {}
    updated_at = format_iso_short(cached_status.get("updated_at"))
    return format_docker_report(server_label, items, updated_at=updated_at)


def _docker_item_kb(server_key: str, name: str, tail: int = DOCKER_LOGS_TAIL_MIN) -> InlineKeyboardMarkup:
    tail = int(tail)
    tail = (
        DOCKER_LOGS_TAIL_MIN
        if tail < DOCKER_LOGS_TAIL_MIN
        else (DOCKER_LOGS_TAIL_MAX if tail > DOCKER_LOGS_TAIL_MAX else tail)
    )
    token = _container_token(server_key, name)
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔎 Inspect", callback_data=f"docker:inspect:{server_key}:{token}"),
                InlineKeyboardButton(f"📜 Logs tail {tail}", callback_data=f"docker:logs:{server_key}:{token}:{tail}"),
            ],
            [InlineKeyboardButton("⬅️ К списку", callback_data=f"docker:list:{server_key}")],
            [InlineKeyboardButton("⬅️ К статусу", callback_data=f"docker:back:{server_key}")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


def _parse_server_and_name(data: str, action: str) -> tuple[str, str] | None:
    m = re.fullmatch(rf"docker:{action}:({SERVER_KEY_PATTERN}):([a-zA-Z0-9_.\-]{{1,64}})", data or "")
    if not m:
        return None
    server_key, token = m.group(1), m.group(2)
    name = _resolve_container_token(server_key, token)
    if not name:
        return None
    return server_key, name


@require_admin
async def docker_list_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(rf"docker:list:({SERVER_KEY_PATTERN})", q.data or "")
    server_key = m.group(1) if m else None
    srv = get_server_target(server_key)
    if not srv:
        await q.edit_message_text(ui_error_text("сервер не найден."))
        return
    await q.edit_message_text(
        _docker_report(srv.key, srv.label),
        parse_mode=ParseMode.HTML,
        reply_markup=_docker_list_kb(srv.key),
    )


@require_admin
async def docker_back_to_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(rf"docker:back:({SERVER_KEY_PATTERN})", q.data or "")
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
    await q.answer("Загружаю…")
    parsed = _parse_server_and_name(q.data or "", "inspect")
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
    if srv.mode == "ssh":
        summary = await remote_docker_inspect_summary(srv.ssh_target, name)
    else:
        summary = await docker_inspect_summary(name)
    payload = (
        f"<b>{html_escape(breadcrumbs('Статус', srv.label, 'Docker', name, 'Inspect'))}</b>\n\n"
        + wrap_as_codeblock_html(summary)
    )
    await q.edit_message_text(payload, parse_mode=ParseMode.HTML, reply_markup=_docker_item_kb(server_key, name))


@require_admin
async def docker_logs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer("Загружаю логи…")
    m = re.fullmatch(rf"docker:logs:({SERVER_KEY_PATTERN}):([a-zA-Z0-9_.\-]{{1,64}}):(\d{{1,4}})", q.data or "")
    if not m:
        return
    server_key, token, tail_s = m.group(1), m.group(2), m.group(3)
    name = _resolve_container_token(server_key, token)
    if not name:
        await q.edit_message_text(ui_error_text("контейнер недоступен."), reply_markup=_docker_list_kb(server_key))
        return
    tail = int(tail_s)
    tail = (
        DOCKER_LOGS_TAIL_MIN
        if tail < DOCKER_LOGS_TAIL_MIN
        else (DOCKER_LOGS_TAIL_MAX if tail > DOCKER_LOGS_TAIL_MAX else tail)
    )

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

    is_at_max = tail >= DOCKER_LOGS_TAIL_MAX
    token = _container_token(server_key, name)
    first_row = [InlineKeyboardButton("🔎 Inspect", callback_data=f"docker:inspect:{server_key}:{token}")]
    if not is_at_max:
        next_tail = min(tail + DOCKER_LOGS_TAIL_STEP, DOCKER_LOGS_TAIL_MAX)
        first_row.append(InlineKeyboardButton("📜 Ещё", callback_data=f"docker:logs:{server_key}:{token}:{next_tail}"))
    kb = InlineKeyboardMarkup(
        [
            first_row,
            [InlineKeyboardButton("⬅️ К списку", callback_data=f"docker:list:{server_key}")],
            [InlineKeyboardButton("⬅️ К статусу", callback_data=f"docker:back:{server_key}")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )
    max_note = f"\n<i>Достигнут предел tail={DOCKER_LOGS_TAIL_MAX}.</i>\n" if is_at_max else ""
    payload = (
        f"<b>{html_escape(breadcrumbs('Статус', srv.label, 'Docker', name, 'Logs'))}</b>\n"
        f"<code>tail={tail}</code>{max_note}\n" + wrap_as_codeblock_html(log_text)
    )
    await q.edit_message_text(payload, parse_mode=ParseMode.HTML, reply_markup=kb)
