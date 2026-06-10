from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import Any, Dict, List, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..config import (
    FAIL2BAN_DIGEST_MAX_BYTES,
    FAIL2BAN_DIGEST_TAIL_LINES,
    FAIL2BAN_STATE_PATH,
    SERVER_KEY_PATTERN,
    SERVERS,
    TZ,
    logger,
)
from ..services.remote_service import remote_fail2ban_events, remote_fail2ban_stat, remote_tail_text_file
from ..services.system_fail2ban import (
    Fail2banEvent,
    fail2ban_stat_with_sudo_async,
    load_json_file,
    parse_fail2ban_events,
    save_json_file,
    tail_text_file_with_sudo_async,
)
from .common import breadcrumbs, authorized_ids, html_escape, require_admin, send_to_many, ui_error_text, wrap_as_codeblock_html
from .status import build_status_message, get_server_target


def _f2b_menu_kb(server_key: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📜 Логи (tail)", callback_data=f"f2b:tail:{server_key}:200")],
        [InlineKeyboardButton("🧾 Выжимка за сутки", callback_data=f"f2b:digest:{server_key}")],
        [InlineKeyboardButton("🔙 Назад", callback_data=f"f2b:back:{server_key}")],
        [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
    ]
    return InlineKeyboardMarkup(rows)


def _f2b_tail_kb(server_key: str, current: int) -> InlineKeyboardMarkup:
    choices = [200, 600, 2000, 5000]
    row1: List[InlineKeyboardButton] = []
    row2: List[InlineKeyboardButton] = []
    for n in choices:
        label = f"{n} строк" + (" ✅" if n == current else "")
        button = InlineKeyboardButton(label, callback_data=f"f2b:tail:{server_key}:{n}")
        if len(row1) < 2:
            row1.append(button)
        else:
            row2.append(button)
    rows = [row1]
    if row2:
        rows.append(row2)
    rows.append([InlineKeyboardButton("🔙 Назад", callback_data=f"f2b:menu:{server_key}")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def _f2b_digest_kb(server_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔙 Назад", callback_data=f"f2b:menu:{server_key}")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


def _fmt_dt(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def _parse_server_key(data: str, action: str) -> Optional[str]:
    m = re.fullmatch(rf"f2b:{action}:({SERVER_KEY_PATTERN})", data or "")
    return m.group(1) if m else None


def _parse_server_tail(data: str) -> Optional[tuple[str, int]]:
    m = re.fullmatch(rf"f2b:tail:({SERVER_KEY_PATTERN}):(\d{{1,5}})", data or "")
    if not m:
        return None
    return m.group(1), int(m.group(2))


def _daily_state_path_for_server(server_key: str) -> str:
    p = Path(FAIL2BAN_STATE_PATH)
    suffix = p.suffix or ".json"
    stem = p.name[: -len(suffix)] if p.name.endswith(suffix) else p.name
    fname = f"{stem}.{server_key}{suffix}"
    return str(p.with_name(fname))


def _window_from_state(st: Dict[str, Any], until: datetime) -> datetime:
    try:
        if st.get("updated_at"):
            return datetime.fromisoformat(str(st["updated_at"])).astimezone(TZ) - timedelta(seconds=1)
    except Exception:
        pass
    return until - timedelta(days=1)


async def _daily_digest_events_for_server(server_key: str, since: datetime, until: datetime) -> tuple[Optional[object], Optional[str]]:
    srv = SERVERS.get(server_key)
    if not srv:
        return None, "server_not_found"
    try:
        if srv.mode == "ssh":
            events = await remote_fail2ban_events(
                srv.ssh_target, srv.fail2ban_log_path, n_lines=FAIL2BAN_DIGEST_TAIL_LINES
            )
        else:
            raw_tail = await tail_text_file_with_sudo_async(
                srv.fail2ban_log_path,
                n_lines=FAIL2BAN_DIGEST_TAIL_LINES,
                max_bytes=FAIL2BAN_DIGEST_MAX_BYTES,
            )
            events = parse_fail2ban_events(raw_tail.splitlines())
        return [e for e in events if since <= e.ts <= until], None
    except FileNotFoundError:
        return None, f"Лог-файл не найден: {srv.fail2ban_log_path}"
    except PermissionError:
        return None, f"Нет прав на чтение: {srv.fail2ban_log_path}"
    except Exception as e:
        return None, str(e)


async def build_fail2ban_menu_text(server_key: str) -> str:
    srv = get_server_target(server_key)
    if not srv:
        return "Сервер не найден."
    p = srv.fail2ban_log_path
    if srv.mode == "ssh":
        st = await remote_fail2ban_stat(srv.ssh_target, p)
        if st is not None:
            size_bytes, mtime = st
            return (
                f"<b>{html_escape(breadcrumbs('Админ-панель', 'Fail2ban', srv.label))}</b>\n\n"
                f"Файл: <code>{html_escape(str(p))}</code>\n"
                f"SSH host: <code>{html_escape(srv.ssh_target)}</code>\n"
                f"Размер: <code>{size_bytes / 1024.0:.1f} KiB</code>\n"
                f"Изменён: <code>{html_escape(_fmt_dt(mtime))}</code>\n\n"
                "Действия:"
            )
        return (
            f"<b>{html_escape(breadcrumbs('Админ-панель', 'Fail2ban', srv.label))}</b>\n\n"
            f"Файл: <code>{html_escape(str(p))}</code>\n"
            f"SSH host: <code>{html_escape(srv.ssh_target)}</code>\n\n"
            "Действия:"
        )

    try:
        st_local = await fail2ban_stat_with_sudo_async(p)
        if st_local is None:
            raise RuntimeError("stat unavailable")
        size_bytes, mtime = st_local
        size_kb = size_bytes / 1024.0
        return (
            f"<b>{html_escape(breadcrumbs('Админ-панель', 'Fail2ban', srv.label))}</b>\n\n"
            f"Файл: <code>{html_escape(str(p))}</code>\n"
            f"Размер: <code>{size_kb:.1f} KiB</code>\n"
            f"Изменён: <code>{html_escape(_fmt_dt(mtime))}</code>\n\n"
            "Действия:"
        )
    except Exception:
        return (
            f"<b>{html_escape(breadcrumbs('Админ-панель', 'Fail2ban', srv.label))}</b>\n\n"
            f"Файл: <code>{html_escape(str(p))}</code>\n\n"
            "Действия:"
        )


def build_fail2ban_digest_text(events: List[Fail2banEvent], since: datetime, until: datetime) -> str:
    per_jail: Dict[str, Dict[str, Any]] = {}
    for ev in events:
        j = per_jail.setdefault(ev.jail, {"ban": [], "unban": 0, "restore": 0, "started": 0, "stopped": 0})
        if ev.action == "Ban":
            j["ban"].append(ev)
        elif ev.action == "Unban":
            j["unban"] += 1
        elif ev.action == "Restore Ban":
            j["restore"] += 1
            j["ban"].append(ev)
        elif ev.action == "Jail started":
            j["started"] += 1
        elif ev.action == "Jail stopped":
            j["stopped"] += 1

    total_bans = sum(len(v["ban"]) for v in per_jail.values())
    header = (
        "🧾 <b>Fail2ban: выжимка</b>\n"
        f"Период: <code>{html_escape(_fmt_dt(since))}</code> — <code>{html_escape(_fmt_dt(until))}</code>\n"
    )
    if total_bans == 0 and not any((v["unban"] or v["started"] or v["stopped"]) for v in per_jail.values()):
        return header + "\nСобытий не найдено."

    lines: List[str] = [header]
    for jail in sorted(per_jail.keys()):
        v = per_jail[jail]
        bans: List[Fail2banEvent] = v["ban"]
        if not bans and not (v["unban"] or v["started"] or v["stopped"]):
            continue
        lines.append(f"\n<b>[{html_escape(jail)}]</b>")
        if v["started"]:
            lines.append(f"• jail started: <code>{v['started']}</code>")
        if v["stopped"]:
            lines.append(f"• jail stopped: <code>{v['stopped']}</code>")
        if bans:
            lines.append(f"• bans: <code>{len(bans)}</code> (включая restore={v['restore']})")
            last = sorted(bans, key=lambda e: e.ts)[-20:]
            for ev in last:
                ip = ev.ip or "-"
                lines.append(
                    f"  • <code>{html_escape(_fmt_dt(ev.ts))}</code> — <code>{html_escape(ip)}</code> ({html_escape(ev.action)})"
                )
            if len(bans) > 20:
                lines.append(f"  … ещё <code>{len(bans) - 20}</code> событий")
        if v["unban"]:
            lines.append(f"• unbans: <code>{v['unban']}</code>")

    limit = 3800
    out_lines: List[str] = []
    cur_len = 0
    truncated = False
    for line in lines:
        extra = len(line) + (1 if out_lines else 0)
        if cur_len + extra > limit - 40:
            truncated = True
            break
        out_lines.append(line)
        cur_len += extra
    if truncated:
        out_lines.append("… (обрезано из-за лимита Telegram)")
    return "\n".join(out_lines)


@require_admin
async def fail2ban_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    msg = update.effective_message
    if not msg:
        return
    server_key = next(iter(SERVERS.keys()), "")
    text = await build_fail2ban_menu_text(server_key)
    if q:
        await q.answer()
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=_f2b_menu_kb(server_key))
    else:
        await msg.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=_f2b_menu_kb(server_key))


@require_admin
async def f2b_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    server_key = _parse_server_key(q.data or "", "menu") or next(iter(SERVERS.keys()), "")
    await q.edit_message_text(
        await build_fail2ban_menu_text(server_key),
        parse_mode=ParseMode.HTML,
        reply_markup=_f2b_menu_kb(server_key),
    )


@require_admin
async def f2b_tail_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    parsed = _parse_server_tail(q.data or "")
    if not parsed:
        return
    server_key, n = parsed
    n = 200 if n < 50 else (5000 if n > 5000 else n)
    srv = get_server_target(server_key)
    if not srv:
        await q.edit_message_text(ui_error_text("сервер не найден."))
        return

    try:
        if srv.mode == "ssh":
            tail_txt = await remote_tail_text_file(srv.ssh_target, srv.fail2ban_log_path, n_lines=n)
        else:
            tail_txt = await tail_text_file_with_sudo_async(srv.fail2ban_log_path, n_lines=n)
        if not tail_txt.strip():
            payload = f"🛡 <b>Fail2ban: tail ({html_escape(srv.label)})</b>\n\nЛог пуст или отсутствуют строки."
        else:
            payload = (
                f"<b>{html_escape(breadcrumbs('Админ-панель', 'Fail2ban', srv.label, 'Tail'))}</b>\n\n"
                + wrap_as_codeblock_html(tail_txt)
            )
    except FileNotFoundError:
        payload = ui_error_text(f"лог-файл не найден: {srv.fail2ban_log_path}")
    except PermissionError:
        payload = ui_error_text(f"нет прав на чтение: {srv.fail2ban_log_path}")
    except Exception as e:
        payload = ui_error_text(str(e))

    await q.edit_message_text(payload, parse_mode=ParseMode.HTML, reply_markup=_f2b_tail_kb(server_key, current=n))


@require_admin
async def f2b_digest_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    server_key = _parse_server_key(q.data or "", "digest") or next(iter(SERVERS.keys()), "")
    srv = get_server_target(server_key)
    if not srv:
        await q.edit_message_text(ui_error_text("сервер не найден."))
        return

    until = datetime.now(tz=TZ)
    since = until - timedelta(days=1)
    try:
        if srv.mode == "ssh":
            events = await remote_fail2ban_events(
                srv.ssh_target, srv.fail2ban_log_path, n_lines=FAIL2BAN_DIGEST_TAIL_LINES
            )
        else:
            raw_tail = await tail_text_file_with_sudo_async(
                srv.fail2ban_log_path,
                n_lines=FAIL2BAN_DIGEST_TAIL_LINES,
                max_bytes=FAIL2BAN_DIGEST_MAX_BYTES,
            )
            events = parse_fail2ban_events(raw_tail.splitlines())
        events = [e for e in events if since <= e.ts <= until]
        payload = f"🌍 <b>Сервер:</b> {html_escape(srv.label)}\n" + build_fail2ban_digest_text(events, since=since, until=until)
    except FileNotFoundError:
        payload = (
            f"🧾 <b>Fail2ban: выжимка ({html_escape(srv.label)})</b>\n\n"
            f"Лог-файл не найден: <code>{html_escape(srv.fail2ban_log_path)}</code>"
        )
    except PermissionError:
        payload = (
            f"🧾 <b>Fail2ban: выжимка ({html_escape(srv.label)})</b>\n\n"
            f"Нет прав на чтение: <code>{html_escape(srv.fail2ban_log_path)}</code>"
        )
    except Exception as e:
        payload = f"🧾 <b>Fail2ban: выжимка ({html_escape(srv.label)})</b>\n\nОшибка: <code>{html_escape(str(e))}</code>"

    await q.edit_message_text(payload, parse_mode=ParseMode.HTML, reply_markup=_f2b_digest_kb(server_key))


@require_admin
async def f2b_back_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    server_key = _parse_server_key(q.data or "", "back") or next(iter(SERVERS.keys()), "")
    text, markup = await build_status_message(update, server_key=server_key)
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


async def fail2ban_daily_digest(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        admin_ids = authorized_ids(role_filter="admin")
        if not admin_ids:
            return

        until = datetime.now(tz=TZ)
        for server_key, srv in SERVERS.items():
            state_path = _daily_state_path_for_server(server_key)
            st_before = await load_json_file(state_path)
            since = _window_from_state(st_before, until)

            events, err = await _daily_digest_events_for_server(server_key, since=since, until=until)

            if err:
                logger.warning("fail2ban_daily_digest %s (%s) skipped: %s", server_key, srv.label, err)
                continue

            new_state = {"updated_at": until.isoformat(), "server_key": server_key}

            if not isinstance(events, list):
                logger.warning(
                    "fail2ban_daily_digest %s (%s) skipped: parser returned non-list (%s); state preserved",
                    server_key,
                    srv.label,
                    type(events).__name__,
                )
                continue

            ban_events = [e for e in events if e.action in ("Ban", "Restore Ban")]
            if not ban_events:
                await save_json_file(state_path, new_state)
                continue

            payload = f"🌍 <b>Сервер:</b> {html_escape(srv.label)}\n" + build_fail2ban_digest_text(events, since=since, until=until)
            await send_to_many(context, admin_ids, payload)
            await save_json_file(state_path, new_state)
    except Exception:
        logger.exception("fail2ban_daily_digest error")
