from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import Any, Dict, List, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..config import FAIL2BAN_LOG_PATH, FAIL2BAN_STATE_PATH, SERVERS, TZ, logger
from ..services.remote_service import remote_fail2ban_events_last_day, remote_fail2ban_stat, remote_tail_text_file
from ..services.system_service import (
    Fail2banEvent,
    load_json_file,
    parse_fail2ban_events,
    read_fail2ban_new_lines_with_state_async,
    save_json_file,
    tail_text_file_async,
)
from .common import authorized_ids, clip_text, html_escape, require_admin, send_to_many, wrap_as_codeblock_html
from .status import build_status_message, get_server_target


def _f2b_menu_kb(server_key: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📜 Логи (tail)", callback_data=f"f2b:tail:{server_key}:200")],
        [InlineKeyboardButton("🧾 Выжимка за сутки", callback_data=f"f2b:digest:{server_key}")],
        [InlineKeyboardButton("🔙 Назад", callback_data=f"f2b:back:{server_key}")],
    ]
    return InlineKeyboardMarkup(rows)


def _f2b_tail_kb(server_key: str, current: int) -> InlineKeyboardMarkup:
    choices = [200, 600, 2000]
    row: List[InlineKeyboardButton] = []
    for n in choices:
        label = f"{n} строк" + (" ✅" if n == current else "")
        row.append(InlineKeyboardButton(label, callback_data=f"f2b:tail:{server_key}:{n}"))
    return InlineKeyboardMarkup([row, [InlineKeyboardButton("🔙 Назад", callback_data=f"f2b:menu:{server_key}")]])


def _f2b_digest_kb(server_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=f"f2b:menu:{server_key}")]])


def _fmt_dt(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def _parse_server_key(data: str, action: str) -> Optional[str]:
    m = re.fullmatch(rf"f2b:{action}:([a-z0-9_-]{{1,12}})", data or "")
    return m.group(1) if m else None


def _parse_server_tail(data: str) -> Optional[tuple[str, int]]:
    m = re.fullmatch(r"f2b:tail:([a-z0-9_-]{1,12}):(\d{1,5})", data or "")
    if not m:
        return None
    return m.group(1), int(m.group(2))


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
                f"🛡 <b>Fail2ban ({html_escape(srv.label)})</b>\n\n"
                f"Файл: <code>{html_escape(str(p))}</code>\n"
                f"SSH host: <code>{html_escape(srv.ssh_target)}</code>\n"
                f"Размер: <code>{size_bytes / 1024.0:.1f} KiB</code>\n"
                f"Изменён: <code>{html_escape(_fmt_dt(mtime))}</code>\n\n"
                "Действия:"
            )
        return (
            f"🛡 <b>Fail2ban ({html_escape(srv.label)})</b>\n\n"
            f"Файл: <code>{html_escape(str(p))}</code>\n"
            f"SSH host: <code>{html_escape(srv.ssh_target)}</code>\n\n"
            "Действия:"
        )

    try:
        st_local = Path(p).stat()
        mtime = datetime.fromtimestamp(st_local.st_mtime, tz=TZ)
        size_kb = st_local.st_size / 1024.0
        return (
            f"🛡 <b>Fail2ban ({html_escape(srv.label)})</b>\n\n"
            f"Файл: <code>{html_escape(str(p))}</code>\n"
            f"Размер: <code>{size_kb:.1f} KiB</code>\n"
            f"Изменён: <code>{html_escape(_fmt_dt(mtime))}</code>\n\n"
            "Действия:"
        )
    except Exception:
        return (
            f"🛡 <b>Fail2ban ({html_escape(srv.label)})</b>\n\n"
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
    msg = update.effective_message
    if not msg:
        return
    server_key = next(iter(SERVERS.keys()), "")
    await msg.reply_text(
        await build_fail2ban_menu_text(server_key),
        parse_mode=ParseMode.HTML,
        reply_markup=_f2b_menu_kb(server_key),
    )


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
        await q.edit_message_text("Сервер не найден.")
        return

    try:
        if srv.mode == "ssh":
            tail_txt = await remote_tail_text_file(srv.ssh_target, srv.fail2ban_log_path, n_lines=n)
        else:
            tail_txt = await tail_text_file_async(srv.fail2ban_log_path, n_lines=n)
        if not tail_txt.strip():
            payload = f"🛡 <b>Fail2ban: tail ({html_escape(srv.label)})</b>\n\nЛог пуст или отсутствуют строки."
        else:
            payload = f"🛡 <b>Fail2ban: tail ({html_escape(srv.label)})</b>\n\n" + wrap_as_codeblock_html(clip_text(tail_txt))
    except FileNotFoundError:
        payload = (
            f"🛡 <b>Fail2ban: tail ({html_escape(srv.label)})</b>\n\n"
            f"Лог-файл не найден: <code>{html_escape(srv.fail2ban_log_path)}</code>"
        )
    except PermissionError:
        payload = (
            f"🛡 <b>Fail2ban: tail ({html_escape(srv.label)})</b>\n\n"
            f"Нет прав на чтение: <code>{html_escape(srv.fail2ban_log_path)}</code>"
        )
    except Exception as e:
        payload = f"🛡 <b>Fail2ban: tail ({html_escape(srv.label)})</b>\n\nОшибка: <code>{html_escape(str(e))}</code>"

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
        await q.edit_message_text("Сервер не найден.")
        return

    until = datetime.now(tz=TZ)
    since = until - timedelta(days=1)
    try:
        if srv.mode == "ssh":
            events = await remote_fail2ban_events_last_day(srv.ssh_target, srv.fail2ban_log_path)
        else:
            raw_tail = await tail_text_file_async(srv.fail2ban_log_path, n_lines=20000, max_bytes=3_000_000)
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
        st_before = await load_json_file(FAIL2BAN_STATE_PATH)
        raw_lines, new_state = await read_fail2ban_new_lines_with_state_async(FAIL2BAN_LOG_PATH, FAIL2BAN_STATE_PATH)
        if not raw_lines:
            if new_state is not None:
                await save_json_file(FAIL2BAN_STATE_PATH, new_state)
            return
        events = parse_fail2ban_events(raw_lines)

        ban_events = [e for e in events if e.action in ("Ban", "Restore Ban")]
        if not ban_events:
            if new_state is not None:
                await save_json_file(FAIL2BAN_STATE_PATH, new_state)
            return

        until = datetime.now(tz=TZ)
        since = None
        try:
            if st_before.get("updated_at"):
                since = datetime.fromisoformat(st_before["updated_at"]).astimezone(TZ) - timedelta(seconds=1)
        except Exception:
            since = None
        if since is None:
            since = until - timedelta(days=1)

        payload = build_fail2ban_digest_text(events, since=since, until=until)

        admin_ids = authorized_ids(role_filter="admin")
        if not admin_ids:
            if new_state is not None:
                await save_json_file(FAIL2BAN_STATE_PATH, new_state)
            return
        await send_to_many(context, admin_ids, payload)
        if new_state is not None:
            await save_json_file(FAIL2BAN_STATE_PATH, new_state)
    except Exception:
        logger.exception("fail2ban_daily_digest error")
