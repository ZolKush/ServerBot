from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import Any, Dict, List

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..config import FAIL2BAN_LOG_PATH, FAIL2BAN_STATE_PATH, TZ, logger
from ..services.system_service import (
    Fail2banEvent,
    load_json_file,
    parse_fail2ban_events,
    read_fail2ban_new_lines_async,
    tail_text_file_async,
)
from .common import authorized_ids, clip_text, html_escape, require_admin, send_to_many, wrap_as_codeblock_html
from .status import build_status_message


def _f2b_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📜 Логи (tail)", callback_data="f2b:tail:200")],
            [InlineKeyboardButton("🧾 Выжимка за сутки", callback_data="f2b:digest")],
            [InlineKeyboardButton("🔙 Назад", callback_data="f2b:back")],
        ]
    )


def _f2b_tail_kb(current: int) -> InlineKeyboardMarkup:
    choices = [200, 600, 2000]
    rows = []
    row: List[InlineKeyboardButton] = []
    for n in choices:
        label = f"{n} строк" + (" ✅" if n == current else "")
        row.append(InlineKeyboardButton(label, callback_data=f"f2b:tail:{n}"))
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("🔙 Назад", callback_data="f2b:menu")])
    return InlineKeyboardMarkup(rows)


def _f2b_digest_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="f2b:menu")]])


def _fmt_dt(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def build_fail2ban_menu_text() -> str:
    p = FAIL2BAN_LOG_PATH
    try:
        st = Path(p).stat()
        mtime = datetime.fromtimestamp(st.st_mtime, tz=TZ)
        size_kb = st.st_size / 1024.0
        return (
            "🛡 <b>Fail2ban</b>\n\n"
            f"Файл: <code>{html_escape(str(p))}</code>\n"
            f"Размер: <code>{size_kb:.1f} KiB</code>\n"
            f"Изменён: <code>{html_escape(_fmt_dt(mtime))}</code>\n\n"
            "Действия:"
        )
    except Exception:
        return (
            "🛡 <b>Fail2ban</b>\n\n"
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

    out = "\n".join(lines)

    if len(out) > 3800:
        out = out[:3700] + "\n… (обрезано из-за лимита Telegram)"
    return out


@require_admin
async def fail2ban_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if msg:
        await msg.reply_text(build_fail2ban_menu_text(), parse_mode=ParseMode.HTML, reply_markup=_f2b_menu_kb())


@require_admin
async def f2b_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    await q.edit_message_text(build_fail2ban_menu_text(), parse_mode=ParseMode.HTML, reply_markup=_f2b_menu_kb())


@require_admin
async def f2b_tail_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"f2b:tail:(\d{1,5})", q.data or "")
    if not m:
        return
    n = int(m.group(1))
    n = 200 if n < 50 else (5000 if n > 5000 else n)

    try:
        tail_txt = await tail_text_file_async(FAIL2BAN_LOG_PATH, n_lines=n)
        if not tail_txt.strip():
            payload = "🛡 <b>Fail2ban: tail</b>\n\nЛог пуст или отсутствуют строки."
        else:
            payload = "🛡 <b>Fail2ban: tail</b>\n\n" + wrap_as_codeblock_html(clip_text(tail_txt))
    except FileNotFoundError:
        payload = (
            "🛡 <b>Fail2ban: tail</b>\n\n"
            f"Лог-файл не найден: <code>{html_escape(FAIL2BAN_LOG_PATH)}</code>"
        )
    except PermissionError:
        payload = (
            "🛡 <b>Fail2ban: tail</b>\n\n"
            f"Нет прав на чтение: <code>{html_escape(FAIL2BAN_LOG_PATH)}</code>\n"
            "Запустите бота с правами, позволяющими читать лог, либо настройте права на файл."
        )
    except Exception as e:
        payload = f"🛡 <b>Fail2ban: tail</b>\n\nОшибка: <code>{html_escape(str(e))}</code>"

    await q.edit_message_text(payload, parse_mode=ParseMode.HTML, reply_markup=_f2b_tail_kb(current=n))


@require_admin
async def f2b_digest_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    until = datetime.now(tz=TZ)
    since = until - timedelta(days=1)

    try:
        raw_tail = await tail_text_file_async(FAIL2BAN_LOG_PATH, n_lines=20000, max_bytes=3_000_000)
        events = parse_fail2ban_events(raw_tail.splitlines())
        events = [e for e in events if since <= e.ts <= until]
        payload = build_fail2ban_digest_text(events, since=since, until=until)
    except FileNotFoundError:
        payload = (
            "🧾 <b>Fail2ban: выжимка</b>\n\n"
            f"Лог-файл не найден: <code>{html_escape(FAIL2BAN_LOG_PATH)}</code>"
        )
    except PermissionError:
        payload = (
            "🧾 <b>Fail2ban: выжимка</b>\n\n"
            f"Нет прав на чтение: <code>{html_escape(FAIL2BAN_LOG_PATH)}</code>"
        )
    except Exception as e:
        payload = f"🧾 <b>Fail2ban: выжимка</b>\n\nОшибка: <code>{html_escape(str(e))}</code>"

    await q.edit_message_text(payload, parse_mode=ParseMode.HTML, reply_markup=_f2b_digest_kb())


@require_admin
async def f2b_back_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    text, markup = await build_status_message(update)
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


async def fail2ban_daily_digest(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        st_before = await load_json_file(FAIL2BAN_STATE_PATH)
        raw_lines = await read_fail2ban_new_lines_async(FAIL2BAN_LOG_PATH, FAIL2BAN_STATE_PATH)
        if not raw_lines:
            return
        events = parse_fail2ban_events(raw_lines)

        ban_events = [e for e in events if e.action in ("Ban", "Restore Ban")]
        if not ban_events:
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
            return
        await send_to_many(context, admin_ids, payload)
    except Exception:
        logger.exception("fail2ban_daily_digest error")
