import asyncio
import contextlib
import hashlib
import re
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..config import (
    FAIL2BAN_DIGEST_MAX_BYTES,
    FAIL2BAN_DIGEST_TAIL_LINES,
    SERVER_KEY_PATTERN,
    SERVERS,
    TZ,
    logger,
)
from ..services.outbox import message_payload
from ..services.remote_service import (
    remote_fail2ban_events,
    remote_fail2ban_identity,
    remote_fail2ban_stat,
    remote_read_text_range,
    remote_tail_text_file,
)
from ..services.system_fail2ban import (
    Fail2banEvent,
    FileIdentity,
    fail2ban_identity_with_sudo_async,
    fail2ban_stat_with_sudo_async,
    parse_fail2ban_events,
    read_text_range_with_sudo_async,
    tail_text_file_with_sudo_async,
)
from ..storage import (
    ImportantData,
    enqueue_important_outbox,
    get_fail2ban_cursor,
    make_outbox_event,
    outbox_snapshot,
    update_important_data,
)
from .common import authorized_ids, html_escape, require_admin, ui_error_text, wrap_as_codeblock_html
from .status import build_status_message, get_server_target
from .ui import SEP, plural_ru


def _f2b_menu_kb(server_key: str) -> InlineKeyboardMarkup:
    server = SERVERS.get(server_key)
    if server and not server.fail2ban_enabled:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⬅️ Назад", callback_data=f"f2b:back:{server_key}")],
                [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
            ]
        )
    rows = [
        [InlineKeyboardButton("📜 Логи (tail)", callback_data=f"f2b:tail:{server_key}:200")],
        [InlineKeyboardButton("🧾 Выжимка за сутки", callback_data=f"f2b:digest:{server_key}")],
        [InlineKeyboardButton("⬅️ Назад", callback_data=f"f2b:back:{server_key}")],
        [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
    ]
    return InlineKeyboardMarkup(rows)


def _f2b_tail_kb(server_key: str, current: int) -> InlineKeyboardMarkup:
    choices = [200, 600, 2000, 5000]
    row1: list[InlineKeyboardButton] = []
    row2: list[InlineKeyboardButton] = []
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
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"f2b:menu:{server_key}")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def _f2b_digest_kb(server_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"f2b:menu:{server_key}")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


def _fmt_dt(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%d.%m.%Y %H:%M")


def _fmt_dt_event(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%d.%m %H:%M")


def _parse_server_key(data: str, action: str) -> str | None:
    m = re.fullmatch(rf"f2b:{action}:({SERVER_KEY_PATTERN})", data or "")
    return m.group(1) if m else None


def _parse_server_tail(data: str) -> tuple[str, int] | None:
    m = re.fullmatch(rf"f2b:tail:({SERVER_KEY_PATTERN}):(\d{{1,5}})", data or "")
    if not m:
        return None
    return m.group(1), int(m.group(2))


def _server_timezone(server_key: str) -> ZoneInfo:
    server = SERVERS[server_key]
    try:
        return ZoneInfo(server.fail2ban_timezone or str(TZ))
    except Exception:
        logger.warning("Invalid fail2ban timezone for server=%s; using bot timezone", server_key)
        return TZ


async def _file_identity(server_key: str, path: str) -> FileIdentity | None:
    server = SERVERS[server_key]
    if server.mode == "ssh":
        return await remote_fail2ban_identity(server.ssh_target, path)
    return await fail2ban_identity_with_sudo_async(path)


async def _try_file_identity(server_key: str, path: str) -> FileIdentity | None:
    try:
        return await _file_identity(server_key, path)
    except (FileNotFoundError, PermissionError):
        return None


async def _read_range(server_key: str, path: str, offset: int, limit: int) -> tuple[str, int]:
    server = SERVERS[server_key]
    if server.mode == "ssh":
        return await remote_read_text_range(server.ssh_target, path, offset, limit)
    return await read_text_range_with_sudo_async(path, offset, limit)


def _identity_matches(cursor: dict[str, Any], identity: FileIdentity) -> bool:
    try:
        return int(cursor.get("device", -1)) == identity.device and int(cursor.get("inode", -1)) == identity.inode
    except (TypeError, ValueError):
        return False


def _cursor_has_pending_delivery(server_key: str) -> bool:
    for source, event in outbox_snapshot():
        if source != "important":
            continue
        completion = event.get("completion")
        if (
            isinstance(completion, dict)
            and completion.get("type") == "fail2ban_cursor"
            and completion.get("server_key") == server_key
        ):
            return True
    return False


async def _read_fail2ban_increment(
    server_key: str,
    cursor: dict[str, Any] | None,
) -> tuple[list[Fail2banEvent], dict[str, Any], datetime, bool]:
    server = SERVERS[server_key]
    base_path = server.fail2ban_log_path
    current_identity = await _file_identity(server_key, base_path)
    if current_identity is None:
        raise RuntimeError("stat fail2ban log unavailable")

    source_path = base_path
    source_identity = current_identity
    offset = 0
    carry = ""
    first_run = not cursor
    drop_prefix = False
    if cursor:
        source_path = str(cursor.get("path") or base_path)
        source_identity = await _try_file_identity(server_key, source_path) or current_identity
        if not _identity_matches(cursor, source_identity):
            rotated_path = base_path + ".1"
            rotated_identity = await _try_file_identity(server_key, rotated_path)
            if rotated_identity and _identity_matches(cursor, rotated_identity):
                source_path = rotated_path
                source_identity = rotated_identity
            else:
                source_path = base_path
                source_identity = current_identity
                offset = 0
        if _identity_matches(cursor, source_identity):
            try:
                offset = max(0, int(cursor.get("offset", 0) or 0))
            except (TypeError, ValueError):
                offset = 0
            carry = str(cursor.get("carry") or "")[-8192:]
        if source_identity.size < offset:
            # copytruncate rotation
            offset = 0
            carry = ""
    else:
        offset = max(0, current_identity.size - FAIL2BAN_DIGEST_MAX_BYTES)
        drop_prefix = offset > 0

    text, consumed = await _read_range(server_key, source_path, offset, FAIL2BAN_DIGEST_MAX_BYTES)
    if drop_prefix and "\n" in text:
        text = text.split("\n", 1)[1]
    elif drop_prefix:
        text = ""

    combined = carry + text
    if combined.endswith("\n"):
        complete_lines = combined.splitlines()
        next_carry = ""
    else:
        parts = combined.split("\n")
        complete_lines = parts[:-1]
        next_carry = parts[-1][-8192:] if parts else ""

    next_offset = offset + consumed
    next_path = source_path
    next_identity = source_identity
    has_more = next_offset < source_identity.size
    if source_path != base_path and next_offset >= source_identity.size:
        # The old inode has been drained; the next run starts from the new log.
        if next_carry:
            complete_lines.append(next_carry)
            next_carry = ""
        next_path = base_path
        next_identity = current_identity
        next_offset = 0
        has_more = current_identity.size > 0

    timezone = _server_timezone(server_key)
    events = parse_fail2ban_events(complete_lines, timezone=timezone)
    previous_fingerprint_list = [
        str(value) for value in ((cursor or {}).get("recent_fingerprints") or []) if isinstance(value, str)
    ][-200:]
    seen_fingerprints = set(previous_fingerprint_list)
    deduplicated: list[Fail2banEvent] = []
    new_fingerprints: list[str] = []
    for event in events:
        fingerprint = hashlib.sha256(event.raw.encode("utf-8", errors="replace")).hexdigest()[:24]
        if fingerprint in seen_fingerprints:
            continue
        seen_fingerprints.add(fingerprint)
        deduplicated.append(event)
        new_fingerprints.append(fingerprint)

    since = datetime.now(TZ) - timedelta(days=1)
    if cursor and cursor.get("updated_at"):
        with contextlib.suppress(TypeError, ValueError):
            since = datetime.fromisoformat(str(cursor["updated_at"])).astimezone(TZ)
    elif first_run:
        deduplicated = [event for event in deduplicated if event.ts.astimezone(TZ) >= since]

    next_cursor = {
        "path": next_path,
        "device": next_identity.device,
        "inode": next_identity.inode,
        "offset": next_offset,
        "carry": next_carry,
        "updated_at": datetime.now(TZ).isoformat(),
        "recent_fingerprints": [*previous_fingerprint_list[-100:], *new_fingerprints][-200:],
    }
    return deduplicated, next_cursor, since, has_more


async def build_fail2ban_menu_text(server_key: str) -> str:
    srv = get_server_target(server_key)
    if not srv:
        return "Сервер не найден."
    p = srv.fail2ban_log_path
    title = f"🛡 <b>Fail2ban — {html_escape(srv.label)}</b>\n{SEP}\n"
    if not srv.fail2ban_enabled:
        return title + "Ежедневный сбор и просмотр отключены для этого сервера в конфигурации."
    if srv.mode == "ssh":
        st = await remote_fail2ban_stat(srv.ssh_target, p)
        if st is not None:
            size_bytes, mtime = st
            return (
                title + f"Файл: <code>{html_escape(str(p))}</code>\n"
                f"SSH host: <code>{html_escape(srv.ssh_target)}</code>\n"
                f"Размер: <code>{size_bytes / 1024.0:.1f} KiB</code>\n"
                f"Изменён: <code>{html_escape(_fmt_dt(mtime))}</code>\n\n"
                "Выберите действие:"
            )
        return (
            title + f"Файл: <code>{html_escape(str(p))}</code>\n"
            f"SSH host: <code>{html_escape(srv.ssh_target)}</code>\n\n"
            "Выберите действие:"
        )

    try:
        st_local = await fail2ban_stat_with_sudo_async(p)
        if st_local is None:
            raise RuntimeError("stat unavailable")
        size_bytes, mtime = st_local
        size_kb = size_bytes / 1024.0
        return (
            title + f"Файл: <code>{html_escape(str(p))}</code>\n"
            f"Размер: <code>{size_kb:.1f} KiB</code>\n"
            f"Изменён: <code>{html_escape(_fmt_dt(mtime))}</code>\n\n"
            "Выберите действие:"
        )
    except Exception:
        return title + f"Файл: <code>{html_escape(str(p))}</code>\n\nВыберите действие:"


def build_fail2ban_digest_text(events: list[Fail2banEvent], since: datetime, until: datetime) -> str:
    per_jail: dict[str, dict[str, Any]] = {}
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
        "🛡 <b>Fail2ban — выжимка за сутки</b>\n"
        f"Период: <code>{html_escape(_fmt_dt(since))}</code> — <code>{html_escape(_fmt_dt(until))}</code>\n"
        f"{SEP}\n"
    )
    if total_bans == 0 and not any((v["unban"] or v["started"] or v["stopped"]) for v in per_jail.values()):
        return header + "\nСобытий не найдено."

    lines: list[str] = [header]
    for jail in sorted(per_jail.keys()):
        v = per_jail[jail]
        bans: list[Fail2banEvent] = v["ban"]
        if not bans and not (v["unban"] or v["started"] or v["stopped"]):
            continue
        lines.append(f"\n<b>[{html_escape(jail)}]</b>")
        if v["started"]:
            lines.append(f"• Запусков jail: <code>{v['started']}</code>")
        if v["stopped"]:
            lines.append(f"• Остановок jail: <code>{v['stopped']}</code>")
        if bans:
            ban_line = f"• {plural_ru(len(bans), 'Бан', 'Бана', 'Банов')}: <code>{len(bans)}</code>"
            if v["restore"]:
                ban_line += f" (повторных: <code>{v['restore']}</code>)"
            lines.append(ban_line)
            last = sorted(bans, key=lambda e: e.ts)[-20:]
            for ev in last:
                ip = ev.ip or "-"
                lines.append(
                    f"  • <code>{html_escape(_fmt_dt_event(ev.ts))}</code> — <code>{html_escape(ip)}</code> ({html_escape(ev.action)})"
                )
            if len(bans) > 20:
                hidden = len(bans) - 20
                lines.append(f"  … ещё <code>{hidden}</code> {plural_ru(hidden, 'событие', 'события', 'событий')}")
        if v["unban"]:
            lines.append(f"• Разбанов: <code>{v['unban']}</code>")

    limit = 3800
    out_lines: list[str] = []
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
    await q.answer("Загружаю логи…")
    parsed = _parse_server_tail(q.data or "")
    if not parsed:
        return
    server_key, n = parsed
    n = 200 if n < 50 else (5000 if n > 5000 else n)
    srv = get_server_target(server_key)
    if not srv:
        await q.edit_message_text(ui_error_text("сервер не найден."))
        return
    if not srv.fail2ban_enabled:
        await q.edit_message_text("Fail2ban отключён для этого сервера.", reply_markup=_f2b_menu_kb(server_key))
        return

    try:
        if srv.mode == "ssh":
            tail_txt = await remote_tail_text_file(srv.ssh_target, srv.fail2ban_log_path, n_lines=n)
        else:
            tail_txt = await tail_text_file_with_sudo_async(srv.fail2ban_log_path, n_lines=n)
        if not tail_txt.strip():
            payload = f"🛡 <b>Fail2ban — {html_escape(srv.label)} · tail</b>\n\nЛог пуст или отсутствуют строки."
        else:
            payload = f"🛡 <b>Fail2ban — {html_escape(srv.label)} · tail</b>\n{SEP}\n" + wrap_as_codeblock_html(tail_txt)
    except FileNotFoundError:
        payload = ui_error_text(f"лог-файл не найден: {html_escape(str(srv.fail2ban_log_path))}")
    except PermissionError:
        payload = ui_error_text(f"нет прав на чтение: {html_escape(str(srv.fail2ban_log_path))}")
    except Exception as e:
        payload = ui_error_text(html_escape(str(e)))

    await q.edit_message_text(payload, parse_mode=ParseMode.HTML, reply_markup=_f2b_tail_kb(server_key, current=n))


@require_admin
async def f2b_digest_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer("Готовлю выжимку…")
    server_key = _parse_server_key(q.data or "", "digest") or next(iter(SERVERS.keys()), "")
    srv = get_server_target(server_key)
    if not srv:
        await q.edit_message_text(ui_error_text("сервер не найден."))
        return
    if not srv.fail2ban_enabled:
        await q.edit_message_text("Fail2ban отключён для этого сервера.", reply_markup=_f2b_menu_kb(server_key))
        return

    until = datetime.now(tz=TZ)
    since = until - timedelta(days=1)
    try:
        if srv.mode == "ssh":
            events = await remote_fail2ban_events(
                srv.ssh_target,
                srv.fail2ban_log_path,
                n_lines=FAIL2BAN_DIGEST_TAIL_LINES,
                timezone=_server_timezone(server_key),
                max_bytes=FAIL2BAN_DIGEST_MAX_BYTES,
            )
        else:
            raw_tail = await tail_text_file_with_sudo_async(
                srv.fail2ban_log_path,
                n_lines=FAIL2BAN_DIGEST_TAIL_LINES,
                max_bytes=FAIL2BAN_DIGEST_MAX_BYTES,
            )
            events = parse_fail2ban_events(raw_tail.splitlines(), timezone=_server_timezone(server_key))
        events = [e for e in events if since <= e.ts <= until]
        payload = f"🌍 <b>Сервер:</b> {html_escape(srv.label)}\n" + build_fail2ban_digest_text(
            events, since=since, until=until
        )
    except FileNotFoundError:
        payload = (
            f"🛡 <b>Fail2ban — выжимка ({html_escape(srv.label)})</b>\n\n"
            f"Лог-файл не найден: <code>{html_escape(srv.fail2ban_log_path)}</code>"
        )
    except PermissionError:
        payload = (
            f"🛡 <b>Fail2ban — выжимка ({html_escape(srv.label)})</b>\n\n"
            f"Нет прав на чтение: <code>{html_escape(srv.fail2ban_log_path)}</code>"
        )
    except Exception as e:
        payload = (
            f"🛡 <b>Fail2ban — выжимка ({html_escape(srv.label)})</b>\n\nОшибка: <code>{html_escape(str(e))}</code>"
        )

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
    admin_ids = authorized_ids(role_filter="admin")
    if not admin_ids:
        return
    semaphore = asyncio.Semaphore(3)

    async def _process_server(server_key: str) -> None:
        server = SERVERS[server_key]
        if not server.fail2ban_enabled or _cursor_has_pending_delivery(server_key):
            return
        async with semaphore:
            try:
                original_cursor = get_fail2ban_cursor(server_key)
                working_cursor = original_cursor
                all_events: list[Fail2banEvent] = []
                since = datetime.now(TZ) - timedelta(days=1)
                # A bounded catch-up loop avoids dropping high-volume logs while
                # still limiting RAM and SSH output per scheduled run.
                for _ in range(4):
                    events, next_cursor, batch_since, has_more = await _read_fail2ban_increment(
                        server_key, working_cursor
                    )
                    all_events.extend(events)
                    since = min(since, batch_since)
                    working_cursor = next_cursor
                    if not has_more:
                        break

                if working_cursor is None:
                    return
                until = datetime.now(TZ)
                ban_events = [event for event in all_events if event.action in {"Ban", "Restore Ban"}]
                expected_cursor = original_cursor or {}

                if not ban_events:

                    def _advance_without_delivery(cfg: ImportantData) -> bool:
                        current = cfg.fail2ban_cursors.get(server_key) or {}
                        if current != expected_cursor:
                            return False
                        cfg.fail2ban_cursors[server_key] = working_cursor
                        return True

                    advanced = await update_important_data(_advance_without_delivery)
                    if not advanced:
                        logger.info("Fail2ban cursor changed concurrently; skip advance server=%s", server_key)
                    return

                payload = f"🌍 <b>Сервер:</b> {html_escape(server.label)}\n" + build_fail2ban_digest_text(
                    all_events, since=since, until=until
                )
                event = make_outbox_event(
                    kind="fail2ban_daily_digest",
                    recipient_ids=admin_ids,
                    payload=message_payload(payload),
                )
                event["completion"] = {
                    "type": "fail2ban_cursor",
                    "server_key": server_key,
                    "cursor": working_cursor,
                }

                def _queue_digest(cfg: ImportantData) -> bool:
                    current = cfg.fail2ban_cursors.get(server_key) or {}
                    if current != expected_cursor:
                        return False
                    for pending in cfg.outbox.values():
                        if not isinstance(pending, dict):
                            continue
                        completion = pending.get("completion")
                        if (
                            isinstance(completion, dict)
                            and completion.get("type") == "fail2ban_cursor"
                            and completion.get("server_key") == server_key
                        ):
                            return False
                    enqueue_important_outbox(cfg, event)
                    return True

                queued = await update_important_data(_queue_digest)
                if not queued:
                    logger.info("Fail2ban digest already queued or cursor changed server=%s", server_key)
            except FileNotFoundError:
                logger.warning("fail2ban_daily_digest server=%s: log file not found", server_key)
            except PermissionError:
                logger.warning("fail2ban_daily_digest server=%s: permission denied", server_key)
            except Exception:
                logger.exception("fail2ban_daily_digest failed for server=%s", server_key)

    await asyncio.gather(*(_process_server(server_key) for server_key in SERVERS))
