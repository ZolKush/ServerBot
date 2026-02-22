import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ..config import SERVERS, TZ, TZ_NAME, logger
from ..storage import _clear_maintenance, _get_active_maintenance, _set_maintenance, update_important_data
from .common import authorized_ids, display_name, get_user_id, html_escape, require_admin, send_to_many

STATE_MAINT_SCOPE, STATE_MAINT_URGENCY, STATE_MAINT_DURATION, STATE_MAINT_EXTEND = range(4)
MAINT_SCOPE_ALL = "all"


def _server_items() -> List[Tuple[str, str]]:
    return [(k, v.label) for k, v in SERVERS.items()]


def _normalize_scope(scope: Optional[str]) -> str:
    s = (scope or "").strip().lower()
    if s == MAINT_SCOPE_ALL:
        return MAINT_SCOPE_ALL
    return s if s in SERVERS else MAINT_SCOPE_ALL


def _scope_label(scope: Optional[str]) -> str:
    scope_n = _normalize_scope(scope)
    if scope_n == MAINT_SCOPE_ALL:
        labels = [lbl for _, lbl in _server_items()]
        return ", ".join(labels) if labels else "Все серверы"
    srv = SERVERS.get(scope_n)
    return srv.label if srv else scope_n


def _scope_line(scope: Optional[str]) -> str:
    scope_n = _normalize_scope(scope)
    if scope_n == MAINT_SCOPE_ALL:
        return f"• Серверы: <b>{html_escape(_scope_label(scope_n))}</b>"
    return f"• Сервер: <b>{html_escape(_scope_label(scope_n))}</b>"


def scope_kb() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton("🌐 Общие техработы", callback_data=f"maint:scope:{MAINT_SCOPE_ALL}")]
    ]
    for key, label in _server_items():
        rows.append([InlineKeyboardButton(f"🖥 {label}", callback_data=f"maint:scope:{key}")])
    return InlineKeyboardMarkup(rows)


def urgency_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔥 Срочные", callback_data="maint:urgency:urgent"),
                InlineKeyboardButton("🗓 Плановые", callback_data="maint:urgency:planned"),
            ]
        ]
    )


def parse_hhmm(text: str) -> Optional[Tuple[int, int]]:
    m = re.fullmatch(r"\s*(\d{1,3})\s*:\s*([0-5]\d)\s*", text or "")
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def plural_ru(n: int, one: str, few: str, many: str) -> str:
    n = abs(n) % 100
    n1 = n % 10
    if 11 <= n <= 19:
        return many
    if n1 == 1:
        return one
    if 2 <= n1 <= 4:
        return few
    return many


def humanize_hhmm(h: int, m: int) -> str:
    parts = []
    if h:
        parts.append(f"{h} {plural_ru(h, 'час', 'часа', 'часов')}")
    if m:
        parts.append(f"{m} {plural_ru(m, 'минута', 'минуты', 'минут')}")
    return " ".join(parts) if parts else "0 минут"


def format_maint(scope: str, urgency: str, hh: int, mm: int, author: str) -> str:
    now = datetime.now(TZ).strftime("%d.%m.%Y %H:%M")
    urgency_label = "срочные" if urgency == "urgent" else "плановые"
    return (
        "⚠️ <b>Технические работы</b>\n"
        f"{_scope_line(scope)}\n"
        f"• Тип: <b>{html_escape(urgency_label)}</b>\n"
        f"• Оценка простоя: <b>{html_escape(humanize_hhmm(hh, mm))}</b>\n"
        f"• Ответственный: <b>{html_escape(author)}</b>\n"
        f"• Старт: <code>{html_escape(now)}</code> ({html_escape(TZ_NAME)})"
    )


def _hhmm_to_minutes(hh: int, mm: int) -> int:
    return max(0, (int(hh) * 60) + int(mm))


def _minutes_to_hhmm(total: int) -> Tuple[int, int]:
    total = max(0, int(total))
    return total // 60, total % 60


def _fmt_dt_short(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%d.%m.%Y %H:%M")


def _build_maint_record(scope: str, urgency: str, hh: int, mm: int, author_id: Optional[int], author_name: str) -> Dict[str, Any]:
    now = datetime.now(TZ)
    duration_min = _hhmm_to_minutes(hh, mm)
    expected_end = now + timedelta(minutes=duration_min)
    maint_id = uuid4().hex
    return {
        "id": maint_id,
        "active": True,
        "scope": _normalize_scope(scope),
        "urgency": urgency,
        "duration_min": duration_min,
        "started_at": now.isoformat(),
        "expected_end": expected_end.isoformat(),
        "author_id": author_id,
        "author_name": author_name,
        "updated_at": now.isoformat(),
    }


def _maint_control_kb(maint_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("✅ Завершить", callback_data=f"maint:end:{maint_id}"),
            InlineKeyboardButton("⏳ Продлить", callback_data=f"maint:extend:{maint_id}"),
        ]]
    )


def _maint_panel_text(maint: Dict[str, Any]) -> str:
    urgency_label = "срочные" if maint.get("urgency") == "urgent" else "плановые"
    duration_min = int(maint.get("duration_min", 0) or 0)
    hh, mm = _minutes_to_hhmm(duration_min)
    started_at = maint.get("started_at")
    expected_end = maint.get("expected_end")
    scope = maint.get("scope", MAINT_SCOPE_ALL)
    try:
        started_dt = datetime.fromisoformat(started_at) if started_at else None
    except Exception:
        started_dt = None
    try:
        end_dt = datetime.fromisoformat(expected_end) if expected_end else None
    except Exception:
        end_dt = None
    lines = [
        "🛠️ <b>Техработы активны</b>",
        _scope_line(str(scope)),
        f"• Тип: <b>{html_escape(urgency_label)}</b>",
        f"• Оценка простоя: <b>{html_escape(humanize_hhmm(hh, mm))}</b>",
    ]
    if started_dt:
        lines.append(f"• Старт: <code>{html_escape(_fmt_dt_short(started_dt))}</code> ({html_escape(TZ_NAME)})")
    if end_dt:
        lines.append(f"• Окончание: <code>{html_escape(_fmt_dt_short(end_dt))}</code> ({html_escape(TZ_NAME)})")
    lines.append("\nВыберите действие:")
    return "\n".join(lines)


def _maint_extend_notice(maint: Dict[str, Any], hh: int, mm: int, author: str) -> str:
    expected_end = maint.get("expected_end")
    end_dt = None
    try:
        end_dt = datetime.fromisoformat(expected_end) if expected_end else None
    except Exception:
        end_dt = None
    end_txt = _fmt_dt_short(end_dt) if end_dt else "-"
    scope = maint.get("scope", MAINT_SCOPE_ALL)
    return (
        "⏳ <b>Техработы продлены</b>\n"
        f"{_scope_line(str(scope))}\n"
        f"• Новый ориентир простоя: <b>{html_escape(humanize_hhmm(hh, mm))}</b>\n"
        f"• Окончание: <code>{html_escape(end_txt)}</code> ({html_escape(TZ_NAME)})\n"
        f"• Ответственный: <b>{html_escape(author)}</b>\n\n"
        "Спасибо за понимание 🙏"
    )


def _maint_end_notice(maint: Dict[str, Any], author: str) -> str:
    ended_at = datetime.now(TZ)
    scope = maint.get("scope", MAINT_SCOPE_ALL)
    return (
        "✅ <b>Техработы завершены</b>\n"
        f"{_scope_line(str(scope))}\n"
        f"• Время: <code>{html_escape(_fmt_dt_short(ended_at))}</code> ({html_escape(TZ_NAME)})\n"
        f"• Ответственный: <b>{html_escape(author)}</b>\n\n"
        "Спасибо за терпение 🙌"
    )


def _maint_restart_text(maint: Dict[str, Any]) -> str:
    return "♻️ <b>Бот перезапущен</b>\n\n" + _maint_panel_text(maint)


@require_admin
async def maint_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    active = _get_active_maintenance()
    if active and str(active.get("id") or ""):
        msg = update.effective_message
        if msg:
            await msg.reply_text(
                _maint_panel_text(active),
                parse_mode=ParseMode.HTML,
                reply_markup=_maint_control_kb(str(active["id"])),
            )
        return ConversationHandler.END

    msg = update.effective_message
    if msg:
        await msg.reply_text("Выберите область техработ:", reply_markup=scope_kb())
    return STATE_MAINT_SCOPE


@require_admin
async def maint_scope(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    m = re.fullmatch(r"maint:scope:([a-z0-9_-]{1,12})", q.data or "")
    if not m:
        return ConversationHandler.END
    scope = _normalize_scope(m.group(1))
    context.user_data["maint_scope"] = scope
    if q.message and q.message.chat:
        context.user_data["maint_panel_chat_id"] = q.message.chat.id
        context.user_data["maint_panel_msg_id"] = q.message.message_id
    await q.edit_message_text(
        f"Область: {html_escape(_scope_label(scope))}\n\nВыберите тип работ:",
        parse_mode=ParseMode.HTML,
        reply_markup=urgency_kb(),
    )
    return STATE_MAINT_URGENCY


@require_admin
async def maint_urgency(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    if q.data not in ("maint:urgency:urgent", "maint:urgency:planned"):
        return ConversationHandler.END
    context.user_data["maint_urgency"] = "urgent" if q.data.endswith("urgent") else "planned"
    await q.edit_message_text("Введите ожидаемое время простоя в формате ЧЧ:ММ (например, 1:35):")
    return STATE_MAINT_DURATION


@require_admin
async def maint_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    parsed = parse_hhmm((msg.text if msg else "") or "")
    if not parsed:
        if msg:
            await msg.reply_text("Некорректно. Введите ЧЧ:ММ, например 0:45 или 2:00:")
        return STATE_MAINT_DURATION

    hh, mm = parsed
    scope = _normalize_scope(str(context.user_data.get("maint_scope", MAINT_SCOPE_ALL)))
    urgency = str(context.user_data.get("maint_urgency", "planned"))
    author = display_name(update)
    author_id = get_user_id(update)
    msg_text = format_maint(scope, urgency, hh, mm, author)

    recipients = authorized_ids(role_filter="user", exclude=set())
    if not recipients:
        if msg:
            await msg.reply_text("Нет получателей: отсутствуют авторизованные пользователи.")
        return ConversationHandler.END

    ok, fail = await send_to_many(context, recipients, msg_text)

    maint = _build_maint_record(scope, urgency, hh, mm, author_id, author)
    maint_id = maint.get("id")
    if maint_id:
        await update_important_data(lambda cfg: _set_maintenance(cfg, maint))

    panel_text = f"{_maint_panel_text(maint)}\n\nОповещены: ✅ {ok}, ❌ {fail}"
    panel_chat_id = context.user_data.get("maint_panel_chat_id")
    panel_msg_id = context.user_data.get("maint_panel_msg_id")
    if panel_chat_id and panel_msg_id:
        try:
            await context.bot.edit_message_text(
                chat_id=panel_chat_id,
                message_id=panel_msg_id,
                text=panel_text,
                parse_mode=ParseMode.HTML,
                reply_markup=_maint_control_kb(str(maint_id)),
            )
            return ConversationHandler.END
        except Exception:
            pass

    if msg:
        await msg.reply_text(panel_text, parse_mode=ParseMode.HTML, reply_markup=_maint_control_kb(str(maint_id)))
    return ConversationHandler.END


@require_admin
async def maint_extend_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    m = re.fullmatch(r"maint:extend:([0-9a-f]+)", q.data or "")
    if not m:
        return ConversationHandler.END
    maint_id = m.group(1)
    maint = _get_active_maintenance()
    if not maint or str(maint.get("id")) != maint_id:
        await q.edit_message_text("Техработы не активны или уже завершены.")
        return ConversationHandler.END
    context.user_data["maint_extend_id"] = maint_id
    await q.edit_message_text("⏳ Продление техработ.\nВведите новое время простоя в формате ЧЧ:ММ (например, 1:35):")
    return STATE_MAINT_EXTEND


@require_admin
async def maint_extend_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    parsed = parse_hhmm((msg.text if msg else "") or "")
    if not parsed:
        if msg:
            await msg.reply_text("Некорректно. Введите ЧЧ:ММ, например 0:45 или 2:00:")
        return STATE_MAINT_EXTEND

    maint_id = context.user_data.get("maint_extend_id")
    maint = _get_active_maintenance()
    if not maint or str(maint.get("id")) != str(maint_id):
        if msg:
            await msg.reply_text("Техработы не активны или уже завершены.")
        return ConversationHandler.END

    hh, mm = parsed
    duration_min = _hhmm_to_minutes(hh, mm)

    def _extend_current(cfg):
        current = getattr(cfg, "maintenance", {})
        if not isinstance(current, dict) or not current.get("active"):
            raise RuntimeError("maintenance_not_active")
        if str(current.get("id")) != str(maint_id):
            raise RuntimeError("maintenance_changed")
        now = datetime.now(TZ)
        updated = dict(current)
        updated["duration_min"] = duration_min
        updated["expected_end"] = (now + timedelta(minutes=duration_min)).isoformat()
        updated["updated_at"] = now.isoformat()
        return _set_maintenance(cfg, updated)

    try:
        maint = await update_important_data(_extend_current)
    except RuntimeError:
        if msg:
            await msg.reply_text("Техработы не активны или уже завершены.")
        return ConversationHandler.END

    author = display_name(update)
    notice = _maint_extend_notice(maint, hh, mm, author)
    recipients = authorized_ids(role_filter="user", exclude=set())
    ok, fail = await send_to_many(context, recipients, notice) if recipients else (0, 0)

    panel_text = f"{_maint_panel_text(maint)}\n\nОповещены: ✅ {ok}, ❌ {fail}"
    context.user_data.pop("maint_extend_id", None)
    if msg:
        await msg.reply_text(panel_text, parse_mode=ParseMode.HTML, reply_markup=_maint_control_kb(str(maint_id)))
    return ConversationHandler.END


@require_admin
async def maint_end_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"maint:end:([0-9a-f]+)", q.data or "")
    if not m:
        return
    maint_id = m.group(1)
    maint = _get_active_maintenance()
    if not maint or str(maint.get("id")) != maint_id:
        await q.edit_message_text("Техработы не активны или уже завершены.")
        return

    author = display_name(update)
    notice = _maint_end_notice(maint, author)
    recipients = authorized_ids(role_filter="user", exclude=set())
    ok, fail = await send_to_many(context, recipients, notice) if recipients else (0, 0)

    await update_important_data(lambda cfg: _clear_maintenance(cfg))
    await q.edit_message_text(f"✅ Техработы завершены. Оповещены: ✅ {ok}, ❌ {fail}")


async def maint_restart_notify(context: ContextTypes.DEFAULT_TYPE) -> None:
    maint = _get_active_maintenance()
    if not maint:
        return
    maint_id = str(maint.get("id", "") or "")
    if not maint_id:
        return
    admin_ids = authorized_ids(role_filter="admin", exclude=set())
    if not admin_ids:
        return
    text = _maint_restart_text(maint)
    kb = _maint_control_kb(maint_id)
    for uid in admin_ids:
        try:
            await context.bot.send_message(chat_id=uid, text=text, parse_mode=ParseMode.HTML, reply_markup=kb)
        except Exception as e:
            logger.warning("Не удалось отправить админу %s: %s", uid, e)
