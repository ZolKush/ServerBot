import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from ..config import SERVERS, TZ, TZ_NAME
from .common import html_escape

MAINT_SCOPE_ALL = "all"
MAX_MAINT_HOURS = 72


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
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def urgency_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔥 Срочные", callback_data="maint:urgency:urgent"),
                InlineKeyboardButton("🗓 Плановые", callback_data="maint:urgency:planned"),
            ],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


def maint_mode_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🗓 Запланировать техработы", callback_data="maint:mode:schedule")],
            [InlineKeyboardButton("🚨 Объявить техработы", callback_data="maint:mode:announce")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


def parse_hhmm(text: str) -> Optional[Tuple[int, int]]:
    m = re.fullmatch(r"\s*(\d{1,3})\s*:\s*([0-5]\d)\s*", text or "")
    if not m:
        return None
    hh, mm = int(m.group(1)), int(m.group(2))
    if hh > MAX_MAINT_HOURS:
        return None
    return hh, mm


def parse_clock_range(text: str) -> Optional[Tuple[int, int, int, int]]:
    m = re.fullmatch(r"\s*(\d{1,2})\s*:\s*([0-5]\d)\s*[-–]\s*(\d{1,2})\s*:\s*([0-5]\d)\s*", text or "")
    if not m:
        return None
    sh, sm, eh, em = (int(m.group(i)) for i in range(1, 5))
    if not (0 <= sh <= 23 and 0 <= eh <= 23):
        return None
    return sh, sm, eh, em


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


def format_scheduled_maint(scope: str, start_at: datetime, end_at: datetime, author: str) -> str:
    return (
        "🗓 <b>Запланированы технические работы</b>\n"
        f"{_scope_line(scope)}\n"
        f"• Начало: <code>{html_escape(_fmt_dt_short(start_at))}</code> ({html_escape(TZ_NAME)})\n"
        f"• Окончание: <code>{html_escape(_fmt_dt_short(end_at))}</code> ({html_escape(TZ_NAME)})\n"
        f"• Ответственный: <b>{html_escape(author)}</b>"
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


def _build_scheduled_maint_record(
    scope: str,
    start_at: datetime,
    end_at: datetime,
    author_id: Optional[int],
    author_name: str,
) -> Dict[str, Any]:
    duration_min = max(1, int((end_at - start_at).total_seconds() // 60))
    now = datetime.now(TZ)
    return {
        "id": uuid4().hex,
        "scope": _normalize_scope(scope),
        "urgency": "planned",
        "duration_min": duration_min,
        "scheduled_start": start_at.isoformat(),
        "scheduled_end": end_at.isoformat(),
        "author_id": author_id,
        "author_name": author_name,
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
        "notified_before": False,
        "notified_start": False,
    }


def _scheduled_to_active_record(scheduled: Dict[str, Any]) -> Dict[str, Any]:
    start_at = datetime.now(TZ)
    duration_min = int(scheduled.get("duration_min", 0) or 0)
    expected_end = start_at + timedelta(minutes=max(duration_min, 1))
    return {
        "id": str(scheduled.get("id") or uuid4().hex),
        "active": True,
        "scope": _normalize_scope(str(scheduled.get("scope") or MAINT_SCOPE_ALL)),
        "urgency": "planned",
        "duration_min": max(duration_min, 1),
        "started_at": start_at.isoformat(),
        "expected_end": expected_end.isoformat(),
        "author_id": scheduled.get("author_id"),
        "author_name": scheduled.get("author_name") or "администратор",
        "updated_at": start_at.isoformat(),
    }


def _maint_control_kb(maint_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Завершить", callback_data=f"maint:endconfirm:{maint_id}"),
                InlineKeyboardButton("⏳ Продлить", callback_data=f"maint:extend:{maint_id}"),
            ],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


def _maint_end_confirm_kb(maint_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Подтвердить завершение", callback_data=f"maint:end:{maint_id}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"maint:cancelend:{maint_id}")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
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


def _scheduled_panel_text(scheduled: Dict[str, Any]) -> str:
    scope = scheduled.get("scope", MAINT_SCOPE_ALL)
    try:
        start_dt = datetime.fromisoformat(str(scheduled.get("scheduled_start") or ""))
    except Exception:
        start_dt = None
    try:
        end_dt = datetime.fromisoformat(str(scheduled.get("scheduled_end") or ""))
    except Exception:
        end_dt = None
    lines = [
        "🗓️ <b>Техработы запланированы</b>",
        _scope_line(str(scope)),
    ]
    if start_dt:
        lines.append(f"• Начало: <code>{html_escape(_fmt_dt_short(start_dt))}</code> ({html_escape(TZ_NAME)})")
    if end_dt:
        lines.append(f"• Окончание: <code>{html_escape(_fmt_dt_short(end_dt))}</code> ({html_escape(TZ_NAME)})")
    lines.append("• Уведомления уйдут за 30 минут и в момент начала.")
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


def _maint_scheduled_soon_notice(scheduled: Dict[str, Any]) -> str:
    try:
        start_dt = datetime.fromisoformat(str(scheduled.get("scheduled_start") or ""))
        end_dt = datetime.fromisoformat(str(scheduled.get("scheduled_end") or ""))
    except Exception:
        start_dt = end_dt = None
    scope = scheduled.get("scope", MAINT_SCOPE_ALL)
    author = scheduled.get("author_name") or "администратор"
    return (
        "⏳ <b>Технические работы начнутся через 30 минут</b>\n"
        f"{_scope_line(str(scope))}\n"
        f"• Начало: <code>{html_escape(_fmt_dt_short(start_dt) if start_dt else '-')}</code> ({html_escape(TZ_NAME)})\n"
        f"• Окончание: <code>{html_escape(_fmt_dt_short(end_dt) if end_dt else '-')}</code> ({html_escape(TZ_NAME)})\n"
        f"• Ответственный: <b>{html_escape(str(author))}</b>"
    )


def _maint_scheduled_start_notice(scheduled: Dict[str, Any]) -> str:
    try:
        end_dt = datetime.fromisoformat(str(scheduled.get("scheduled_end") or ""))
    except Exception:
        end_dt = None
    scope = scheduled.get("scope", MAINT_SCOPE_ALL)
    author = scheduled.get("author_name") or "администратор"
    return (
        "⚠️ <b>Технические работы начались</b>\n"
        f"{_scope_line(str(scope))}\n"
        f"• Плановое окончание: <code>{html_escape(_fmt_dt_short(end_dt) if end_dt else '-')}</code> ({html_escape(TZ_NAME)})\n"
        f"• Ответственный: <b>{html_escape(str(author))}</b>"
    )
