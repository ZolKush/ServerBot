import calendar as _calendar
import re
from datetime import date, datetime, timedelta
from typing import Any
from uuid import uuid4

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from ..config import SERVERS, TZ, TZ_NAME
from ..models import Maintenance, ScheduledMaintenance
from .common import html_escape
from .ui import SEP, humanize_hhmm, humanize_until, plural_ru  # noqa: F401 — реэкспорт для существующих импортов

MAINT_SCOPE_ALL = "all"
MAX_MAINT_HOURS = 72

# Пороги предупреждений о запланированных техработах (минуты до старта):
# 3 суток, 12 часов, 30 минут.
MAINT_WARN_THRESHOLDS_MIN = (4320, 720, 30)


def _initial_notified_thresholds(remaining_min: int) -> list[int]:
    """Какие пороги считать уже «отправленными» при создании плана.

    Взводим (НЕ помечаем отправленными) пороги, чей момент предупреждения ещё
    в будущем (remaining > T). Если в будущем не осталось ни одного порога, но
    старт ещё впереди (план ближе 30 минут) — взводим самый малый порог, чтобы
    ушло ровно одно динамичное предупреждение.
    """
    thresholds = set(MAINT_WARN_THRESHOLDS_MIN)
    armed = {t for t in thresholds if remaining_min > t}
    if not armed and remaining_min > 0:
        armed = {min(thresholds)}
    return sorted(thresholds - armed)


def _due_thresholds(notified: list[int], remaining_min: int) -> list[int]:
    """Пороги, которые наступили (remaining <= T) и ещё не были отправлены."""
    sent = set(notified)
    return [t for t in MAINT_WARN_THRESHOLDS_MIN if t not in sent and remaining_min <= t]


def _server_items() -> list[tuple[str, str]]:
    return [(k, v.label) for k, v in SERVERS.items()]


def _normalize_scope(scope: str | None) -> str:
    s = (scope or "").strip().lower()
    if s == MAINT_SCOPE_ALL:
        return MAINT_SCOPE_ALL
    return s if s in SERVERS else MAINT_SCOPE_ALL


def _scope_label(scope: str | None) -> str:
    scope_n = _normalize_scope(scope)
    if scope_n == MAINT_SCOPE_ALL:
        labels = [lbl for _, lbl in _server_items()]
        return ", ".join(labels) if labels else "Все серверы"
    srv = SERVERS.get(scope_n)
    return srv.label if srv else scope_n


def _scope_line(scope: str | None) -> str:
    scope_n = _normalize_scope(scope)
    if scope_n == MAINT_SCOPE_ALL:
        return f"• Серверы: <b>{html_escape(_scope_label(scope_n))}</b>"
    return f"• Сервер: <b>{html_escape(_scope_label(scope_n))}</b>"


def scope_kb() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
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


CAL_NOOP = "maint:cal:noop"

_RU_MONTHS = [
    "",
    "Январь",
    "Февраль",
    "Март",
    "Апрель",
    "Май",
    "Июнь",
    "Июль",
    "Август",
    "Сентябрь",
    "Октябрь",
    "Ноябрь",
    "Декабрь",
]
_RU_WEEKDAYS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


def _calendar_grid(year: int, month: int, today: date, horizon: date) -> list[list[tuple[str, str]]]:
    """Сетка месяца: список недель, каждая — 7 ячеек (подпись, callback_data).

    Дни не из текущего месяца, прошедшие дни и дни за горизонтом — некликабельны
    (callback = CAL_NOOP). Кликабельные дни дают callback maint:cal:day:YYYY-MM-DD.
    """
    cal = _calendar.Calendar(firstweekday=0)  # неделя с понедельника
    weeks: list[list[tuple[str, str]]] = []
    for week in cal.monthdatescalendar(year, month):
        row: list[tuple[str, str]] = []
        for d in week:
            if d.month != month:
                row.append((" ", CAL_NOOP))
            elif d < today or d > horizon:
                row.append((f"·{d.day}", CAL_NOOP))
            else:
                row.append((str(d.day), f"maint:cal:day:{d.isoformat()}"))
        weeks.append(row)
    return weeks


def schedule_calendar_kb(year: int, month: int, *, today: date, horizon_days: int = 365) -> InlineKeyboardMarkup:
    horizon = today + timedelta(days=horizon_days)
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(f"{_RU_MONTHS[month]} {year}", callback_data=CAL_NOOP)],
        [InlineKeyboardButton(wd, callback_data=CAL_NOOP) for wd in _RU_WEEKDAYS],
    ]
    for week in _calendar_grid(year, month, today, horizon):
        rows.append([InlineKeyboardButton(label, callback_data=cb) for label, cb in week])

    prev_ok = (year, month) > (today.year, today.month)
    next_first = date(year + (1 if month == 12 else 0), 1 if month == 12 else month + 1, 1)
    next_ok = next_first <= horizon
    nav: list[InlineKeyboardButton] = []
    if prev_ok:
        py, pm = (year - 1, 12) if month == 1 else (year, month - 1)
        nav.append(InlineKeyboardButton("◀", callback_data=f"maint:cal:nav:{py:04d}-{pm:02d}"))
    if next_ok:
        nav.append(
            InlineKeyboardButton("▶", callback_data=f"maint:cal:nav:{next_first.year:04d}-{next_first.month:02d}")
        )
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def parse_hhmm(text: str) -> tuple[int, int] | None:
    m = re.fullmatch(r"\s*(\d{1,3})\s*:\s*([0-5]\d)\s*", text or "")
    if not m:
        return None
    hh, mm = int(m.group(1)), int(m.group(2))
    if hh == 0 and mm == 0:
        return None
    if hh > MAX_MAINT_HOURS:
        return None
    if (hh * 60 + mm) > MAX_MAINT_HOURS * 60:
        return None
    return hh, mm


def parse_clock_range(text: str) -> tuple[int, int, int, int] | None:
    m = re.fullmatch(r"\s*(\d{1,2})\s*:\s*([0-5]\d)\s*[-–]\s*(\d{1,2})\s*:\s*([0-5]\d)\s*", text or "")
    if not m:
        return None
    sh, sm, eh, em = (int(m.group(i)) for i in range(1, 5))
    if not (0 <= sh <= 23 and 0 <= eh <= 23):
        return None
    return sh, sm, eh, em


def format_maint(scope: str, urgency: str, hh: int, mm: int, author: str) -> str:
    now = datetime.now(TZ).strftime("%d.%m.%Y %H:%M")
    urgency_label = "срочные" if urgency == "urgent" else "плановые"
    return (
        "🛠 <b>Техработы</b> · ⚠️ начались\n"
        f"{SEP}\n"
        f"{_scope_line(scope)}\n"
        f"• Тип: <b>{html_escape(urgency_label)}</b>\n"
        f"• Оценка простоя: <b>{html_escape(humanize_hhmm(hh, mm))}</b>\n"
        f"• Ответственный: <b>{html_escape(author)}</b>\n"
        f"• Старт: <code>{html_escape(now)}</code> ({html_escape(TZ_NAME)})"
    )


def format_scheduled_maint(scope: str, start_at: datetime, end_at: datetime, author: str) -> str:
    return (
        "🛠 <b>Техработы</b> · 🗓 запланированы\n"
        f"{SEP}\n"
        f"{_scope_line(scope)}\n"
        f"• Начало: <code>{html_escape(_fmt_dt_short(start_at))}</code> ({html_escape(TZ_NAME)})\n"
        f"• Окончание: <code>{html_escape(_fmt_dt_short(end_at))}</code> ({html_escape(TZ_NAME)})\n"
        f"• Ответственный: <b>{html_escape(author)}</b>"
    )


def _hhmm_to_minutes(hh: int, mm: int) -> int:
    return max(0, (int(hh) * 60) + int(mm))


def _minutes_to_hhmm(total: int) -> tuple[int, int]:
    total = max(0, int(total))
    return total // 60, total % 60


def _fmt_dt_short(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%d.%m.%Y %H:%M")


def _build_maint_record(
    scope: str, urgency: str, hh: int, mm: int, author_id: int | None, author_name: str
) -> dict[str, Any]:
    now = datetime.now(TZ)
    duration_min = _hhmm_to_minutes(hh, mm)
    expected_end = now + timedelta(minutes=duration_min)
    maint_id = uuid4().hex
    record: dict[str, Any] = {
        "id": maint_id,
        "active": True,
        "scope": _normalize_scope(scope),
        "urgency": urgency,
        "duration_min": duration_min,
        "started_at": now.isoformat(),
        "expected_end": expected_end.isoformat(),
        "author_id": author_id,
        "author_name": author_name,
        "author_signature_version": 1,
        "updated_at": now.isoformat(),
    }
    return record


def _build_scheduled_maint_record(
    scope: str,
    start_at: datetime,
    end_at: datetime,
    author_id: int | None,
    author_name: str,
) -> ScheduledMaintenance:
    duration_min = max(1, int((end_at - start_at).total_seconds() // 60))
    now = datetime.now(TZ)
    remaining_min = int((start_at - now).total_seconds() // 60)
    record: ScheduledMaintenance = {
        "id": uuid4().hex,
        "scope": _normalize_scope(scope),
        "urgency": "planned",
        "duration_min": duration_min,
        "scheduled_start": start_at.isoformat(),
        "scheduled_end": end_at.isoformat(),
        "author_id": author_id,
        "author_name": author_name,
        "author_signature_version": 1,
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
        "notified_thresholds": _initial_notified_thresholds(remaining_min),
        "announced_thresholds": [],
        "notified_start": False,
    }
    return record


def _scheduled_to_active_record(scheduled: ScheduledMaintenance) -> Maintenance:
    start_at = datetime.now(TZ)
    duration_min = int(scheduled.get("duration_min", 0) or 0)
    try:
        expected_end = datetime.fromisoformat(str(scheduled.get("scheduled_end") or ""))
        if expected_end.tzinfo is None:
            expected_end = expected_end.replace(tzinfo=TZ)
    except (TypeError, ValueError):
        expected_end = start_at + timedelta(minutes=max(duration_min, 1))
    record: Maintenance = {
        "id": str(scheduled.get("id") or uuid4().hex),
        "active": True,
        "scope": _normalize_scope(str(scheduled.get("scope") or MAINT_SCOPE_ALL)),
        "urgency": "planned",
        "duration_min": max(duration_min, 1),
        "started_at": start_at.isoformat(),
        "expected_end": expected_end.isoformat(),
        "author_id": scheduled.get("author_id"),
        "author_name": (
            str(scheduled.get("author_name") or "Техническая поддержка")
            if scheduled.get("author_signature_version") == 1
            else "Техническая поддержка"
        ),
        "author_signature_version": 1,
        "updated_at": start_at.isoformat(),
    }
    return record


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


def _scheduled_control_kb(scheduled_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🚨 Объявить техработы сейчас", callback_data="maint:mode:announce")],
            [InlineKeyboardButton("❌ Отменить план", callback_data=f"maint:schedcancel:{scheduled_id}")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


def _scheduled_cancel_confirm_kb(scheduled_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Подтвердить отмену", callback_data=f"maint:schedcancelconfirm:{scheduled_id}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"maint:schedcancelback:{scheduled_id}")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


def _maint_panel_text(maint: dict[str, Any]) -> str:
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
        "🛠 <b>Техработы</b> · ⚠️ активны",
        SEP,
        _scope_line(str(scope)),
        f"• Тип: <b>{html_escape(urgency_label)}</b>",
        f"• Оценка простоя: <b>{html_escape(humanize_hhmm(hh, mm))}</b>",
    ]
    if started_dt:
        lines.append(f"• Старт: <code>{html_escape(_fmt_dt_short(started_dt))}</code> ({html_escape(TZ_NAME)})")
    if end_dt:
        lines.append(f"• Окончание: <code>{html_escape(_fmt_dt_short(end_dt))}</code> ({html_escape(TZ_NAME)})")
    lines.append("")
    lines.append("Выберите действие:")
    return "\n".join(lines)


def _scheduled_panel_text(scheduled: dict[str, Any]) -> str:
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
        "🛠 <b>Техработы</b> · 🗓 запланированы",
        SEP,
        _scope_line(str(scope)),
    ]
    if start_dt:
        lines.append(f"• Начало: <code>{html_escape(_fmt_dt_short(start_dt))}</code> ({html_escape(TZ_NAME)})")
    if end_dt:
        lines.append(f"• Окончание: <code>{html_escape(_fmt_dt_short(end_dt))}</code> ({html_escape(TZ_NAME)})")
    lines.append("• Уведомления уйдут за 3 суток, 12 часов и 30 минут до начала, а также в момент старта.")
    return "\n".join(lines)


def _maint_extend_notice(maint: dict[str, Any], hh: int, mm: int, author: str) -> str:
    expected_end = maint.get("expected_end")
    end_dt = None
    try:
        end_dt = datetime.fromisoformat(expected_end) if expected_end else None
    except Exception:
        end_dt = None
    end_txt = _fmt_dt_short(end_dt) if end_dt else "-"
    scope = maint.get("scope", MAINT_SCOPE_ALL)
    return (
        "🛠 <b>Техработы</b> · ⏳ продлены\n"
        f"{SEP}\n"
        f"{_scope_line(str(scope))}\n"
        f"• Новый ориентир простоя: <b>{html_escape(humanize_hhmm(hh, mm))}</b>\n"
        f"• Окончание: <code>{html_escape(end_txt)}</code> ({html_escape(TZ_NAME)})\n"
        f"• Ответственный: <b>{html_escape(author)}</b>\n\n"
        "Спасибо за понимание 🙏"
    )


def _maint_end_notice(maint: dict[str, Any], author: str, ended_at: datetime | None = None) -> str:
    if ended_at is None:
        ended_at = datetime.now(TZ)
    scope = maint.get("scope", MAINT_SCOPE_ALL)
    return (
        "🛠 <b>Техработы</b> · ✅ завершены\n"
        f"{SEP}\n"
        f"{_scope_line(str(scope))}\n"
        f"• Время: <code>{html_escape(_fmt_dt_short(ended_at))}</code> ({html_escape(TZ_NAME)})\n"
        f"• Ответственный: <b>{html_escape(author)}</b>\n\n"
        "Спасибо за терпение 🙌"
    )


def _maint_active_reminder_text(maint: dict[str, Any]) -> str:
    return "🔔 <b>Напоминание</b>\n" + _maint_panel_text(maint)


def _maint_scheduled_soon_notice(scheduled: dict[str, Any], remaining_min: int) -> str:
    start_dt: datetime | None
    end_dt: datetime | None
    try:
        start_dt = datetime.fromisoformat(str(scheduled.get("scheduled_start") or ""))
        end_dt = datetime.fromisoformat(str(scheduled.get("scheduled_end") or ""))
    except Exception:
        start_dt = end_dt = None
    scope = scheduled.get("scope", MAINT_SCOPE_ALL)
    author = scheduled.get("author_name") if scheduled.get("author_signature_version") == 1 else "Техническая поддержка"
    return (
        f"🛠 <b>Техработы</b> · ⏳ начнутся через {html_escape(humanize_until(remaining_min))}\n"
        f"{SEP}\n"
        f"{_scope_line(str(scope))}\n"
        f"• Начало: <code>{html_escape(_fmt_dt_short(start_dt) if start_dt else '-')}</code> ({html_escape(TZ_NAME)})\n"
        f"• Окончание: <code>{html_escape(_fmt_dt_short(end_dt) if end_dt else '-')}</code> ({html_escape(TZ_NAME)})\n"
        f"• Ответственный: <b>{html_escape(str(author))}</b>"
    )


def _maint_scheduled_cancel_notice(scheduled: dict[str, Any]) -> str:
    try:
        start_dt = datetime.fromisoformat(str(scheduled.get("scheduled_start") or ""))
    except Exception:
        start_dt = None
    scope = scheduled.get("scope", MAINT_SCOPE_ALL)
    return (
        "🛠 <b>Техработы</b> · ❌ отменены\n"
        f"{SEP}\n"
        f"{_scope_line(str(scope))}\n"
        f"• Планировались на: <code>{html_escape(_fmt_dt_short(start_dt) if start_dt else '-')}</code> ({html_escape(TZ_NAME)})\n\n"
        "Ранее анонсированные работы проводиться не будут."
    )


def _maint_scheduled_start_notice(scheduled: dict[str, Any]) -> str:
    try:
        end_dt = datetime.fromisoformat(str(scheduled.get("scheduled_end") or ""))
    except Exception:
        end_dt = None
    scope = scheduled.get("scope", MAINT_SCOPE_ALL)
    author = scheduled.get("author_name") if scheduled.get("author_signature_version") == 1 else "Техническая поддержка"
    return (
        "🛠 <b>Техработы</b> · ⚠️ начались\n"
        f"{SEP}\n"
        f"{_scope_line(str(scope))}\n"
        f"• Плановое окончание: <code>{html_escape(_fmt_dt_short(end_dt) if end_dt else '-')}</code> ({html_escape(TZ_NAME)})\n"
        f"• Ответственный: <b>{html_escape(str(author))}</b>"
    )
