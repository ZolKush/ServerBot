"""Tests for pure maintenance scheduling and presentation helpers."""

from datetime import date, timedelta
from datetime import datetime as _dt

from app.config import TZ
from app.maintenance.calendar import CAL_NOOP, _calendar_grid, schedule_calendar_kb
from app.maintenance.policy import (
    MAINT_WARN_THRESHOLDS_MIN,
    due_thresholds,
    initial_notified_thresholds,
)
from app.maintenance.records import (
    _build_scheduled_maint_record,
    _scheduled_to_active_record,
)
from app.maintenance.views import (
    _maint_scheduled_cancel_notice,
    _maint_scheduled_soon_notice,
    _scheduled_control_kb,
    format_maint,
    format_scheduled_maint,
)


def test_thresholds_constant() -> None:
    assert MAINT_WARN_THRESHOLDS_MIN == (4320, 720, 30)


def test_initial_notified_far_future_arms_all() -> None:
    # 5000 минут до старта: все пороги в будущем -> ничего не отправлено
    assert initial_notified_thresholds(5000) == []


def test_initial_notified_two_days_skips_three_day_threshold() -> None:
    # 2880 минут (2 суток): порог 3 суток пропущен, 12ч и 30м ещё впереди
    assert initial_notified_thresholds(2880) == [4320]


def test_initial_notified_under_30min_rearms_smallest() -> None:
    # 19 минут до старта: все пороги в прошлом, но самый малый (30) взводится,
    # поэтому помечены отправленными только 720 и 4320
    assert initial_notified_thresholds(19) == [720, 4320]


def test_initial_notified_zero_marks_all() -> None:
    # старт прямо сейчас: предупреждать не о чем
    assert initial_notified_thresholds(0) == [30, 720, 4320]


def test_due_thresholds_returns_crossed_unsent() -> None:
    # осталось 19 минут, ничего не отправлено из {30}: порог 30 наступил
    assert due_thresholds([720, 4320], 19) == [30]


def test_due_thresholds_skips_already_sent() -> None:
    assert due_thresholds([30, 720, 4320], 19) == []


def test_due_thresholds_not_yet_reached() -> None:
    # осталось 800 минут: порог 720 ещё не наступил (800 > 720)
    assert due_thresholds([4320], 800) == []


def test_build_scheduled_record_sets_notified_thresholds() -> None:
    now = _dt.now(TZ)
    start = now + timedelta(minutes=19)  # ближе 30 минут
    end = start + timedelta(minutes=30)
    rec = _build_scheduled_maint_record("all", start, end, 111, "admin")
    # ближе 30 минут -> 720 и 4320 помечены отправленными, 30 взведён
    assert rec["notified_thresholds"] == [720, 4320]
    assert "notified_before" not in rec
    assert rec["notified_start"] is False


def test_calendar_grid_marks_past_and_selectable_days() -> None:
    today = date(2026, 6, 14)
    horizon = today + timedelta(days=365)
    weeks = _calendar_grid(2026, 6, today, horizon)
    flat = [cell for week in weeks for cell in week]
    # день из июня раньше сегодня -> noop
    past = [cb for label, cb in flat if cb.endswith("2026-06-13")]
    assert past == []  # прошедшие дни не дают day-callback
    selectable = {cb for _, cb in flat if cb.startswith("maint:cal:day:")}
    assert "maint:cal:day:2026-06-14" in selectable
    assert "maint:cal:day:2026-06-20" in selectable
    # дни предыдущего/следующего месяца и прошлые дни помечены noop
    assert any(cb == CAL_NOOP for _, cb in flat)


def test_calendar_kb_header_and_nav() -> None:
    today = date(2026, 6, 14)
    kb = schedule_calendar_kb(2026, 6, today=today, horizon_days=365)
    rows = kb.inline_keyboard
    # первая строка — заголовок месяца
    assert "Июнь 2026" in rows[0][0].text
    # где-то есть кнопка "вперёд" на июль
    nav_cbs = {btn.callback_data for row in rows for btn in row}
    assert "maint:cal:nav:2026-07" in nav_cbs
    # назад в прошлый месяц (май) недоступно из текущего месяца
    assert "maint:cal:nav:2026-05" not in nav_cbs


def test_calendar_kb_horizon_blocks_next() -> None:
    today = date(2026, 6, 14)
    # горизонт 10 дней: следующий месяц целиком за горизонтом
    kb = schedule_calendar_kb(2026, 6, today=today, horizon_days=10)
    nav_cbs = {btn.callback_data for row in kb.inline_keyboard for btn in row}
    assert "maint:cal:nav:2026-07" not in nav_cbs


def test_soon_notice_uses_dynamic_remaining() -> None:
    now = _dt.now(TZ)
    start = now + timedelta(minutes=19)
    end = start + timedelta(minutes=30)
    rec = _build_scheduled_maint_record("all", start, end, 111, "admin")
    text = _maint_scheduled_soon_notice(rec, 19)
    assert "через 19 минут" in text
    assert "30 минут" not in text.split("через", 1)[1][:20]


def test_cancel_notice_mentions_cancellation() -> None:
    now = _dt.now(TZ)
    start = now + timedelta(hours=5)
    end = start + timedelta(minutes=30)
    rec = _build_scheduled_maint_record("all", start, end, 111, "admin")
    text = _maint_scheduled_cancel_notice(rec)
    assert "отменены" in text.lower()


def test_scheduled_control_kb_has_cancel_and_announce() -> None:
    kb = _scheduled_control_kb("abc123")
    cbs = {btn.callback_data for row in kb.inline_keyboard for btn in row}
    assert "maint:schedcancel:abc123" in cbs
    assert "maint:mode:announce" in cbs
    assert "menu:home" in cbs


def test_late_activation_keeps_original_scheduled_end() -> None:
    now = _dt.now(TZ)
    scheduled_end = now + timedelta(minutes=20)
    scheduled = {
        "id": "abc123",
        "scope": "all",
        "duration_min": 60,
        "scheduled_start": (now - timedelta(minutes=40)).isoformat(),
        "scheduled_end": scheduled_end.isoformat(),
        "author_id": 1,
        "author_name": "admin",
    }

    active = _scheduled_to_active_record(scheduled)

    assert _dt.fromisoformat(active["expected_end"]) == scheduled_end
    assert active["author_name"] == "Техническая поддержка"


def test_urgent_and_planned_maintenance_are_visually_distinct() -> None:
    urgent = format_maint("all", "urgent", 1, 30, "Специалист поддержки")
    planned = format_scheduled_maint(
        "all",
        _dt.now(TZ) + timedelta(hours=1),
        _dt.now(TZ) + timedelta(hours=2),
        "Специалист поддержки",
    )

    assert "🚨🚨" in urgent
    assert "СРОЧНЫЕ ТЕХНИЧЕСКИЕ РАБОТЫ" in urgent
    assert "ПЛАНОВЫЕ ТЕХНИЧЕСКИЕ РАБОТЫ" in planned
    assert "🚨🚨" not in planned
