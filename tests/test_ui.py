"""Тесты чистых функций UI-kit (:mod:`app.bot.ui`)."""

from app.bot.ui import (
    SEP,
    extract_percent,
    footer_updated,
    header,
    humanize_hhmm,
    humanize_until,
    metric_line,
    plural_ru,
    progress_bar,
    section,
    status_emoji,
    urgency_label,
    used_total_percent,
)


def test_sep_length() -> None:
    assert SEP == ""


def test_progress_bar_bounds() -> None:
    assert progress_bar(0) == "▱" * 10
    assert progress_bar(100) == "▰" * 10
    assert progress_bar(52) == "▰" * 5 + "▱" * 5
    # Клампинг за пределами диапазона
    assert progress_bar(-10) == "▱" * 10
    assert progress_bar(250) == "▰" * 10
    assert progress_bar("мусор") == "▱" * 10
    assert len(progress_bar(33, width=20)) == 20


def test_plural_ru() -> None:
    forms = ("тикет", "тикета", "тикетов")
    assert plural_ru(1, *forms) == "тикет"
    assert plural_ru(2, *forms) == "тикета"
    assert plural_ru(5, *forms) == "тикетов"
    assert plural_ru(11, *forms) == "тикетов"
    assert plural_ru(21, *forms) == "тикет"
    assert plural_ru(104, *forms) == "тикета"


def test_humanize_hhmm() -> None:
    assert humanize_hhmm(1, 35) == "1 час 35 минут"
    assert humanize_hhmm(0, 0) == "0 минут"
    assert humanize_hhmm(2, 0) == "2 часа"
    assert humanize_hhmm(0, 1) == "1 минута"


def test_urgency_label() -> None:
    assert urgency_label("p1") == "🔥 Критично"
    assert urgency_label("P2") == "⚠️ Важно"
    assert urgency_label("p3") == "📋 Обычно"
    # Неизвестное значение возвращается как есть
    assert urgency_label("p9") == "p9"
    assert urgency_label(None) == "-"


def test_status_emoji() -> None:
    assert status_emoji("ok") == "🟢"
    assert status_emoji("down") == "🔴"
    assert status_emoji("degraded") == "⚠️"
    assert status_emoji("неизвестно") == "⚠️"


def test_extract_percent_real_formats() -> None:
    # Формат disk_raw из remote_service.py
    assert extract_percent("18G / 40G (avail 22G, 45%) mount /") == 45
    assert extract_percent("ничего") is None
    assert extract_percent(None) is None
    assert extract_percent("999%") is None


def test_used_total_percent_real_formats() -> None:
    # memory_raw = "{used} / {total} MiB"
    assert used_total_percent(2150, 4096) == 52
    assert used_total_percent(0, 4096) == 0
    assert used_total_percent(4096, 4096) == 100
    assert used_total_percent(1, 0) is None
    assert used_total_percent("x", "y") is None


def test_metric_line_with_percent() -> None:
    line = metric_line("RAM", 52, "2.1/4.0 GB")
    assert line.startswith("<code>RAM  ")
    assert "▰▰▰▰▰▱▱▱▱▱" in line
    assert "52%" in line
    assert line.endswith("(2.1/4.0 GB)")


def test_metric_line_without_percent_falls_back() -> None:
    line = metric_line("Диск", None, "n/a")
    assert "<code>" not in line
    assert line == "Диск: n/a"


def test_header_and_section_escape_html() -> None:
    assert header("🖥", "a<b>&c", "🟢 онлайн") == "🖥 <b>a&lt;b&gt;&amp;c</b> · 🟢 онлайн"
    assert section("Ре<сурсы>", "📊") == "📊 <b>Ре&lt;сурсы&gt;</b>"
    assert section("Без иконки") == "<b>Без иконки</b>"


def test_metric_line_escapes_detail() -> None:
    line = metric_line("RAM", 50, "<b>haxor</b> & co")
    assert "<b>haxor</b>" not in line
    assert "&lt;b&gt;haxor&lt;/b&gt; &amp; co" in line


def test_footer_updated() -> None:
    assert footer_updated("12.06.2026 14:30:05") == "<i>обновлено 12.06.2026 14:30:05</i>"


def test_humanize_until_minutes() -> None:
    assert humanize_until(0) == "0 минут"
    assert humanize_until(1) == "1 минута"
    assert humanize_until(19) == "19 минут"
    assert humanize_until(30) == "30 минут"


def test_humanize_until_hours() -> None:
    assert humanize_until(60) == "1 час"
    assert humanize_until(700) == "11 часов 40 минут"
    assert humanize_until(720) == "12 часов"


def test_humanize_until_days() -> None:
    assert humanize_until(1440) == "1 сутки"
    assert humanize_until(1500) == "1 сутки 1 час"
    assert humanize_until(4320) == "3 суток"


def test_humanize_until_negative_clamps_to_zero() -> None:
    assert humanize_until(-5) == "0 минут"
