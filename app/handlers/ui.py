"""UI-kit: единые визуальные элементы бота.

Все экраны строятся из этих примитивов: разделители, прогресс-бары,
статус-эмодзи, заголовки секций и фабрики кнопок. Модуль не зависит
от хендлеров (только от common), поэтому импортируется отовсюду.
"""

from __future__ import annotations

import re

from telegram import InlineKeyboardButton

from .common import html_escape

# Тонкий разделитель секций (18 × U+2500)
SEP = "──────────────────"

STATUS_EMOJI = {
    "ok": "🟢",
    "down": "🔴",
    "degraded": "⚠️",
}


def status_emoji(state: str) -> str:
    return STATUS_EMOJI.get((state or "").strip().lower(), STATUS_EMOJI["degraded"])


URGENCY_LABELS = {
    "p1": "🔥 Критично",
    "p2": "⚠️ Важно",
    "p3": "📋 Обычно",
}


def urgency_label(code: str | None) -> str:
    key = str(code or "").strip().lower()
    return URGENCY_LABELS.get(key, str(code or "-"))


def urgency_emoji(code: str | None) -> str:
    """Только эмодзи срочности — для компактных кнопок."""
    key = str(code or "").strip().lower()
    label = URGENCY_LABELS.get(key)
    return label.split()[0] if label else "📋"


def progress_bar(percent: float, width: int = 10) -> str:
    try:
        pct = float(percent)
    except (TypeError, ValueError):
        pct = 0.0
    pct = max(0.0, min(100.0, pct))
    filled = round(width * pct / 100.0)
    return "▰" * filled + "▱" * (width - filled)


# Ширина подписи метрики внутри <code>: "Диск " / "RAM  "
_METRIC_LABEL_WIDTH = 5


def metric_line(label: str, percent: float | None, detail: str = "") -> str:
    """Строка метрики с баром: <code>RAM   ▰▰▰▰▰▱▱▱▱▱   52%</code>  (2.1/4.0 GB).

    Бар и подпись внутри <code> — моноширинный шрифт выравнивает колонки;
    детали остаются снаружи обычным шрифтом. Без процента — простой текст.
    """
    if percent is None:
        suffix = f": {html_escape(detail)}" if detail else ""
        return f"{html_escape(label)}{suffix}"
    pct = int(max(0, min(100, round(float(percent)))))
    padded = str(label).ljust(_METRIC_LABEL_WIDTH)
    line = f"<code>{html_escape(padded)}{progress_bar(pct)}  {pct:>3}%</code>"
    if detail:
        line += f"  ({html_escape(detail)})"
    return line


_PERCENT_RE = re.compile(r"(\d{1,3})\s*%")


def extract_percent(text: str | None) -> int | None:
    m = _PERCENT_RE.search(str(text or ""))
    if not m:
        return None
    value = int(m.group(1))
    return value if 0 <= value <= 100 else None


def used_total_percent(used: float, total: float) -> int | None:
    try:
        used_f = float(used)
        total_f = float(total)
    except (TypeError, ValueError):
        return None
    if total_f <= 0 or used_f < 0:
        return None
    return int(max(0, min(100, round(used_f / total_f * 100))))


def header(icon: str, title: str, status_text: str = "") -> str:
    """Заголовок экрана: 🖥 <b>Germany</b> · 🟢 онлайн."""
    base = f"{icon} <b>{html_escape(title)}</b>".strip()
    return f"{base} · {status_text}" if status_text else base


def section(title: str, icon: str = "") -> str:
    """Заголовок секции: 📊 <b>Ресурсы</b>."""
    return f"{icon} <b>{html_escape(title)}</b>".strip()


def footer_updated(dt_text: str) -> str:
    return f"<i>обновлено {html_escape(dt_text)}</i>"


def btn_back(callback_data: str, label: str = "⬅️ Назад") -> InlineKeyboardButton:
    return InlineKeyboardButton(label, callback_data=callback_data)


def btn_home() -> InlineKeyboardButton:
    return InlineKeyboardButton("🏠 Меню", callback_data="menu:home")


def pager_row(cb_prefix: str, page: int, total_pages: int) -> list[InlineKeyboardButton]:
    """Навигация по страницам: ◀ / N/M / ▶ (callback = f"{cb_prefix}{page}")."""
    row: list[InlineKeyboardButton] = []
    if page > 0:
        row.append(InlineKeyboardButton("◀", callback_data=f"{cb_prefix}{page - 1}"))
    row.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data=f"{cb_prefix}{page}"))
    if page < total_pages - 1:
        row.append(InlineKeyboardButton("▶", callback_data=f"{cb_prefix}{page + 1}"))
    return row


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


def humanize_until(minutes: int) -> str:
    """Человекочитаемый остаток времени до события (до двух единиц).

    Примеры: «19 минут», «11 часов 40 минут», «12 часов», «3 суток»,
    «1 сутки 1 час». Отрицательные значения трактуются как 0.
    """
    total = max(0, int(minutes))
    days, rem = divmod(total, 1440)
    hours, mins = divmod(rem, 60)
    if days:
        parts = [f"{days} {plural_ru(days, 'сутки', 'суток', 'суток')}"]
        if hours:
            parts.append(f"{hours} {plural_ru(hours, 'час', 'часа', 'часов')}")
        return " ".join(parts)
    if hours:
        parts = [f"{hours} {plural_ru(hours, 'час', 'часа', 'часов')}"]
        if mins:
            parts.append(f"{mins} {plural_ru(mins, 'минута', 'минуты', 'минут')}")
        return " ".join(parts)
    return f"{mins} {plural_ru(mins, 'минута', 'минуты', 'минут')}"
