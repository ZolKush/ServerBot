"""Shared Telegram presentation primitives.

This module is deliberately independent from feature handlers.  Feature-specific
views may build on these primitives, but the primitives never import features.
"""

from __future__ import annotations

import html
import re
from datetime import datetime
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.error import BadRequest

from ..config import TZ, logger


def html_escape(value: str) -> str:
    return html.escape(value or "", quote=False)


UI_OK = "✅"
UI_WARN = "⚠️"
UI_ERR = "❌"
UI_INFO = "ℹ️"


def ui_ok_text(text: str) -> str:
    return f"{UI_OK} {text}"


def ui_warn_text(text: str) -> str:
    return f"{UI_WARN} {text}"


def ui_error_text(text: str) -> str:
    return f"{UI_ERR} Ошибка: {text}"


def ui_info_text(text: str) -> str:
    return f"{UI_INFO} {text}"


def breadcrumbs(*parts: str) -> str:
    items = [str(part).strip() for part in parts if str(part or "").strip()]
    return " > ".join(items)


def clip_text(value: str, limit: int = 3300) -> str:
    if value is None:
        return ""
    text = str(value)
    return text if len(text) <= limit else (text[:limit] + "\n…(truncated)…")


def clip_html(value: str, limit: int = 3300) -> str:
    """Escape HTML and then truncate to the Telegram-oriented limit."""
    escaped = html_escape("" if value is None else str(value))
    if len(escaped) <= limit:
        return escaped
    cut = escaped[:limit]
    amp = cut.rfind("&")
    if amp != -1 and ";" not in cut[amp:]:
        cut = cut[:amp]
    return cut + "\n…(truncated)…"


def wrap_as_codeblock_html(text: str, limit: int = 3300) -> str:
    return f"<pre><code>{clip_html(text, limit)}</code></pre>"


def clip_html_message(text: str, limit: int = 4000) -> str:
    value = str(text or "")
    if len(value) <= limit:
        return value
    suffix = "\n<i>…сообщение сокращено из-за лимита Telegram</i>"
    kept: list[str] = []
    length = 0
    for line in value.splitlines():
        extra = len(line) + (1 if kept else 0)
        if length + extra + len(suffix) > limit:
            break
        kept.append(line)
        length += extra
    return ("\n".join(kept) + suffix) if kept else html_escape(value[: limit - len(suffix)]) + suffix


def now_str() -> str:
    return datetime.now(TZ).strftime("%d.%m.%Y %H:%M:%S")


def format_dt_human(value: Any, *, empty: str = "-", tz_label: str = "по МСК") -> str:
    raw = str(value or "").strip()
    if not raw:
        return empty
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return raw
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=TZ)
    else:
        parsed = parsed.astimezone(TZ)
    return f"{parsed.strftime('%d.%m.%Y %H:%M')} {tz_label}"


async def safe_edit_or_reply(
    message: Any,
    text: str,
    *,
    reply_markup: InlineKeyboardMarkup | None = None,
    parse_mode: str = ParseMode.HTML,
) -> None:
    """Edit a Telegram message and fall back to a reply without duplicates."""
    if message is None:
        return
    text = clip_html_message(text)
    try:
        await message.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
        return
    except BadRequest as error:
        if "message is not modified" in str(error).lower():
            return
        logger.warning("edit_text не удался (%s), отправляю новое сообщение", error)
    except Exception as error:
        logger.warning("edit_text не удался (%s), отправляю новое сообщение", error)
    try:
        await message.reply_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception as error:
        logger.error("Не удалось отправить сообщение после неудачного edit_text: %s", error)


# Existing formatters insert SEP on a separate line.  An empty value preserves
# vertical spacing without showing a decorative rule.
SEP = ""

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
    key = str(code or "").strip().lower()
    label = URGENCY_LABELS.get(key)
    return label.split()[0] if label else "📋"


def progress_bar(percent: float, width: int = 10) -> str:
    try:
        value = float(percent)
    except (TypeError, ValueError):
        value = 0.0
    value = max(0.0, min(100.0, value))
    filled = round(width * value / 100.0)
    return "▰" * filled + "▱" * (width - filled)


_METRIC_LABEL_WIDTH = 5


def metric_line(label: str, percent: float | None, detail: str = "") -> str:
    if percent is None:
        suffix = f": {html_escape(detail)}" if detail else ""
        return f"{html_escape(label)}{suffix}"
    value = int(max(0, min(100, round(float(percent)))))
    padded = str(label).ljust(_METRIC_LABEL_WIDTH)
    line = f"<code>{html_escape(padded)}{progress_bar(value)}  {value:>3}%</code>"
    if detail:
        line += f"  ({html_escape(detail)})"
    return line


_PERCENT_RE = re.compile(r"(\d{1,3})\s*%")


def extract_percent(text: str | None) -> int | None:
    match = _PERCENT_RE.search(str(text or ""))
    if not match:
        return None
    value = int(match.group(1))
    return value if 0 <= value <= 100 else None


def used_total_percent(used: float, total: float) -> int | None:
    try:
        used_value = float(used)
        total_value = float(total)
    except (TypeError, ValueError):
        return None
    if total_value <= 0 or used_value < 0:
        return None
    return int(max(0, min(100, round(used_value / total_value * 100))))


def header(icon: str, title: str, status_text: str = "") -> str:
    base = f"{icon} <b>{html_escape(title)}</b>".strip()
    return f"{base} · {status_text}" if status_text else base


def section(title: str, icon: str = "") -> str:
    return f"{icon} <b>{html_escape(title)}</b>".strip()


def footer_updated(dt_text: str) -> str:
    return f"<i>обновлено {html_escape(dt_text)}</i>"


def btn_back(callback_data: str, label: str = "⬅️ Назад") -> InlineKeyboardButton:
    return InlineKeyboardButton(label, callback_data=callback_data)


def btn_home() -> InlineKeyboardButton:
    return InlineKeyboardButton("🏠 Меню", callback_data="menu:home")


def pager_row(cb_prefix: str, page: int, total_pages: int) -> list[InlineKeyboardButton]:
    row: list[InlineKeyboardButton] = []
    if page > 0:
        row.append(InlineKeyboardButton("◀", callback_data=f"{cb_prefix}{page - 1}"))
    row.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data=f"{cb_prefix}{page}"))
    if page < total_pages - 1:
        row.append(InlineKeyboardButton("▶", callback_data=f"{cb_prefix}{page + 1}"))
    return row


def plural_ru(number: int, one: str, few: str, many: str) -> str:
    value = abs(number) % 100
    last = value % 10
    if 11 <= value <= 19:
        return many
    if last == 1:
        return one
    if 2 <= last <= 4:
        return few
    return many


def humanize_hhmm(hours: int, minutes: int) -> str:
    parts = []
    if hours:
        parts.append(f"{hours} {plural_ru(hours, 'час', 'часа', 'часов')}")
    if minutes:
        parts.append(f"{minutes} {plural_ru(minutes, 'минута', 'минуты', 'минут')}")
    return " ".join(parts) if parts else "0 минут"


def humanize_until(minutes: int) -> str:
    total = max(0, int(minutes))
    days, remainder = divmod(total, 1440)
    hours, mins = divmod(remainder, 60)
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
