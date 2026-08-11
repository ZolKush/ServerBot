"""Presentation for the persisted TLS certificate report."""

from __future__ import annotations

from datetime import datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from ...bot.ui import SEP, breadcrumbs, html_escape
from ..status.models import TLSCertificateView

MAX_REPORT_CERTIFICATES = 15


def tls_report_keyboard(server_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("⬅️ К статусу", callback_data=f"status:show:{server_key}")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


def _time_text(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return value or "н/д"
    return parsed.strftime("%d.%m.%Y %H:%M %Z").strip()


def _state(item: TLSCertificateView) -> tuple[str, str]:
    if item.status == "ok":
        return "🟢", "действителен"
    if item.status == "expiring":
        return "⚠️", f"истекает через {max(0, item.remaining_seconds // 3600)} ч."
    if item.status == "expired":
        return "🔴", "просрочен"
    if item.status == "invalid":
        return "🔴", "невалиден"
    return "⚠️", "проверка не удалась"


def format_tls_report(server_label: str, items: list[TLSCertificateView]) -> str:
    lines = [f"<b>{html_escape(breadcrumbs('Статус', server_label, 'TLS'))}</b>", SEP]
    if not items:
        lines.append("⚠️ Сертификаты ещё не проверялись или TLS-домены не настроены.")
        return "\n".join(lines)

    for item in items[:MAX_REPORT_CERTIFICATES]:
        emoji, state = _state(item)
        lines.append(f"{emoji} <code>{html_escape(item.domain)}:{item.port}</code> — <b>{html_escape(state)}</b>")
        if item.used_fallback:
            lines.append(f"   порт: основной <code>{item.primary_port}</code> → fallback <code>{item.port}</code>")
        elif item.fallback_ports:
            fallbacks = ", ".join(str(port) for port in item.fallback_ports)
            lines.append(f"   порт: <code>{item.primary_port}</code>, fallback: <code>{fallbacks}</code>")
        if item.not_after:
            lines.append(f"   действителен до: <code>{html_escape(_time_text(item.not_after))}</code>")
        if item.last_attempt_at:
            lines.append(f"   последняя попытка: <code>{html_escape(_time_text(item.last_attempt_at))}</code>")
        if item.last_success_at:
            lines.append(f"   последний успех: <code>{html_escape(_time_text(item.last_success_at))}</code>")
        if item.error:
            lines.append(f"   <i>{html_escape(item.error[:300])}</i>")
        lines.append("")
    hidden = len(items) - MAX_REPORT_CERTIFICATES
    if hidden > 0:
        lines.append(f"<i>… ещё {hidden} сертификатов</i>")
    lines.append("Сетевая проверка выполняется при запуске бота, затем один раз в неделю.")
    return "\n".join(lines).strip()


__all__ = ["format_tls_report", "tls_report_keyboard"]
