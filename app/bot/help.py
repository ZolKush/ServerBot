"""Editable help and support-contact presentation."""

from __future__ import annotations

import html
from typing import Any

DEFAULT_HELP_TEXT = (
    "Если у вас возникли проблемы с подключением, выполните эти шаги до обращения к администрации:\n\n"
    "1. Обновите подписку в приложении.\n"
    "2. Проверьте соединение с каждым сервером.\n"
    "3. Обновите приложение Happ.\n"
    "4. Попробуйте включить фрагментирование в настройках.\n"
    "5. Сбросьте данные приложения и добавьте подписку заново.\n"
    "6. Если ничего не помогло — создайте тикет в боте."
)


def help_text_from_settings(settings: dict[str, Any] | None) -> str:
    configured = str((settings or {}).get("help_text") or "").strip()
    return configured or DEFAULT_HELP_TEXT


def render_help_message(settings: dict[str, Any] | None) -> str:
    body = html.escape(help_text_from_settings(settings), quote=False)
    return f"ℹ️ <b>Помощь</b>\n\n{body}{render_support_contact(settings)}"


def render_support_contact(settings: dict[str, Any] | None) -> str:
    support_email = str((settings or {}).get("support_email") or "").strip()
    if not support_email:
        return ""
    escaped = html.escape(support_email, quote=False)
    return f"\n\n📧 <b>Резервный канал связи с администрацией</b>\n<code>{escaped}</code>"


__all__ = ["DEFAULT_HELP_TEXT", "help_text_from_settings", "render_help_message", "render_support_contact"]
