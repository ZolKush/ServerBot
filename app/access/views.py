from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from ..bot.ui import html_escape

ACCESS_RESULT_TEXTS = {
    "approved": "Доступ уже одобрен. Откройте /menu.",
    "blocked": "🚫 Доступ к боту отключён\n\nВы отключены от данного бота по решению администрации.",
    "admin": "Для возврата в администраторскую учётную запись используйте команду /auth.",
    "pending": "Заявка уже ожидает решения администратора.",
    "cooldown": "Повторная заявка отправлялась недавно. Попробуйте позже.",
    "created": "Заявка отправлена администраторам. Бот сообщит о решении.",
    "restored_paid": "Оплаченный доступ восстановлен автоматически. Откройте /menu.",
}

ACCESS_REVIEW_RESULT_TEXTS = {
    "missing": "Пользователь больше не найден.",
    "admin": "Нельзя изменить доступ администратора.",
    "already": "Это решение уже было применено.",
    "stale": "Заявка уже обработана другим администратором.",
}

ACCESS_DECISION_LABELS = {
    "approve": "одобрена",
    "reject": "отклонена",
    "block": "заблокирована",
}

ACCESS_NOTIFICATION_TEXTS = {
    "approved": "✅ Ваша заявка на доступ одобрена. Используйте /menu.",
    "rejected": "❌ Ваша заявка на доступ отклонена.",
    "blocked": "🚫 Доступ к боту отключён\n\nВы отключены от данного бота по решению администрации.",
}


def access_request_markup_descriptor(user_id: int) -> list[list[dict[str, str]]]:
    return [
        [
            {"text": "✅ Одобрить", "callback_data": f"access:approve:{user_id}"},
            {"text": "❌ Отклонить", "callback_data": f"access:reject:{user_id}"},
        ],
        [{"text": "🚫 Заблокировать", "callback_data": f"access:block:{user_id}"}],
    ]


def access_request_card(meta: dict[str, object]) -> str:
    """Render a canonical access-review card from persisted state."""

    username = str(meta.get("username") or "").strip().lstrip("@")
    real_name = " ".join(
        str(value).strip() for value in (meta.get("first_name"), meta.get("last_name")) if str(value or "").strip()
    )
    user_id = int(str(meta.get("user_id", 0) or 0))
    status = {
        "pending": "ожидает решения",
        "approved": "одобрена",
        "rejected": "отклонена",
        "blocked": "пользователь заблокирован",
        "logged_out": "пользователь вышел",
    }.get(str(meta.get("access_state") or ""), "неизвестно")
    lines = [
        "🔐 <b>Заявка на доступ</b>",
        "",
        f"Пользователь: <b>{html_escape(real_name or ('@' + username if username else str(user_id)))}</b>",
        f"Username: <code>{html_escape('@' + username if username else '-')}</code>",
        f"ID: <code>{user_id}</code>",
        f"Статус: <b>{html_escape(status)}</b>",
    ]
    reviewed_by = str(meta.get("access_reviewed_by_name") or "").strip()
    if reviewed_by:
        lines.append(f"Решение: <b>{html_escape(reviewed_by)}</b>")
    return "\n".join(lines)


def access_request_markup(meta: dict[str, object]) -> InlineKeyboardMarkup | None:
    user_id = int(str(meta.get("user_id", 0) or 0))
    state = str(meta.get("access_state") or "")
    if user_id <= 0:
        return None
    if state == "pending":
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("✅ Одобрить", callback_data=f"access:approve:{user_id}"),
                    InlineKeyboardButton("❌ Отклонить", callback_data=f"access:reject:{user_id}"),
                ],
                [InlineKeyboardButton("🚫 Заблокировать", callback_data=f"access:block:{user_id}")],
            ]
        )
    if state == "rejected":
        return post_rejection_markup(user_id)
    return None


def post_rejection_markup(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🚫 Заблокировать пользователя", callback_data=f"access:block:{user_id}")]]
    )


def request_access_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔐 Запросить доступ", callback_data="access:request")]])


__all__ = [
    "ACCESS_DECISION_LABELS",
    "ACCESS_NOTIFICATION_TEXTS",
    "ACCESS_RESULT_TEXTS",
    "ACCESS_REVIEW_RESULT_TEXTS",
    "access_request_markup_descriptor",
    "access_request_card",
    "access_request_markup",
    "post_rejection_markup",
    "request_access_markup",
]
