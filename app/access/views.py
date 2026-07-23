from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

ACCESS_RESULT_TEXTS = {
    "approved": "Доступ уже одобрен. Откройте /menu.",
    "blocked": "🚫 Доступ к боту отключён\n\nВы отключены от данного бота по решению администрации.",
    "admin": "Для возврата в администраторскую учётную запись используйте команду /auth.",
    "pending": "Заявка уже ожидает решения администратора.",
    "cooldown": "Повторная заявка отправлялась недавно. Попробуйте позже.",
    "created": "Заявка отправлена администраторам. Бот сообщит о решении.",
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
    "post_rejection_markup",
    "request_access_markup",
]
