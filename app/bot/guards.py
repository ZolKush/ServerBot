"""Identity, authorization checks, and reusable PTB guard decorators."""

from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatType
from telegram.ext import ContextTypes, ConversationHandler

from ..config import MENU_SUBSCRIPTION
from ..storage import authorized_users_snapshot, get_user_meta_copy
from ..users.staff import (
    can_manage_maintenance_meta,
    is_lead_or_owner_meta,
    is_owner_meta,
    staff_public_signature,
    staff_title_label,
)


def is_private(update: Update) -> bool:
    return bool(update.effective_chat and update.effective_chat.type == ChatType.PRIVATE)


def get_user_id(update: Update) -> int | None:
    user = update.effective_user
    return int(user.id) if user else None


def get_user_meta(uid: int) -> dict[str, Any] | None:
    return get_user_meta_copy(uid)


def is_authorized(update: Update) -> bool:
    uid = get_user_id(update)
    meta = get_user_meta_copy(uid) if uid is not None else None
    return bool(meta and meta.get("access_state", "approved") == "approved")


def is_enabled(update: Update) -> bool:
    uid = get_user_id(update)
    if uid is None:
        return False
    meta = get_user_meta(uid)
    return bool(meta and meta.get("access_state", "approved") == "approved" and meta.get("enabled", True))


def is_admin(update: Update) -> bool:
    uid = get_user_id(update)
    if uid is None:
        return False
    meta = get_user_meta(uid)
    return bool(
        meta
        and meta.get("role") == "admin"
        and meta.get("access_state", "approved") == "approved"
        and meta.get("enabled", True)
    )


def is_owner(update: Update) -> bool:
    uid = get_user_id(update)
    return bool(uid is not None and is_owner_meta(get_user_meta(uid)))


def is_lead_or_owner(update: Update) -> bool:
    uid = get_user_id(update)
    return bool(uid is not None and is_lead_or_owner_meta(get_user_meta(uid)))


def has_subscriber_access(meta: dict[str, Any] | None) -> bool:
    if not meta:
        return False
    return bool(meta.get("role") == "admin" or meta.get("service_tier") in {"subscriber", "unlimited_trial"})


def staff_signature(update: Update, *, allow_alias: bool = True) -> str:
    uid = get_user_id(update)
    return staff_public_signature(get_user_meta(uid) if uid is not None else None, allow_alias=allow_alias)


def staff_title(update: Update) -> str:
    uid = get_user_id(update)
    return staff_title_label(get_user_meta(uid) if uid is not None else None)


async def reply_disabled(update: Update) -> None:
    message = update.effective_message
    if not message:
        return
    meta = get_user_meta(get_user_id(update) or 0) or {}
    state = str(meta.get("access_state") or "blocked")
    texts = {
        "blocked": "🚫 Доступ к боту отключён\n\nВы отключены от данного бота по решению администрации.",
        "pending": "Заявка на доступ ожидает решения администратора.",
        "rejected": "Заявка на доступ была отклонена. Вы можете отправить новую позднее.",
        "logged_out": "Вы вышли из бота. Для возврата отправьте новую заявку на доступ.",
    }
    await message.reply_text(texts.get(state, "Доступ к боту сейчас отключён."))


async def reply_need_auth(update: Update) -> None:
    message = update.effective_message
    if not message:
        return
    meta = get_user_meta(get_user_id(update) or 0) or {}
    state = str(meta.get("access_state") or "")
    if state == "blocked":
        await reply_disabled(update)
        return
    if state == "pending":
        await message.reply_text("Заявка на доступ уже отправлена и ожидает решения администратора.")
        return
    if meta.get("role") == "admin":
        await message.reply_text("Сессия администратора завершена. Войдите снова командой /auth.")
        return
    await message.reply_text(
        "Доступ предоставляется после одобрения администратором.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔐 Запросить доступ", callback_data="access:request")]]
        ),
    )


def require_private(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args: Any, **kwargs: Any):
        if not is_private(update):
            return ConversationHandler.END
        return await func(update, context, *args, **kwargs)

    return wrapper


def require_auth(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args: Any, **kwargs: Any):
        if not await _ensure_access(update, role=None):
            return ConversationHandler.END
        return await func(update, context, *args, **kwargs)

    return wrapper


def require_admin(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args: Any, **kwargs: Any):
        if not await _ensure_access(update, role="admin"):
            return ConversationHandler.END
        return await func(update, context, *args, **kwargs)

    return wrapper


def require_subscriber(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args: Any, **kwargs: Any):
        if not await _ensure_access(update, role="subscriber"):
            return ConversationHandler.END
        return await func(update, context, *args, **kwargs)

    return wrapper


def require_owner(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args: Any, **kwargs: Any):
        if not await _ensure_access(update, role="owner"):
            return ConversationHandler.END
        return await func(update, context, *args, **kwargs)

    return wrapper


def require_lead(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args: Any, **kwargs: Any):
        if not await _ensure_access(update, role="lead"):
            return ConversationHandler.END
        return await func(update, context, *args, **kwargs)

    return wrapper


def require_maintenance(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args: Any, **kwargs: Any):
        if not await _ensure_access(update, role="maintenance"):
            return ConversationHandler.END
        return await func(update, context, *args, **kwargs)

    return wrapper


async def _ensure_access(update: Update, role: str | None) -> bool:
    if not is_private(update):
        return False
    if not is_authorized(update):
        await reply_need_auth(update)
        return False
    if not is_enabled(update):
        await reply_disabled(update)
        return False
    message = update.effective_message
    if role == "admin" and not is_admin(update):
        if message:
            await message.reply_text("Доступ только для администратора.")
        return False
    if role == "subscriber" and not has_subscriber_access(get_user_meta(get_user_id(update) or 0)):
        if message:
            await message.reply_text(
                "🔒 Этот раздел доступен подписчикам. Откройте раздел подключения, чтобы запросить тест или купить подписку.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton(MENU_SUBSCRIPTION, callback_data="menu:subscription")]]
                ),
            )
        return False
    if role == "owner" and not is_owner(update):
        if message:
            await message.reply_text("Это действие доступно только руководителю сервиса.")
        return False
    if role == "lead" and not is_lead_or_owner(update):
        if message:
            await message.reply_text("Это действие доступно ведущему инженеру сопровождения или руководителю сервиса.")
        return False
    if role == "maintenance" and not can_manage_maintenance_meta(get_user_meta(get_user_id(update) or 0)):
        if message:
            await message.reply_text(
                "Техработами могут управлять инженер сопровождения, "
                "ведущий инженер сопровождения или руководитель сервиса."
            )
        return False
    return True


def display_name_from_meta(meta: dict[str, Any] | None) -> str:
    if not meta:
        return "пользователь"
    nickname = str(meta.get("nickname") or "").strip()
    if nickname:
        return nickname
    username = meta.get("username")
    if username:
        return f"@{username}"
    name = " ".join(str(value) for value in [meta.get("first_name"), meta.get("last_name")] if value)
    if name.strip():
        return name.strip()
    uid = meta.get("user_id")
    return str(uid) if uid is not None else "пользователь"


def display_name(update: Update) -> str:
    user = update.effective_user
    if not user:
        return "пользователь"
    meta = get_user_meta(user.id)
    if meta:
        return display_name_from_meta(meta)
    if user.username:
        return f"@{user.username}"
    name = " ".join(value for value in [user.first_name, user.last_name] if value)
    return name if name else str(user.id)


def authorized_ids(role_filter: str | None = None, exclude: set[int] | None = None) -> list[int]:
    excluded = exclude or set()
    ids: list[int] = []
    for key, meta in authorized_users_snapshot().items():
        try:
            uid = int(meta.get("user_id", key))
        except (TypeError, ValueError, OverflowError):
            continue
        if uid in excluded:
            continue
        if meta.get("access_state", "approved") != "approved" or not bool(meta.get("enabled", True)):
            continue
        if role_filter and meta.get("role") != role_filter:
            continue
        ids.append(uid)
    return sorted(set(ids))


def maintenance_manager_ids(exclude: set[int] | None = None) -> list[int]:
    return [
        uid
        for uid in authorized_ids(role_filter="admin", exclude=exclude)
        if can_manage_maintenance_meta(get_user_meta(uid))
    ]
