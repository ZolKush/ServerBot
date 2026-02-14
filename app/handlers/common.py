from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from telegram import KeyboardButton, ReplyKeyboardMarkup, Update
from telegram.constants import ChatType, ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ..config import MENU_MAINT, MENU_STATUS, MENU_TICKET, MENU_USERS, TZ, logger
from ..storage import USER_DATA


def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def wrap_as_codeblock_html(text: str) -> str:
    return f"<pre><code>{html_escape(text or '')}</code></pre>"


def clip_text(s: str, limit: int = 3300) -> str:
    if s is None:
        return ""
    s = str(s)
    return s if len(s) <= limit else (s[:limit] + "\n…(truncated)…")


def now_str() -> str:
    return datetime.now(TZ).strftime("%d.%m.%Y %H:%M:%S")


def is_private(update: Update) -> bool:
    return bool(update.effective_chat and update.effective_chat.type == ChatType.PRIVATE)


def get_user_id(update: Update) -> Optional[int]:
    u = update.effective_user
    return int(u.id) if u else None


def get_user_meta(uid: int) -> Optional[Dict[str, Any]]:
    return USER_DATA.authorized_users.get(str(uid))


def is_authorized(update: Update) -> bool:
    uid = get_user_id(update)
    return bool(uid is not None and str(uid) in USER_DATA.authorized_users)


def is_enabled(update: Update) -> bool:
    uid = get_user_id(update)
    if uid is None:
        return False
    meta = get_user_meta(uid)
    return bool(meta and meta.get("enabled", True))


def is_admin(update: Update) -> bool:
    uid = get_user_id(update)
    if uid is None:
        return False
    meta = get_user_meta(uid)
    return bool(meta and meta.get("role") == "admin")


async def reply_disabled(update: Update) -> None:
    msg = update.effective_message
    if msg:
        await msg.reply_text("Вы отключены от этого бота. Обратитесь к администратору напрямую.")


async def reply_need_auth(update: Update) -> None:
    msg = update.effective_message
    if msg:
        await msg.reply_text(
            "Доступ ограничен. Авторизуйтесь командой:\n<b>/auth пароль</b>",
            parse_mode=ParseMode.HTML,
        )


def require_private(func: Callable[..., Any]) -> Callable[..., Any]:
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args: Any, **kwargs: Any):
        if not is_private(update):
            return ConversationHandler.END
        return await func(update, context, *args, **kwargs)

    return wrapper


def require_auth(func: Callable[..., Any]) -> Callable[..., Any]:
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args: Any, **kwargs: Any):
        if not await _ensure_access(update, role=None):
            return ConversationHandler.END
        return await func(update, context, *args, **kwargs)

    return wrapper


def require_admin(func: Callable[..., Any]) -> Callable[..., Any]:
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args: Any, **kwargs: Any):
        if not await _ensure_access(update, role="admin"):
            return ConversationHandler.END
        return await func(update, context, *args, **kwargs)

    return wrapper


async def _ensure_access(update: Update, role: Optional[str]) -> bool:
    if not is_private(update):
        return False
    if not is_authorized(update):
        await reply_need_auth(update)
        return False
    if not is_enabled(update):
        await reply_disabled(update)
        return False
    if role == "admin" and not is_admin(update):
        msg = update.effective_message
        if msg:
            await msg.reply_text("Доступ только для администратора.")
        return False
    return True


def display_name_from_meta(meta: Optional[Dict[str, Any]]) -> str:
    if not meta:
        return "пользователь"
    nick = (meta.get("nickname") or "").strip()
    if nick:
        return nick
    uname = meta.get("username")
    if uname:
        return f"@{uname}"
    nm = " ".join([x for x in [meta.get("first_name"), meta.get("last_name")] if x])
    if nm.strip():
        return nm.strip()
    uid = meta.get("user_id")
    return str(uid) if uid is not None else "пользователь"


def display_name(update: Update) -> str:
    u = update.effective_user
    if not u:
        return "пользователь"
    meta = get_user_meta(u.id)
    if meta:
        return display_name_from_meta(meta)
    if u.username:
        return f"@{u.username}"
    nm = " ".join([x for x in [u.first_name, u.last_name] if x])
    return nm if nm else str(u.id)


def main_menu_kb(update: Update) -> ReplyKeyboardMarkup:
    rows = [[KeyboardButton(MENU_STATUS), KeyboardButton(MENU_TICKET)]]
    if is_admin(update):
        rows.append([KeyboardButton(MENU_USERS), KeyboardButton(MENU_MAINT)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


async def send_to_many(
    context: ContextTypes.DEFAULT_TYPE,
    user_ids: Iterable[int],
    text: str,
) -> Tuple[int, int]:
    ok = 0
    fail = 0
    for uid in user_ids:
        try:
            await context.bot.send_message(chat_id=uid, text=text, parse_mode=ParseMode.HTML)
            ok += 1
        except Exception as e:
            logger.warning("Не удалось отправить пользователю %s: %s", uid, e)
            fail += 1
    return ok, fail


def authorized_ids(role_filter: Optional[str] = None, exclude: Optional[Set[int]] = None) -> List[int]:
    exclude = exclude or set()
    ids: List[int] = []
    for k, meta in USER_DATA.authorized_users.items():
        try:
            uid = int(meta.get("user_id", k))
        except Exception:
            continue
        if uid in exclude:
            continue
        if not bool(meta.get("enabled", True)):
            continue
        if role_filter and meta.get("role") != role_filter:
            continue
        ids.append(uid)
    return sorted(set(ids))


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg:
        await msg.reply_text("Действие отменено.")
    return ConversationHandler.END
