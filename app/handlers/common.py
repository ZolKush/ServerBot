from __future__ import annotations

import asyncio
import html
import random
from dataclasses import dataclass, field
from functools import wraps
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatType, ParseMode
from telegram.error import BadRequest, NetworkError, RetryAfter, TimedOut
from telegram.ext import ContextTypes, ConversationHandler

from ..config import MENU_MAINT, MENU_STATUS, MENU_SUBSCRIPTION, MENU_TICKET, MENU_USERS, TZ, logger
from ..storage import authorized_users_snapshot, get_user_meta_copy


def html_escape(s: str) -> str:
    return html.escape(s or "", quote=False)


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
    items = [str(p).strip() for p in parts if str(p or "").strip()]
    return " > ".join(items)


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
    return get_user_meta_copy(uid)


def is_authorized(update: Update) -> bool:
    uid = get_user_id(update)
    return bool(uid is not None and get_user_meta_copy(uid) is not None)


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


def main_menu_inline_kb_for_admin(is_admin_user: bool) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(MENU_STATUS, callback_data="menu:status"),
            InlineKeyboardButton(MENU_SUBSCRIPTION, callback_data="menu:subscription"),
        ]
    ]
    rows.append([InlineKeyboardButton(MENU_TICKET, callback_data="menu:ticket")])
    if is_admin_user:
        rows.append(
            [
                InlineKeyboardButton(MENU_USERS, callback_data="menu:users"),
                InlineKeyboardButton(MENU_MAINT, callback_data="menu:maint"),
            ]
        )
    rows.append([InlineKeyboardButton("ℹ️ Помощь", callback_data="menu:help")])
    return InlineKeyboardMarkup(rows)


def main_menu_inline_kb(update: Update) -> InlineKeyboardMarkup:
    return main_menu_inline_kb_for_admin(is_admin(update))


def main_menu_text(is_admin_user: bool, text: str = "Меню:") -> str:
    if text == "Меню:":
        return "👑 <b>Админ-панель</b>\n\nВыберите раздел:" if is_admin_user else "👤 <b>Главное меню</b>\n\nВыберите раздел:"
    return text


async def show_main_menu(update: Update, text: str = "Меню:") -> None:
    q = update.callback_query
    markup = main_menu_inline_kb(update)
    text = main_menu_text(is_admin(update), text=text)
    if q:
        await q.answer()
        try:
            await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
        except BadRequest as e:
            if "message is not modified" in str(e).lower():
                return
            raise
        return
    msg = update.effective_message
    if msg:
        await msg.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


@require_auth
async def menu_home_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await show_main_menu(update)


@require_auth
async def cancel_to_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    for key in tuple(context.user_data.keys()):
        if key.startswith("ticket_") or key.startswith("maint_") or key in {"selected_uid", "subscription_delivery_mode"}:
            context.user_data.pop(key, None)
    await show_main_menu(update)
    return ConversationHandler.END


@dataclass
class SendErrorInfo:
    user_id: int
    error_type: str
    message: str


@dataclass
class SendManyReport:
    ok: int = 0
    fail: int = 0
    errors: List[SendErrorInfo] = field(default_factory=list)

    def __iter__(self):
        yield self.ok
        yield self.fail


def _retry_after_seconds(exc: RetryAfter) -> float:
    ra = getattr(exc, "retry_after", 1)
    try:
        return max(0.5, float(ra))
    except Exception:
        return 1.0


async def send_to_many(
    context: ContextTypes.DEFAULT_TYPE,
    user_ids: Iterable[int],
    text: str,
    *,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    max_concurrency: int = 8,
    max_attempts: int = 3,
) -> SendManyReport:
    ids = sorted(set(int(uid) for uid in user_ids))
    if not ids:
        return SendManyReport()
    report = SendManyReport()
    report_lock = asyncio.Lock()
    queue: asyncio.Queue[int] = asyncio.Queue()
    for uid in ids:
        queue.put_nowait(uid)

    async def _send_one(uid: int) -> tuple[bool, Optional[SendErrorInfo]]:
        last_exc: Optional[Exception] = None
        for attempt in range(1, max(1, max_attempts) + 1):
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup,
                )
                return True, None
            except RetryAfter as e:
                last_exc = e
                if attempt >= max_attempts:
                    break
                await asyncio.sleep(_retry_after_seconds(e))
            except (TimedOut, NetworkError) as e:
                last_exc = e
                if attempt >= max_attempts:
                    break
                await asyncio.sleep(min(2.0, 0.25 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.2))
            except Exception as e:
                last_exc = e
                break
        err = SendErrorInfo(
            user_id=uid,
            error_type=(last_exc.__class__.__name__ if last_exc else "Error"),
            message=(str(last_exc).strip() if last_exc else "unknown error"),
        )
        return False, err

    async def _worker() -> None:
        while True:
            try:
                uid = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            ok, err = await _send_one(uid)
            async with report_lock:
                if ok:
                    report.ok += 1
                else:
                    report.fail += 1
                    if err:
                        report.errors.append(err)
                        logger.warning("Не удалось отправить пользователю %s: %s: %s", err.user_id, err.error_type, err.message)
            queue.task_done()

    workers = [asyncio.create_task(_worker()) for _ in range(min(len(ids), max(1, max_concurrency)))]
    await asyncio.gather(*workers)
    return report


def authorized_ids(role_filter: Optional[str] = None, exclude: Optional[Set[int]] = None) -> List[int]:
    exclude = exclude or set()
    ids: List[int] = []
    for k, meta in authorized_users_snapshot().items():
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
    for key in tuple(context.user_data.keys()):
        if key.startswith("ticket_") or key.startswith("maint_") or key in {"selected_uid", "subscription_delivery_mode"}:
            context.user_data.pop(key, None)
    msg = update.effective_message
    if msg:
        await msg.reply_text("Действие отменено.")
    return ConversationHandler.END
