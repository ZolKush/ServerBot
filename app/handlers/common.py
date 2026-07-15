from __future__ import annotations

import html
from collections.abc import Callable
from datetime import datetime
from functools import wraps
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatType, ParseMode
from telegram.error import BadRequest
from telegram.ext import ContextTypes, ConversationHandler

from ..config import (
    MENU_MAINT,
    MENU_REQUESTS,
    MENU_STAFF_PROFILE,
    MENU_STATUS,
    MENU_SUBSCRIPTION,
    MENU_TICKET,
    MENU_USERS,
    TZ,
    logger,
)
from ..staff import (
    is_lead_or_owner_meta,
    is_owner_meta,
    staff_public_signature,
    staff_title_label,
)
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


def clip_text(s: str, limit: int = 3300) -> str:
    if s is None:
        return ""
    s = str(s)
    return s if len(s) <= limit else (s[:limit] + "\n…(truncated)…")


def clip_html(s: str, limit: int = 3300) -> str:
    """Экранирует HTML и обрезает по длине уже экранированного текста.

    Лимиты Telegram считаются по итоговому тексту, поэтому клип до эскейпа
    (& -> &amp; и т.п.) может превысить 4096 символов.
    """
    escaped = html_escape("" if s is None else str(s))
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

    dt: datetime | None = None
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S,%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                dt = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue

    if dt is None:
        return raw

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    else:
        dt = dt.astimezone(TZ)
    return f"{dt.strftime('%d.%m.%Y %H:%M')} {tz_label}"


def is_private(update: Update) -> bool:
    return bool(update.effective_chat and update.effective_chat.type == ChatType.PRIVATE)


def get_user_id(update: Update) -> int | None:
    u = update.effective_user
    return int(u.id) if u else None


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
    msg = update.effective_message
    if msg:
        meta = get_user_meta(get_user_id(update) or 0) or {}
        state = str(meta.get("access_state") or "blocked")
        texts = {
            "blocked": "🚫 Доступ к боту отключён\n\nВы отключены от данного бота по решению администрации.",
            "pending": "Заявка на доступ ожидает решения администратора.",
            "rejected": "Заявка на доступ была отклонена. Вы можете отправить новую позднее.",
            "logged_out": "Вы вышли из бота. Для возврата отправьте новую заявку на доступ.",
        }
        await msg.reply_text(texts.get(state, "Доступ к боту сейчас отключён."))


async def reply_need_auth(update: Update) -> None:
    msg = update.effective_message
    if msg:
        meta = get_user_meta(get_user_id(update) or 0) or {}
        state = str(meta.get("access_state") or "")
        if state == "blocked":
            await reply_disabled(update)
            return
        if state == "pending":
            await msg.reply_text("Заявка на доступ уже отправлена и ожидает решения администратора.")
            return
        if meta.get("role") == "admin":
            await msg.reply_text("Сессия администратора завершена. Войдите снова командой /auth.")
            return
        await msg.reply_text(
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


async def _ensure_access(update: Update, role: str | None) -> bool:
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
    if role == "subscriber" and not has_subscriber_access(get_user_meta(get_user_id(update) or 0)):
        msg = update.effective_message
        if msg:
            await msg.reply_text(
                "🔒 Этот раздел доступен подписчикам. Откройте раздел подключения, чтобы запросить тест или купить подписку.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton(MENU_SUBSCRIPTION, callback_data="menu:subscription")]]
                ),
            )
        return False
    if role == "owner" and not is_owner(update):
        msg = update.effective_message
        if msg:
            await msg.reply_text("Это действие доступно только руководителю сервиса.")
        return False
    if role == "lead" and not is_lead_or_owner(update):
        msg = update.effective_message
        if msg:
            await msg.reply_text("Это действие доступно ведущему инженеру сопровождения или руководителю сервиса.")
        return False
    return True


def display_name_from_meta(meta: dict[str, Any] | None) -> str:
    if not meta:
        return "пользователь"
    nick = str(meta.get("nickname") or "").strip()
    if nick:
        return nick
    uname = meta.get("username")
    if uname:
        return f"@{str(uname)}"
    nm = " ".join(str(x) for x in [meta.get("first_name"), meta.get("last_name")] if x)
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


def main_menu_inline_kb_for_meta(meta: dict[str, Any] | None) -> InlineKeyboardMarkup:
    is_admin_user = bool(meta and meta.get("role") == "admin")
    rows: list[list[InlineKeyboardButton]] = []
    if has_subscriber_access(meta):
        rows.append(
            [
                InlineKeyboardButton(MENU_STATUS, callback_data="menu:status"),
                InlineKeyboardButton(MENU_SUBSCRIPTION, callback_data="menu:subscription"),
            ]
        )
    else:
        rows.append([InlineKeyboardButton(MENU_SUBSCRIPTION, callback_data="menu:subscription")])
    rows.append([InlineKeyboardButton(MENU_TICKET, callback_data="menu:ticket")])
    if is_admin_user:
        rows.append(
            [
                InlineKeyboardButton(MENU_USERS, callback_data="menu:users"),
                InlineKeyboardButton(MENU_REQUESTS, callback_data="product:requests"),
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(MENU_MAINT, callback_data="menu:maint"),
                InlineKeyboardButton(MENU_STAFF_PROFILE, callback_data="staff:profile"),
            ]
        )
    rows.append([InlineKeyboardButton("ℹ️ Помощь", callback_data="menu:help")])
    return InlineKeyboardMarkup(rows)


def main_menu_inline_kb_for_admin(is_admin_user: bool) -> InlineKeyboardMarkup:
    meta = {"role": "admin", "service_tier": "subscriber"} if is_admin_user else {"service_tier": "subscriber"}
    return main_menu_inline_kb_for_meta(meta)


def main_menu_inline_kb(update: Update) -> InlineKeyboardMarkup:
    uid = get_user_id(update)
    return main_menu_inline_kb_for_meta(get_user_meta(uid) if uid is not None else None)


def main_menu_text(is_admin_user: bool, text: str = "Меню:") -> str:
    if text == "Меню:":
        title = "👑 <b>Админ-панель</b>" if is_admin_user else "👤 <b>Главное меню</b>"
        return f"{title}\n\nВыберите раздел:"
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


async def safe_edit_or_reply(
    message: Any,
    text: str,
    *,
    reply_markup: InlineKeyboardMarkup | None = None,
    parse_mode: str = ParseMode.HTML,
) -> None:
    """Редактирует сообщение без дубликатов.

    «message is not modified» — успех (молча выходим); прочие ошибки
    логируются, после чего текст уходит новым сообщением.
    """
    if message is None:
        return
    text = clip_html_message(text)
    try:
        await message.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
        return
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            return
        logger.warning("edit_text не удался (%s), отправляю новое сообщение", e)
    except Exception as e:
        logger.warning("edit_text не удался (%s), отправляю новое сообщение", e)
    try:
        await message.reply_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception as e:
        logger.error("Не удалось отправить сообщение после неудачного edit_text: %s", e)


@require_auth
async def menu_home_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await show_main_menu(update)


def clear_transient_user_context(context: ContextTypes.DEFAULT_TYPE) -> None:
    ud = context.user_data
    if ud is None:
        return
    transient_keys = {"selected_uid", "subscription_delivery_mode", "users_all_broadcast_text"}
    for key in tuple(ud.keys()):
        if key.startswith(("ticket_", "maint_", "product_")) or key in transient_keys:
            ud.pop(key, None)


@require_auth
async def cancel_to_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_transient_user_context(context)
    await show_main_menu(update)
    return ConversationHandler.END


def authorized_ids(role_filter: str | None = None, exclude: set[int] | None = None) -> list[int]:
    exclude = exclude or set()
    ids: list[int] = []
    for k, meta in authorized_users_snapshot().items():
        try:
            uid = int(meta.get("user_id", k))
        except (TypeError, ValueError, OverflowError):
            continue
        if uid in exclude:
            continue
        if meta.get("access_state", "approved") != "approved" or not bool(meta.get("enabled", True)):
            continue
        if role_filter and meta.get("role") != role_filter:
            continue
        ids.append(uid)
    return sorted(set(ids))


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_transient_user_context(context)
    msg = update.effective_message
    if msg:
        await msg.reply_text("Действие отменено.")
    return ConversationHandler.END
