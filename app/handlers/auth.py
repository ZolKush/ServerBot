from datetime import datetime
import hmac
from time import monotonic
from typing import Optional

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..config import (
    ADMIN_PASSWORD,
    AUTH_FAIL_WINDOW_SEC,
    AUTH_LOCKOUT_SEC,
    AUTH_MAX_FAILS_IN_WINDOW,
    AUTH_PASSWORD,
    TZ,
    logger,
)
from ..storage import get_user_meta_copy, remove_user_meta, upsert_user_meta
from .common import (
    get_user_id,
    get_user_meta,
    is_admin,
    is_authorized,
    is_enabled,
    main_menu_inline_kb,
    reply_disabled,
    require_private,
    show_main_menu,
)

_AUTH_FAILS: dict[str, list[float]] = {}
_AUTH_LOCKED_UNTIL: dict[str, float] = {}


def _auth_prune(now: Optional[float] = None) -> None:
    now = monotonic() if now is None else now
    active_fails: dict[str, list[float]] = {}
    for key, attempts in _AUTH_FAILS.items():
        filtered = [ts for ts in attempts if (now - ts) <= AUTH_FAIL_WINDOW_SEC]
        if filtered:
            active_fails[key] = filtered
    _AUTH_FAILS.clear()
    _AUTH_FAILS.update(active_fails)

    expired_keys = [key for key, until in _AUTH_LOCKED_UNTIL.items() if until <= now]
    for key in expired_keys:
        _AUTH_LOCKED_UNTIL.pop(key, None)


def _auth_actor_key(update: Update) -> str:
    u = update.effective_user
    if u:
        return f"user:{u.id}"
    chat = update.effective_chat
    if chat:
        return f"chat:{chat.id}"
    return "unknown"


def _auth_lock_remaining_sec(update: Update) -> int:
    key = _auth_actor_key(update)
    now = monotonic()
    _auth_prune(now)
    until = _AUTH_LOCKED_UNTIL.get(key, 0.0)
    if until <= now:
        _AUTH_LOCKED_UNTIL.pop(key, None)
        return 0
    return int(until - now) + 1


def _auth_register_failure(update: Update) -> None:
    key = _auth_actor_key(update)
    now = monotonic()
    _auth_prune(now)
    attempts = [ts for ts in _AUTH_FAILS.get(key, []) if (now - ts) <= AUTH_FAIL_WINDOW_SEC]
    attempts.append(now)
    _AUTH_FAILS[key] = attempts
    if len(attempts) >= AUTH_MAX_FAILS_IN_WINDOW:
        _AUTH_LOCKED_UNTIL[key] = now + AUTH_LOCKOUT_SEC
        _AUTH_FAILS[key] = []


def _auth_reset_limits(update: Update) -> None:
    _auth_prune()
    key = _auth_actor_key(update)
    _AUTH_FAILS.pop(key, None)
    _AUTH_LOCKED_UNTIL.pop(key, None)


async def auth_prune_task(context) -> None:
    _auth_prune()


async def _auth_delete_sensitive_message(update: Update) -> None:
    msg = update.effective_message
    if not msg:
        return
    try:
        await msg.delete()
    except Exception:
        pass


@require_private
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if is_authorized(update) and not is_enabled(update):
        await reply_disabled(update)
        return
    await show_main_menu(update)


@require_private
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if is_authorized(update) and not is_enabled(update):
        await reply_disabled(update)
        return

    lines = [
        "<b>Руководство по диагностике подключения</b>",
        "",
        "Если у вас возникли какие-либо проблемы с подключением к серверу просьба воспользоваться данным руководством перед тем как обращаться к администраторам.",
        "",
        "а. Прожмите кнопку обновления подписки",
        "б. Протестируйте соединение с каждым сервером",
        "в. Обновите приложение",
        "г. Попробуйте в настройках включить фрагментирование",
        "д. Сбросьте все данные приложения и добавьте подписку заново",
        "е. Если ничего не помогло создайте тикет в боте",
    ]
    q = update.callback_query
    msg = update.effective_message
    if q and msg:
        await q.answer()
        await q.edit_message_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=main_menu_inline_kb(update))
        return
    if msg:
        await msg.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=main_menu_inline_kb(update))


@require_private
async def cmd_auth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    left = _auth_lock_remaining_sec(update)
    if left > 0:
        if msg:
            await msg.reply_text(f"Слишком много попыток. Повторите через {left} сек.")
        return

    text = (msg.text if msg else "") or ""
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        if msg:
            await msg.reply_text("Формат: <b>/auth пароль</b>", parse_mode=ParseMode.HTML)
        return

    passwd = parts[1]
    if not passwd:
        if msg:
            await msg.reply_text("Пустой пароль.")
        return

    try:
        role: Optional[str] = None
        if ADMIN_PASSWORD and hmac.compare_digest(passwd, ADMIN_PASSWORD):
            role = "admin"
        elif AUTH_PASSWORD and hmac.compare_digest(passwd, AUTH_PASSWORD):
            role = "user"

        if role is None:
            _auth_register_failure(update)
            logger.warning("Auth failed for %s", _auth_actor_key(update))
            if msg:
                await msg.reply_text("Пароль неверный.")
            return

        u = update.effective_user
        if not u:
            if msg:
                await msg.reply_text("Ошибка: не удалось определить пользователя.")
            return

        existing = get_user_meta(u.id) or {}
        preserved_enabled = bool(existing.get("enabled", True))
        preserved_nick = existing.get("nickname")
        preserved_role = existing.get("role")
        preserved_paid = bool(existing.get("is_paid", False))

        role_to_set = preserved_role if (preserved_role and not preserved_enabled) else role

        meta = dict(existing)
        new_values = {
            "user_id": u.id,
            "role": role_to_set,
            "enabled": preserved_enabled,
            "nickname": preserved_nick,
            "username": u.username,
            "first_name": u.first_name,
            "last_name": u.last_name,
            "auth_at": datetime.now(TZ).isoformat(),
            "is_paid": preserved_paid,
        }
        relevant_keys = [k for k in new_values if k != "auth_at"]
        meta_changed = any(existing.get(k) != new_values[k] for k in relevant_keys) or not existing
        meta.update(new_values)
        if meta_changed:
            await upsert_user_meta(u.id, meta)
        _auth_reset_limits(update)

        if not preserved_enabled:
            await reply_disabled(update)
            return

        await show_main_menu(update, text="Авторизация успешна ✅\n\nМеню:")
    finally:
        await _auth_delete_sensitive_message(update)


@require_private
async def cmd_logout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if is_authorized(update) and not is_enabled(update):
        await reply_disabled(update)
        return

    uid = get_user_id(update)
    msg = update.effective_message
    if uid is None:
        return

    if get_user_meta_copy(uid) is not None:
        await remove_user_meta(uid)
        if msg:
            await msg.reply_text("Вы удалены из списка авторизованных.")
    else:
        if msg:
            await msg.reply_text("Вы не в списке авторизованных.")
