from datetime import datetime
from typing import Optional

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..config import ADMIN_PASSWORD, AUTH_PASSWORD, TZ
from ..storage import USER_DATA, update_user_data
from ..storage import _remove_user, _set_user_meta
from .common import (
    get_user_id,
    get_user_meta,
    is_admin,
    is_authorized,
    is_enabled,
    main_menu_kb,
    reply_disabled,
    require_private,
)


@require_private
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if is_authorized(update) and not is_enabled(update):
        await reply_disabled(update)
        return
    msg = update.effective_message
    if msg:
        await msg.reply_text("Меню:", reply_markup=main_menu_kb(update))


@require_private
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if is_authorized(update) and not is_enabled(update):
        await reply_disabled(update)
        return

    lines = [
        "<b>Команды</b>",
        "• /auth &lt;пароль&gt; — авторизация (роль определяется паролем)",
        "• /logout — выйти",
        "• /health — статус сервера",
        "• /ticket — создать тикет",
    ]
    if is_admin(update):
        lines += ["", "<b>Админ</b>", "• кнопка «Пользователи» — сообщения/никнеймы"]
    msg = update.effective_message
    if msg:
        await msg.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


@require_private
async def cmd_auth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    text = (msg.text if msg else "") or ""
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        if msg:
            await msg.reply_text("Формат: <b>/auth пароль</b>", parse_mode=ParseMode.HTML)
        return

    passwd = parts[1].strip()
    if not passwd:
        if msg:
            await msg.reply_text("Пустой пароль.")
        return

    role: Optional[str] = None
    if ADMIN_PASSWORD and passwd == ADMIN_PASSWORD:
        role = "admin"
    elif AUTH_PASSWORD and passwd == AUTH_PASSWORD:
        role = "user"

    if role is None:
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

    meta = {
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
    await update_user_data(lambda cfg: _set_user_meta(cfg, u.id, meta))

    if not preserved_enabled:
        await reply_disabled(update)
        return

    if msg:
        await msg.reply_text("Авторизация успешна ✅", reply_markup=main_menu_kb(update))


@require_private
async def cmd_logout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if is_authorized(update) and not is_enabled(update):
        await reply_disabled(update)
        return

    uid = get_user_id(update)
    msg = update.effective_message
    if uid is None:
        return

    if str(uid) in USER_DATA.authorized_users:
        await update_user_data(lambda cfg: _remove_user(cfg, uid))
        if msg:
            await msg.reply_text("Вы удалены из списка авторизованных.")
    else:
        if msg:
            await msg.reply_text("Вы не в списке авторизованных.")
