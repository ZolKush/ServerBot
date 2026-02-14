import re
from typing import Any, Dict, List, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ..config import logger
from ..storage import USER_DATA, update_user_data, _set_user_meta
from .common import (
    authorized_ids,
    display_name_from_meta,
    get_user_id,
    get_user_meta,
    html_escape,
    main_menu_kb,
    require_admin,
    send_to_many,
    wrap_as_codeblock_html,
)

(
    ADMIN_PICK,
    ADMIN_ALL_MENU,
    ADMIN_ALL_MSG_TEXT,
    ADMIN_USER_MENU,
    ADMIN_USER_MSG_TEXT,
    ADMIN_USER_NICK_TEXT,
    ADMIN_USER_CFG_TEXT,
) = range(7)


def users_list_kb() -> InlineKeyboardMarkup:
    buttons: List[List[InlineKeyboardButton]] = []
    buttons.append([InlineKeyboardButton("📣 Все пользователи", callback_data="users:all")])

    items: List[Tuple[str, bool, bool, str, int, str]] = []
    for k, meta in USER_DATA.authorized_users.items():
        try:
            uid = int(meta.get("user_id", k))
        except Exception:
            continue
        name = display_name_from_meta(meta)
        role = meta.get("role", "user")
        enabled = bool(meta.get("enabled", True))
        is_paid = bool(meta.get("is_paid", False))
        items.append((role, enabled, is_paid, name.lower(), uid, name))

    items.sort(key=lambda x: (0 if x[0] == "user" else 1, x[3], x[4]))

    row: List[InlineKeyboardButton] = []
    for role, enabled, is_paid, _, uid, name in items:
        prefix = ""
        if not enabled:
            prefix += "⛔ "
        if role == "admin":
            prefix += "👑⭐ " if is_paid else "👑 "
        else:
            if is_paid:
                prefix += "⭐ "
        label = f"{prefix}{name}"
        row.append(InlineKeyboardButton(label[:50], callback_data=f"users:user:{uid}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("⬅️ В меню", callback_data="users:main")])
    return InlineKeyboardMarkup(buttons)


def users_all_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✉️ Сообщение всем", callback_data="users:allmsg")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="users:back")],
        ]
    )


def user_card_kb(uid: int) -> InlineKeyboardMarkup:
    meta = get_user_meta(uid) or {}
    enabled = bool(meta.get("enabled", True))
    role = meta.get("role", "user")

    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton("✉️ Написать сообщение", callback_data=f"users:msg:{uid}")],
        [InlineKeyboardButton("🏷 Добавить/изменить ник", callback_data=f"users:nick:{uid}")],
        [InlineKeyboardButton("⭐ Переключить оплату", callback_data=f"users:paid:{uid}")],
        [InlineKeyboardButton("📦 Отправить конфигурацию", callback_data=f"users:cfg:{uid}")],
    ]

    if role != "admin":
        rows.append([InlineKeyboardButton("🚫 Забанить" if enabled else "✅ Разбанить", callback_data=f"users:toggle:{uid}")])

    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="users:back")])
    return InlineKeyboardMarkup(rows)


def format_user_card(meta: Dict[str, Any]) -> str:
    uid = meta.get("user_id", "-")
    role = meta.get("role", "user")
    nick = meta.get("nickname") or "-"
    uname = meta.get("username")
    nm = " ".join([x for x in [meta.get("first_name"), meta.get("last_name")] if x]) or "-"
    auth_at = meta.get("auth_at") or "-"
    status = "enabled" if meta.get("enabled", True) else "disabled"
    return (
        "<b>Пользователь</b>\n"
        f"• ID: <code>{html_escape(str(uid))}</code>\n"
        f"• Роль: <b>{html_escape(str(role))}</b>\n"
        f"• Статус: <b>{html_escape(status)}</b>\n"
        f"• Подписка: <b>{'оплачена' if bool(meta.get('is_paid', False)) else 'не оплачена'}</b>\n"
        f"• Ник: <b>{html_escape(str(nick))}</b>\n"
        f"• Username: <b>{html_escape(('@' + uname) if uname else '-')}</b>\n"
        f"• Имя: <b>{html_escape(str(nm))}</b>\n"
        f"• Авторизация: <code>{html_escape(str(auth_at))}</code>"
    )


@require_admin
async def users_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg:
        await msg.reply_text("Выберите пользователя:", reply_markup=users_list_kb())
    return ADMIN_PICK


@require_admin
async def users_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    data = q.data or ""

    if data == "users:main":
        try:
            await q.edit_message_text("Меню:", reply_markup=None)
        except Exception:
            pass
        chat_id = q.message.chat.id if q.message and q.message.chat else None
        if chat_id is not None:
            await context.bot.send_message(chat_id=chat_id, text="Меню:", reply_markup=main_menu_kb(update))
        return ConversationHandler.END

    if data == "users:all":
        await q.edit_message_text("Все пользователи:", reply_markup=users_all_kb())
        return ADMIN_ALL_MENU

    m = re.fullmatch(r"users:user:(\d+)", data)
    if m:
        uid = int(m.group(1))
        meta = get_user_meta(uid)
        if not meta:
            await q.edit_message_text("Пользователь не найден (возможно, удалён из списка).", reply_markup=users_list_kb())
            return ADMIN_PICK
        context.user_data["selected_uid"] = uid
        await q.edit_message_text(format_user_card(meta), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
        return ADMIN_USER_MENU

    if data == "users:back":
        await q.edit_message_text("Выберите пользователя:", reply_markup=users_list_kb())
        return ADMIN_PICK

    await q.edit_message_text("Выберите пользователя:", reply_markup=users_list_kb())
    return ADMIN_PICK


@require_admin
async def users_all_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()

    if q.data == "users:back":
        await q.edit_message_text("Выберите пользователя:", reply_markup=users_list_kb())
        return ADMIN_PICK
    if q.data == "users:allmsg":
        await q.edit_message_text("Введите текст сообщения всем пользователям:")
        return ADMIN_ALL_MSG_TEXT

    await q.edit_message_text("Все пользователи:", reply_markup=users_all_kb())
    return ADMIN_ALL_MENU


@require_admin
async def users_all_msg_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    text = ((msg.text if msg else "") or "").strip()
    if not text:
        if msg:
            await msg.reply_text("Пустой текст. Введите сообщение:")
        return ADMIN_ALL_MSG_TEXT

    sender = get_user_id(update)
    recipients = authorized_ids(role_filter=None, exclude={sender} if sender else set())
    if not recipients:
        if msg:
            await msg.reply_text("Нет получателей.")
        return ADMIN_PICK

    payload = f"📩 <b>Сообщение администратора</b>\n\n{html_escape(text)}"
    ok, fail = await send_to_many(context, recipients, payload)
    if msg:
        await msg.reply_text(f"Отправлено всем: ✅ {ok}, ❌ {fail}")
        await msg.reply_text("Выберите пользователя:", reply_markup=users_list_kb())
    return ADMIN_PICK


@require_admin
async def users_user_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    data = q.data or ""

    if data == "users:back":
        await q.edit_message_text("Выберите пользователя:", reply_markup=users_list_kb())
        return ADMIN_PICK

    m_toggle = re.fullmatch(r"users:toggle:(\d+)", data)
    if m_toggle:
        uid = int(m_toggle.group(1))
        meta = get_user_meta(uid)
        if not meta:
            await q.edit_message_text("Пользователь не найден.", reply_markup=users_list_kb())
            return ADMIN_PICK

        if meta.get("role") == "admin":
            await q.edit_message_text(
                format_user_card(meta) + "\n\n<b>Администраторов банить нельзя.</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=user_card_kb(uid),
            )
            return ADMIN_USER_MENU

        meta["enabled"] = not bool(meta.get("enabled", True))
        updated = await update_user_data(lambda cfg: _set_user_meta(cfg, uid, meta))
        await q.edit_message_text(format_user_card(updated), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
        return ADMIN_USER_MENU

    m_paid = re.fullmatch(r"users:paid:(\d+)", data)
    if m_paid:
        uid = int(m_paid.group(1))
        meta = get_user_meta(uid)
        if not meta:
            await q.edit_message_text("Пользователь не найден.", reply_markup=users_list_kb())
            return ADMIN_PICK
        meta["is_paid"] = not bool(meta.get("is_paid", False))
        updated = await update_user_data(lambda cfg: _set_user_meta(cfg, uid, meta))
        await q.edit_message_text(format_user_card(updated), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
        return ADMIN_USER_MENU

    m_msg = re.fullmatch(r"users:msg:(\d+)", data)
    if m_msg:
        context.user_data["selected_uid"] = int(m_msg.group(1))
        await q.edit_message_text("Введите текст личного сообщения пользователю:")
        return ADMIN_USER_MSG_TEXT

    m_nick = re.fullmatch(r"users:nick:(\d+)", data)
    if m_nick:
        context.user_data["selected_uid"] = int(m_nick.group(1))
        await q.edit_message_text("Введите никнейм (как должен отображаться в списке):")
        return ADMIN_USER_NICK_TEXT

    m_cfg = re.fullmatch(r"users:cfg:(\d+)", data)
    if m_cfg:
        uid = int(m_cfg.group(1))
        context.user_data["selected_uid"] = uid
        await q.edit_message_text(
            "Вставьте конфигурацию одним сообщением. Она будет отправлена пользователю как <b>кодовый блок</b>."
            "\n\nПодсказка: можно вставлять vless/URL/JSON без изменений.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data=f"users:user:{uid}")]]),
        )
        return ADMIN_USER_CFG_TEXT

    uid = context.user_data.get("selected_uid")
    meta = get_user_meta(uid) if isinstance(uid, int) else None
    if meta:
        await q.edit_message_text(format_user_card(meta), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
        return ADMIN_USER_MENU

    await q.edit_message_text("Выберите пользователя:", reply_markup=users_list_kb())
    return ADMIN_PICK


@require_admin
async def users_user_msg_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data.get("selected_uid")
    msg = update.effective_message
    if not isinstance(uid, int):
        if msg:
            await msg.reply_text("Ошибка: пользователь не выбран.")
            await msg.reply_text("Выберите пользователя:", reply_markup=users_list_kb())
        return ADMIN_PICK

    meta = get_user_meta(uid)
    if not meta:
        if msg:
            await msg.reply_text("Пользователь не найден.")
            await msg.reply_text("Выберите пользователя:", reply_markup=users_list_kb())
        return ADMIN_PICK

    text = ((msg.text if msg else "") or "").strip()
    if not text:
        if msg:
            await msg.reply_text("Пустой текст. Введите сообщение:")
        return ADMIN_USER_MSG_TEXT

    payload = f"📩 <b>Сообщение от администратора</b>\n\n{html_escape(text)}"
    try:
        await context.bot.send_message(chat_id=uid, text=payload, parse_mode=ParseMode.HTML)
        if msg:
            await msg.reply_text("Отправлено ✅")
    except Exception as e:
        logger.warning("Не удалось отправить пользователю %s: %s", uid, e)
        if msg:
            await msg.reply_text("Не удалось отправить (пользователь мог заблокировать бота).")

    if msg:
        await msg.reply_text(format_user_card(meta), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
    return ADMIN_USER_MENU


@require_admin
async def users_user_nick_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data.get("selected_uid")
    msg = update.effective_message
    if not isinstance(uid, int):
        if msg:
            await msg.reply_text("Ошибка: пользователь не выбран.")
            await msg.reply_text("Выберите пользователя:", reply_markup=users_list_kb())
        return ADMIN_PICK

    meta = get_user_meta(uid)
    if not meta:
        if msg:
            await msg.reply_text("Пользователь не найден.")
            await msg.reply_text("Выберите пользователя:", reply_markup=users_list_kb())
        return ADMIN_PICK

    nick = ((msg.text if msg else "") or "").strip()
    if len(nick) < 2:
        if msg:
            await msg.reply_text("Ник слишком короткий. Введите минимум 2 символа:")
        return ADMIN_USER_NICK_TEXT

    meta["nickname"] = nick
    await update_user_data(lambda cfg: _set_user_meta(cfg, uid, meta))

    if msg:
        await msg.reply_text("Никнейм сохранён ✅")
        await msg.reply_text(format_user_card(meta), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
    return ADMIN_USER_MENU


@require_admin
async def users_user_cfg_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data.get("selected_uid")
    msg = update.effective_message
    if not isinstance(uid, int):
        if msg:
            await msg.reply_text("Пользователь не выбран.")
        return ADMIN_PICK

    cfg = (msg.text if msg else "") or ""
    if not cfg.strip():
        if msg:
            await msg.reply_text("Пустая конфигурация. Вставьте текст одним сообщением.")
        return ADMIN_USER_CFG_TEXT

    meta = get_user_meta(uid)
    if not meta:
        if msg:
            await msg.reply_text("Пользователь не найден (возможно, удалён из списка).")
        return ADMIN_PICK

    header = "📦 <b>Конфигурация от администратора</b>\n\n"
    payload = header + wrap_as_codeblock_html(cfg)

    try:
        await context.bot.send_message(chat_id=uid, text=payload, parse_mode=ParseMode.HTML)
        if msg:
            await msg.reply_text("Отправлено ✅")
    except Exception as e:
        logger.warning("Не удалось отправить конфигурацию пользователю %s: %s", uid, e)
        if msg:
            await msg.reply_text("Не удалось отправить (пользователь мог заблокировать бота).")

    if msg:
        await msg.reply_text(format_user_card(meta), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
    return ADMIN_USER_MENU
