import re

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ..config import logger
from ..storage import upsert_user_meta
from .common import (
    authorized_ids,
    breadcrumbs,
    clip_text,
    get_user_id,
    get_user_meta,
    html_escape,
    require_admin,
    send_to_many,
    show_main_menu,
    ui_error_text,
    ui_ok_text,
    ui_warn_text,
    wrap_as_codeblock_html,
)
from .users_constants import (
    ADMIN_ALL_MENU,
    ADMIN_ALL_MSG_TEXT,
    ADMIN_ALL_MSG_CONFIRM,
    ADMIN_PICK,
    ADMIN_USER_CFG_TEXT,
    ADMIN_USER_MENU,
    ADMIN_USER_MSG_TEXT,
    ADMIN_USER_NICK_TEXT,
    MAX_USER_NICK_LEN,
)
from .users_ui import (
    USER_FILTERS,
    USER_FILTER_ALL,
    confirm_paid_kb,
    confirm_toggle_kb,
    format_user_card,
    user_card_kb,
    users_all_confirm_kb,
    users_all_kb,
    users_list_kb,
    users_list_title,
)


def _get_users_filter(context: ContextTypes.DEFAULT_TYPE) -> str:
    cur = str(context.user_data.get("users_filter", USER_FILTER_ALL))
    return cur if cur in USER_FILTERS else USER_FILTER_ALL


def _set_users_filter(context: ContextTypes.DEFAULT_TYPE, value: str) -> str:
    v = value if value in USER_FILTERS else USER_FILTER_ALL
    context.user_data["users_filter"] = v
    return v


@require_admin
async def users_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    msg = update.effective_message
    active_filter = _get_users_filter(context)
    title = users_list_title(active_filter)
    if q and msg:
        await q.answer()
        await q.edit_message_text(title, parse_mode=ParseMode.HTML, reply_markup=users_list_kb(active_filter))
    elif msg:
        await msg.reply_text(title, parse_mode=ParseMode.HTML, reply_markup=users_list_kb(active_filter))
    return ADMIN_PICK


@require_admin
async def users_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    data = q.data or ""

    if data == "users:main":
        await show_main_menu(update)
        return ConversationHandler.END

    if data == "users:all":
        await q.edit_message_text(
            f"<b>{html_escape(breadcrumbs('Админ-панель', 'Пользователи', 'Рассылка'))}</b>\n\nВыберите действие:",
            parse_mode=ParseMode.HTML,
            reply_markup=users_all_kb(),
        )
        return ADMIN_ALL_MENU

    m_filter = re.fullmatch(r"users:filter:(all|active|disabled|unpaid|admins)", data)
    if m_filter:
        active_filter = _set_users_filter(context, m_filter.group(1))
        await q.edit_message_text(
            users_list_title(active_filter),
            parse_mode=ParseMode.HTML,
            reply_markup=users_list_kb(active_filter),
        )
        return ADMIN_PICK

    if data == "users:noop":
        active_filter = _get_users_filter(context)
        await q.edit_message_text(
            users_list_title(active_filter),
            parse_mode=ParseMode.HTML,
            reply_markup=users_list_kb(active_filter),
        )
        return ADMIN_PICK

    m = re.fullmatch(r"users:user:(\d+)", data)
    if m:
        uid = int(m.group(1))
        meta = get_user_meta(uid)
        if not meta:
            active_filter = _get_users_filter(context)
            await q.edit_message_text(
                ui_error_text("пользователь не найден (возможно, удалён из списка)."),
                reply_markup=users_list_kb(active_filter),
            )
            return ADMIN_PICK
        context.user_data["selected_uid"] = uid
        await q.edit_message_text(format_user_card(meta), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
        return ADMIN_USER_MENU

    if data == "users:back":
        active_filter = _get_users_filter(context)
        await q.edit_message_text(users_list_title(active_filter), parse_mode=ParseMode.HTML, reply_markup=users_list_kb(active_filter))
        return ADMIN_PICK

    active_filter = _get_users_filter(context)
    await q.edit_message_text(users_list_title(active_filter), parse_mode=ParseMode.HTML, reply_markup=users_list_kb(active_filter))
    return ADMIN_PICK


@require_admin
async def users_all_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()

    if q.data == "users:back":
        active_filter = _get_users_filter(context)
        await q.edit_message_text(users_list_title(active_filter), parse_mode=ParseMode.HTML, reply_markup=users_list_kb(active_filter))
        return ADMIN_PICK
    if q.data == "users:allmsg":
        await q.edit_message_text(
            f"<b>{html_escape(breadcrumbs('Админ-панель', 'Пользователи', 'Рассылка', 'Текст'))}</b>\n\n"
            "Введите текст сообщения всем пользователям:"
        )
        return ADMIN_ALL_MSG_TEXT

    await q.edit_message_text(
        f"<b>{html_escape(breadcrumbs('Админ-панель', 'Пользователи', 'Рассылка'))}</b>\n\nВыберите действие:",
        parse_mode=ParseMode.HTML,
        reply_markup=users_all_kb(),
    )
    return ADMIN_ALL_MENU


@require_admin
async def users_all_msg_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    text = ((msg.text if msg else "") or "").strip()
    if not text:
        if msg:
            await msg.reply_text(ui_error_text("пустой текст. Введите сообщение:"))
        return ADMIN_ALL_MSG_TEXT
    context.user_data["users_all_broadcast_text"] = text
    preview = (
        f"<b>{html_escape(breadcrumbs('Админ-панель', 'Пользователи', 'Рассылка', 'Проверка'))}</b>\n\n"
        "Сообщение будет отправлено всем авторизованным пользователям (кроме вас).\n\n"
        + wrap_as_codeblock_html(clip_text(text, limit=3000))
        + "\n\nПодтвердите действие:"
    )
    if msg:
        await msg.reply_text(preview, parse_mode=ParseMode.HTML, reply_markup=users_all_confirm_kb())
    return ADMIN_ALL_MSG_CONFIRM


@require_admin
async def users_all_msg_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    data = q.data or ""
    if data in ("users:back", "users:all"):
        await q.edit_message_text(
            f"<b>{html_escape(breadcrumbs('Админ-панель', 'Пользователи', 'Рассылка'))}</b>\n\nВыберите действие:",
            parse_mode=ParseMode.HTML,
            reply_markup=users_all_kb(),
        )
        return ADMIN_ALL_MENU
    if data != "users:allsend":
        return ADMIN_ALL_MSG_CONFIRM

    text = str(context.user_data.get("users_all_broadcast_text", "")).strip()
    if not text:
        await q.edit_message_text(ui_error_text("текст рассылки потерян. Повторите позже."))
        return ADMIN_PICK

    sender = get_user_id(update)
    recipients = authorized_ids(role_filter=None, exclude={sender} if sender else set())
    if not recipients:
        await q.edit_message_text(ui_warn_text("нет получателей для рассылки."))
        return ADMIN_PICK

    payload = f"📩 <b>Сообщение администратора</b>\n\n{html_escape(clip_text(text, limit=3000))}"
    ok, fail = await send_to_many(context, recipients, payload)
    logger.info("Admin user_id=%s broadcast message ok=%s fail=%s recipients=%s", sender, ok, fail, len(recipients))
    context.user_data.pop("users_all_broadcast_text", None)
    await q.edit_message_text(ui_ok_text(f"Рассылка завершена (ok={ok}, fail={fail})"))
    return ADMIN_PICK


@require_admin
async def users_user_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    data = q.data or ""

    if data == "users:back":
        active_filter = _get_users_filter(context)
        await q.edit_message_text(users_list_title(active_filter), parse_mode=ParseMode.HTML, reply_markup=users_list_kb(active_filter))
        return ADMIN_PICK

    m_toggle = re.fullmatch(r"users:toggle:(\d+)", data)
    if m_toggle:
        uid = int(m_toggle.group(1))
        meta = get_user_meta(uid)
        if not meta:
            active_filter = _get_users_filter(context)
            await q.edit_message_text(ui_error_text("пользователь не найден."), reply_markup=users_list_kb(active_filter))
            return ADMIN_PICK

        if meta.get("role") == "admin":
            await q.edit_message_text(
                format_user_card(meta) + "\n\n" + ui_warn_text("администраторов банить нельзя."),
                parse_mode=ParseMode.HTML,
                reply_markup=user_card_kb(uid),
            )
            return ADMIN_USER_MENU
        action = "забанить" if bool(meta.get("enabled", True)) else "разбанить"
        await q.edit_message_text(
            format_user_card(meta) + f"\n\n{ui_warn_text(f'Подтвердите действие: {action}.')}",
            parse_mode=ParseMode.HTML,
            reply_markup=confirm_toggle_kb(uid, enabled_now=bool(meta.get("enabled", True))),
        )
        return ADMIN_USER_MENU

    m_toggle_apply = re.fullmatch(r"users:toggleapply:(\d+)", data)
    if m_toggle_apply:
        uid = int(m_toggle_apply.group(1))
        meta = get_user_meta(uid)
        if not meta:
            active_filter = _get_users_filter(context)
            await q.edit_message_text(ui_error_text("пользователь не найден."), reply_markup=users_list_kb(active_filter))
            return ADMIN_PICK
        if meta.get("role") == "admin":
            await q.edit_message_text(
                format_user_card(meta) + "\n\n" + ui_warn_text("администраторов банить нельзя."),
                parse_mode=ParseMode.HTML,
                reply_markup=user_card_kb(uid),
            )
            return ADMIN_USER_MENU
        meta["enabled"] = not bool(meta.get("enabled", True))
        updated = await upsert_user_meta(uid, meta)
        logger.info("Admin user_id=%s toggled enabled=%s target_uid=%s", get_user_id(update), updated.get("enabled"), uid)
        await q.edit_message_text(
            format_user_card(updated) + "\n\n" + ui_ok_text("Статус пользователя обновлён."),
            parse_mode=ParseMode.HTML,
            reply_markup=user_card_kb(uid),
        )
        return ADMIN_USER_MENU

    m_paid = re.fullmatch(r"users:paid:(\d+)", data)
    if m_paid:
        uid = int(m_paid.group(1))
        meta = get_user_meta(uid)
        if not meta:
            active_filter = _get_users_filter(context)
            await q.edit_message_text(ui_error_text("пользователь не найден."), reply_markup=users_list_kb(active_filter))
            return ADMIN_PICK
        await q.edit_message_text(
            format_user_card(meta)
            + "\n\n"
            + ui_warn_text(
                "Подтвердите переключение оплаты "
                + ("(снять оплату)." if bool(meta.get("is_paid", False)) else "(отметить оплачено).")
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=confirm_paid_kb(uid, is_paid_now=bool(meta.get("is_paid", False))),
        )
        return ADMIN_USER_MENU

    m_paid_apply = re.fullmatch(r"users:paidapply:(\d+)", data)
    if m_paid_apply:
        uid = int(m_paid_apply.group(1))
        meta = get_user_meta(uid)
        if not meta:
            active_filter = _get_users_filter(context)
            await q.edit_message_text(ui_error_text("пользователь не найден."), reply_markup=users_list_kb(active_filter))
            return ADMIN_PICK
        meta["is_paid"] = not bool(meta.get("is_paid", False))
        updated = await upsert_user_meta(uid, meta)
        logger.info("Admin user_id=%s toggled is_paid=%s target_uid=%s", get_user_id(update), updated.get("is_paid"), uid)
        await q.edit_message_text(
            format_user_card(updated) + "\n\n" + ui_ok_text("Статус оплаты обновлён."),
            parse_mode=ParseMode.HTML,
            reply_markup=user_card_kb(uid),
        )
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
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("⬅️ Назад", callback_data=f"users:user:{uid}")],
                    [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
                ]
            ),
        )
        return ADMIN_USER_CFG_TEXT

    uid = context.user_data.get("selected_uid")
    meta = get_user_meta(uid) if isinstance(uid, int) else None
    if meta:
        await q.edit_message_text(format_user_card(meta), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
        return ADMIN_USER_MENU

    active_filter = _get_users_filter(context)
    await q.edit_message_text(users_list_title(active_filter), parse_mode=ParseMode.HTML, reply_markup=users_list_kb(active_filter))
    return ADMIN_PICK


@require_admin
async def users_user_msg_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data.get("selected_uid")
    msg = update.effective_message
    if not isinstance(uid, int):
        if msg:
            active_filter = _get_users_filter(context)
            await msg.reply_text(ui_error_text("пользователь не выбран."))
            await msg.reply_text(users_list_title(active_filter), parse_mode=ParseMode.HTML, reply_markup=users_list_kb(active_filter))
        return ADMIN_PICK

    meta = get_user_meta(uid)
    if not meta:
        if msg:
            active_filter = _get_users_filter(context)
            await msg.reply_text(ui_error_text("пользователь не найден."))
            await msg.reply_text(users_list_title(active_filter), parse_mode=ParseMode.HTML, reply_markup=users_list_kb(active_filter))
        return ADMIN_PICK

    text = ((msg.text if msg else "") or "").strip()
    if not text:
        if msg:
            await msg.reply_text(ui_error_text("пустой текст. Введите сообщение:"))
        return ADMIN_USER_MSG_TEXT

    payload = f"📩 <b>Сообщение от администратора</b>\n\n{html_escape(clip_text(text, limit=3000))}"
    try:
        await context.bot.send_message(chat_id=uid, text=payload, parse_mode=ParseMode.HTML)
        logger.info("Admin user_id=%s sent direct message target_uid=%s", get_user_id(update), uid)
        if msg:
            await msg.reply_text(ui_ok_text("Отправлено"))
    except Exception as e:
        logger.warning("Не удалось отправить пользователю %s: %s", uid, e)
        if msg:
            await msg.reply_text(ui_error_text("не удалось отправить (пользователь мог заблокировать бота)."))

    if msg:
        await msg.reply_text(format_user_card(meta), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
    return ADMIN_USER_MENU


@require_admin
async def users_user_nick_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data.get("selected_uid")
    msg = update.effective_message
    if not isinstance(uid, int):
        if msg:
            active_filter = _get_users_filter(context)
            await msg.reply_text(ui_error_text("пользователь не выбран."))
            await msg.reply_text(users_list_title(active_filter), parse_mode=ParseMode.HTML, reply_markup=users_list_kb(active_filter))
        return ADMIN_PICK

    meta = get_user_meta(uid)
    if not meta:
        if msg:
            active_filter = _get_users_filter(context)
            await msg.reply_text(ui_error_text("пользователь не найден."))
            await msg.reply_text(users_list_title(active_filter), parse_mode=ParseMode.HTML, reply_markup=users_list_kb(active_filter))
        return ADMIN_PICK

    nick = ((msg.text if msg else "") or "").strip()
    if len(nick) < 2:
        if msg:
            await msg.reply_text(ui_error_text("ник слишком короткий. Введите минимум 2 символа:"))
        return ADMIN_USER_NICK_TEXT
    if len(nick) > MAX_USER_NICK_LEN:
        if msg:
            await msg.reply_text(ui_error_text(f"ник слишком длинный. Максимум {MAX_USER_NICK_LEN} символов:"))
        return ADMIN_USER_NICK_TEXT

    meta["nickname"] = nick
    await upsert_user_meta(uid, meta)
    logger.info("Admin user_id=%s updated nickname target_uid=%s", get_user_id(update), uid)

    if msg:
        await msg.reply_text(ui_ok_text("Никнейм сохранён"))
        await msg.reply_text(format_user_card(meta), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
    return ADMIN_USER_MENU


@require_admin
async def users_user_cfg_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data.get("selected_uid")
    msg = update.effective_message
    if not isinstance(uid, int):
        if msg:
            await msg.reply_text(ui_error_text("пользователь не выбран."))
        return ADMIN_PICK

    cfg = (msg.text if msg else "") or ""
    if not cfg.strip():
        if msg:
            await msg.reply_text(ui_error_text("пустая конфигурация. Вставьте текст одним сообщением."))
        return ADMIN_USER_CFG_TEXT

    meta = get_user_meta(uid)
    if not meta:
        if msg:
            await msg.reply_text(ui_error_text("пользователь не найден (возможно, удалён из списка)."))
        return ADMIN_PICK

    header = "📦 <b>Конфигурация от администратора</b>\n\n"
    payload = header + wrap_as_codeblock_html(clip_text(cfg, limit=3000))

    try:
        await context.bot.send_message(chat_id=uid, text=payload, parse_mode=ParseMode.HTML)
        logger.info("Admin user_id=%s sent config target_uid=%s", get_user_id(update), uid)
        if msg:
            await msg.reply_text(ui_ok_text("Отправлено"))
    except Exception as e:
        logger.warning("Не удалось отправить конфигурацию пользователю %s: %s", uid, e)
        if msg:
            await msg.reply_text(ui_error_text("не удалось отправить (пользователь мог заблокировать бота)."))

    if msg:
        await msg.reply_text(format_user_card(meta), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
    return ADMIN_USER_MENU
