import re
from datetime import datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import ContextTypes, ConversationHandler

from ..config import TZ, logger
from ..storage import mutate_user_meta
from .common import (
    authorized_ids,
    breadcrumbs,
    clip_html,
    clip_text,
    display_name,
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
from .subscription import (
    SUBSCRIPTION_TEXT_KEY,
    SUBSCRIPTION_UPDATED_AT_KEY,
    SUBSCRIPTION_UPDATED_BY_ID_KEY,
    SUBSCRIPTION_UPDATED_BY_NAME_KEY,
    send_subscription_payload,
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


def _subscription_mode_prompt(mode: str) -> str:
    if mode == "assign":
        return (
            "Вставьте подписку одним сообщением. Она будет только сохранена за пользователем в "
            "<code>data/user_data.json</code> без отправки уведомления."
            "\n\nПодсказка: можно вставлять vless/URL/JSON без изменений."
        )
    return (
        "Вставьте подписку одним сообщением. Она будет сохранена за пользователем в "
        "<code>data/user_data.json</code> и сразу отправлена ему уведомлением."
        "\n\nПодсказка: можно вставлять vless/URL/JSON без изменений."
    )


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

    m_page = re.fullmatch(r"users:page:(\d+)", data)
    if m_page:
        active_filter = _get_users_filter(context)
        try:
            await q.edit_message_text(
                users_list_title(active_filter),
                parse_mode=ParseMode.HTML,
                reply_markup=users_list_kb(active_filter, page=int(m_page.group(1))),
            )
        except BadRequest as e:
            # Нажатие на номер текущей страницы — сообщение не меняется.
            if "message is not modified" not in str(e).lower():
                raise
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
            "Введите текст сообщения всем пользователям:",
            parse_mode=ParseMode.HTML,
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

    payload = f"📩 <b>Сообщение администратора</b>\n\n{clip_html(text, limit=3000)}"
    ok, fail = await send_to_many(context, recipients, payload)
    logger.info("Admin user_id=%s broadcast message ok=%s fail=%s recipients=%s", sender, ok, fail, len(recipients))
    context.user_data.pop("users_all_broadcast_text", None)
    await q.edit_message_text(
        ui_ok_text(f"Рассылка завершена (ok={ok}, fail={fail})"),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]]),
    )
    return ADMIN_PICK


_USER_ACTION_RE = re.compile(
    r"^users:(?P<action>toggle|toggleapply|paid|paidapply|msg|nick|subassign|subsend):(?P<uid>\d+)$"
)


async def _back_to_user_list(q, context):
    active_filter = _get_users_filter(context)
    await q.edit_message_text(
        users_list_title(active_filter),
        parse_mode=ParseMode.HTML,
        reply_markup=users_list_kb(active_filter),
    )
    return ADMIN_PICK


async def _resolve_user_or_redirect(q, context, uid: int):
    meta = get_user_meta(uid)
    if meta:
        return meta
    active_filter = _get_users_filter(context)
    await q.edit_message_text(
        ui_error_text("пользователь не найден."),
        reply_markup=users_list_kb(active_filter),
    )
    return None


async def _action_toggle(q, context, uid: int, meta):
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


async def _action_toggle_apply(update: Update, q, context, uid: int, meta):
    if meta.get("role") == "admin":
        await q.edit_message_text(
            format_user_card(meta) + "\n\n" + ui_warn_text("администраторов банить нельзя."),
            parse_mode=ParseMode.HTML,
            reply_markup=user_card_kb(uid),
        )
        return ADMIN_USER_MENU
    updated = await mutate_user_meta(uid, lambda m: {**m, "enabled": not bool(m.get("enabled", True))})
    if updated is None:
        return await _back_to_user_list(q, context)
    logger.info(
        "Admin user_id=%s toggled enabled=%s target_uid=%s",
        get_user_id(update),
        updated.get("enabled"),
        uid,
    )
    await q.edit_message_text(
        format_user_card(updated) + "\n\n" + ui_ok_text("Статус пользователя обновлён."),
        parse_mode=ParseMode.HTML,
        reply_markup=user_card_kb(uid),
    )
    return ADMIN_USER_MENU


async def _action_paid(q, context, uid: int, meta):
    suffix = "(снять оплату)." if bool(meta.get("is_paid", False)) else "(отметить оплачено)."
    await q.edit_message_text(
        format_user_card(meta)
        + "\n\n"
        + ui_warn_text("Подтвердите переключение оплаты " + suffix),
        parse_mode=ParseMode.HTML,
        reply_markup=confirm_paid_kb(uid, is_paid_now=bool(meta.get("is_paid", False))),
    )
    return ADMIN_USER_MENU


async def _action_paid_apply(update: Update, q, context, uid: int, meta):
    updated = await mutate_user_meta(uid, lambda m: {**m, "is_paid": not bool(m.get("is_paid", False))})
    if updated is None:
        return await _back_to_user_list(q, context)
    logger.info(
        "Admin user_id=%s toggled is_paid=%s target_uid=%s",
        get_user_id(update),
        updated.get("is_paid"),
        uid,
    )
    await q.edit_message_text(
        format_user_card(updated) + "\n\n" + ui_ok_text("Статус оплаты обновлён."),
        parse_mode=ParseMode.HTML,
        reply_markup=user_card_kb(uid),
    )
    return ADMIN_USER_MENU


async def _action_msg(q, context, uid: int):
    context.user_data["selected_uid"] = uid
    await q.edit_message_text("Введите текст личного сообщения пользователю:")
    return ADMIN_USER_MSG_TEXT


async def _action_nick(q, context, uid: int):
    context.user_data["selected_uid"] = uid
    await q.edit_message_text("Введите никнейм (как должен отображаться в списке):")
    return ADMIN_USER_NICK_TEXT


async def _action_subscription(q, context, uid: int, mode: str):
    context.user_data["selected_uid"] = uid
    context.user_data["subscription_delivery_mode"] = mode
    await q.edit_message_text(
        _subscription_mode_prompt(mode),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⬅️ Назад", callback_data=f"users:user:{uid}")],
                [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
            ]
        ),
    )
    return ADMIN_USER_CFG_TEXT


@require_admin
async def users_user_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    data = q.data or ""

    if data == "users:back":
        return await _back_to_user_list(q, context)

    m = _USER_ACTION_RE.fullmatch(data)
    if m:
        action = m.group("action")
        uid = int(m.group("uid"))

        if action == "msg":
            return await _action_msg(q, context, uid)
        if action == "nick":
            return await _action_nick(q, context, uid)
        if action == "subassign":
            return await _action_subscription(q, context, uid, "assign")
        if action == "subsend":
            return await _action_subscription(q, context, uid, "send")

        meta = await _resolve_user_or_redirect(q, context, uid)
        if meta is None:
            return ADMIN_PICK

        if action == "toggle":
            return await _action_toggle(q, context, uid, meta)
        if action == "toggleapply":
            return await _action_toggle_apply(update, q, context, uid, meta)
        if action == "paid":
            return await _action_paid(q, context, uid, meta)
        if action == "paidapply":
            return await _action_paid_apply(update, q, context, uid, meta)

    selected = context.user_data.get("selected_uid")
    meta = get_user_meta(selected) if isinstance(selected, int) else None
    if meta:
        await q.edit_message_text(format_user_card(meta), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(selected))
        return ADMIN_USER_MENU

    return await _back_to_user_list(q, context)


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

    payload = f"📩 <b>Сообщение от администратора</b>\n\n{clip_html(text, limit=3000)}"
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

    updated = await mutate_user_meta(uid, lambda m: {**m, "nickname": nick})
    if updated is None:
        if msg:
            await msg.reply_text(ui_error_text("пользователь не найден."))
        return ADMIN_PICK
    logger.info("Admin user_id=%s updated nickname target_uid=%s", get_user_id(update), uid)

    if msg:
        await msg.reply_text(ui_ok_text("Никнейм сохранён"))
        await msg.reply_text(format_user_card(updated), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
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

    delivery_mode = str(context.user_data.get("subscription_delivery_mode", "send"))
    author_id = get_user_id(update)
    author_name = display_name(update)

    def _set_subscription(m: dict) -> dict:
        m = dict(m)
        m[SUBSCRIPTION_TEXT_KEY] = cfg
        m[SUBSCRIPTION_UPDATED_AT_KEY] = datetime.now(TZ).isoformat()
        m[SUBSCRIPTION_UPDATED_BY_ID_KEY] = author_id
        m[SUBSCRIPTION_UPDATED_BY_NAME_KEY] = author_name
        return m

    updated = await mutate_user_meta(uid, _set_subscription)
    if updated is None:
        if msg:
            await msg.reply_text(ui_error_text("пользователь не найден (возможно, удалён из списка)."))
        return ADMIN_PICK

    if delivery_mode == "assign":
        if msg:
            await msg.reply_text(ui_ok_text("Подписка сохранена в базе без отправки пользователю"))
        logger.info("Admin user_id=%s assigned subscription target_uid=%s mode=assign", author_id, uid)
    else:
        try:
            await send_subscription_payload(
                context,
                chat_id=uid,
                meta=updated,
                title="📦 <b>Подписка от администратора</b>",
                filename_prefix=f"subscription_{uid}",
            )
            logger.info("Admin user_id=%s assigned subscription target_uid=%s mode=send", author_id, uid)
            if msg:
                await msg.reply_text(ui_ok_text("Подписка сохранена и отправлена пользователю"))
        except Exception as e:
            logger.warning("Подписка сохранена, но не отправлена пользователю %s: %s", uid, e)
            if msg:
                await msg.reply_text(ui_warn_text("Подписка сохранена в базе, но отправить пользователю её не удалось."))

    context.user_data.pop("subscription_delivery_mode", None)
    if msg:
        await msg.reply_text(format_user_card(updated), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
    return ADMIN_USER_MENU
