import re
from datetime import datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import ContextTypes, ConversationHandler

from ..config import TZ, logger
from ..services.outbox import message_payload
from ..storage import (
    UserData,
    append_audit_entry,
    enqueue_user_outbox,
    make_outbox_event,
    suppress_user_outbox_recipient,
    update_user_data,
)
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
    show_main_menu,
    staff_title,
    ui_error_text,
    ui_ok_text,
    ui_warn_text,
    wrap_as_codeblock_html,
)
from .subscription import (
    CONNECTION_UPDATED_AT_KEY,
    CONNECTION_UPDATED_BY_ID_KEY,
    CONNECTION_UPDATED_BY_NAME_KEY,
    CONNECTION_URL_KEY,
    MAX_CONNECTION_BYTES,
    connection_outbox_payload,
    is_valid_connection_url,
)
from .users_constants import (
    ADMIN_ALL_MENU,
    ADMIN_ALL_MSG_CONFIRM,
    ADMIN_ALL_MSG_TEXT,
    ADMIN_PICK,
    ADMIN_USER_CFG_TEXT,
    ADMIN_USER_MENU,
    ADMIN_USER_MSG_TEXT,
    ADMIN_USER_NICK_TEXT,
    MAX_USER_NICK_LEN,
)
from .users_ui import (
    USER_FILTER_ALL,
    USER_FILTERS,
    confirm_access_kb,
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
            "Вставьте персональную ссылку подключения одним сообщением. Она будет только сохранена за пользователем в "
            "<code>data/user_data.json</code> без отправки уведомления."
            "\n\nСсылка должна начинаться с http:// или https://."
        )
    return (
        "Вставьте персональную ссылку подключения одним сообщением. Она будет сохранена за пользователем в "
        "<code>data/user_data.json</code> и сразу отправлена ему уведомлением."
        "\n\nСсылка должна начинаться с http:// или https://."
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

    m_filter = re.fullmatch(r"users:filter:(all|active|disabled|unpaid|admins|blocked)", data)
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
        await q.edit_message_text(
            users_list_title(active_filter), parse_mode=ParseMode.HTML, reply_markup=users_list_kb(active_filter)
        )
        return ADMIN_PICK

    active_filter = _get_users_filter(context)
    await q.edit_message_text(
        users_list_title(active_filter), parse_mode=ParseMode.HTML, reply_markup=users_list_kb(active_filter)
    )
    return ADMIN_PICK


@require_admin
async def users_all_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()

    if q.data == "users:back":
        active_filter = _get_users_filter(context)
        await q.edit_message_text(
            users_list_title(active_filter), parse_mode=ParseMode.HTML, reply_markup=users_list_kb(active_filter)
        )
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

    sender_title = staff_title(update)
    payload = (
        "📣 <b>Массовая рассылка</b>\n\n"
        f"Отправитель: <b>{html_escape(sender_title)}</b>\n\n"
        f"{clip_html(text, limit=3000)}"
    )
    event = make_outbox_event(
        kind="admin_broadcast",
        recipient_ids=recipients,
        payload=message_payload(payload),
    )

    def _queue_broadcast(cfg: UserData) -> None:
        enqueue_user_outbox(cfg, event)
        append_audit_entry(
            cfg,
            action="broadcast_queued",
            actor_meta=cfg.authorized_users.get(str(sender)),
            details={"recipient_count": len(recipients)},
        )

    await update_user_data(_queue_broadcast)
    logger.info("Admin user_id=%s queued broadcast recipients=%s", sender, len(recipients))
    context.user_data.pop("users_all_broadcast_text", None)
    await q.edit_message_text(
        ui_ok_text(f"Рассылка сохранена в очереди для {len(recipients)} получателей."),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]]),
    )
    return ADMIN_PICK


_USER_ACTION_RE = re.compile(r"^users:(?P<action>toggle|toggleapply|msg|nick|subassign|subsend):(?P<uid>\d+)$")
_USER_ACCESS_ACTION_RE = re.compile(r"^users:(?P<stage>access|accessapply):(?P<decision>approve|block):(?P<uid>\d+)$")


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


async def _action_access(q, uid: int, meta: dict, *, desired_state: str):
    if meta.get("role") == "admin":
        await q.edit_message_text(
            format_user_card(meta) + "\n\n" + ui_warn_text("доступ администраторов здесь изменять нельзя."),
            parse_mode=ParseMode.HTML,
            reply_markup=user_card_kb(uid),
        )
        return ADMIN_USER_MENU
    current_state = str(meta.get("access_state") or ("approved" if meta.get("enabled", True) else "blocked"))
    if desired_state == "blocked":
        action = "забанить"
    else:
        action = "разбанить" if current_state == "blocked" else "одобрить доступ"
    await q.edit_message_text(
        format_user_card(meta) + f"\n\n{ui_warn_text(f'Подтвердите действие: {action}.')}",
        parse_mode=ParseMode.HTML,
        reply_markup=confirm_access_kb(uid, desired_state=desired_state, current_state=current_state),
    )
    return ADMIN_USER_MENU


async def _action_toggle(q, context, uid: int, meta):
    if meta.get("role") == "admin":
        await q.edit_message_text(
            format_user_card(meta) + "\n\n" + ui_warn_text("администраторов банить нельзя."),
            parse_mode=ParseMode.HTML,
            reply_markup=user_card_kb(uid),
        )
        return ADMIN_USER_MENU
    state = str(meta.get("access_state") or ("approved" if meta.get("enabled", True) else "blocked"))
    action = "забанить" if state == "approved" else ("разбанить" if state == "blocked" else "одобрить доступ")
    await q.edit_message_text(
        format_user_card(meta) + f"\n\n{ui_warn_text(f'Подтвердите действие: {action}.')}",
        parse_mode=ParseMode.HTML,
        reply_markup=confirm_toggle_kb(uid, access_state=state),
    )
    return ADMIN_USER_MENU


async def _action_toggle_apply(
    update: Update,
    q,
    context,
    uid: int,
    meta,
    *,
    desired_state: str | None = None,
):
    actor_id = get_user_id(update)
    actor_name = display_name(update)
    now = datetime.now(TZ).isoformat()

    def _apply(cfg: UserData) -> tuple[str, dict | None]:
        current = cfg.authorized_users.get(str(uid))
        if not isinstance(current, dict):
            return "missing", None
        current = dict(current)
        # Role is checked under the same lock as the mutation, so a concurrent
        # promotion to admin cannot race with a stale confirmation screen.
        if current.get("role") == "admin":
            return "admin", current
        old_state = str(current.get("access_state") or ("approved" if current.get("enabled", True) else "blocked"))
        new_state = desired_state or ("blocked" if old_state == "approved" else "approved")
        if new_state not in {"approved", "blocked"}:
            return "invalid", current
        if old_state == new_state:
            return "already", current
        current.update(
            {
                "access_state": new_state,
                "enabled": new_state == "approved",
                "access_reviewed_at": now,
                "access_reviewed_by_id": actor_id,
                "access_reviewed_by_name": actor_name,
                "blocked_at": now if new_state == "blocked" else None,
                "blocked_by_id": actor_id if new_state == "blocked" else None,
                "blocked_by_name": actor_name if new_state == "blocked" else None,
                "blocked_reason": "manual_admin_action" if new_state == "blocked" else None,
            }
        )
        updated_meta = UserData._normalize_user(current)
        cfg.authorized_users[str(uid)] = updated_meta
        append_audit_entry(
            cfg,
            action="access_blocked" if new_state == "blocked" else "access_approved",
            actor_meta=cfg.authorized_users.get(str(actor_id)),
            target_user_id=uid,
            details={},
        )
        notification = (
            "🚫 Доступ к боту отключён\n\nВы отключены от данного бота по решению администрации."
            if new_state == "blocked"
            else "✅ Доступ к боту одобрен. Используйте /menu."
        )
        notification_event = make_outbox_event(
            kind=f"access_{new_state}",
            recipient_ids=[uid],
            payload=message_payload(notification, parse_mode=None),
            allow_blocked_delivery=new_state == "blocked",
        )
        enqueue_user_outbox(cfg, notification_event)
        if new_state == "blocked":
            suppress_user_outbox_recipient(cfg, uid, keep_event_id=str(notification_event["id"]))
        return "updated", updated_meta

    outcome, updated = await update_user_data(_apply)
    if outcome == "missing" or updated is None:
        return await _back_to_user_list(q, context)
    if outcome == "admin":
        await q.edit_message_text(
            format_user_card(updated) + "\n\n" + ui_warn_text("администраторов банить нельзя."),
            parse_mode=ParseMode.HTML,
            reply_markup=user_card_kb(uid),
        )
        return ADMIN_USER_MENU
    if outcome in {"already", "invalid"}:
        note = "Статус пользователя уже установлен." if outcome == "already" else "Некорректное действие."
        await q.edit_message_text(
            format_user_card(updated) + "\n\n" + ui_warn_text(note),
            parse_mode=ParseMode.HTML,
            reply_markup=user_card_kb(uid),
        )
        return ADMIN_USER_MENU
    logger.info(
        "Admin user_id=%s changed access_state=%s target_uid=%s",
        actor_id,
        updated.get("access_state"),
        uid,
        extra={"user_id": actor_id, "action": "access_toggle"},
    )
    await q.edit_message_text(
        format_user_card(updated) + "\n\n" + ui_ok_text("Статус пользователя обновлён."),
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

    access_match = _USER_ACCESS_ACTION_RE.fullmatch(data)
    if access_match:
        uid = int(access_match.group("uid"))
        meta = await _resolve_user_or_redirect(q, context, uid)
        if meta is None:
            return ADMIN_PICK
        desired_state = "approved" if access_match.group("decision") == "approve" else "blocked"
        if access_match.group("stage") == "access":
            return await _action_access(q, uid, meta, desired_state=desired_state)
        return await _action_toggle_apply(
            update,
            q,
            context,
            uid,
            meta,
            desired_state=desired_state,
        )

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
    selected = context.user_data.get("selected_uid")
    meta = get_user_meta(selected) if isinstance(selected, int) else None
    if meta:
        await q.edit_message_text(
            format_user_card(meta), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(selected)
        )
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
            await msg.reply_text(
                users_list_title(active_filter), parse_mode=ParseMode.HTML, reply_markup=users_list_kb(active_filter)
            )
        return ADMIN_PICK

    meta = get_user_meta(uid)
    if not meta:
        if msg:
            active_filter = _get_users_filter(context)
            await msg.reply_text(ui_error_text("пользователь не найден."))
            await msg.reply_text(
                users_list_title(active_filter), parse_mode=ParseMode.HTML, reply_markup=users_list_kb(active_filter)
            )
        return ADMIN_PICK

    text = ((msg.text if msg else "") or "").strip()
    if not text:
        if msg:
            await msg.reply_text(ui_error_text("пустой текст. Введите сообщение:"))
        return ADMIN_USER_MSG_TEXT

    sender_title = staff_title(update)
    payload = (
        "✉️ <b>Персональное сообщение</b>\n\n"
        f"Отправитель: <b>{html_escape(sender_title)}</b>\n\n"
        f"{clip_html(text, limit=3000)}"
    )
    event = make_outbox_event(
        kind="admin_direct_message",
        recipient_ids=[uid],
        payload=message_payload(payload),
    )

    def _queue_direct(cfg: UserData) -> None:
        enqueue_user_outbox(cfg, event)
        append_audit_entry(
            cfg,
            action="direct_message_queued",
            actor_meta=cfg.authorized_users.get(str(get_user_id(update))),
            target_user_id=uid,
            details={},
        )

    await update_user_data(_queue_direct)
    logger.info("Admin user_id=%s queued direct message target_uid=%s", get_user_id(update), uid)
    if msg:
        await msg.reply_text(ui_ok_text("Сообщение сохранено в очереди отправки"))

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
            await msg.reply_text(
                users_list_title(active_filter), parse_mode=ParseMode.HTML, reply_markup=users_list_kb(active_filter)
            )
        return ADMIN_PICK

    meta = get_user_meta(uid)
    if not meta:
        if msg:
            active_filter = _get_users_filter(context)
            await msg.reply_text(ui_error_text("пользователь не найден."))
            await msg.reply_text(
                users_list_title(active_filter), parse_mode=ParseMode.HTML, reply_markup=users_list_kb(active_filter)
            )
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

    actor_id = get_user_id(update)

    def _set_nickname(cfg: UserData) -> dict | None:
        current = cfg.authorized_users.get(str(uid))
        if not isinstance(current, dict):
            return None
        updated_meta = UserData._normalize_user({**current, "nickname": nick})
        cfg.authorized_users[str(uid)] = updated_meta
        append_audit_entry(
            cfg,
            action="nickname_changed",
            actor_meta=cfg.authorized_users.get(str(actor_id)),
            target_user_id=uid,
            details={},
        )
        return updated_meta

    updated = await update_user_data(_set_nickname)
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

    cfg = ((msg.text if msg else "") or "").strip()
    if not cfg.strip():
        if msg:
            await msg.reply_text(ui_error_text("пустая ссылка. Вставьте её одним сообщением."))
        return ADMIN_USER_CFG_TEXT
    if len(cfg.encode("utf-8")) > MAX_CONNECTION_BYTES:
        if msg:
            await msg.reply_text(ui_error_text("ссылка превышает лимит 1 МБ."))
        return ADMIN_USER_CFG_TEXT
    if not is_valid_connection_url(cfg):
        if msg:
            await msg.reply_text(ui_error_text("нужна полная ссылка, начинающаяся с http:// или https://."))
        return ADMIN_USER_CFG_TEXT

    delivery_mode = str(context.user_data.get("subscription_delivery_mode", "send"))
    author_id = get_user_id(update)
    author_name = staff_title(update)

    def _set_subscription(data: UserData) -> dict | None:
        current = data.authorized_users.get(str(uid))
        if not isinstance(current, dict):
            return None
        updated_meta = dict(current)
        updated_meta[CONNECTION_URL_KEY] = cfg
        updated_meta[CONNECTION_UPDATED_AT_KEY] = datetime.now(TZ).isoformat()
        updated_meta[CONNECTION_UPDATED_BY_ID_KEY] = author_id
        updated_meta[CONNECTION_UPDATED_BY_NAME_KEY] = author_name
        updated_meta = UserData._normalize_user(updated_meta)
        data.authorized_users[str(uid)] = updated_meta
        append_audit_entry(
            data,
            action="connection_assigned",
            actor_meta=data.authorized_users.get(str(author_id)),
            target_user_id=uid,
            details={"delivery_mode": delivery_mode},
        )
        if delivery_mode != "assign":
            delivery_event = make_outbox_event(
                kind="subscription_assigned",
                recipient_ids=[uid],
                payload=connection_outbox_payload(
                    updated_meta,
                    title=(
                        "🔗 <b>Ссылка подключения готова</b>\n\n"
                        "Для вашей учётной записи назначена персональная ссылка подключения.\n"
                        "Откройте её, чтобы посмотреть инструкцию, или скопируйте ссылку и добавьте её в Happ."
                    ),
                    filename_prefix=f"connection_{uid}",
                ),
            )
            enqueue_user_outbox(data, delivery_event)
        return updated_meta

    updated = await update_user_data(_set_subscription)
    if updated is None:
        if msg:
            await msg.reply_text(ui_error_text("пользователь не найден (возможно, удалён из списка)."))
        return ADMIN_PICK

    if delivery_mode == "assign":
        if msg:
            await msg.reply_text(ui_ok_text("Персональная ссылка сохранена без отправки пользователю"))
        logger.info("Admin user_id=%s assigned connection target_uid=%s mode=assign", author_id, uid)
    else:
        logger.info("Admin user_id=%s assigned connection target_uid=%s mode=queued", author_id, uid)
        if msg:
            await msg.reply_text(ui_ok_text("Персональная ссылка сохранена и поставлена в очередь отправки"))

    context.user_data.pop("subscription_delivery_mode", None)
    if msg:
        await msg.reply_text(format_user_card(updated), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
    return ADMIN_USER_MENU
