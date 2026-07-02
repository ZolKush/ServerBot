from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ..config import TZ, logger
from ..models import Attachment, Ticket, TicketMessage
from ..storage import (
    UpdateAborted,
    get_admin_name_by_id,
    get_all_tickets_snapshot,
    get_ticket_copy,
    get_user_open_tickets,
    next_ticket_seq,
    set_ticket_record,
    update_important_data,
)
from .common import (
    authorized_ids,
    clip_text,
    display_name,
    format_dt_human,
    get_user_id,
    html_escape,
    is_admin,
    require_admin,
    require_auth,
    safe_edit_or_reply,
    show_main_menu,
    ui_error_text,
    ui_ok_text,
    ui_warn_text,
    wrap_as_codeblock_html,
)
from .ui import SEP, pager_row, plural_ru, urgency_emoji, urgency_label

TICKET_SUBJECT, TICKET_URGENCY, TICKET_TEXT, TICKET_CONFIRM, TICKET_USER_REPLY_TEXT, TICKET_ADMIN_REPLY_TEXT = range(6)
MAX_TICKET_SUBJECT_LEN = 160
MAX_TICKET_TEXT_LEN = 3200
MAX_TICKET_HISTORY_ITEMS = 6
MAX_TICKET_HISTORY_CHARS = 2500  # суммарно на историю (лимит Telegram 4096 минус шапка)
MAX_TICKET_HISTORY_ITEM_CHARS = 900  # на одно сообщение (после HTML-эскейпа)
MAX_TICKET_MESSAGES_STORED = 100
ARCHIVE_PAGE_SIZE = 10


class TicketFlowError(RuntimeError):
    pass


def _now_iso() -> str:
    return datetime.now(TZ).isoformat()


def _clear_ticket_ctx(context: ContextTypes.DEFAULT_TYPE) -> None:
    for key in (
        "ticket_subject",
        "ticket_urgency",
        "ticket_text",
        "ticket_attachment",
        "ticket_send_in_progress",
        "ticket_edit_field",
        "ticket_reply_ticket_id",
        "ticket_reply_role",
    ):
        context.user_data.pop(key, None)


def _ticket_status_label(ticket: Ticket) -> str:
    status = str(ticket.get("status", "open"))
    if status == "in_progress":
        return "в работе"
    if status == "closed":
        return "закрыт"
    return "ожидает исполнителя"


def _ticket_can_user_reply(ticket: Ticket, uid: int) -> bool:
    return (
        int(ticket.get("user_id", 0) or 0) == uid
        and str(ticket.get("status", "open")) != "closed"
        and bool(ticket.get("assignee_id"))
        and bool(ticket.get("user_reply_allowed", False))
    )


def _ticket_is_assignee(ticket: Ticket, uid: int) -> bool:
    return int(ticket.get("assignee_id", 0) or 0) == uid


def _ticket_messages(ticket: Ticket) -> list[TicketMessage]:
    items = ticket.get("messages", [])
    return [dict(x) for x in items] if isinstance(items, list) else []


def _append_ticket_message(
    ticket: Ticket,
    *,
    sender_role: str,
    sender_id: int | None,
    sender_name: str,
    text: str,
    kind: str,
    attachment: Attachment | None = None,
) -> Ticket:
    updated: Ticket = dict(ticket)  # type: ignore[assignment]
    messages = _ticket_messages(updated)
    item: TicketMessage = {
        "ts": _now_iso(),
        "sender_role": sender_role,  # type: ignore[typeddict-item]
        "sender_id": sender_id,
        "sender_name": sender_name,
        "text": text,
        "kind": kind,
    }
    if attachment:
        item["attachment"] = dict(attachment)  # type: ignore[typeddict-item]
    messages.append(item)
    if len(messages) > MAX_TICKET_MESSAGES_STORED:
        messages = messages[-MAX_TICKET_MESSAGES_STORED:]
    updated["messages"] = messages
    updated["updated_at"] = _now_iso()
    return updated


def _extract_message_payload(msg: Message | None) -> tuple[str, dict[str, Any] | None]:
    if msg is None:
        return "", None
    text = (msg.text or msg.caption or "").strip()
    attachment: dict[str, Any] | None = None
    if msg.photo:
        photo = msg.photo[-1]
        attachment = {
            "type": "photo",
            "file_id": photo.file_id,
            "file_unique_id": getattr(photo, "file_unique_id", None),
        }
    elif msg.document:
        attachment = {
            "type": "document",
            "file_id": msg.document.file_id,
            "file_unique_id": getattr(msg.document, "file_unique_id", None),
            "filename": msg.document.file_name,
            "mime_type": msg.document.mime_type,
            "file_size": msg.document.file_size,
        }
    return text, attachment


def _attachment_label(attachment: dict[str, Any]) -> str:
    a_type = str(attachment.get("type") or "")
    if a_type == "photo":
        return "📎 Фото"
    if a_type == "document":
        name = str(attachment.get("filename") or "").strip()
        return f"📎 Файл: {name}" if name else "📎 Файл"
    return "📎 Вложение"


async def _forward_ticket_attachment(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    attachment: dict[str, Any] | None,
) -> None:
    if not attachment:
        return
    a_type = str(attachment.get("type") or "")
    file_id = attachment.get("file_id")
    if not file_id:
        return
    try:
        if a_type == "photo":
            await context.bot.send_photo(chat_id=chat_id, photo=file_id)
        elif a_type == "document":
            await context.bot.send_document(chat_id=chat_id, document=file_id)
    except Exception as e:
        logger.warning("Не удалось переслать вложение тикета chat_id=%s: %s", chat_id, e)


def _format_ticket_history(ticket: dict[str, Any], *, limit: int = MAX_TICKET_HISTORY_CHARS) -> str:
    messages = _ticket_messages(ticket)[-MAX_TICKET_HISTORY_ITEMS:]
    if not messages:
        return "История пуста."
    blocks: list[str] = []
    for item in messages:
        parts: list[str] = []
        sender_role = "Админ" if item.get("sender_role") == "admin" else "Пользователь"
        sender_name = str(item.get("sender_name") or sender_role)
        ts = format_dt_human(item.get("ts"))
        text = str(item.get("text") or "")
        parts.append(f"<b>{html_escape(sender_role)}:</b> {html_escape(sender_name)}")
        parts.append(f"• Время: <code>{html_escape(ts)}</code>")
        if text:
            parts.append(wrap_as_codeblock_html(text, limit=MAX_TICKET_HISTORY_ITEM_CHARS))
        attachment = item.get("attachment")
        if isinstance(attachment, dict):
            parts.append(f"<i>{html_escape(_attachment_label(attachment))}</i>")
        blocks.append("\n".join(parts))
    # Лимит Telegram — 4096 символов на сообщение вместе с шапкой тикета,
    # поэтому отбрасываем старые сообщения целиком (нельзя резать HTML посередине).
    dropped = 0
    while len(blocks) > 1 and sum(len(b) + 1 for b in blocks) > limit:
        blocks.pop(0)
        dropped += 1
    out = "\n".join(blocks)
    if dropped:
        out = f"<i>…{dropped} старых сообщений скрыто из-за лимита</i>\n" + out
    return out


def _ticket_user_kb(ticket: dict[str, Any], uid: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    ticket_id = int(ticket.get("id", 0) or 0)
    if ticket_id and _ticket_can_user_reply(ticket, uid):
        rows.append([InlineKeyboardButton("💬 Ответить администратору", callback_data=f"ticket:userreply:{ticket_id}")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def _ticket_admin_kb(ticket: dict[str, Any], admin_uid: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    ticket_id = int(ticket.get("id", 0) or 0)
    status = str(ticket.get("status", "open"))
    assignee_id = int(ticket.get("assignee_id", 0) or 0)
    if ticket_id and status != "closed":
        if not assignee_id:
            rows.append([InlineKeyboardButton("🫳 Взять в работу", callback_data=f"ticket:take:{ticket_id}")])
        elif assignee_id == admin_uid:
            rows.append([InlineKeyboardButton("💬 Ответить пользователю", callback_data=f"ticket:adminreply:{ticket_id}")])
            rows.append([InlineKeyboardButton("🔄 Передать", callback_data=f"ticket:transfer_init:{ticket_id}")])
            rows.append([InlineKeyboardButton("✅ Закрыть тикет", callback_data=f"ticket:close:{ticket_id}")])
    rows.append([InlineKeyboardButton("📋 К панели", callback_data="ticket:list")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def _ticket_status_emoji(ticket: dict[str, Any]) -> str:
    if str(ticket.get("status", "open")) == "closed":
        return "✅"
    return "🟡" if int(ticket.get("assignee_id", 0) or 0) else "🔴"


def _ticket_header(ticket: dict[str, Any], *, title_prefix: str = "Тикет") -> str:
    ticket_no = f"#{ticket.get('id', '-')}"
    status_label = _ticket_status_label(ticket)
    return f"🎫 <b>{html_escape(title_prefix)} {html_escape(ticket_no)}</b> · {_ticket_status_emoji(ticket)} {html_escape(status_label)}"


def _format_ticket_for_admin(ticket: dict[str, Any], admin_uid: int, *, event_line: str | None = None) -> str:
    assignee_name = str(ticket.get("assignee_name") or "-")
    user_name = str(ticket.get("user_name") or "пользователь")
    user_username = str(ticket.get("user_username") or "").strip()
    user_id = int(ticket.get("user_id", 0) or 0)
    subject = str(ticket.get("subject") or "-")
    created_at = format_dt_human(ticket.get("created_at"))
    lines = [
        _ticket_header(ticket),
        SEP,
        f"• Исполнитель: <b>{html_escape(assignee_name)}</b>",
        f"• Пользователь: <b>{html_escape(user_name)}</b> (<code>{html_escape(str(user_id))}</code>)",
        f"• Срочность: {urgency_label(ticket.get('urgency'))}",
        f"• Тема: <code>{html_escape(subject)}</code>",
        f"• Создан: <code>{html_escape(created_at)}</code>",
    ]
    if user_username:
        lines.append(f"• Username: <code>@{html_escape(user_username.lstrip('@'))}</code>")
    if str(ticket.get("status", "open")) == "closed":
        lines.append(f"• Закрыт: <code>{html_escape(format_dt_human(ticket.get('closed_at')))}</code>")
    if event_line:
        lines.extend(["", event_line])
    if _ticket_is_assignee(ticket, admin_uid):
        lines.extend(["", "<b>Доступ:</b> вы исполнитель этого тикета."])
    lines.extend([SEP, "🕘 <b>История</b>", _format_ticket_history(ticket)])
    return "\n".join(lines)


def _format_ticket_for_user(ticket: dict[str, Any], *, event_line: str | None = None) -> str:
    assignee_name = str(ticket.get("assignee_name") or "ещё не назначен")
    subject = str(ticket.get("subject") or "-")
    lines = [
        _ticket_header(ticket, title_prefix="Мой тикет"),
        SEP,
        f"• Исполнитель: <b>{html_escape(assignee_name)}</b>",
        f"• Срочность: {urgency_label(ticket.get('urgency'))}",
        f"• Тема: <code>{html_escape(subject)}</code>",
    ]
    if str(ticket.get("status", "open")) == "closed":
        lines.append(f"• Закрыт: <code>{html_escape(format_dt_human(ticket.get('closed_at')))}</code>")
    if event_line:
        lines.extend(["", event_line])
    lines.extend([SEP, "🕘 <b>История</b>", _format_ticket_history(ticket)])
    return "\n".join(lines)


def _build_ticket_record(
    ticket_id: int,
    *,
    user_id: int,
    user_name: str,
    user_username: str | None,
    subject: str,
    urgency: str,
    text: str,
    attachment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = _now_iso()
    initial_message: dict[str, Any] = {
        "ts": now,
        "sender_role": "user",
        "sender_id": user_id,
        "sender_name": user_name,
        "text": text,
        "kind": "initial",
    }
    if attachment:
        initial_message["attachment"] = dict(attachment)
    return {
        "id": ticket_id,
        "status": "open",
        "subject": subject,
        "urgency": urgency,
        "user_id": user_id,
        "user_name": user_name,
        "user_username": (user_username or None),
        "assignee_id": None,
        "assignee_name": None,
        "created_at": now,
        "updated_at": now,
        "closed_at": None,
        "closed_by_id": None,
        "closed_by_name": None,
        "user_reply_allowed": False,
        "messages": [initial_message],
    }


def _last_attachment(ticket: dict[str, Any]) -> dict[str, Any] | None:
    messages = _ticket_messages(ticket)
    if not messages:
        return None
    last = messages[-1]
    att = last.get("attachment") if isinstance(last, dict) else None
    return dict(att) if isinstance(att, dict) else None


async def _notify_admins_full(
    context: ContextTypes.DEFAULT_TYPE,
    ticket: dict[str, Any],
    *,
    exclude: set[int] | None = None,
    event_line: str | None = None,
) -> None:
    """Полное уведомление всем (или всем кроме exclude): текст тикета + кнопки + вложение."""
    exclude = exclude or set()
    admin_ids = [aid for aid in authorized_ids(role_filter="admin") if aid not in exclude]
    if not admin_ids:
        return
    attachment = _last_attachment(ticket)

    async def _send_one(admin_id: int) -> bool:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=_format_ticket_for_admin(ticket, admin_id, event_line=event_line),
                parse_mode=ParseMode.HTML,
                reply_markup=_ticket_admin_kb(ticket, admin_id),
            )
            await _forward_ticket_attachment(context, admin_id, attachment)
            return True
        except Exception as e:
            logger.warning("Не удалось отправить ticket update админу %s ticket_id=%s: %s", admin_id, ticket.get("id"), e)
            return False

    results = await asyncio.gather(*[_send_one(aid) for aid in admin_ids], return_exceptions=False)
    sent = sum(1 for r in results if r)
    logger.info(
        "Ticket full admin notifications ticket_id=%s sent=%s failed=%s event=%s",
        ticket.get("id"), sent, len(results) - sent, event_line or "",
    )


async def _notify_admins_brief(
    context: ContextTypes.DEFAULT_TYPE,
    ticket_id: int,
    text: str,
    *,
    exclude: set[int] | None = None,
) -> None:
    """Краткое текстовое уведомление без деталей тикета и кнопок."""
    exclude = exclude or set()
    admin_ids = [aid for aid in authorized_ids(role_filter="admin") if aid not in exclude]
    if not admin_ids:
        return

    async def _send_brief(admin_id: int) -> bool:
        try:
            await context.bot.send_message(chat_id=admin_id, text=text, parse_mode=ParseMode.HTML)
            return True
        except Exception as e:
            logger.warning("Не удалось отправить brief update админу %s ticket_id=%s: %s", admin_id, ticket_id, e)
            return False

    results = await asyncio.gather(*[_send_brief(aid) for aid in admin_ids], return_exceptions=False)
    sent = sum(1 for r in results if r)
    logger.info("Ticket brief admin notifications ticket_id=%s sent=%s failed=%s", ticket_id, sent, len(results) - sent)


async def _notify_assignee_full(
    context: ContextTypes.DEFAULT_TYPE,
    ticket: dict[str, Any],
    *,
    event_line: str | None = None,
) -> None:
    """Полное уведомление только исполнителю тикета."""
    assignee_id = int(ticket.get("assignee_id", 0) or 0)
    if not assignee_id:
        return
    attachment = _last_attachment(ticket)
    try:
        await context.bot.send_message(
            chat_id=assignee_id,
            text=_format_ticket_for_admin(ticket, assignee_id, event_line=event_line),
            parse_mode=ParseMode.HTML,
            reply_markup=_ticket_admin_kb(ticket, assignee_id),
        )
        await _forward_ticket_attachment(context, assignee_id, attachment)
        logger.info(
            "Ticket assignee notification ticket_id=%s assignee_id=%s event=%s",
            ticket.get("id"), assignee_id, event_line or "",
        )
    except Exception as e:
        logger.warning(
            "Не удалось отправить ticket update исполнителю %s ticket_id=%s: %s",
            assignee_id, ticket.get("id"), e,
        )


async def _notify_user(context: ContextTypes.DEFAULT_TYPE, ticket: dict[str, Any], *, event_line: str | None = None) -> None:
    uid = int(ticket.get("user_id", 0) or 0)
    if not uid:
        return
    attachment = _last_attachment(ticket)
    try:
        await context.bot.send_message(
            chat_id=uid,
            text=_format_ticket_for_user(ticket, event_line=event_line),
            parse_mode=ParseMode.HTML,
            reply_markup=_ticket_user_kb(ticket, uid),
        )
        await _forward_ticket_attachment(context, uid, attachment)
        logger.info("Ticket user notification ticket_id=%s user_id=%s event=%s", ticket.get("id"), uid, event_line or "")
    except Exception as e:
        logger.warning("Не удалось отправить ticket update пользователю %s ticket_id=%s: %s", uid, ticket.get("id"), e)


async def _ticket_update(ticket_id: int, mutator) -> dict[str, Any]:
    flow_state: dict[str, str] = {}

    def _apply(cfg):
        tickets = dict(getattr(cfg, "tickets", {}) or {})
        raw = tickets.get(str(ticket_id))
        if not isinstance(raw, dict):
            flow_state["error"] = "ticket_not_found"
            raise UpdateAborted()
        try:
            updated = mutator(dict(raw))
        except TicketFlowError as exc:
            flow_state["error"] = str(exc) or "ticket_error"
            raise UpdateAborted() from exc
        tickets[str(ticket_id)] = dict(updated)
        cfg.tickets = tickets
        return dict(updated)

    try:
        return await update_important_data(_apply)
    except UpdateAborted:
        raise TicketFlowError(flow_state.get("error", "ticket_error")) from None


def _parse_ticket_callback_id(data: str | None, action: str) -> int:
    parts = (data or "").split(":")
    if len(parts) != 3 or parts[0] != "ticket" or parts[1] != action or not parts[2].isdigit():
        return 0
    return int(parts[2])


def ticket_urgency_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔥 Критично", callback_data="ticket:p1")],
            [InlineKeyboardButton("⚠️ Важно", callback_data="ticket:p2")],
            [InlineKeyboardButton("📋 Обычно", callback_data="ticket:p3")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


def ticket_input_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]])


def ticket_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Отправить", callback_data="ticket:send")],
            [
                InlineKeyboardButton("✏️ Изменить тему", callback_data="ticket:edit_subj"),
                InlineKeyboardButton("✏️ Изменить описание", callback_data="ticket:edit_text"),
            ],
            [InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


def _ticket_preview_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    subj = context.user_data.get("ticket_subject", "-")
    urg = str(context.user_data.get("ticket_urgency", "p3"))
    text = str(context.user_data.get("ticket_text", ""))
    text_for_preview = clip_text(text, limit=3000)
    attachment = context.user_data.get("ticket_attachment")
    attachment_line = ""
    if isinstance(attachment, dict):
        attachment_line = f"\n<i>{html_escape(_attachment_label(attachment))}</i>"
    return (
        f"🎫 <b>Новый тикет — проверка</b>\n{SEP}\n"
        f"• Тема: <code>{html_escape(str(subj))}</code>\n"
        f"• Срочность: {urgency_label(urg)}\n\n"
        "Описание:\n"
        + wrap_as_codeblock_html(text_for_preview)
        + attachment_line
        + "\n\nВыберите действие:"
    )


@require_auth
async def ticket_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = get_user_id(update)
    if uid is None:
        return ConversationHandler.END

    if is_admin(update):
        if q:
            await q.answer()
        _clear_ticket_ctx(context)
        return await _show_ticket_dashboard(update, context)

    open_tickets = get_user_open_tickets(uid)
    if open_tickets:
        ticket = open_tickets[0]
        msg = update.effective_message
        text = _format_ticket_for_user(
            ticket,
            event_line=ui_warn_text("У вас уже есть открытый тикет. Новый можно создать после его закрытия."),
        )
        kb = _ticket_user_kb(ticket, uid)
        if q and msg:
            await q.answer()
            await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        elif msg:
            await msg.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return ConversationHandler.END
    _clear_ticket_ctx(context)
    msg = update.effective_message
    if q and msg:
        await q.answer()
        await q.edit_message_text(
            "<b>Тикет > Тема</b>\n\nВведите тему тикета (кратко).\nДля отмены: /cancel",
            parse_mode=ParseMode.HTML,
            reply_markup=ticket_input_kb(),
        )
    elif msg:
        await msg.reply_text(
            "<b>Тикет > Тема</b>\n\nВведите тему тикета (кратко).\nДля отмены: /cancel",
            parse_mode=ParseMode.HTML,
            reply_markup=ticket_input_kb(),
        )
    return TICKET_SUBJECT


@require_auth
async def ticket_subject(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    subj = (msg.text if msg else "").strip()
    if len(subj) < 3:
        if msg:
            await msg.reply_text("Тема слишком короткая. Введите минимум 3 символа.")
        return TICKET_SUBJECT
    if len(subj) > MAX_TICKET_SUBJECT_LEN:
        if msg:
            await msg.reply_text(f"Тема слишком длинная. Максимум {MAX_TICKET_SUBJECT_LEN} символов.")
        return TICKET_SUBJECT

    context.user_data["ticket_subject"] = subj
    edit_field = context.user_data.pop("ticket_edit_field", None)
    if edit_field == "subject" and context.user_data.get("ticket_text"):
        if msg:
            await msg.reply_text(_ticket_preview_text(context), parse_mode=ParseMode.HTML, reply_markup=ticket_confirm_kb())
        return TICKET_CONFIRM
    if msg:
        await msg.reply_text("Срочность:", reply_markup=ticket_urgency_kb())
    return TICKET_URGENCY


@require_auth
async def ticket_urgency(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    if q.data not in ("ticket:p1", "ticket:p2", "ticket:p3"):
        return ConversationHandler.END
    context.user_data["ticket_urgency"] = q.data.split(":")[1]
    await q.edit_message_text(
        "<b>Тикет > Описание</b>\n\nОпишите проблему (лучше одним сообщением). Для отмены: /cancel",
        parse_mode=ParseMode.HTML,
        reply_markup=ticket_input_kb(),
    )
    return TICKET_TEXT


@require_auth
async def ticket_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    text, attachment = _extract_message_payload(msg)
    if len(text) > MAX_TICKET_TEXT_LEN:
        if msg:
            await msg.reply_text(f"Описание слишком длинное. Максимум {MAX_TICKET_TEXT_LEN} символов.")
        return TICKET_TEXT
    if not attachment and len(text) < 10:
        if msg:
            await msg.reply_text(
                "Описание слишком короткое. Дайте больше деталей (>= 10 символов) "
                "или приложите фото/файл с пояснением."
            )
        return TICKET_TEXT
    if attachment and not text:
        text = "(вложение)"

    context.user_data["ticket_text"] = text
    if attachment:
        context.user_data["ticket_attachment"] = attachment
    else:
        context.user_data.pop("ticket_attachment", None)
    context.user_data.pop("ticket_edit_field", None)
    preview = _ticket_preview_text(context)
    if msg:
        await msg.reply_text(preview, parse_mode=ParseMode.HTML, reply_markup=ticket_confirm_kb())
    return TICKET_CONFIRM


@require_auth
async def ticket_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    if q.data == "ticket:cancel":
        _clear_ticket_ctx(context)
        await show_main_menu(update, text=ui_warn_text("Создание тикета отменено.") + "\n\nВыберите раздел:")
        return ConversationHandler.END
    if q.data == "ticket:edit_subj":
        context.user_data["ticket_edit_field"] = "subject"
        await q.edit_message_text(
            "<b>Тикет > Тема</b>\n\nВведите новую тему:",
            parse_mode=ParseMode.HTML,
            reply_markup=ticket_input_kb(),
        )
        return TICKET_SUBJECT
    if q.data == "ticket:edit_text":
        context.user_data["ticket_edit_field"] = "text"
        await q.edit_message_text(
            "<b>Тикет > Описание</b>\n\nВведите новое описание:",
            parse_mode=ParseMode.HTML,
            reply_markup=ticket_input_kb(),
        )
        return TICKET_TEXT
    if q.data != "ticket:send":
        return ConversationHandler.END
    if context.user_data.get("ticket_send_in_progress"):
        await q.edit_message_text(ui_warn_text("тикет уже отправляется, подождите..."))
        return ConversationHandler.END
    context.user_data["ticket_send_in_progress"] = True

    try:
        uid = get_user_id(update)
        author_name = display_name(update)
        author_username = getattr(update.effective_user, "username", None)
        subj = str(context.user_data.get("ticket_subject", "-"))
        urg = str(context.user_data.get("ticket_urgency", "p3")).lower()
        txt = str(context.user_data.get("ticket_text", "-"))
        admins = authorized_ids(role_filter="admin")
        if uid is None:
            _clear_ticket_ctx(context)
            await q.edit_message_text(ui_error_text("не удалось определить пользователя."))
            return ConversationHandler.END
        if not admins:
            _clear_ticket_ctx(context)
            await q.edit_message_text(ui_error_text("нет авторизованных администраторов."))
            return ConversationHandler.END

        ticket_id = await next_ticket_seq()
        attachment = context.user_data.get("ticket_attachment")
        attachment_data = attachment if isinstance(attachment, dict) else None
        ticket = _build_ticket_record(
            ticket_id,
            user_id=uid,
            user_name=author_name,
            user_username=author_username,
            subject=subj,
            urgency=urg,
            text=txt,
            attachment=attachment_data,
        )
        await set_ticket_record(ticket_id, ticket)
        logger.info("Ticket created ticket_id=%s user_id=%s urgency=%s subject=%s", ticket_id, uid, urg, subj)

        await _notify_admins_full(context, ticket, event_line="🆕 <b>Новый тикет ожидает исполнителя</b>")
        await _notify_user(context, ticket, event_line="✅ <b>Тикет создан и отправлен администраторам</b>")
        _clear_ticket_ctx(context)
        await q.edit_message_text(ui_ok_text(f"Тикет #{ticket_id} создан. Ожидайте ответа администратора."))
        return ConversationHandler.END
    finally:
        context.user_data.pop("ticket_send_in_progress", None)


@require_admin
async def ticket_take_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    ticket_id = _parse_ticket_callback_id(q.data, "take")
    admin_id = get_user_id(update)
    if not ticket_id or admin_id is None:
        await q.answer()
        return
    admin_name = display_name(update)

    try:
        def _assign(ticket: dict[str, Any]) -> dict[str, Any]:
            if str(ticket.get("status", "open")) == "closed":
                raise TicketFlowError("ticket_closed")
            assignee_id = int(ticket.get("assignee_id", 0) or 0)
            if assignee_id and assignee_id != admin_id:
                raise TicketFlowError("ticket_taken")
            if assignee_id == admin_id:
                raise TicketFlowError("already_assigned")
            updated = dict(ticket)
            updated["assignee_id"] = admin_id
            updated["assignee_name"] = admin_name
            updated["status"] = "in_progress"
            updated["updated_at"] = _now_iso()
            return updated

        ticket = await _ticket_update(ticket_id, _assign)
    except TicketFlowError as e:
        code = str(e)
        logger.warning("Ticket take denied ticket_id=%s admin_id=%s reason=%s", ticket_id, admin_id, code)
        if code == "ticket_taken":
            await q.answer("Тикет уже взят другим администратором.", show_alert=True)
        elif code == "already_assigned":
            await q.answer("Этот тикет уже назначен вам.", show_alert=True)
        elif code == "ticket_not_found":
            await q.answer("Тикет не найден.", show_alert=True)
        else:
            await q.answer("Тикет уже закрыт.", show_alert=True)
        return

    await q.answer()
    logger.info(
        "Ticket assigned ticket_id=%s admin_id=%s user_id=%s",
        ticket_id,
        admin_id,
        ticket.get("user_id"),
    )
    await _notify_admins_brief(
        context,
        ticket_id,
        f"🫳 Тикет #{ticket_id} взят в работу: {html_escape(admin_name)}",
        exclude={admin_id},
    )
    await _notify_user(context, ticket, event_line=f"👤 <b>Исполнитель назначен:</b> {html_escape(admin_name)}")
    await q.edit_message_text(
        _format_ticket_for_admin(ticket, admin_id, event_line="🫳 <b>Вы взяли тикет в работу</b>"),
        parse_mode=ParseMode.HTML,
        reply_markup=_ticket_admin_kb(ticket, admin_id),
    )


@require_admin
async def ticket_admin_reply_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    ticket_id = _parse_ticket_callback_id(q.data, "adminreply")
    admin_id = get_user_id(update)
    ticket = get_ticket_copy(ticket_id) if ticket_id else None
    if not ticket or admin_id is None:
        logger.warning("Ticket admin reply start failed ticket_id=%s admin_id=%s reason=ticket_not_found", ticket_id, admin_id)
        await q.answer("Тикет не найден.", show_alert=True)
        return ConversationHandler.END
    if str(ticket.get("status", "open")) == "closed":
        logger.warning("Ticket admin reply start denied ticket_id=%s admin_id=%s reason=ticket_closed", ticket_id, admin_id)
        await q.answer("Тикет уже закрыт.", show_alert=True)
        return ConversationHandler.END
    if not _ticket_is_assignee(ticket, admin_id):
        logger.warning("Ticket admin reply denied ticket_id=%s admin_id=%s reason=not_assignee", ticket_id, admin_id)
        await q.answer("Ответить может только исполнитель тикета.", show_alert=True)
        return ConversationHandler.END
    await q.answer()
    context.user_data["ticket_reply_ticket_id"] = ticket_id
    context.user_data["ticket_reply_role"] = "admin"
    await q.edit_message_text(
        f"🎫 <b>Тикет #{ticket_id} — ответ</b>\n{SEP}\nВведите ответ пользователю:",
        parse_mode=ParseMode.HTML,
        reply_markup=ticket_input_kb(),
    )
    return TICKET_ADMIN_REPLY_TEXT


@require_auth
async def ticket_user_reply_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    ticket_id = _parse_ticket_callback_id(q.data, "userreply")
    uid = get_user_id(update)
    ticket = get_ticket_copy(ticket_id) if ticket_id else None
    if not ticket or uid is None:
        logger.warning("Ticket user reply start failed ticket_id=%s user_id=%s reason=ticket_not_found", ticket_id, uid)
        await q.answer("Тикет не найден.", show_alert=True)
        return ConversationHandler.END
    if not _ticket_can_user_reply(ticket, uid):
        logger.warning(
            "Ticket user reply denied ticket_id=%s user_id=%s reason=reply_not_allowed assignee_id=%s status=%s",
            ticket_id,
            uid,
            ticket.get("assignee_id"),
            ticket.get("status"),
        )
        await q.answer("Сейчас ответить на этот тикет нельзя.", show_alert=True)
        return ConversationHandler.END
    await q.answer()
    context.user_data["ticket_reply_ticket_id"] = ticket_id
    context.user_data["ticket_reply_role"] = "user"
    await q.edit_message_text(
        f"🎫 <b>Мой тикет #{ticket_id} — ответ</b>\n{SEP}\nВведите ответ администратору:",
        parse_mode=ParseMode.HTML,
        reply_markup=ticket_input_kb(),
    )
    return TICKET_USER_REPLY_TEXT


@require_admin
async def ticket_admin_reply_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    text, attachment = _extract_message_payload(msg)
    ticket_id = int(context.user_data.get("ticket_reply_ticket_id", 0) or 0)
    admin_id = get_user_id(update)
    if not ticket_id or admin_id is None:
        return ConversationHandler.END
    if not attachment and not text:
        if msg:
            await msg.reply_text("Пустой ответ. Введите текст или приложите фото/файл.")
        return TICKET_ADMIN_REPLY_TEXT
    if len(text) > MAX_TICKET_TEXT_LEN:
        if msg:
            await msg.reply_text(f"Ответ слишком длинный. Максимум {MAX_TICKET_TEXT_LEN} символов.")
        return TICKET_ADMIN_REPLY_TEXT
    if attachment and not text:
        text = "(вложение)"

    admin_name = display_name(update)
    try:
        def _reply(ticket: dict[str, Any]) -> dict[str, Any]:
            if str(ticket.get("status", "open")) == "closed":
                raise TicketFlowError("ticket_closed")
            if not _ticket_is_assignee(ticket, admin_id):
                raise TicketFlowError("not_assignee")
            updated = _append_ticket_message(
                ticket,
                sender_role="admin",
                sender_id=admin_id,
                sender_name=admin_name,
                text=text,
                kind="reply",
                attachment=attachment,
            )
            updated["status"] = "in_progress"
            updated["user_reply_allowed"] = True
            return updated

        ticket = await _ticket_update(ticket_id, _reply)
    except TicketFlowError as e:
        logger.warning("Ticket admin reply failed ticket_id=%s admin_id=%s reason=%s", ticket_id, admin_id, e)
        if msg:
            await msg.reply_text("Не удалось отправить ответ: тикет закрыт или закреплён за другим администратором.")
        _clear_ticket_ctx(context)
        return ConversationHandler.END

    logger.info(
        "Ticket admin reply ticket_id=%s admin_id=%s user_id=%s text_len=%s",
        ticket_id,
        admin_id,
        ticket.get("user_id"),
        len(text),
    )
    await _notify_user(context, ticket, event_line="💬 <b>Администратор ответил на ваш тикет</b>")
    if msg:
        await msg.reply_text(
            _format_ticket_for_admin(ticket, admin_id, event_line="✅ <b>Ответ отправлен пользователю</b>"),
            parse_mode=ParseMode.HTML,
            reply_markup=_ticket_admin_kb(ticket, admin_id),
        )
    _clear_ticket_ctx(context)
    return ConversationHandler.END


@require_auth
async def ticket_user_reply_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    text, attachment = _extract_message_payload(msg)
    ticket_id = int(context.user_data.get("ticket_reply_ticket_id", 0) or 0)
    uid = get_user_id(update)
    if not ticket_id or uid is None:
        return ConversationHandler.END
    if not attachment and not text:
        if msg:
            await msg.reply_text("Пустой ответ. Введите текст или приложите фото/файл.")
        return TICKET_USER_REPLY_TEXT
    if len(text) > MAX_TICKET_TEXT_LEN:
        if msg:
            await msg.reply_text(f"Ответ слишком длинный. Максимум {MAX_TICKET_TEXT_LEN} символов.")
        return TICKET_USER_REPLY_TEXT
    if attachment and not text:
        text = "(вложение)"

    user_name = display_name(update)
    try:
        def _reply(ticket: dict[str, Any]) -> dict[str, Any]:
            if not _ticket_can_user_reply(ticket, uid):
                raise TicketFlowError("user_reply_not_allowed")
            updated = _append_ticket_message(
                ticket,
                sender_role="user",
                sender_id=uid,
                sender_name=user_name,
                text=text,
                kind="reply",
                attachment=attachment,
            )
            updated["updated_at"] = _now_iso()
            return updated

        ticket = await _ticket_update(ticket_id, _reply)
    except TicketFlowError as e:
        logger.warning("Ticket user reply failed ticket_id=%s user_id=%s reason=%s", ticket_id, uid, e)
        if msg:
            await msg.reply_text("Сейчас ответить на этот тикет нельзя.")
        _clear_ticket_ctx(context)
        return ConversationHandler.END

    logger.info(
        "Ticket user reply ticket_id=%s user_id=%s assignee_id=%s text_len=%s",
        ticket_id,
        uid,
        ticket.get("assignee_id"),
        len(text),
    )
    await _notify_assignee_full(context, ticket, event_line="💬 <b>Пользователь ответил по тикету</b>")
    if msg:
        await msg.reply_text(
            _format_ticket_for_user(ticket, event_line="✅ <b>Ваш ответ отправлен администратору</b>"),
            parse_mode=ParseMode.HTML,
            reply_markup=_ticket_user_kb(ticket, uid),
        )
    _clear_ticket_ctx(context)
    return ConversationHandler.END


@require_admin
async def ticket_close_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    ticket_id = _parse_ticket_callback_id(q.data, "close")
    admin_id = get_user_id(update)
    if not ticket_id or admin_id is None:
        await q.answer()
        return
    admin_name = display_name(update)
    try:
        def _close(ticket: dict[str, Any]) -> dict[str, Any]:
            if str(ticket.get("status", "open")) == "closed":
                raise TicketFlowError("ticket_closed")
            if not _ticket_is_assignee(ticket, admin_id):
                raise TicketFlowError("not_assignee")
            updated = dict(ticket)
            updated["status"] = "closed"
            updated["closed_at"] = _now_iso()
            updated["closed_by_id"] = admin_id
            updated["closed_by_name"] = admin_name
            updated["user_reply_allowed"] = False
            updated["updated_at"] = _now_iso()
            return updated

        ticket = await _ticket_update(ticket_id, _close)
    except TicketFlowError as e:
        logger.warning("Ticket close failed ticket_id=%s admin_id=%s reason=%s", ticket_id, admin_id, e)
        if str(e) == "ticket_closed":
            await q.answer("Тикет уже закрыт.", show_alert=True)
        elif str(e) == "ticket_not_found":
            await q.answer("Тикет не найден.", show_alert=True)
        else:
            await q.answer("Закрыть тикет может только его исполнитель.", show_alert=True)
        return

    await q.answer()
    logger.info(
        "Ticket closed ticket_id=%s admin_id=%s user_id=%s",
        ticket_id,
        admin_id,
        ticket.get("user_id"),
    )
    await _notify_user(context, ticket, event_line=f"✅ <b>Тикет закрыт администратором:</b> {html_escape(admin_name)}")
    await _notify_admins_brief(
        context,
        ticket_id,
        f"✅ Тикет #{ticket_id} закрыт: {html_escape(admin_name)}",
        exclude={admin_id},
    )
    await q.edit_message_text(
        _format_ticket_for_admin(ticket, admin_id, event_line="✅ <b>Вы закрыли тикет</b>"),
        parse_mode=ParseMode.HTML,
        reply_markup=_ticket_admin_kb(ticket, admin_id),
    )


async def _show_ticket_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    uid = get_user_id(update)
    if uid is None:
        return ConversationHandler.END

    all_tickets = list(get_all_tickets_snapshot().values())
    unpicked: list[dict[str, Any]] = []
    mine: list[dict[str, Any]] = []
    others: list[dict[str, Any]] = []

    for t in sorted(all_tickets, key=lambda x: int(x.get("id", 0))):
        status = str(t.get("status", "open"))
        if status == "closed":
            continue
        assignee_id = int(t.get("assignee_id", 0) or 0)
        if not assignee_id:
            unpicked.append(t)
        elif assignee_id == uid:
            mine.append(t)
        else:
            others.append(t)

    lines = ["🎫 <b>Тикеты — панель</b>", SEP]
    rows: list[list[InlineKeyboardButton]] = []

    if unpicked:
        lines.append("🔴 <b>Без исполнителя:</b>")
        for t in unpicked:
            tid = int(t.get("id", 0))
            subj = clip_text(str(t.get("subject") or "-"), limit=35)
            urg = t.get("urgency", "p3")
            lines.append(f"  • #{tid} {urgency_emoji(urg)} {html_escape(subj)}")
            rows.append([InlineKeyboardButton(f"🔴 #{tid} {urgency_emoji(urg)} {subj}", callback_data=f"ticket:open:{tid}")])
    else:
        lines.append("🔴 Тикетов без исполнителя нет.")

    lines.append("")

    if mine:
        lines.append("🟡 <b>Мои в работе:</b>")
        for t in mine:
            tid = int(t.get("id", 0))
            subj = clip_text(str(t.get("subject") or "-"), limit=35)
            urg = t.get("urgency", "p3")
            lines.append(f"  • #{tid} {urgency_emoji(urg)} {html_escape(subj)}")
            rows.append([InlineKeyboardButton(f"🟡 #{tid} {urgency_emoji(urg)} {subj}", callback_data=f"ticket:open:{tid}")])
    else:
        lines.append("🟡 У вас нет тикетов в работе.")

    lines.append("")

    if others:
        lines.append("🟠 <b>В работе у других:</b>")
        for t in others:
            tid = int(t.get("id", 0))
            subj = clip_text(str(t.get("subject") or "-"), limit=30)
            assignee_name = str(t.get("assignee_name") or "-")
            lines.append(f"  • #{tid} {html_escape(subj)} → {html_escape(assignee_name)}")
            rows.append([InlineKeyboardButton(f"🟠 #{tid} {subj}", callback_data=f"ticket:open:{tid}")])

    rows.append([InlineKeyboardButton("🗂 Архив", callback_data="ticket:archive")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])

    text = "\n".join(lines)
    kb = InlineKeyboardMarkup(rows)
    await safe_edit_or_reply(update.effective_message, text, reply_markup=kb)
    return ConversationHandler.END


@require_admin
async def ticket_list_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if q:
        await q.answer()
    _clear_ticket_ctx(context)
    return await _show_ticket_dashboard(update, context)


@require_admin
async def ticket_open_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    admin_id = get_user_id(update)
    if admin_id is None:
        await q.answer()
        return ConversationHandler.END

    parts = (q.data or "").split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        await q.answer()
        return ConversationHandler.END
    ticket_id = int(parts[2])
    if not ticket_id:
        await q.answer()
        return ConversationHandler.END

    ticket = get_ticket_copy(ticket_id)
    if not ticket:
        await q.answer("Тикет не найден.", show_alert=True)
        return ConversationHandler.END

    await q.answer()
    _clear_ticket_ctx(context)
    text = _format_ticket_for_admin(ticket, admin_id)
    kb = _ticket_admin_kb(ticket, admin_id)
    await safe_edit_or_reply(update.effective_message, text, reply_markup=kb)
    return ConversationHandler.END


async def _show_archive_page(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int) -> int:
    all_tickets = list(get_all_tickets_snapshot().values())
    closed = sorted(
        [t for t in all_tickets if str(t.get("status", "open")) == "closed"],
        key=lambda x: str(x.get("closed_at") or x.get("updated_at") or ""),
        reverse=True,
    )
    total = len(closed)
    total_pages = max(1, (total + ARCHIVE_PAGE_SIZE - 1) // ARCHIVE_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * ARCHIVE_PAGE_SIZE
    items = closed[start: start + ARCHIVE_PAGE_SIZE]

    lines = [
        "🗂 <b>Тикеты — архив</b>",
        f"Страница {page + 1}/{total_pages} · всего {total} {plural_ru(total, 'закрытый тикет', 'закрытых тикета', 'закрытых тикетов')}",
        SEP,
    ]
    rows: list[list[InlineKeyboardButton]] = []

    for t in items:
        tid = int(t.get("id", 0) or 0)
        subj = clip_text(str(t.get("subject") or "-"), limit=35)
        urg = t.get("urgency", "p3")
        user_name = str(t.get("user_name") or "-")
        closed_by = str(t.get("closed_by_name") or "-")
        closed_at = format_dt_human(t.get("closed_at"))
        lines.append(
            f"• <b>#{tid}</b> {urgency_emoji(urg)} {html_escape(subj)}\n"
            f"  {html_escape(user_name)} → закрыл {html_escape(closed_by)} | {html_escape(closed_at)}"
        )
        rows.append([InlineKeyboardButton(f"#{tid} {urgency_emoji(urg)} {subj}", callback_data=f"ticket:open:{tid}")])

    if not items:
        lines.append("Архив пуст.")

    if total_pages > 1:
        rows.append(pager_row("ticket:archive_page:", page, total_pages))

    rows.append([InlineKeyboardButton("⬅️ К панели", callback_data="ticket:list")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])

    text = "\n".join(lines)
    kb = InlineKeyboardMarkup(rows)
    await safe_edit_or_reply(update.effective_message, text, reply_markup=kb)
    return ConversationHandler.END


@require_admin
async def ticket_archive_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if q:
        await q.answer()
    return await _show_archive_page(update, context, page=0)


@require_admin
async def ticket_archive_page_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    parts = (q.data or "").split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        return ConversationHandler.END
    return await _show_archive_page(update, context, page=int(parts[2]))


@require_admin
async def ticket_transfer_init_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    admin_id = get_user_id(update)
    if admin_id is None:
        await q.answer()
        return ConversationHandler.END

    parts = (q.data or "").split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        await q.answer()
        return ConversationHandler.END
    ticket_id = int(parts[2])

    ticket = get_ticket_copy(ticket_id)
    if not ticket:
        await q.answer("Тикет не найден.", show_alert=True)
        return ConversationHandler.END
    if str(ticket.get("status", "open")) == "closed":
        await q.answer("Тикет уже закрыт.", show_alert=True)
        return ConversationHandler.END
    if not _ticket_is_assignee(ticket, admin_id):
        await q.answer("Передать можно только свой тикет.", show_alert=True)
        return ConversationHandler.END

    other_admins = [aid for aid in authorized_ids(role_filter="admin") if aid != admin_id]
    if not other_admins:
        await q.answer("Нет других администраторов.", show_alert=True)
        return ConversationHandler.END

    await q.answer()
    rows: list[list[InlineKeyboardButton]] = []
    for aid in other_admins:
        name = get_admin_name_by_id(aid) or str(aid)
        rows.append([InlineKeyboardButton(f"👤 {name}", callback_data=f"ticket:transfer_to:{ticket_id}:{aid}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"ticket:open:{ticket_id}")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])

    text = (
        f"🔄 <b>Тикет #{ticket_id} — передача</b>\n{SEP}\n"
        "Выберите администратора для передачи тикета:"
    )
    await safe_edit_or_reply(update.effective_message, text, reply_markup=InlineKeyboardMarkup(rows))
    return ConversationHandler.END


async def _notify_new_assignee_on_transfer(
    context: ContextTypes.DEFAULT_TYPE,
    ticket: dict[str, Any],
    new_admin_id: int,
) -> None:
    try:
        await context.bot.send_message(
            chat_id=new_admin_id,
            text=_format_ticket_for_admin(ticket, new_admin_id, event_line="🔄 <b>Тикет передан вам</b>"),
            parse_mode=ParseMode.HTML,
            reply_markup=_ticket_admin_kb(ticket, new_admin_id),
        )
    except Exception as e:
        logger.warning(
            "Не удалось отправить тикет новому исполнителю %s ticket_id=%s: %s",
            new_admin_id, ticket.get("id"), e,
        )
        return
    for msg_item in _ticket_messages(ticket):
        attachment = msg_item.get("attachment") if isinstance(msg_item, dict) else None
        if isinstance(attachment, dict):
            await _forward_ticket_attachment(context, new_admin_id, attachment)


@require_admin
async def ticket_transfer_to_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    admin_id = get_user_id(update)
    if admin_id is None:
        await q.answer()
        return ConversationHandler.END

    parts = (q.data or "").split(":")
    if len(parts) != 4 or not parts[2].isdigit() or not parts[3].isdigit():
        await q.answer()
        return ConversationHandler.END
    ticket_id = int(parts[2])
    new_admin_id = int(parts[3])
    if not ticket_id or not new_admin_id or new_admin_id == admin_id:
        await q.answer()
        return ConversationHandler.END

    new_admin_name = get_admin_name_by_id(new_admin_id)
    if not new_admin_name:
        await q.answer("Администратор не найден.", show_alert=True)
        return ConversationHandler.END

    admin_name = display_name(update)

    try:
        def _transfer(ticket: dict[str, Any]) -> dict[str, Any]:
            if str(ticket.get("status", "open")) == "closed":
                raise TicketFlowError("ticket_closed")
            if not _ticket_is_assignee(ticket, admin_id):
                raise TicketFlowError("not_assignee")
            updated = dict(ticket)
            updated["assignee_id"] = new_admin_id
            updated["assignee_name"] = new_admin_name
            updated["updated_at"] = _now_iso()
            return _append_ticket_message(
                updated,
                sender_role="admin",
                sender_id=admin_id,
                sender_name=admin_name,
                text=f"Тикет передан администратору {new_admin_name}",
                kind="transfer",
            )

        ticket = await _ticket_update(ticket_id, _transfer)
    except TicketFlowError as e:
        code = str(e)
        logger.warning("Ticket transfer failed ticket_id=%s admin_id=%s reason=%s", ticket_id, admin_id, code)
        if code == "ticket_closed":
            await q.answer("Тикет уже закрыт.", show_alert=True)
        elif code == "ticket_not_found":
            await q.answer("Тикет не найден.", show_alert=True)
        else:
            await q.answer("Передать можно только свой тикет.", show_alert=True)
        return ConversationHandler.END

    await q.answer()
    logger.info(
        "Ticket transferred ticket_id=%s from_admin=%s to_admin=%s",
        ticket_id, admin_id, new_admin_id,
    )

    await _notify_new_assignee_on_transfer(context, ticket, new_admin_id)

    await safe_edit_or_reply(
        update.effective_message,
        ui_ok_text(f"Тикет #{ticket_id} передан: {html_escape(new_admin_name)}"),
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⬅️ К панели", callback_data="ticket:list"),
        ]]),
    )

    await _notify_admins_brief(
        context,
        ticket_id,
        f"🔄 Тикет #{ticket_id} передан: {html_escape(new_admin_name)}",
        exclude={admin_id, new_admin_id},
    )

    return ConversationHandler.END
