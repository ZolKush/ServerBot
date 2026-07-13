from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ..config import TZ, logger
from ..models import Attachment, Ticket, TicketMessage
from ..services.outbox import message_payload
from ..storage import (
    ImportantData,
    UpdateAborted,
    enqueue_important_outbox,
    get_admin_name_by_id,
    get_all_tickets_snapshot,
    get_ticket_copy,
    get_user_open_tickets,
    make_outbox_event,
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
MAX_TRANSFER_ATTACHMENTS = 3
ACTIVE_PAGE_SIZE = 12
ARCHIVE_PAGE_SIZE = 10


class TicketFlowError(RuntimeError):
    pass


def _now_iso() -> str:
    return datetime.now(TZ).isoformat()


def _safe_int(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


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
        _safe_int(ticket.get("user_id")) == uid
        and str(ticket.get("status", "open")) != "closed"
        and bool(ticket.get("assignee_id"))
        and bool(ticket.get("user_reply_allowed", False))
    )


def _ticket_is_assignee(ticket: Ticket, uid: int) -> bool:
    return _safe_int(ticket.get("assignee_id")) == uid


def _ticket_assignee_is_active(ticket: Ticket) -> bool:
    try:
        assignee_id = int(ticket.get("assignee_id", 0) or 0)
    except (TypeError, ValueError):
        return False
    return bool(assignee_id and assignee_id in set(authorized_ids(role_filter="admin")))


def _ticket_messages(ticket: Ticket) -> list[TicketMessage]:
    items = ticket.get("messages", [])
    return [dict(x) for x in items if isinstance(x, dict)] if isinstance(items, list) else []


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
        first = messages[0]
        if first.get("kind") == "initial":
            messages = [first, *messages[-(MAX_TICKET_MESSAGES_STORED - 1) :]]
        else:
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
        name = str(attachment.get("filename") or "").strip()[:180]
        return f"📎 Файл: {name}" if name else "📎 Файл"
    return "📎 Вложение"


def _format_ticket_history(ticket: dict[str, Any], *, limit: int = MAX_TICKET_HISTORY_CHARS) -> str:
    messages = _ticket_messages(ticket)[-MAX_TICKET_HISTORY_ITEMS:]
    if not messages:
        return "История пуста."
    blocks: list[str] = []
    for item in messages:
        parts: list[str] = []
        sender_role = "Админ" if item.get("sender_role") == "admin" else "Пользователь"
        sender_name = str(item.get("sender_name") or sender_role)[:160]
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
    ticket_id = _safe_int(ticket.get("id"))
    if ticket_id and _ticket_can_user_reply(ticket, uid):
        rows.append([InlineKeyboardButton("💬 Ответить администратору", callback_data=f"ticket:userreply:{ticket_id}")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def _ticket_admin_kb(ticket: dict[str, Any], admin_uid: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    ticket_id = _safe_int(ticket.get("id"))
    status = str(ticket.get("status", "open"))
    assignee_id = _safe_int(ticket.get("assignee_id"))
    if ticket_id and status != "closed":
        if not assignee_id or not _ticket_assignee_is_active(ticket):
            rows.append([InlineKeyboardButton("🫳 Взять в работу", callback_data=f"ticket:take:{ticket_id}")])
        elif assignee_id == admin_uid:
            rows.append(
                [InlineKeyboardButton("💬 Ответить пользователю", callback_data=f"ticket:adminreply:{ticket_id}")]
            )
            rows.append([InlineKeyboardButton("🔄 Передать", callback_data=f"ticket:transfer_init:{ticket_id}")])
            rows.append([InlineKeyboardButton("✅ Закрыть тикет", callback_data=f"ticket:close:{ticket_id}")])
    rows.append([InlineKeyboardButton("📋 К панели", callback_data="ticket:list")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def _ticket_status_emoji(ticket: dict[str, Any]) -> str:
    if str(ticket.get("status", "open")) == "closed":
        return "✅"
    return "🟡" if _safe_int(ticket.get("assignee_id")) else "🔴"


def _ticket_header(ticket: dict[str, Any], *, title_prefix: str = "Тикет") -> str:
    ticket_no = f"#{ticket.get('id', '-')}"
    status_label = _ticket_status_label(ticket)
    return f"🎫 <b>{html_escape(title_prefix)} {html_escape(ticket_no)}</b> · {_ticket_status_emoji(ticket)} {html_escape(status_label)}"


def _format_ticket_for_admin(ticket: dict[str, Any], admin_uid: int, *, event_line: str | None = None) -> str:
    assignee_name = str(ticket.get("assignee_name") or "-")[:160]
    user_name = str(ticket.get("user_name") or "пользователь")[:160]
    user_username = str(ticket.get("user_username") or "").strip()
    user_id = _safe_int(ticket.get("user_id"))
    subject = str(ticket.get("subject") or "-")[:300]
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
    assignee_name = str(ticket.get("assignee_name") or "ещё не назначен")[:160]
    subject = str(ticket.get("subject") or "-")[:300]
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


def _markup_descriptor(markup: InlineKeyboardMarkup | None) -> list[list[dict[str, str]]]:
    if markup is None:
        return []
    rows: list[list[dict[str, str]]] = []
    for row in markup.inline_keyboard:
        encoded: list[dict[str, str]] = []
        for button in row:
            if button.callback_data:
                encoded.append({"text": button.text, "callback_data": str(button.callback_data)})
            elif button.url:
                encoded.append({"text": button.text, "url": str(button.url)})
        if encoded:
            rows.append(encoded)
    return rows


def _queue_ticket_text(
    cfg: ImportantData,
    *,
    uid: int,
    text: str,
    markup: InlineKeyboardMarkup | None,
    kind: str,
) -> None:
    enqueue_important_outbox(
        cfg,
        make_outbox_event(
            kind=kind,
            recipient_ids=[uid],
            payload=message_payload(text, reply_markup=_markup_descriptor(markup)),
        ),
    )


def _queue_ticket_attachment(
    cfg: ImportantData,
    *,
    uid: int,
    attachment: dict[str, Any] | None,
    kind: str,
) -> None:
    if not isinstance(attachment, dict):
        return
    attachment_type = str(attachment.get("type") or "")
    file_id = str(attachment.get("file_id") or "")
    if attachment_type not in {"photo", "document"} or not file_id:
        return
    enqueue_important_outbox(
        cfg,
        make_outbox_event(
            kind=kind,
            recipient_ids=[uid],
            payload={"method": f"send_{attachment_type}", "file_id": file_id},
        ),
    )


def _queue_admin_full_notifications(
    cfg: ImportantData,
    ticket: dict[str, Any],
    admin_ids: list[int],
    *,
    event_line: str,
    kind: str,
    attachment_limit: int = 1,
) -> None:
    attachments = [
        dict(item["attachment"]) for item in _ticket_messages(ticket) if isinstance(item.get("attachment"), dict)
    ][-max(0, attachment_limit) :]
    for admin_id in admin_ids:
        _queue_ticket_text(
            cfg,
            uid=admin_id,
            text=_format_ticket_for_admin(ticket, admin_id, event_line=event_line),
            markup=_ticket_admin_kb(ticket, admin_id),
            kind=kind,
        )
        for attachment in attachments:
            _queue_ticket_attachment(
                cfg,
                uid=admin_id,
                attachment=attachment,
                kind=f"{kind}_attachment",
            )


def _queue_user_notification(
    cfg: ImportantData,
    ticket: dict[str, Any],
    *,
    event_line: str,
    kind: str,
    include_attachment: bool = True,
) -> None:
    try:
        uid = int(ticket.get("user_id", 0) or 0)
    except (TypeError, ValueError):
        return
    if not uid:
        return
    _queue_ticket_text(
        cfg,
        uid=uid,
        text=_format_ticket_for_user(ticket, event_line=event_line),
        markup=_ticket_user_kb(ticket, uid),
        kind=kind,
    )
    if include_attachment:
        _queue_ticket_attachment(
            cfg,
            uid=uid,
            attachment=_last_attachment(ticket),
            kind=f"{kind}_attachment",
        )


async def _ticket_update(ticket_id: int, mutator, outbox_builder=None) -> dict[str, Any]:
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
        if outbox_builder is not None:
            outbox_builder(cfg, dict(updated))
        return dict(updated)

    try:
        return await update_important_data(_apply)
    except UpdateAborted:
        raise TicketFlowError(flow_state.get("error", "ticket_error")) from None


async def release_orphaned_tickets(context: ContextTypes.DEFAULT_TYPE) -> None:
    active_admin_ids = authorized_ids(role_filter="admin")
    active_set = set(active_admin_ids)

    def _release(cfg: ImportantData) -> int:
        tickets = dict(cfg.tickets or {})
        released = 0
        for key, raw in list(tickets.items()):
            if not isinstance(raw, dict) or str(raw.get("status", "open")) == "closed":
                continue
            try:
                assignee_id = int(raw.get("assignee_id", 0) or 0)
            except (TypeError, ValueError):
                assignee_id = 0
            if not assignee_id or assignee_id in active_set:
                continue
            updated = dict(raw)
            old_name = str(updated.get("assignee_name") or assignee_id)[:160]
            updated["assignee_id"] = None
            updated["assignee_name"] = None
            updated["status"] = "open"
            updated["user_reply_allowed"] = False
            updated = _append_ticket_message(
                updated,
                sender_role="admin",
                sender_id=None,
                sender_name="Система",
                text=f"Исполнитель {old_name} недоступен; тикет возвращён в общую очередь",
                kind="assignee_released",
            )
            tickets[key] = updated
            _queue_user_notification(
                cfg,
                updated,
                event_line="⚠️ <b>Исполнитель временно недоступен; тикет возвращён в очередь</b>",
                kind="ticket_orphaned_user",
                include_attachment=False,
            )
            _queue_admin_full_notifications(
                cfg,
                updated,
                active_admin_ids,
                event_line="⚠️ <b>Тикет освобождён: предыдущий исполнитель недоступен</b>",
                kind="ticket_orphaned_admin",
                attachment_limit=0,
            )
            released += 1
        cfg.tickets = tickets
        if not released:
            raise UpdateAborted()
        return released

    try:
        released = await update_important_data(_release)
    except UpdateAborted:
        return
    if released:
        logger.warning("Released orphaned tickets: %s", released, extra={"action": "ticket_orphan_release"})


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
        + wrap_as_codeblock_html(text_for_preview, limit=2400)
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
            await msg.reply_text(
                _ticket_preview_text(context), parse_mode=ParseMode.HTML, reply_markup=ticket_confirm_kb()
            )
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
                "Описание слишком короткое. Дайте больше деталей (>= 10 символов) или приложите фото/файл с пояснением."
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
    in_progress_raw = context.user_data.get("ticket_send_in_progress")
    if in_progress_raw:
        try:
            started = datetime.fromisoformat(str(in_progress_raw))
            if started.tzinfo is None:
                started = started.replace(tzinfo=TZ)
            elapsed = (datetime.now(TZ) - started.astimezone(TZ)).total_seconds()
            if 0 <= elapsed < 120:
                await q.edit_message_text(ui_warn_text("тикет уже отправляется, подождите..."))
                return ConversationHandler.END
        except (TypeError, ValueError):
            pass
        context.user_data.pop("ticket_send_in_progress", None)
    context.user_data["ticket_send_in_progress"] = _now_iso()

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

        attachment = context.user_data.get("ticket_attachment")
        attachment_data = attachment if isinstance(attachment, dict) else None
        create_conflict = False

        def _create(cfg: ImportantData) -> dict[str, Any]:
            nonlocal create_conflict
            tickets = dict(cfg.tickets or {})
            for raw_ticket in tickets.values():
                if not isinstance(raw_ticket, dict):
                    continue
                try:
                    same_user = int(raw_ticket.get("user_id", 0) or 0) == uid
                except (TypeError, ValueError):
                    same_user = False
                if same_user and str(raw_ticket.get("status", "open")) != "closed":
                    create_conflict = True
                    raise UpdateAborted()
            cfg.tickets_seq = max(int(cfg.tickets_seq or 0), 0) + 1
            created = _build_ticket_record(
                cfg.tickets_seq,
                user_id=uid,
                user_name=author_name,
                user_username=author_username,
                subject=subj,
                urgency=urg,
                text=txt,
                attachment=attachment_data,
            )
            tickets[str(cfg.tickets_seq)] = created
            cfg.tickets = tickets
            _queue_admin_full_notifications(
                cfg,
                created,
                admins,
                event_line="🆕 <b>Новый тикет ожидает исполнителя</b>",
                kind="ticket_created_admin",
            )
            _queue_user_notification(
                cfg,
                created,
                event_line="✅ <b>Тикет создан и отправлен администраторам</b>",
                kind="ticket_created_user",
                include_attachment=False,
            )
            return created

        try:
            ticket = await update_important_data(_create)
        except UpdateAborted:
            _clear_ticket_ctx(context)
            await q.edit_message_text(
                ui_warn_text("у вас уже есть незакрытый тикет." if create_conflict else "создание тикета отменено.")
            )
            return ConversationHandler.END
        ticket_id = int(ticket["id"])
        logger.info("Ticket created ticket_id=%s user_id=%s urgency=%s subject=%s", ticket_id, uid, urg, subj)
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
    active_admins = authorized_ids(role_filter="admin")

    try:

        def _assign(ticket: dict[str, Any]) -> dict[str, Any]:
            if str(ticket.get("status", "open")) == "closed":
                raise TicketFlowError("ticket_closed")
            assignee_id = _safe_int(ticket.get("assignee_id"))
            if assignee_id and assignee_id != admin_id and assignee_id in active_admins:
                raise TicketFlowError("ticket_taken")
            if assignee_id == admin_id:
                raise TicketFlowError("already_assigned")
            updated = dict(ticket)
            was_orphaned = bool(assignee_id and assignee_id not in active_admins)
            updated["assignee_id"] = admin_id
            updated["assignee_name"] = admin_name
            updated["status"] = "in_progress"
            updated["updated_at"] = _now_iso()
            if was_orphaned:
                updated = _append_ticket_message(
                    updated,
                    sender_role="admin",
                    sender_id=admin_id,
                    sender_name=admin_name,
                    text="Тикет переназначен после выхода предыдущего исполнителя",
                    kind="reclaim",
                )
            return updated

        def _queue_assignment(cfg: ImportantData, updated: dict[str, Any]) -> None:
            for other_id in [aid for aid in active_admins if aid != admin_id]:
                _queue_ticket_text(
                    cfg,
                    uid=other_id,
                    text=f"🫳 Тикет #{ticket_id} взят в работу: {html_escape(admin_name)}",
                    markup=None,
                    kind="ticket_assigned_admin",
                )
            _queue_user_notification(
                cfg,
                updated,
                event_line=f"👤 <b>Исполнитель назначен:</b> {html_escape(admin_name)}",
                kind="ticket_assigned_user",
                include_attachment=False,
            )

        ticket = await _ticket_update(ticket_id, _assign, _queue_assignment)
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
        logger.warning(
            "Ticket admin reply start failed ticket_id=%s admin_id=%s reason=ticket_not_found", ticket_id, admin_id
        )
        await q.answer("Тикет не найден.", show_alert=True)
        return ConversationHandler.END
    if str(ticket.get("status", "open")) == "closed":
        logger.warning(
            "Ticket admin reply start denied ticket_id=%s admin_id=%s reason=ticket_closed", ticket_id, admin_id
        )
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

        def _queue_reply(cfg: ImportantData, updated: dict[str, Any]) -> None:
            _queue_user_notification(
                cfg,
                updated,
                event_line="💬 <b>Администратор ответил на ваш тикет</b>",
                kind="ticket_admin_reply",
            )

        ticket = await _ticket_update(ticket_id, _reply, _queue_reply)
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

        def _queue_reply(cfg: ImportantData, updated: dict[str, Any]) -> None:
            try:
                assignee_id = int(updated.get("assignee_id", 0) or 0)
            except (TypeError, ValueError):
                assignee_id = 0
            if assignee_id:
                _queue_admin_full_notifications(
                    cfg,
                    updated,
                    [assignee_id],
                    event_line="💬 <b>Пользователь ответил по тикету</b>",
                    kind="ticket_user_reply",
                )

        ticket = await _ticket_update(ticket_id, _reply, _queue_reply)
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
    other_admin_ids = [aid for aid in authorized_ids(role_filter="admin") if aid != admin_id]
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

        def _queue_close(cfg: ImportantData, updated: dict[str, Any]) -> None:
            _queue_user_notification(
                cfg,
                updated,
                event_line=f"✅ <b>Тикет закрыт администратором:</b> {html_escape(admin_name)}",
                kind="ticket_closed_user",
                include_attachment=False,
            )
            for other_id in other_admin_ids:
                _queue_ticket_text(
                    cfg,
                    uid=other_id,
                    text=f"✅ Тикет #{ticket_id} закрыт: {html_escape(admin_name)}",
                    markup=None,
                    kind="ticket_closed_admin",
                )

        ticket = await _ticket_update(ticket_id, _close, _queue_close)
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
    await q.edit_message_text(
        _format_ticket_for_admin(ticket, admin_id, event_line="✅ <b>Вы закрыли тикет</b>"),
        parse_mode=ParseMode.HTML,
        reply_markup=_ticket_admin_kb(ticket, admin_id),
    )


async def _show_ticket_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0) -> int:
    uid = get_user_id(update)
    if uid is None:
        return ConversationHandler.END

    active: list[dict[str, Any]] = []
    for ticket in get_all_tickets_snapshot().values():
        if str(ticket.get("status", "open")) != "closed":
            active.append(ticket)

    def _sort_key(ticket: dict[str, Any]) -> tuple[int, int]:
        try:
            ticket_id = int(ticket.get("id", 0) or 0)
        except (TypeError, ValueError):
            ticket_id = 0
        try:
            assignee_id = int(ticket.get("assignee_id", 0) or 0)
        except (TypeError, ValueError):
            assignee_id = 0
        group = 0 if not assignee_id else (1 if assignee_id == uid else 2)
        return group, -ticket_id

    active.sort(key=_sort_key)
    total = len(active)
    total_pages = max(1, (total + ACTIVE_PAGE_SIZE - 1) // ACTIVE_PAGE_SIZE)
    page = max(0, min(int(page), total_pages - 1))
    page_items = active[page * ACTIVE_PAGE_SIZE : (page + 1) * ACTIVE_PAGE_SIZE]
    counts = {"unpicked": 0, "mine": 0, "others": 0}
    for ticket in active:
        try:
            assignee_id = int(ticket.get("assignee_id", 0) or 0)
        except (TypeError, ValueError):
            assignee_id = 0
        key = "unpicked" if not assignee_id else ("mine" if assignee_id == uid else "others")
        counts[key] += 1

    lines = [
        "🎫 <b>Тикеты — панель</b>",
        f"Страница {page + 1}/{total_pages} · активных: {total}",
        f"🔴 без исполнителя: {counts['unpicked']} · 🟡 моих: {counts['mine']} · 🟠 у других: {counts['others']}",
        SEP,
    ]
    rows: list[list[InlineKeyboardButton]] = []

    if not page_items:
        lines.append("Активных тикетов нет.")
    for ticket in page_items:
        try:
            ticket_id = int(ticket.get("id", 0) or 0)
            assignee_id = int(ticket.get("assignee_id", 0) or 0)
        except (TypeError, ValueError):
            continue
        subject = clip_text(str(ticket.get("subject") or "-"), limit=35)
        urgency = ticket.get("urgency", "p3")
        if not assignee_id:
            icon, suffix = "🔴", "без исполнителя"
        elif assignee_id == uid:
            icon, suffix = "🟡", "у вас"
        else:
            icon, suffix = "🟠", f"у {ticket.get('assignee_name') or assignee_id}"
        lines.append(
            f"{icon} <b>#{ticket_id}</b> {urgency_emoji(urgency)} {html_escape(subject)} · {html_escape(str(suffix))}"
        )
        rows.append(
            [
                InlineKeyboardButton(
                    f"{icon} #{ticket_id} {urgency_emoji(urgency)} {subject}"[:60],
                    callback_data=f"ticket:open:{ticket_id}",
                )
            ]
        )

    if total_pages > 1:
        rows.append(pager_row("ticket:list:", page, total_pages))

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
    page = 0
    if q:
        match = re.fullmatch(r"ticket:list(?::(\d+))?", q.data or "")
        if match and match.group(1):
            page = int(match.group(1))
    return await _show_ticket_dashboard(update, context, page=page)


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
    items = closed[start : start + ARCHIVE_PAGE_SIZE]

    lines = [
        "🗂 <b>Тикеты — архив</b>",
        f"Страница {page + 1}/{total_pages} · всего {total} {plural_ru(total, 'закрытый тикет', 'закрытых тикета', 'закрытых тикетов')}",
        SEP,
    ]
    rows: list[list[InlineKeyboardButton]] = []

    for t in items:
        tid = _safe_int(t.get("id"))
        if not tid:
            continue
        subj = clip_text(str(t.get("subject") or "-"), limit=35)
        urg = t.get("urgency", "p3")
        user_name = str(t.get("user_name") or "-")[:160]
        closed_by = str(t.get("closed_by_name") or "-")[:160]
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
        rows.append([InlineKeyboardButton(f"👤 {name}"[:60], callback_data=f"ticket:transfer_to:{ticket_id}:{aid}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"ticket:open:{ticket_id}")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])

    text = f"🔄 <b>Тикет #{ticket_id} — передача</b>\n{SEP}\nВыберите администратора для передачи тикета:"
    await safe_edit_or_reply(update.effective_message, text, reply_markup=InlineKeyboardMarkup(rows))
    return ConversationHandler.END


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

    active_admin_ids = authorized_ids(role_filter="admin")
    new_admin_name = get_admin_name_by_id(new_admin_id)
    if not new_admin_name or new_admin_id not in active_admin_ids:
        await q.answer("Администратор не найден.", show_alert=True)
        return ConversationHandler.END

    admin_name = display_name(update)

    try:

        def _transfer(ticket: dict[str, Any]) -> dict[str, Any]:
            if str(ticket.get("status", "open")) == "closed":
                raise TicketFlowError("ticket_closed")
            if not _ticket_is_assignee(ticket, admin_id):
                raise TicketFlowError("not_assignee")
            if get_admin_name_by_id(new_admin_id) is None:
                raise TicketFlowError("target_inactive")
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

        def _queue_transfer(cfg: ImportantData, updated: dict[str, Any]) -> None:
            _queue_admin_full_notifications(
                cfg,
                updated,
                [new_admin_id],
                event_line="🔄 <b>Тикет передан вам</b>",
                kind="ticket_transferred_assignee",
                attachment_limit=MAX_TRANSFER_ATTACHMENTS,
            )
            _queue_user_notification(
                cfg,
                updated,
                event_line=f"🔄 <b>Новый исполнитель:</b> {html_escape(new_admin_name)}",
                kind="ticket_transferred_user",
                include_attachment=False,
            )
            for other_id in [aid for aid in active_admin_ids if aid not in {admin_id, new_admin_id}]:
                _queue_ticket_text(
                    cfg,
                    uid=other_id,
                    text=f"🔄 Тикет #{ticket_id} передан: {html_escape(new_admin_name)}",
                    markup=None,
                    kind="ticket_transferred_admin",
                )

        await _ticket_update(ticket_id, _transfer, _queue_transfer)
    except TicketFlowError as e:
        code = str(e)
        logger.warning("Ticket transfer failed ticket_id=%s admin_id=%s reason=%s", ticket_id, admin_id, code)
        if code == "ticket_closed":
            await q.answer("Тикет уже закрыт.", show_alert=True)
        elif code == "ticket_not_found":
            await q.answer("Тикет не найден.", show_alert=True)
        elif code == "target_inactive":
            await q.answer("Новый исполнитель уже вышел из бота.", show_alert=True)
        else:
            await q.answer("Передать можно только свой тикет.", show_alert=True)
        return ConversationHandler.END

    await q.answer()
    logger.info(
        "Ticket transferred ticket_id=%s from_admin=%s to_admin=%s",
        ticket_id,
        admin_id,
        new_admin_id,
    )

    await safe_edit_or_reply(
        update.effective_message,
        ui_ok_text(f"Тикет #{ticket_id} передан: {html_escape(new_admin_name)}"),
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("⬅️ К панели", callback_data="ticket:list"),
                ]
            ]
        ),
    )

    return ConversationHandler.END
