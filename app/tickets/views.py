"""Ticket HTML presentation and inline keyboards."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from ..bot.guards import authorized_ids
from ..bot.help import render_support_contact
from ..bot.ui import (
    SEP,
    clip_text,
    format_dt_human,
    html_escape,
    urgency_label,
    wrap_as_codeblock_html,
)
from ..storage import get_user_meta_copy, product_settings_snapshot
from ..users.staff import staff_internal_identity, staff_internal_name
from .history import (
    MAX_TICKET_HISTORY_CHARS,
    MAX_TICKET_HISTORY_ITEM_CHARS,
    MAX_TICKET_HISTORY_ITEMS,
    _ticket_messages,
)
from .models import Ticket
from .operations import _safe_int, _ticket_can_user_reply, _ticket_is_assignee
from .workflow import ticket_context_data


def _ticket_status_label(ticket: Ticket | dict[str, Any]) -> str:
    status = str(ticket.get("status", "open"))
    if status == "in_progress":
        return "в работе"
    if status == "closed":
        return "закрыт"
    return "ожидает исполнителя"


def _ticket_assignee_is_active(ticket: Ticket | dict[str, Any]) -> bool:
    assignee_id = _safe_int(ticket.get("assignee_id"))
    return bool(assignee_id and assignee_id in set(authorized_ids(role_filter="admin")))


def _attachment_label(attachment: Mapping[str, Any]) -> str:
    attachment_type = str(attachment.get("type") or "")
    if attachment_type == "photo":
        return "📎 Фото"
    if attachment_type == "document":
        filename = str(attachment.get("filename") or "").strip()[:180]
        return f"📎 Файл: {filename}" if filename else "📎 Файл"
    return "📎 Вложение"


def _format_ticket_history(
    ticket: dict[str, Any],
    *,
    limit: int = MAX_TICKET_HISTORY_CHARS,
    public_view: bool = False,
) -> str:
    messages = _ticket_messages(ticket)[-MAX_TICKET_HISTORY_ITEMS:]
    if not messages:
        return "История пуста."

    blocks: list[str] = []
    for item in messages:
        sender_role = "Админ" if item.get("sender_role") == "admin" else "Пользователь"
        sender_name = str(item.get("sender_name") or sender_role)[:160]
        if item.get("sender_role") == "admin":
            is_system = sender_name.strip().casefold() == "система"
            if public_view and item.get("sender_signature_version") != 1 and not is_system:
                sender_name = "Техническая поддержка"
            elif not public_view and item.get("sender_id") is not None:
                admin_meta = get_user_meta_copy(_safe_int(item.get("sender_id")))
                if admin_meta:
                    sender_name = (
                        f"{sender_name} — {staff_internal_name(admin_meta)}, ID {admin_meta.get('user_id') or '-'}"
                    )
        timestamp = format_dt_human(item.get("ts"))
        text = str(item.get("text") or "")
        parts = [
            f"<b>{html_escape(sender_role)}:</b> {html_escape(sender_name)}",
            f"• Время: <code>{html_escape(timestamp)}</code>",
        ]
        if text:
            parts.append(wrap_as_codeblock_html(text, limit=MAX_TICKET_HISTORY_ITEM_CHARS))
        attachment = item.get("attachment")
        if isinstance(attachment, dict):
            parts.append(f"<i>{html_escape(_attachment_label(attachment))}</i>")
        blocks.append("\n".join(parts))

    dropped = 0
    while len(blocks) > 1 and sum(len(block) + 1 for block in blocks) > limit:
        blocks.pop(0)
        dropped += 1
    output = "\n".join(blocks)
    if dropped:
        output = f"<i>…{dropped} старых сообщений скрыто из-за лимита</i>\n{output}"
    return output


def _ticket_user_kb(ticket: dict[str, Any], uid: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    ticket_id = _safe_int(ticket.get("id"))
    if ticket_id and _ticket_can_user_reply(ticket, uid):
        rows.append(
            [
                InlineKeyboardButton(
                    "💬 Ответить администратору",
                    callback_data=f"ticket:userreply:{ticket_id}",
                ),
            ],
        )
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def _ticket_admin_kb(ticket: dict[str, Any], admin_uid: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    ticket_id = _safe_int(ticket.get("id"))
    status = str(ticket.get("status", "open"))
    assignee_id = _safe_int(ticket.get("assignee_id"))
    if ticket_id and status != "closed":
        if not assignee_id or not _ticket_assignee_is_active(ticket):
            rows.append(
                [InlineKeyboardButton("🫳 Взять в работу", callback_data=f"ticket:take:{ticket_id}")],
            )
        elif assignee_id == admin_uid:
            rows.append(
                [
                    InlineKeyboardButton(
                        "💬 Ответить пользователю",
                        callback_data=f"ticket:adminreply:{ticket_id}",
                    ),
                ],
            )
            rows.append(
                [InlineKeyboardButton("🔄 Передать", callback_data=f"ticket:transfer_init:{ticket_id}")],
            )
            rows.append(
                [InlineKeyboardButton("✅ Закрыть тикет", callback_data=f"ticket:close:{ticket_id}")],
            )
    rows.append([InlineKeyboardButton("📋 К панели", callback_data="ticket:list")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def _ticket_status_emoji(ticket: dict[str, Any]) -> str:
    if str(ticket.get("status", "open")) == "closed":
        return "✅"
    return "🟡" if _safe_int(ticket.get("assignee_id")) else "🔴"


def _ticket_header(ticket: dict[str, Any], *, title_prefix: str = "Тикет") -> str:
    ticket_number = f"#{ticket.get('id', '-')}"
    return (
        f"🎫 <b>{html_escape(title_prefix)} {html_escape(ticket_number)}</b> · "
        f"{_ticket_status_emoji(ticket)} {html_escape(_ticket_status_label(ticket))}"
    )


def _format_ticket_for_admin(
    ticket: dict[str, Any],
    admin_uid: int,
    *,
    event_line: str | None = None,
) -> str:
    assignee_name = str(ticket.get("assignee_name") or "-")[:160]
    user_name = str(ticket.get("user_name") or "пользователь")[:160]
    user_username = str(ticket.get("user_username") or "").strip()
    user_id = _safe_int(ticket.get("user_id"))
    user_meta = get_user_meta_copy(user_id) or {}
    nickname = str(user_meta.get("nickname") or user_name).strip()[:160]
    telegram_name = " ".join(
        str(part).strip()
        for part in (user_meta.get("first_name"), user_meta.get("last_name"))
        if str(part or "").strip()
    )
    user_username = str(user_meta.get("username") or user_username).strip()
    contact_email = str(user_meta.get("contact_email") or "").strip()
    subject = str(ticket.get("subject") or "-")[:300]
    lines = [
        _ticket_header(ticket),
        SEP,
        f"• Исполнитель: <b>{html_escape(assignee_name)}</b>",
        f"• Никнейм: <b>{html_escape(nickname)}</b>",
        f"• ID: <code>{html_escape(str(user_id))}</code>",
        f"• Срочность: {urgency_label(ticket.get('urgency'))}",
        f"• Тема: <code>{html_escape(subject)}</code>",
        f"• Создан: <code>{html_escape(format_dt_human(ticket.get('created_at')))}</code>",
    ]
    if user_username:
        lines.append(f"• Username: <code>@{html_escape(user_username.lstrip('@'))}</code>")
    if telegram_name:
        lines.append(f"• Имя Telegram: <b>{html_escape(telegram_name)}</b>")
    if contact_email:
        lines.append(f"• Резервная почта: <code>{html_escape(contact_email)}</code>")
    assignee_id = _safe_int(ticket.get("assignee_id"))
    if assignee_id:
        assignee_meta = get_user_meta_copy(assignee_id)
        if assignee_meta:
            identity = html_escape(staff_internal_identity(assignee_meta))
            lines.append(f"• Внутренний исполнитель: <code>{identity}</code>")
    if str(ticket.get("status", "open")) == "closed":
        closed_at = html_escape(format_dt_human(ticket.get("closed_at")))
        lines.append(f"• Закрыт: <code>{closed_at}</code>")
    if event_line:
        lines.extend(["", event_line])
    if _ticket_is_assignee(ticket, admin_uid):
        lines.extend(["", "<b>Доступ:</b> вы исполнитель этого тикета."])
    lines.extend(
        [
            SEP,
            "🕘 <b>История</b>",
            _format_ticket_history(ticket, public_view=False),
        ],
    )
    return "\n".join(lines)


def _format_ticket_for_user(
    ticket: dict[str, Any],
    *,
    event_line: str | None = None,
) -> str:
    assignee_name = str(ticket.get("assignee_name") or "ещё не назначен")[:160]
    if ticket.get("assignee_id") and ticket.get("assignee_signature_version") != 1:
        assignee_name = "Техническая поддержка"
    subject = str(ticket.get("subject") or "-")[:300]
    lines = [
        _ticket_header(ticket, title_prefix="Мой тикет"),
        SEP,
        f"• Исполнитель: <b>{html_escape(assignee_name)}</b>",
        f"• Срочность: {urgency_label(ticket.get('urgency'))}",
        f"• Тема: <code>{html_escape(subject)}</code>",
    ]
    if str(ticket.get("status", "open")) == "closed":
        closed_at = html_escape(format_dt_human(ticket.get("closed_at")))
        lines.append(f"• Закрыт: <code>{closed_at}</code>")
    if event_line:
        lines.extend(["", event_line])
    lines.extend(
        [
            SEP,
            "🕘 <b>История</b>",
            _format_ticket_history(ticket, public_view=True),
        ],
    )
    return "\n".join(lines) + render_support_contact(product_settings_snapshot())


def ticket_urgency_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔥 Критично", callback_data="ticket:p1")],
            [InlineKeyboardButton("⚠️ Важно", callback_data="ticket:p2")],
            [InlineKeyboardButton("📋 Обычно", callback_data="ticket:p3")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ],
    )


def ticket_input_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]],
    )


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
        ],
    )


def _ticket_preview_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    data = ticket_context_data(context)
    subject = data.get("ticket_subject", "-")
    urgency = str(data.get("ticket_urgency", "p3"))
    text = clip_text(str(data.get("ticket_text", "")), limit=3000)
    attachment = data.get("ticket_attachment")
    attachment_line = f"\n<i>{html_escape(_attachment_label(attachment))}</i>" if isinstance(attachment, dict) else ""
    return (
        f"🎫 <b>Новый тикет — проверка</b>\n{SEP}\n"
        f"• Тема: <code>{html_escape(str(subject))}</code>\n"
        f"• Срочность: {urgency_label(urgency)}\n\n"
        "Описание:\n" + wrap_as_codeblock_html(text, limit=2400) + attachment_line + "\n\nВыберите действие:"
    )


__all__ = ["ticket_confirm_kb", "ticket_input_kb", "ticket_urgency_kb"]
