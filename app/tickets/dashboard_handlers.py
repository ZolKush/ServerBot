"""Staff ticket dashboard, archive and detail navigation."""

from __future__ import annotations

import re
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, ConversationHandler

from ..bot.guards import get_user_id, require_admin
from ..bot.ui import (
    SEP,
    clip_text,
    format_dt_human,
    html_escape,
    pager_row,
    plural_ru,
    safe_edit_or_reply,
    urgency_emoji,
)
from ..messaging.message_cleanup import record_navigation_result
from ..storage import get_all_tickets_snapshot, get_ticket_copy
from .operations import _safe_int
from .routes import ACTIVE_PAGE_SIZE, ARCHIVE_PAGE_SIZE
from .views import _format_ticket_for_admin, _ticket_admin_kb
from .workflow import _clear_ticket_ctx


async def _show_ticket_dashboard(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    page: int = 0,
) -> int:
    uid = get_user_id(update)
    if uid is None:
        return ConversationHandler.END

    active = [ticket for ticket in get_all_tickets_snapshot().values() if str(ticket.get("status", "open")) != "closed"]

    def sort_key(ticket: dict[str, Any]) -> tuple[int, int]:
        ticket_id = _safe_int(ticket.get("id"))
        assignee_id = _safe_int(ticket.get("assignee_id"))
        group = 0 if not assignee_id else (1 if assignee_id == uid else 2)
        return group, -ticket_id

    active.sort(key=sort_key)
    total = len(active)
    total_pages = max(1, (total + ACTIVE_PAGE_SIZE - 1) // ACTIVE_PAGE_SIZE)
    page = max(0, min(int(page), total_pages - 1))
    page_items = active[page * ACTIVE_PAGE_SIZE : (page + 1) * ACTIVE_PAGE_SIZE]
    counts = {"unpicked": 0, "mine": 0, "others": 0}
    for ticket in active:
        assignee_id = _safe_int(ticket.get("assignee_id"))
        group = "unpicked" if not assignee_id else ("mine" if assignee_id == uid else "others")
        counts[group] += 1

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
        ticket_id = _safe_int(ticket.get("id"))
        if not ticket_id:
            continue
        assignee_id = _safe_int(ticket.get("assignee_id"))
        subject = clip_text(str(ticket.get("subject") or "-"), limit=35)
        urgency = ticket.get("urgency", "p3")
        if not assignee_id:
            icon, suffix = "🔴", "без исполнителя"
        elif assignee_id == uid:
            icon, suffix = "🟡", "у вас"
        else:
            icon, suffix = "🟠", f"у {ticket.get('assignee_name') or assignee_id}"
        lines.append(
            f"{icon} <b>#{ticket_id}</b> {urgency_emoji(urgency)} {html_escape(subject)} · {html_escape(str(suffix))}",
        )
        rows.append(
            [
                InlineKeyboardButton(
                    f"{icon} #{ticket_id} {urgency_emoji(urgency)} {subject}"[:60],
                    callback_data=f"ticket:open:{ticket_id}",
                ),
            ],
        )

    if total_pages > 1:
        rows.append(pager_row("ticket:list:", page, total_pages))
    rows.append([InlineKeyboardButton("🗂 Архив", callback_data="ticket:archive")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])

    result = await safe_edit_or_reply(
        update.effective_message,
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(rows),
    )
    await record_navigation_result(update, result)
    return ConversationHandler.END


@require_admin
async def ticket_list_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query:
        await query.answer()
    _clear_ticket_ctx(context)
    page = 0
    if query:
        match = re.fullmatch(r"ticket:list(?::(\d+))?", query.data or "")
        if match and match.group(1):
            page = int(match.group(1))
    return await _show_ticket_dashboard(update, context, page=page)


@require_admin
async def ticket_open_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    admin_id = get_user_id(update)
    if admin_id is None:
        await query.answer()
        return ConversationHandler.END

    parts = (query.data or "").split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        await query.answer()
        return ConversationHandler.END
    ticket_id = int(parts[2])
    if not ticket_id:
        await query.answer()
        return ConversationHandler.END

    ticket = get_ticket_copy(ticket_id)
    if not ticket:
        await query.answer("Тикет не найден.", show_alert=True)
        return ConversationHandler.END

    await query.answer()
    _clear_ticket_ctx(context)
    await safe_edit_or_reply(
        update.effective_message,
        _format_ticket_for_admin(ticket, admin_id),
        reply_markup=_ticket_admin_kb(ticket, admin_id),
    )
    return ConversationHandler.END


async def _show_archive_page(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    page: int,
) -> int:
    del context
    closed = sorted(
        [ticket for ticket in get_all_tickets_snapshot().values() if str(ticket.get("status", "open")) == "closed"],
        key=lambda ticket: str(ticket.get("closed_at") or ticket.get("updated_at") or ""),
        reverse=True,
    )
    total = len(closed)
    total_pages = max(1, (total + ARCHIVE_PAGE_SIZE - 1) // ARCHIVE_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * ARCHIVE_PAGE_SIZE
    items = closed[start : start + ARCHIVE_PAGE_SIZE]

    lines = [
        "🗂 <b>Тикеты — архив</b>",
        f"Страница {page + 1}/{total_pages} · всего {total} "
        f"{plural_ru(total, 'закрытый тикет', 'закрытых тикета', 'закрытых тикетов')}",
        SEP,
    ]
    rows: list[list[InlineKeyboardButton]] = []
    for ticket in items:
        ticket_id = _safe_int(ticket.get("id"))
        if not ticket_id:
            continue
        subject = clip_text(str(ticket.get("subject") or "-"), limit=35)
        urgency = ticket.get("urgency", "p3")
        user_name = str(ticket.get("user_name") or "-")[:160]
        closed_by = str(ticket.get("closed_by_name") or "-")[:160]
        closed_at = format_dt_human(ticket.get("closed_at"))
        lines.append(
            f"• <b>#{ticket_id}</b> {urgency_emoji(urgency)} {html_escape(subject)}\n"
            f"  {html_escape(user_name)} → закрыл {html_escape(closed_by)} | "
            f"{html_escape(closed_at)}",
        )
        rows.append(
            [
                InlineKeyboardButton(
                    f"#{ticket_id} {urgency_emoji(urgency)} {subject}",
                    callback_data=f"ticket:open:{ticket_id}",
                ),
            ],
        )

    if not items:
        lines.append("Архив пуст.")
    if total_pages > 1:
        rows.append(pager_row("ticket:archive_page:", page, total_pages))
    rows.append([InlineKeyboardButton("⬅️ К панели", callback_data="ticket:list")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])

    await safe_edit_or_reply(
        update.effective_message,
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(rows),
    )
    return ConversationHandler.END


@require_admin
async def ticket_archive_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query:
        await query.answer()
    return await _show_archive_page(update, context, page=0)


@require_admin
async def ticket_archive_page_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()
    parts = (query.data or "").split(":")
    if len(parts) != 3 or not parts[2].isdigit():
        return ConversationHandler.END
    return await _show_archive_page(update, context, page=int(parts[2]))


__all__ = [
    "ticket_archive_cb",
    "ticket_archive_page_cb",
    "ticket_list_cb",
    "ticket_open_cb",
]
