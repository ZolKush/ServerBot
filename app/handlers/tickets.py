from datetime import datetime
from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ..config import TZ
from ..storage import ImportantData, update_important_data
from .common import (
    authorized_ids,
    display_name,
    get_user_id,
    html_escape,
    require_auth,
    send_to_many,
    wrap_as_codeblock_html,
)

TICKET_SUBJECT, TICKET_URGENCY, TICKET_TEXT, TICKET_CONFIRM = range(4)


def ticket_urgency_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("P1 (критично)", callback_data="ticket:p1")],
            [InlineKeyboardButton("P2 (важно)", callback_data="ticket:p2")],
            [InlineKeyboardButton("P3 (обычно)", callback_data="ticket:p3")],
        ]
    )


def ticket_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ Отправить", callback_data="ticket:send")]])


@require_auth
async def ticket_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg:
        await msg.reply_text("Тема тикета (кратко).\nДля отмены: /cancel")
    return TICKET_SUBJECT


@require_auth
async def ticket_subject(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    subj = (msg.text if msg else "").strip()
    if len(subj) < 3:
        if msg:
            await msg.reply_text("Тема слишком короткая. Введите минимум 3 символа.")
        return TICKET_SUBJECT

    context.user_data["ticket_subject"] = subj
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
    await q.edit_message_text("Опишите проблему (лучше одним сообщением). Для отмены: /cancel")
    return TICKET_TEXT


@require_auth
async def ticket_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    text = (msg.text if msg else "").strip()
    if len(text) < 10:
        if msg:
            await msg.reply_text("Описание слишком короткое. Дайте больше деталей (>= 10 символов).")
        return TICKET_TEXT

    context.user_data["ticket_text"] = text

    subj = context.user_data.get("ticket_subject", "-")
    urg = str(context.user_data.get("ticket_urgency", "p3")).upper()

    preview = (
        "<b>Проверьте тикет</b>\n"
        f"• Тема: <code>{html_escape(str(subj))}</code>\n"
        f"• Срочность: <code>{html_escape(str(urg))}</code>\n\n"
        "Описание:\n"
        + wrap_as_codeblock_html(str(text))
    )
    if msg:
        await msg.reply_text(preview, parse_mode=ParseMode.HTML, reply_markup=ticket_confirm_kb())
    return TICKET_CONFIRM


@require_auth
async def ticket_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    if q.data != "ticket:send":
        return ConversationHandler.END

    def _next_ticket(cfg: ImportantData) -> int:
        cfg.tickets_seq += 1
        return cfg.tickets_seq

    ticket_id = await update_important_data(_next_ticket)

    uid = get_user_id(update)
    author_name = display_name(update)
    subj = context.user_data.get("ticket_subject", "-")
    urg = str(context.user_data.get("ticket_urgency", "p3")).upper()
    txt = context.user_data.get("ticket_text", "-")
    created = datetime.now(TZ).strftime("%d.%m.%Y %H:%M")

    msg_text = (
        f"🎫 <b>Новый тикет #{ticket_id}</b>\n"
        f"• От: <b>{html_escape(author_name)}</b> (<code>{html_escape(str(uid) if uid is not None else '-')}</code>)\n"
        f"• Время: <code>{html_escape(created)}</code>\n"
        f"• Срочность: <code>{html_escape(str(urg))}</code>\n"
        f"• Тема: <code>{html_escape(str(subj))}</code>\n\n"
        f"Описание:\n{wrap_as_codeblock_html(str(txt))}"
    )

    admins = authorized_ids(role_filter="admin")
    if not admins:
        await q.edit_message_text("Тикет не отправлен: нет авторизованных администраторов.")
        return ConversationHandler.END

    ok, fail = await send_to_many(context, admins, msg_text)
    await q.edit_message_text(f"Тикет отправлен админам ✅ (ok={ok}, fail={fail})")
    return ConversationHandler.END
