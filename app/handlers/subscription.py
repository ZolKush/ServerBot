from __future__ import annotations

from io import BytesIO
from typing import Any

from telegram import InputFile, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..logging_setup import logger
from ..services.outbox import document_text_payload, message_payload
from ..storage import get_user_meta_copy
from .common import (
    format_dt_human,
    get_user_id,
    html_escape,
    main_menu_inline_kb,
    require_auth,
    ui_info_text,
    wrap_as_codeblock_html,
)

SUBSCRIPTION_TEXT_KEY = "subscription_text"
SUBSCRIPTION_UPDATED_AT_KEY = "subscription_updated_at"
SUBSCRIPTION_UPDATED_BY_ID_KEY = "subscription_updated_by_id"
SUBSCRIPTION_UPDATED_BY_NAME_KEY = "subscription_updated_by_name"
_INLINE_SUBSCRIPTION_LIMIT = 3000
MAX_SUBSCRIPTION_BYTES = 1_000_000


def get_subscription_text(meta: dict[str, Any] | None) -> str:
    if not meta:
        return ""
    return str(meta.get(SUBSCRIPTION_TEXT_KEY, "") or "")


def has_subscription(meta: dict[str, Any] | None) -> bool:
    return bool(get_subscription_text(meta).strip())


def _subscription_intro(meta: dict[str, Any]) -> str:
    updated_at = str(meta.get(SUBSCRIPTION_UPDATED_AT_KEY, "") or "").strip()
    lines = ["📦 <b>Моя подписка</b>", "Администрация назначила вам подписку."]
    if updated_at:
        lines.append(f"• Обновлена: <code>{html_escape(format_dt_human(updated_at))}</code>")
    return "\n".join(lines)


def subscription_outbox_payload(
    meta: dict[str, Any],
    *,
    title: str | None = None,
    filename_prefix: str = "subscription",
) -> dict[str, Any]:
    cfg = get_subscription_text(meta)
    if not cfg.strip():
        raise ValueError("subscription is empty")
    if len(cfg.encode("utf-8")) > MAX_SUBSCRIPTION_BYTES:
        raise ValueError("subscription exceeds 1000000 UTF-8 bytes")
    intro = title or _subscription_intro(meta)
    if len(html_escape(cfg)) <= _INLINE_SUBSCRIPTION_LIMIT:
        return message_payload(intro + "\n\n" + wrap_as_codeblock_html(cfg, limit=_INLINE_SUBSCRIPTION_LIMIT))
    return document_text_payload(
        cfg,
        filename=f"{filename_prefix}.txt",
        caption=intro + "\n\nПолная подписка находится в файле.",
    )


async def send_subscription_payload(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    meta: dict[str, Any],
    title: str | None = None,
    filename_prefix: str = "subscription",
) -> None:
    cfg = get_subscription_text(meta)
    if not cfg.strip():
        raise ValueError("subscription is empty")
    if len(cfg.encode("utf-8")) > MAX_SUBSCRIPTION_BYTES:
        raise ValueError("subscription exceeds 1000000 UTF-8 bytes")

    intro = title or _subscription_intro(meta)
    # Лимит Telegram считается по экранированному тексту (& -> &amp; и т.п.).
    if len(html_escape(cfg)) <= _INLINE_SUBSCRIPTION_LIMIT:
        payload = intro + "\n\n" + wrap_as_codeblock_html(cfg, limit=_INLINE_SUBSCRIPTION_LIMIT)
        await context.bot.send_message(chat_id=chat_id, text=payload, parse_mode=ParseMode.HTML)
        return

    await context.bot.send_message(
        chat_id=chat_id,
        text=intro + "\n\nПолная подписка отправлена отдельным файлом.",
        parse_mode=ParseMode.HTML,
    )
    document = InputFile(BytesIO(cfg.encode("utf-8")), filename=f"{filename_prefix}.txt")
    await context.bot.send_document(chat_id=chat_id, document=document, caption="subscription.txt")


@require_auth
async def subscription_show(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    msg = update.effective_message
    uid = get_user_id(update)
    meta = get_user_meta_copy(uid) if uid is not None else None

    if not uid or not meta:
        if q:
            await q.answer()
        return

    if q:
        await q.answer()

    if not has_subscription(meta):
        text = (
            "📦 <b>Моя подписка</b>\n\n"
            "Подписка ещё не назначена. Обратитесь к администратору, чтобы он выдал вам конфиг."
        )
        if q:
            await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_inline_kb(update))
        elif msg:
            await msg.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_inline_kb(update))
        return

    await send_subscription_payload(
        context,
        chat_id=uid,
        meta=meta,
        filename_prefix=f"subscription_{uid}",
    )
    confirmation = ui_info_text("Подписка отправлена отдельным сообщением ниже.")
    if q:
        await q.edit_message_text(confirmation, parse_mode=ParseMode.HTML, reply_markup=main_menu_inline_kb(update))
    elif msg:
        await msg.reply_text(confirmation, parse_mode=ParseMode.HTML, reply_markup=main_menu_inline_kb(update))
    logger.info("Subscription delivered to user_id=%s", uid)
