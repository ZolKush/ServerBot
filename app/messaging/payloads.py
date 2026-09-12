"""Telegram payload validation and delivery, independent of queue state."""

from __future__ import annotations

import re
from io import BytesIO
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, LinkPreviewOptions
from telegram.constants import ParseMode
from telegram.error import BadRequest

from ..bot.text_limits import clip_html_message, clip_plain_text, utf16_length


def _caption(value: object, parse_mode: object) -> str:
    text = str(value or "")
    return clip_html_message(text, 1024) if parse_mode == ParseMode.HTML else clip_plain_text(text, 1024)


def _markup_from_descriptor(raw: object) -> InlineKeyboardMarkup | None:
    if not isinstance(raw, list):
        return None
    rows: list[list[InlineKeyboardButton]] = []
    for raw_row in raw[:20]:
        if not isinstance(raw_row, list):
            continue
        row: list[InlineKeyboardButton] = []
        for raw_button in raw_row[:8]:
            if not isinstance(raw_button, dict):
                continue
            text = str(raw_button.get("text") or "")[:64]
            callback_data = str(raw_button.get("callback_data") or "")
            if len(callback_data.encode("utf-8")) > 64:
                continue
            url = str(raw_button.get("url") or "")
            if text and callback_data:
                row.append(InlineKeyboardButton(text, callback_data=callback_data))
            elif text and url:
                row.append(InlineKeyboardButton(text, url=url))
        if row:
            rows.append(row)
    return InlineKeyboardMarkup(rows) if rows else None


def message_payload(
    text: str,
    *,
    parse_mode: str | None = ParseMode.HTML,
    reply_markup: list[list[dict[str, str]]] | None = None,
    disable_web_page_preview: bool = True,
) -> dict[str, Any]:
    value = str(text)
    if parse_mode == ParseMode.HTML:
        value = clip_html_message(value, limit=4096)
    if not value or utf16_length(value) > 4096:
        raise ValueError("outbox message text must contain 1..4096 characters")
    return {
        "method": "send_message",
        "text": value,
        "parse_mode": str(parse_mode) if parse_mode else "",
        "reply_markup": reply_markup or [],
        "disable_web_page_preview": bool(disable_web_page_preview),
    }


def document_text_payload(
    text: str,
    *,
    filename: str,
    caption: str = "",
    parse_mode: str | None = ParseMode.HTML,
) -> dict[str, Any]:
    value = str(text)
    encoded_size = len(value.encode("utf-8"))
    if not value or encoded_size > 1_000_000:
        raise ValueError("outbox text document must contain 1..1000000 UTF-8 bytes")
    safe_filename = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(filename or "document.txt"))[:100]
    if not safe_filename:
        safe_filename = "document.txt"
    return {
        "method": "send_document_text",
        "text": value,
        "filename": safe_filename,
        "caption": _caption(caption, parse_mode),
        "parse_mode": str(parse_mode) if parse_mode else "",
    }


async def deliver_payload(bot, uid: int, payload: dict[str, Any]) -> Any:
    method = str(payload.get("method") or "send_message")
    markup = _markup_from_descriptor(payload.get("reply_markup"))
    if method == "send_message":
        return await bot.send_message(
            chat_id=uid,
            text=str(payload.get("text") or ""),
            parse_mode=str(payload.get("parse_mode") or "") or None,
            reply_markup=markup,
            link_preview_options=LinkPreviewOptions(is_disabled=bool(payload.get("disable_web_page_preview", True))),
        )
    if method == "send_photo":
        return await bot.send_photo(
            chat_id=uid,
            photo=str(payload.get("file_id") or ""),
            caption=_caption(payload.get("caption"), payload.get("parse_mode")) or None,
            parse_mode=str(payload.get("parse_mode") or "") or None,
            reply_markup=markup,
        )
    if method == "send_document":
        return await bot.send_document(
            chat_id=uid,
            document=str(payload.get("file_id") or ""),
            caption=_caption(payload.get("caption"), payload.get("parse_mode")) or None,
            parse_mode=str(payload.get("parse_mode") or "") or None,
            reply_markup=markup,
        )
    if method == "send_document_text":
        text = str(payload.get("text") or "")
        if not text or len(text.encode("utf-8")) > 1_000_000:
            raise BadRequest("invalid outbox text document")
        filename = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(payload.get("filename") or "document.txt"))[:100]
        return await bot.send_document(
            chat_id=uid,
            document=InputFile(BytesIO(text.encode("utf-8")), filename=filename or "document.txt"),
            caption=_caption(payload.get("caption"), payload.get("parse_mode")) or None,
            parse_mode=str(payload.get("parse_mode") or "") or None,
            reply_markup=markup,
        )
    raise BadRequest(f"unsupported outbox method: {method}")
