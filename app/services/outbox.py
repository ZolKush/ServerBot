from __future__ import annotations

import asyncio
import contextlib
import re
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, LinkPreviewOptions
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TimedOut

from ..config import logger
from ..storage import finalize_outbox_event, mutate_outbox_event, outbox_snapshot
from .telegram_rate import extend_flood_gate, retry_after_seconds, wait_flood_gate

_PROCESS_LOCK = asyncio.Lock()
MAX_DELIVERIES_PER_RUN = 100
MAX_ATTEMPTS = 12


def _retry_after_seconds(exc: RetryAfter) -> float:
    return retry_after_seconds(exc, minimum=1.0)


def _parse_time(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value or ""))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return datetime.min.replace(tzinfo=timezone.utc)


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
            callback_data = str(raw_button.get("callback_data") or "")[:64]
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
    if not value or len(value) > 4096:
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
        "caption": str(caption)[:1024],
        "parse_mode": str(parse_mode) if parse_mode else "",
    }


async def _wait_for_flood_gate() -> None:
    await wait_flood_gate()


async def _extend_flood_gate(seconds: float) -> None:
    await extend_flood_gate(seconds)


async def _deliver(bot, uid: int, payload: dict[str, Any]) -> None:
    method = str(payload.get("method") or "send_message")
    markup = _markup_from_descriptor(payload.get("reply_markup"))
    await _wait_for_flood_gate()
    if method == "send_message":
        await bot.send_message(
            chat_id=uid,
            text=str(payload.get("text") or ""),
            parse_mode=str(payload.get("parse_mode") or "") or None,
            reply_markup=markup,
            link_preview_options=LinkPreviewOptions(is_disabled=bool(payload.get("disable_web_page_preview", True))),
        )
        return
    if method == "send_photo":
        await bot.send_photo(
            chat_id=uid,
            photo=str(payload.get("file_id") or ""),
            caption=str(payload.get("caption") or "")[:1024] or None,
            parse_mode=str(payload.get("parse_mode") or "") or None,
            reply_markup=markup,
        )
        return
    if method == "send_document":
        await bot.send_document(
            chat_id=uid,
            document=str(payload.get("file_id") or ""),
            caption=str(payload.get("caption") or "")[:1024] or None,
            parse_mode=str(payload.get("parse_mode") or "") or None,
            reply_markup=markup,
        )
        return
    if method == "send_document_text":
        text = str(payload.get("text") or "")
        if not text or len(text.encode("utf-8")) > 1_000_000:
            raise BadRequest("invalid outbox text document")
        filename = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(payload.get("filename") or "document.txt"))[:100]
        await bot.send_document(
            chat_id=uid,
            document=InputFile(BytesIO(text.encode("utf-8")), filename=filename or "document.txt"),
            caption=str(payload.get("caption") or "")[:1024] or None,
            parse_mode=str(payload.get("parse_mode") or "") or None,
            reply_markup=markup,
        )
        return
    raise BadRequest(f"unsupported outbox method: {method}")


def _mark_recipient(
    event: dict[str, Any],
    uid: int,
    *,
    status: str,
    attempts: int,
    error: str = "",
    retry_after: float = 0,
) -> dict[str, Any]:
    recipients = event.get("recipients")
    if not isinstance(recipients, dict) or not isinstance(recipients.get(str(uid)), dict):
        return event
    state = dict(recipients[str(uid)])
    state["status"] = status
    state["attempts"] = attempts
    state["last_error"] = error[:500]
    if status == "delivered":
        state["delivered_at"] = datetime.now(timezone.utc).isoformat()
    if status == "pending":
        state["next_attempt_at"] = (datetime.now(timezone.utc) + timedelta(seconds=retry_after)).isoformat()
    recipients[str(uid)] = state
    event["recipients"] = recipients
    return event


def _recipient_mutation(
    uid: int,
    *,
    status: str,
    attempts: int,
    error: str = "",
    retry_after: float = 0,
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    def _apply(current: dict[str, Any]) -> dict[str, Any]:
        return _mark_recipient(
            current,
            uid,
            status=status,
            attempts=attempts,
            error=error,
            retry_after=retry_after,
        )

    return _apply


async def _finalize_if_done(source: str, event_id: str, event: dict[str, Any] | None) -> bool:
    if not isinstance(event, dict):
        return False
    recipients = event.get("recipients")
    if not isinstance(recipients, dict) or not recipients:
        return False
    statuses = [state.get("status") for state in recipients.values() if isinstance(state, dict)]
    if len(statuses) != len(recipients) or any(status == "pending" for status in statuses):
        return False
    success = all(status == "delivered" for status in statuses)
    completion = event.get("completion")
    if (
        not success
        and isinstance(completion, dict)
        and completion.get("type") == "fail2ban_cursor"
        and any(status == "delivered" for status in statuses)
    ):
        # A terminal recipient cannot be retried usefully. Advancing after at
        # least one real delivery prevents duplicate digests for reachable
        # admins; if nobody received it, the old cursor is deliberately kept.
        success = True
    await finalize_outbox_event(source, event_id, success=success)
    return True


async def process_outbox(bot) -> int:
    if _PROCESS_LOCK.locked():
        return 0
    processed = 0
    async with _PROCESS_LOCK:
        now = datetime.now(timezone.utc)
        for source, event in outbox_snapshot():
            if processed >= MAX_DELIVERIES_PER_RUN:
                break
            event_id = str(event.get("id") or "")
            payload = event.get("payload")
            recipients = event.get("recipients")
            if not event_id or not isinstance(payload, dict) or not isinstance(recipients, dict):
                continue
            if await _finalize_if_done(source, event_id, event):
                continue
            for uid_text, raw_state in list(recipients.items()):
                if processed >= MAX_DELIVERIES_PER_RUN:
                    break
                if not isinstance(raw_state, dict) or raw_state.get("status") != "pending":
                    continue
                if _parse_time(raw_state.get("next_attempt_at")) > now:
                    continue
                try:
                    uid = int(uid_text)
                except (TypeError, ValueError):
                    continue
                try:
                    attempts = max(0, int(raw_state.get("attempts", 0) or 0)) + 1
                except (TypeError, ValueError):
                    attempts = 1
                try:
                    await _deliver(bot, uid, payload)
                except RetryAfter as exc:
                    delay = _retry_after_seconds(exc) + 0.5
                    await _extend_flood_gate(delay)
                    updated_event = await mutate_outbox_event(
                        source,
                        event_id,
                        _recipient_mutation(
                            uid,
                            status="pending",
                            attempts=attempts,
                            error="RetryAfter",
                            retry_after=delay,
                        ),
                    )
                except (Forbidden, BadRequest) as exc:
                    logger.warning(
                        "Outbox terminal delivery error event=%s recipient=%s type=%s",
                        event_id,
                        uid,
                        exc.__class__.__name__,
                        extra={"user_id": uid, "action": "outbox_terminal"},
                    )
                    updated_event = await mutate_outbox_event(
                        source,
                        event_id,
                        _recipient_mutation(uid, status="terminal", attempts=attempts, error=str(exc)),
                    )
                except (TimedOut, NetworkError, OSError) as exc:
                    terminal = attempts >= MAX_ATTEMPTS
                    delay = min(3600.0, 2.0 ** min(attempts, 10))
                    if terminal:
                        logger.warning(
                            "Outbox retries exhausted event=%s recipient=%s type=%s attempts=%s",
                            event_id,
                            uid,
                            exc.__class__.__name__,
                            attempts,
                            extra={"user_id": uid, "action": "outbox_terminal"},
                        )
                    updated_event = await mutate_outbox_event(
                        source,
                        event_id,
                        _recipient_mutation(
                            uid,
                            status="terminal" if terminal else "pending",
                            attempts=attempts,
                            error=str(exc),
                            retry_after=delay,
                        ),
                    )
                except Exception as exc:
                    logger.exception("Unexpected outbox delivery error event=%s recipient=%s", event_id, uid)
                    updated_event = await mutate_outbox_event(
                        source,
                        event_id,
                        _recipient_mutation(uid, status="terminal", attempts=attempts, error=str(exc)),
                    )
                else:
                    updated_event = await mutate_outbox_event(
                        source,
                        event_id,
                        _recipient_mutation(uid, status="delivered", attempts=attempts),
                    )
                await _finalize_if_done(source, event_id, updated_event)
                processed += 1
    return processed


async def process_outbox_job(context) -> None:
    with contextlib.suppress(asyncio.CancelledError):
        count = await process_outbox(context.bot)
        if count:
            logger.info("Outbox deliveries processed: %s", count, extra={"action": "outbox"})
