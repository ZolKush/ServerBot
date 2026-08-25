"""Shared request constants, time parsing, and transient PTB state."""

from __future__ import annotations

import contextlib
from datetime import datetime, timedelta
from typing import Any

from telegram import Update
from telegram.ext import ContextTypes

from ...bot.guards import get_user_id
from ...bot.ui import format_dt_human
from ...config import TZ
from ...storage import get_user_meta_copy

PRODUCT_INPUT, PRODUCT_CONFIRM = range(2)
REQUEST_CLAIM_TIMEOUT = timedelta(minutes=15)
ACTIVE_REQUEST_STATUSES = {
    "pending",
    "claimed",
    "awaiting_link",
    "requisites_sent",
    "payment_reported",
}

CTX_ACTION = "product_input_action"
CTX_REQUEST_ID = "product_request_id"
CTX_TARGET_UID = "product_target_uid"
CTX_PENDING = "product_pending_change"


def now() -> datetime:
    return datetime.now(TZ)


def now_iso() -> str:
    return now().isoformat()


def parse_datetime(value: object) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    with contextlib.suppress(ValueError):
        parsed = datetime.fromisoformat(raw)
        return parsed.replace(tzinfo=TZ) if parsed.tzinfo is None else parsed.astimezone(TZ)
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y %H:%M:%S"):
        with contextlib.suppress(ValueError):
            return datetime.strptime(raw, fmt).replace(tzinfo=TZ)
    return None


def parse_input_datetime(value: str) -> datetime | None:
    raw = " ".join(str(value or "").strip().split())
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y %H:%M:%S"):
        with contextlib.suppress(ValueError):
            return datetime.strptime(raw, fmt).replace(tzinfo=TZ)
    return None


def datetime_text(value: object) -> str:
    return format_dt_human(value, empty="не указана")


def actor_meta(update: Update) -> dict[str, Any] | None:
    user_id = get_user_id(update)
    return get_user_meta_copy(user_id) if user_id is not None else None


def context_data(context: ContextTypes.DEFAULT_TYPE) -> dict[str, Any]:
    data = context.user_data
    if data is None:
        raise RuntimeError("Telegram user_data is unavailable")
    return data


def clear_request_context(context: ContextTypes.DEFAULT_TYPE) -> None:
    data = context_data(context)
    for key in (CTX_ACTION, CTX_REQUEST_ID, CTX_TARGET_UID, CTX_PENDING):
        data.pop(key, None)


__all__ = [
    "ACTIVE_REQUEST_STATUSES",
    "CTX_ACTION",
    "CTX_PENDING",
    "CTX_REQUEST_ID",
    "CTX_TARGET_UID",
    "PRODUCT_CONFIRM",
    "PRODUCT_INPUT",
    "REQUEST_CLAIM_TIMEOUT",
    "actor_meta",
    "clear_request_context",
    "context_data",
    "datetime_text",
    "now",
    "now_iso",
    "parse_datetime",
    "parse_input_datetime",
]
