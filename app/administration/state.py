from __future__ import annotations

import re
from typing import Any

from telegram.ext import ContextTypes

ADMINISTRATION_INPUT = 81
ADMINISTRATION_CONFIRM = 82

_CTX_KEY = "administration_flow"
_CTX_ACTION = "action"
_CTX_PENDING = "pending"


def flow_state(context: ContextTypes.DEFAULT_TYPE) -> dict[str, Any]:
    user_data = context.user_data
    if user_data is None:
        raise RuntimeError("Telegram user_data is unavailable")
    value = user_data.get(_CTX_KEY)
    if not isinstance(value, dict):
        value = {}
        user_data[_CTX_KEY] = value
    return value


def clear_flow_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    if context.user_data is not None:
        context.user_data.pop(_CTX_KEY, None)


def flow_action(context: ContextTypes.DEFAULT_TYPE) -> str:
    return str(flow_state(context).get(_CTX_ACTION) or "")


def set_flow_action(context: ContextTypes.DEFAULT_TYPE, action: str) -> None:
    flow_state(context)[_CTX_ACTION] = action


def pending_change(context: ContextTypes.DEFAULT_TYPE) -> dict[str, Any] | None:
    value = flow_state(context).get(_CTX_PENDING)
    return value if isinstance(value, dict) else None


def set_pending_change(context: ContextTypes.DEFAULT_TYPE, pending: dict[str, Any]) -> None:
    flow_state(context)[_CTX_PENDING] = pending


def normalize_input_action(data: str) -> str | None:
    aliases = {
        "staff:alias": "alias",
        "product:input:setting_payment": "payment_message",
        "product:input:setting_current": "period_current",
        "product:input:setting_next": "period_next",
    }
    if data in aliases:
        return aliases[data]
    match = re.fullmatch(
        r"administration:input:(alias|help|support_email|payment_message|"
        r"period_current|period_next)",
        data,
    )
    return match.group(1) if match else None


__all__ = [
    "ADMINISTRATION_CONFIRM",
    "ADMINISTRATION_INPUT",
    "clear_flow_state",
    "flow_action",
    "flow_state",
    "normalize_input_action",
    "pending_change",
    "set_flow_action",
    "set_pending_change",
]
