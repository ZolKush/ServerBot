"""Shared status target lookup and value normalization."""

from __future__ import annotations

from datetime import datetime
from typing import Any, cast

from ...config import SERVERS, TZ, ServerTarget


def safe_nonnegative_int(value: object, default: int = 0) -> int:
    try:
        return max(0, int(cast(Any, value or 0)))
    except (TypeError, ValueError):
        return max(0, default)


def exc_brief(value: object) -> str:
    if not isinstance(value, Exception):
        return "н/д"
    name = value.__class__.__name__
    text = str(value).strip()
    return f"{name}: {text}" if text else name


def server_keys() -> list[str]:
    return list(SERVERS.keys())


def first_server_key() -> str:
    keys = server_keys()
    return keys[0] if keys else "local"


def default_server_target() -> ServerTarget | None:
    return SERVERS.get(first_server_key())


def get_server_target(server_key: str | None) -> ServerTarget | None:
    key = (server_key or "").strip().lower()
    return SERVERS.get(key) if key else None


def server_flag(server: ServerTarget) -> str:
    return server.flag or "🖥"


def format_iso_short(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        timestamp = datetime.fromisoformat(raw)
    except Exception:
        return raw
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=TZ)
    else:
        timestamp = timestamp.astimezone(TZ)
    return timestamp.strftime("%d.%m %H:%M")


__all__ = ["get_server_target"]
