"""Process-wide Telegram flood-limit coordination."""

from __future__ import annotations

import asyncio
import math
from typing import Any

_FLOOD_LOCK: asyncio.Lock | None = None
_FLOOD_UNTIL = 0.0


def retry_after_seconds(value: Any, *, minimum: float = 0.5) -> float:
    raw = getattr(value, "retry_after", value)
    try:
        total_seconds = getattr(raw, "total_seconds", None)
        seconds = float(total_seconds()) if callable(total_seconds) else float(raw)
        if not math.isfinite(seconds):
            return max(minimum, 1.0)
        return max(minimum, seconds)
    except (TypeError, ValueError, OverflowError):
        return max(minimum, 1.0)


def _get_flood_lock() -> asyncio.Lock:
    global _FLOOD_LOCK
    if _FLOOD_LOCK is None:
        _FLOOD_LOCK = asyncio.Lock()
    return _FLOOD_LOCK


def flood_wait_remaining() -> float:
    return max(0.0, _FLOOD_UNTIL - asyncio.get_running_loop().time())


async def wait_flood_gate() -> None:
    while (delay := flood_wait_remaining()) > 0:
        await asyncio.sleep(delay)


async def extend_flood_gate(delay: float) -> None:
    global _FLOOD_UNTIL
    async with _get_flood_lock():
        now = asyncio.get_running_loop().time()
        _FLOOD_UNTIL = max(_FLOOD_UNTIL, now + retry_after_seconds(delay, minimum=0.0))
