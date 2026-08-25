from __future__ import annotations

from time import monotonic
from typing import Any

from telegram import Update

from ..config import (
    AUTH_FAIL_WINDOW_SEC,
    AUTH_GLOBAL_MAX_FAILS_IN_WINDOW,
    AUTH_LOCKOUT_SEC,
    AUTH_MAX_FAILS_IN_WINDOW,
    logger,
)

_AUTH_FAILS: dict[str, list[float]] = {}
_AUTH_LOCKED_UNTIL: dict[str, float] = {}
_AUTH_GLOBAL_FAILS: list[float] = []
_AUTH_GLOBAL_LOCKED_UNTIL = 0.0


def prune_auth_limits(now: float | None = None) -> None:
    global _AUTH_GLOBAL_FAILS, _AUTH_GLOBAL_LOCKED_UNTIL
    current_time = monotonic() if now is None else now
    active_fails: dict[str, list[float]] = {}
    for key, attempts in _AUTH_FAILS.items():
        filtered = [timestamp for timestamp in attempts if (current_time - timestamp) <= AUTH_FAIL_WINDOW_SEC]
        if filtered:
            active_fails[key] = filtered
    _AUTH_FAILS.clear()
    _AUTH_FAILS.update(active_fails)
    _AUTH_GLOBAL_FAILS = [
        timestamp for timestamp in _AUTH_GLOBAL_FAILS if (current_time - timestamp) <= AUTH_FAIL_WINDOW_SEC
    ]

    for key in [key for key, until in _AUTH_LOCKED_UNTIL.items() if until <= current_time]:
        _AUTH_LOCKED_UNTIL.pop(key, None)
    if current_time >= _AUTH_GLOBAL_LOCKED_UNTIL:
        _AUTH_GLOBAL_LOCKED_UNTIL = 0.0


def auth_actor_key(update: Update) -> str:
    user = update.effective_user
    if user:
        return f"user:{user.id}"
    chat = update.effective_chat
    return f"chat:{chat.id}" if chat else "unknown"


def auth_lock_remaining_sec(update: Update) -> int:
    current_time = monotonic()
    prune_auth_limits(current_time)
    locked_until = max(
        _AUTH_LOCKED_UNTIL.get(auth_actor_key(update), 0.0),
        _AUTH_GLOBAL_LOCKED_UNTIL,
    )
    return max(0, int(locked_until - current_time) + 1) if locked_until > current_time else 0


def register_auth_failure(update: Update) -> None:
    global _AUTH_GLOBAL_LOCKED_UNTIL
    key = auth_actor_key(update)
    current_time = monotonic()
    prune_auth_limits(current_time)
    attempts = [
        timestamp for timestamp in _AUTH_FAILS.get(key, []) if (current_time - timestamp) <= AUTH_FAIL_WINDOW_SEC
    ]
    attempts.append(current_time)
    _AUTH_FAILS[key] = attempts
    _AUTH_GLOBAL_FAILS.append(current_time)
    if len(attempts) >= AUTH_MAX_FAILS_IN_WINDOW:
        _AUTH_LOCKED_UNTIL[key] = current_time + AUTH_LOCKOUT_SEC
        _AUTH_FAILS[key] = []
    if len(_AUTH_GLOBAL_FAILS) >= AUTH_GLOBAL_MAX_FAILS_IN_WINDOW:
        _AUTH_GLOBAL_LOCKED_UNTIL = current_time + AUTH_LOCKOUT_SEC
        _AUTH_GLOBAL_FAILS.clear()
        logger.warning("Global admin authentication lockout activated")


def reset_actor_auth_limits(update: Update) -> None:
    prune_auth_limits()
    key = auth_actor_key(update)
    _AUTH_FAILS.pop(key, None)
    _AUTH_LOCKED_UNTIL.pop(key, None)


async def auth_prune_task(_context: Any) -> None:
    prune_auth_limits()


__all__ = [
    "auth_actor_key",
    "auth_lock_remaining_sec",
    "auth_prune_task",
    "prune_auth_limits",
    "register_auth_failure",
    "reset_actor_auth_limits",
]
