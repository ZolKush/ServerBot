"""Bounded Telegram deletion engine; no configuration or storage I/O on import."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Any, Protocol

from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter


class DeletingBot(Protocol):
    async def delete_messages(self, *, chat_id: int, message_ids: list[int]) -> bool: ...


@dataclass
class ChatResult:
    chat_id: int
    accepted_ids: int = 0
    rejected_ids: int = 0
    unresolved_ids: int = 0
    forbidden: bool = False

    @property
    def complete(self) -> bool:
        return not (self.rejected_ids or self.unresolved_ids or self.forbidden)


def known_recipient_ids(users: Mapping[str, Any]) -> list[int]:
    """Include disabled profiles too; never interpret access state as deletability."""
    result = set()
    for key, meta in users.items():
        raw = meta.get("user_id") if isinstance(meta, dict) else None
        raw = key if raw is None else raw
        if isinstance(raw, bool) or not isinstance(raw, (int, str)):
            continue
        try:
            user_id = int(raw)
        except ValueError:
            continue
        if user_id > 0:
            result.add(user_id)
    return sorted(result)


async def cancel_pending_broadcasts() -> int:
    """Explicit operator option; unrelated outbox events are preserved."""
    from app.storage import mutate_outbox_event, outbox_snapshot

    cancelled = 0
    for source, event in outbox_snapshot():
        if event.get("kind") == "admin_broadcast" and event.get("id"):
            await mutate_outbox_event(source, str(event["id"]), lambda _event: None)
            cancelled += 1
    return cancelled


class RangeDeleter:
    def __init__(
        self,
        bot: DeletingBot,
        emit: Callable[..., None],
        *,
        attempts: int = 5,
        batch_size: int = 100,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if attempts < 1 or not 1 <= batch_size <= 100:
            raise ValueError("attempts must be positive and batch_size must be 1..100")
        self.bot = bot
        self.emit = emit
        self.attempts = attempts
        self.batch_size = batch_size
        self.sleep = sleep

    async def _wait(self, seconds: float, chat_id: int) -> None:
        # Keep long Telegram FloodWaits visible, including before the next chat.
        while seconds > 0:
            self.emit("WAIT", chat_id=chat_id, seconds=round(seconds, 1))
            step = min(30.0, seconds)
            await self.sleep(step)
            seconds -= step

    async def _chunk(self, chat_id: int, ids: list[int], result: ChatResult) -> None:
        details = {"chat_id": chat_id, "min_id": ids[0], "max_id": ids[-1]}
        for attempt in range(1, self.attempts + 1):
            self.emit("REQUEST", **details, attempt=attempt, max_attempts=self.attempts)
            try:
                accepted = await self.bot.delete_messages(chat_id=chat_id, message_ids=ids)
            except BadRequest as exc:
                if len(ids) == 1:
                    result.rejected_ids += 1
                    self.emit("REJECTED", **details, reason=str(exc))
                    return
                self.emit("SPLIT", **details, reason=str(exc))
                midpoint = len(ids) // 2
                await self._chunk(chat_id, ids[midpoint:], result)
                await self._chunk(chat_id, ids[:midpoint], result)
                return
            except Forbidden:
                raise
            except RetryAfter as exc:
                delay = exc.retry_after
                seconds = delay.total_seconds() if isinstance(delay, timedelta) else float(delay)
                self.emit("RETRY", **details, reason="RetryAfter", attempt=attempt)
                # Wait even on the last attempt: the limit applies to later requests too.
                await self._wait(max(0.0, seconds) + 1, chat_id)
            except (NetworkError, OSError) as exc:
                self.emit("RETRY", **details, reason=type(exc).__name__, attempt=attempt)
                if attempt < self.attempts:
                    await self._wait(min(8.0, 2.0 ** (attempt - 1)), chat_id)
            else:
                if accepted:
                    result.accepted_ids += len(ids)
                    self.emit("ACCEPTED", **details, ids=len(ids))
                    await self.sleep(0.2)
                    return
                break
        result.unresolved_ids += len(ids)
        self.emit("UNRESOLVED", **details, reason="request not confirmed; rerun this range")

    async def run(self, chats: Iterable[int], lower: int, upper: int) -> list[ChatResult]:
        if not 1 <= lower <= upper <= 2**31 - 1:
            raise ValueError("message ID range must be within 1..2147483647")
        results = []
        for chat_id in sorted(set(chats)):
            if chat_id <= 0:
                raise ValueError("only positive private chat IDs are supported")
            result = ChatResult(chat_id)
            self.emit("CHAT_START", chat_id=chat_id, min_id=lower, max_id=upper)
            try:
                for high in range(upper, lower - 1, -self.batch_size):
                    low = max(lower, high - self.batch_size + 1)
                    await self._chunk(chat_id, list(range(low, high + 1)), result)
            except Forbidden as exc:
                result.forbidden = True
                result.unresolved_ids = upper - lower + 1 - result.accepted_ids - result.rejected_ids
                self.emit("FORBIDDEN", chat_id=chat_id, reason=str(exc))
            self.emit("CHAT_RESULT", **asdict(result), complete=result.complete)
            results.append(result)
        return results
