from __future__ import annotations

import asyncio
from collections.abc import MutableMapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from telegram import Update
from telegram.constants import ChatType
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TimedOut
from telegram.ext import ExtBot

from ..config import logger
from .telegram_rate import extend_flood_gate, retry_after_seconds, wait_flood_gate

_REGISTRY_KEY = "maintbot_message_registry_v1"
_DELETE_BATCH_SIZE = 100
# Telegram only accepts deletion of messages younger than 48 hours. Leave a
# safety margin for clock differences and request latency.
_DELETE_MAX_AGE = timedelta(hours=47, minutes=30)


@dataclass
class MessageCleanupStats:
    tracked_chats: int = 0
    candidates: int = 0
    deleted: int = 0
    expired: int = 0
    failed: int = 0


def _timestamp(value: object) -> float | None:
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).timestamp()
    try:
        parsed_float = float(str(value))
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed_float if parsed_float > 0 else None


def _integer(value: object) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError, OverflowError):
        return None


def _normalize_registry(raw: object) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    raw_chats = source.get("chats")
    chats: dict[str, dict[str, dict[str, float]]] = {}
    if isinstance(raw_chats, dict):
        for raw_chat_id, raw_chat in raw_chats.items():
            try:
                chat_id = int(raw_chat_id)
            except (TypeError, ValueError, OverflowError):
                continue
            if chat_id == 0 or not isinstance(raw_chat, dict):
                continue
            raw_messages = raw_chat.get("messages")
            if not isinstance(raw_messages, dict):
                continue
            messages: dict[str, float] = {}
            for raw_message_id, raw_date in raw_messages.items():
                try:
                    message_id = int(raw_message_id)
                except (TypeError, ValueError, OverflowError):
                    continue
                sent_at = _timestamp(raw_date)
                if message_id > 0 and sent_at is not None:
                    messages[str(message_id)] = sent_at
            if messages:
                chats[str(chat_id)] = {"messages": messages}
    return {"version": 1, "chats": chats}


class MessageTracker:
    """Tracks private-chat message IDs in PTB's persisted bot_data."""

    def __init__(self, *, enabled: bool, retention: timedelta) -> None:
        if retention <= timedelta(0) or retention >= _DELETE_MAX_AGE:
            raise ValueError("message retention must be positive and below Telegram's deletion limit")
        self.enabled = enabled
        self.retention = retention
        self._bot_data: MutableMapping[str, Any] | None = None
        self._lock = asyncio.Lock()
        self._cleanup_lock = asyncio.Lock()

    def bind(self, bot_data: MutableMapping[str, Any]) -> None:
        """Binds the tracker after PTB has restored persistent bot_data."""

        bot_data[_REGISTRY_KEY] = _normalize_registry(bot_data.get(_REGISTRY_KEY))
        self._bot_data = bot_data

    def _registry(self) -> dict[str, Any] | None:
        if self._bot_data is None:
            return None
        registry = self._bot_data.get(_REGISTRY_KEY)
        if not isinstance(registry, dict):
            registry = _normalize_registry(registry)
            self._bot_data[_REGISTRY_KEY] = registry
        return registry

    async def record_message(
        self,
        *,
        chat_id: object,
        message_id: object,
        message_date: object,
        chat_type: object,
    ) -> None:
        if not self.enabled or str(chat_type) != ChatType.PRIVATE:
            return
        normalized_chat_id = _integer(chat_id)
        normalized_message_id = _integer(message_id)
        if normalized_chat_id is None or normalized_message_id is None:
            return
        sent_at = _timestamp(message_date)
        if normalized_chat_id == 0 or normalized_message_id <= 0 or sent_at is None:
            return
        async with self._lock:
            registry = self._registry()
            if registry is None:
                return
            chats = registry.setdefault("chats", {})
            chat = chats.setdefault(str(normalized_chat_id), {"messages": {}})
            messages = chat.setdefault("messages", {})
            messages[str(normalized_message_id)] = sent_at

    async def observe_update(self, update: Update) -> None:
        message = update.effective_message
        if message is None:
            return
        chat = getattr(message, "chat", None)
        await self.record_message(
            chat_id=getattr(chat, "id", None),
            message_id=getattr(message, "message_id", None),
            message_date=getattr(message, "date", None),
            chat_type=getattr(chat, "type", None),
        )

    async def record_api_result(self, endpoint: str, data: dict[str, Any] | None, result: Any) -> None:
        results = result if isinstance(result, list) else [result]
        recorded_full_message = False
        for item in results:
            if not isinstance(item, dict):
                continue
            chat = item.get("chat")
            if not isinstance(chat, dict):
                continue
            recorded_full_message = True
            await self.record_message(
                chat_id=chat.get("id"),
                message_id=item.get("message_id"),
                message_date=item.get("date"),
                chat_type=chat.get("type"),
            )
        if recorded_full_message or endpoint not in {"copyMessage", "copyMessages"} or not isinstance(data, dict):
            return
        chat_id = data.get("chat_id")
        fallback_results = result if isinstance(result, list) else [result]
        for item in fallback_results:
            if isinstance(item, dict):
                await self.record_message(
                    chat_id=chat_id,
                    message_id=item.get("message_id"),
                    message_date=datetime.now(timezone.utc),
                    chat_type=ChatType.PRIVATE if isinstance(chat_id, int) and chat_id > 0 else None,
                )

    async def forget_messages(self, chat_id: object, message_ids: Sequence[object]) -> None:
        normalized_chat_id = _integer(chat_id)
        if normalized_chat_id is None:
            return
        normalized_ids: set[str] = set()
        for raw_message_id in message_ids:
            message_id = _integer(raw_message_id)
            if message_id is None:
                continue
            if message_id > 0:
                normalized_ids.add(str(message_id))
        if not normalized_ids:
            return
        async with self._lock:
            registry = self._registry()
            if registry is None:
                return
            chats = registry.get("chats")
            chat = chats.get(str(normalized_chat_id)) if isinstance(chats, dict) else None
            messages = chat.get("messages") if isinstance(chat, dict) else None
            if not isinstance(messages, dict):
                return
            for message_id_text in normalized_ids:
                messages.pop(message_id_text, None)
            if not messages and isinstance(chats, dict):
                chats.pop(str(normalized_chat_id), None)

    async def handle_api_result(
        self,
        endpoint: str,
        data: dict[str, Any] | None,
        result: Any,
    ) -> None:
        if result is True and isinstance(data, dict) and endpoint in {"deleteMessage", "deleteMessages"}:
            raw_ids = data.get("message_ids") if endpoint == "deleteMessages" else [data.get("message_id")]
            if isinstance(raw_ids, Sequence) and not isinstance(raw_ids, (str, bytes)):
                await self.forget_messages(data.get("chat_id"), raw_ids)
            return
        await self.record_api_result(endpoint, data, result)

    async def snapshot(self) -> dict[int, list[int]]:
        async with self._lock:
            registry = self._registry()
            if registry is None:
                return {}
            chats = registry.get("chats")
            if not isinstance(chats, dict):
                return {}
            result: dict[int, list[int]] = {}
            for chat_id, chat in chats.items():
                messages = chat.get("messages") if isinstance(chat, dict) else None
                if not isinstance(messages, dict):
                    continue
                result[int(chat_id)] = sorted(int(message_id) for message_id in messages)
            return result

    async def cleanup(self, bot: Any, *, now: datetime | None = None) -> MessageCleanupStats:
        stats = MessageCleanupStats()
        if not self.enabled or self._cleanup_lock.locked():
            return stats
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        now_ts = current.astimezone(timezone.utc).timestamp()
        retention_cutoff = now_ts - self.retention.total_seconds()
        expiry_cutoff = now_ts - _DELETE_MAX_AGE.total_seconds()
        plans: list[tuple[int, list[int]]] = []

        async with self._cleanup_lock:
            async with self._lock:
                registry = self._registry()
                if registry is None:
                    return stats
                chats = registry.get("chats")
                if not isinstance(chats, dict):
                    return stats
                stats.tracked_chats = len(chats)
                for chat_id_text, chat in list(chats.items()):
                    messages = chat.get("messages") if isinstance(chat, dict) else None
                    if not isinstance(messages, dict) or not messages:
                        chats.pop(chat_id_text, None)
                        continue
                    ordered = sorted(
                        ((int(message_id), float(sent_at)) for message_id, sent_at in messages.items()),
                        key=lambda item: (item[1], item[0]),
                    )
                    latest_id = ordered[-1][0]
                    expired_ids = [
                        message_id
                        for message_id, sent_at in ordered
                        if message_id != latest_id and sent_at <= expiry_cutoff
                    ]
                    for message_id in expired_ids:
                        messages.pop(str(message_id), None)
                    stats.expired += len(expired_ids)
                    candidates = [
                        message_id
                        for message_id, sent_at in ordered
                        if message_id != latest_id and expiry_cutoff < sent_at <= retention_cutoff
                    ]
                    if candidates:
                        plans.append((int(chat_id_text), candidates))
                        stats.candidates += len(candidates)

            for chat_id, message_ids in plans:
                for offset in range(0, len(message_ids), _DELETE_BATCH_SIZE):
                    chunk = message_ids[offset : offset + _DELETE_BATCH_SIZE]
                    await wait_flood_gate()
                    try:
                        deleted = await bot.delete_messages(chat_id=chat_id, message_ids=chunk)
                    except RetryAfter as exc:
                        delay = retry_after_seconds(exc, minimum=1.0) + 0.5
                        await extend_flood_gate(delay)
                        stats.failed += len(chunk)
                        logger.warning(
                            "Message cleanup rate limited chat_id=%s retry_after=%.1f",
                            chat_id,
                            delay,
                            extra={"user_id": chat_id, "action": "message_cleanup_retry"},
                        )
                        return stats
                    except (Forbidden, BadRequest, TimedOut, NetworkError, OSError) as exc:
                        stats.failed += len(chunk)
                        logger.warning(
                            "Message cleanup failed chat_id=%s count=%s type=%s",
                            chat_id,
                            len(chunk),
                            exc.__class__.__name__,
                            extra={"user_id": chat_id, "action": "message_cleanup_failed"},
                        )
                        continue
                    if deleted:
                        await self.forget_messages(chat_id, chunk)
                        stats.deleted += len(chunk)
                    else:
                        stats.failed += len(chunk)
        return stats


class TrackingExtBot(ExtBot):
    """ExtBot that records all message-shaped Bot API responses."""

    __slots__ = ("message_tracker",)
    message_tracker: MessageTracker

    def __init__(self, token: str, *, message_tracker: MessageTracker) -> None:
        super().__init__(token=token)
        object.__setattr__(self, "message_tracker", message_tracker)

    async def _post(
        self,
        endpoint: str,
        data: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        result = await super()._post(endpoint, data, **kwargs)
        try:
            await self.message_tracker.handle_api_result(endpoint, data, result)
        except Exception:
            # A successfully delivered Telegram message must never be reported as
            # failed merely because local retention tracking had a problem.
            logger.exception("Failed to update Telegram message registry endpoint=%s", endpoint)
        return result


__all__ = ["MessageCleanupStats", "MessageTracker", "TrackingExtBot"]
