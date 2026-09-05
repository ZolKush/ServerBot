"""Emergency best-effort deletion of recent private-chat message ID ranges."""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any, TypeVar

from telegram import Bot
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TimedOut

from ..config import (
    ADMIN_PASSWORD,
    BOT_TOKEN,
    DATA_DIR,
    INSTANCE_LOCK_PATH,
    LOG_JSON,
    LOG_LEVEL,
    OWNER_PASSWORD,
    PTB_PERSISTENCE_PATH,
    REMNAWAVE_METRICS_PASS,
)
from ..bot.persistence import build_atomic_persistence
from ..runtime.lock import ALREADY_RUNNING_EXIT_CODE, InstanceAlreadyRunning, SingleInstanceLock
from ..runtime.logging import configure_logging
from ..storage import (
    authorized_users_snapshot,
    initialize_storage,
    mutate_outbox_event,
    outbox_snapshot,
)
from .telegram_rate import extend_flood_gate, retry_after_seconds, wait_flood_gate

_T = TypeVar("_T")
_BATCH_SIZE = 100
_DEFAULT_SCAN_DEPTH = 200
_DEFAULT_FALLBACK_UPPER = 10_000
_MARKER_TEXT = "."
_REGISTRY_KEY = "maintbot_navigation_registry_v2"


@dataclass
class EmergencyDeleteStats:
    chats_total: int = 0
    chats_marked: int = 0
    chats_failed: int = 0
    ids_accepted: int = 0
    ids_rejected: int = 0
    request_failures: int = 0
    fallback_chats: int = 0
    age_cutoffs: int = 0


def known_recipient_ids(users: dict[str, dict[str, Any]]) -> list[int]:
    """Return every positive Telegram user ID retained by the access store."""

    result: set[int] = set()
    for key, meta in users.items():
        raw_id = meta.get("user_id", key) if isinstance(meta, dict) else key
        try:
            user_id = int(raw_id)
        except (TypeError, ValueError, OverflowError):
            continue
        if user_id > 0:
            result.add(user_id)
    return sorted(result)


async def known_message_anchors(bot: Bot, users: dict[str, dict[str, Any]]) -> dict[int, int]:
    """Collect the newest persisted message ID known for each private chat."""

    anchors: dict[int, int] = {}
    for meta in users.values():
        review_messages = meta.get("review_messages") if isinstance(meta, dict) else None
        if not isinstance(review_messages, dict):
            continue
        for refs in review_messages.values():
            if not isinstance(refs, list):
                continue
            for ref in refs:
                if not isinstance(ref, dict):
                    continue
                try:
                    chat_id = int(ref.get("chat_id", 0))
                    message_id = int(ref.get("message_id", 0))
                except (TypeError, ValueError, OverflowError):
                    continue
                if chat_id and message_id > anchors.get(chat_id, 0):
                    anchors[chat_id] = message_id

    persistence = build_atomic_persistence(PTB_PERSISTENCE_PATH)
    persistence.set_bot(bot)
    try:
        bot_data = await persistence.get_bot_data()
    except (OSError, TypeError):
        return anchors
    registry = bot_data.get(_REGISTRY_KEY) if isinstance(bot_data, dict) else None
    chats = registry.get("chats") if isinstance(registry, dict) else None
    if not isinstance(chats, dict):
        return anchors
    for raw_chat_id, raw_chat in chats.items():
        messages = raw_chat.get("messages") if isinstance(raw_chat, dict) else None
        if not isinstance(messages, dict):
            continue
        try:
            chat_id = int(raw_chat_id)
            message_id = max(int(value) for value in messages)
        except (TypeError, ValueError, OverflowError):
            continue
        if chat_id and message_id > anchors.get(chat_id, 0):
            anchors[chat_id] = message_id
    return anchors


async def cancel_pending_broadcasts() -> int:
    """Remove queued admin broadcasts so restarting cannot continue the leak."""

    cancelled = 0
    for source, event in outbox_snapshot():
        if event.get("kind") != "admin_broadcast":
            continue
        event_id = str(event.get("id") or "")
        if not event_id:
            continue
        await mutate_outbox_event(source, event_id, lambda _event: None)
        cancelled += 1
    return cancelled


async def _with_retry(operation: Callable[[], Awaitable[_T]]) -> _T:
    for attempt in range(4):
        await wait_flood_gate()
        try:
            return await operation()
        except RetryAfter as exc:
            delay = retry_after_seconds(exc, minimum=1.0) + 0.5
            await extend_flood_gate(delay)
        except (TimedOut, NetworkError, OSError):
            if attempt == 3:
                raise
            await asyncio.sleep(min(8.0, 2.0**attempt))
    raise RuntimeError("Telegram retry loop exhausted")


async def _delete_chunk(
    bot: Any,
    *,
    chat_id: int,
    message_ids: list[int],
    stats: EmergencyDeleteStats,
) -> bool:
    if not message_ids:
        return False
    try:
        await _with_retry(lambda: bot.delete_messages(chat_id=chat_id, message_ids=message_ids))
    except BadRequest as exc:
        if len(message_ids) == 1:
            stats.ids_rejected += 1
            return "can't be deleted for everyone" in str(exc).lower()
        midpoint = len(message_ids) // 2
        if await _delete_chunk(bot, chat_id=chat_id, message_ids=message_ids[midpoint:], stats=stats):
            return True
        return await _delete_chunk(bot, chat_id=chat_id, message_ids=message_ids[:midpoint], stats=stats)
    except Forbidden:
        raise
    except (TimedOut, NetworkError, OSError):
        stats.request_failures += 1
        return False
    else:
        # Telegram skips unknown IDs and doesn't return per-message results.
        stats.ids_accepted += len(message_ids)
        return False


async def purge_chat(
    bot: Any,
    *,
    chat_id: int,
    scan_depth: int,
    fallback_upper: int,
    anchor: int | None,
    stats: EmergencyDeleteStats,
) -> None:
    """Create an upper-bound marker and sweep a guessed descending ID range."""

    stats.chats_total += 1
    try:
        marker = await _with_retry(lambda: bot.send_message(chat_id=chat_id, text=_MARKER_TEXT))
        upper = int(marker.message_id)
        lower = 1 if scan_depth == 0 else max(1, upper - scan_depth + 1)
    except (BadRequest, Forbidden, TimedOut, NetworkError, OSError, TypeError, ValueError, OverflowError):
        stats.fallback_chats += 1
        if anchor:
            lower = max(1, anchor - scan_depth)
            upper = anchor + scan_depth
        else:
            lower = 1
            upper = fallback_upper

    stats.chats_marked += 1
    high = upper
    try:
        while high >= lower:
            low = max(lower, high - _BATCH_SIZE + 1)
            if await _delete_chunk(
                bot,
                chat_id=chat_id,
                message_ids=list(range(low, high + 1)),
                stats=stats,
            ):
                stats.age_cutoffs += 1
                break
            high = low - 1
    except Forbidden:
        stats.chats_failed += 1


async def purge_recent_ranges(
    bot: Any,
    recipient_ids: Iterable[int],
    *,
    scan_depth: int,
    fallback_upper: int,
    anchors: dict[int, int] | None = None,
) -> EmergencyDeleteStats:
    stats = EmergencyDeleteStats()
    for chat_id in recipient_ids:
        await purge_chat(
            bot,
            chat_id=int(chat_id),
            scan_depth=scan_depth,
            fallback_upper=fallback_upper,
            anchor=(anchors or {}).get(int(chat_id)),
            stats=stats,
        )
        print(
            f"chat_id={chat_id} chats={stats.chats_marked}/{stats.chats_total} "
            f"accepted_ids={stats.ids_accepted} rejected_ids={stats.ids_rejected}",
            flush=True,
        )
    return stats


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="required acknowledgement that private-chat messages may be deleted",
    )
    parser.add_argument(
        "--scan-depth",
        type=int,
        default=_DEFAULT_SCAN_DEPTH,
        help="message IDs to try below the marker per chat; 0 scans down to ID 1",
    )
    parser.add_argument(
        "--fallback-upper",
        type=int,
        default=_DEFAULT_FALLBACK_UPPER,
        help="highest absolute message ID to try when a chat rejects the marker",
    )
    parser.add_argument(
        "--chat-id",
        type=int,
        action="append",
        default=[],
        help="limit cleanup to this chat ID; repeat for multiple chats",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if not args.execute:
        print("Refusing to delete messages without --execute.", file=sys.stderr)
        return 2
    if args.scan_depth < 0:
        print("--scan-depth must be zero or positive.", file=sys.stderr)
        return 2
    if args.fallback_upper <= 0:
        print("--fallback-upper must be positive.", file=sys.stderr)
        return 2

    configure_logging(
        level=LOG_LEVEL,
        use_json=LOG_JSON,
        force=True,
        secrets=(BOT_TOKEN, ADMIN_PASSWORD, OWNER_PASSWORD, REMNAWAVE_METRICS_PASS),
    )
    lock = SingleInstanceLock(INSTANCE_LOCK_PATH)
    try:
        lock.acquire()
    except InstanceAlreadyRunning as exc:
        print(f"Stop MaintBot before cleanup: {exc}", file=sys.stderr)
        return ALREADY_RUNNING_EXIT_CODE

    try:
        initialize_storage(DATA_DIR)
        users = authorized_users_snapshot()
        recipients = (
            sorted(set(user_id for user_id in args.chat_id if user_id > 0))
            if args.chat_id
            else known_recipient_ids(users)
        )
        if not recipients:
            print("No recipient chat IDs found.", file=sys.stderr)
            return 1

        cancelled = asyncio.run(cancel_pending_broadcasts())

        async def run() -> EmergencyDeleteStats:
            async with Bot(token=BOT_TOKEN) as bot:
                anchors = await known_message_anchors(bot, users)
                return await purge_recent_ranges(
                    bot,
                    recipients,
                    scan_depth=args.scan_depth,
                    fallback_upper=args.fallback_upper,
                    anchors=anchors,
                )

        stats = asyncio.run(run())
    except Exception as exc:
        detail = " ".join(str(exc).split()) or exc.__class__.__name__
        print(f"Emergency cleanup failed: {detail}", file=sys.stderr)
        return 1
    finally:
        lock.release()

    print(
        "Emergency cleanup finished: "
        f"cancelled_broadcasts={cancelled} chats={stats.chats_marked}/{stats.chats_total} "
        f"chat_failures={stats.chats_failed} accepted_ids={stats.ids_accepted} "
        f"rejected_ids={stats.ids_rejected} fallback_chats={stats.fallback_chats} "
        f"age_cutoffs={stats.age_cutoffs} request_failures={stats.request_failures}"
    )
    return 0 if stats.chats_marked else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "EmergencyDeleteStats",
    "cancel_pending_broadcasts",
    "known_message_anchors",
    "known_recipient_ids",
    "main",
    "purge_chat",
    "purge_recent_ranges",
]
