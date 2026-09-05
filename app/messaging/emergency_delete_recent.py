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
    REMNAWAVE_METRICS_PASS,
)
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
_MARKER_TEXT = "."


@dataclass
class EmergencyDeleteStats:
    chats_total: int = 0
    chats_marked: int = 0
    chats_failed: int = 0
    ids_accepted: int = 0
    ids_rejected: int = 0
    request_failures: int = 0


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
) -> None:
    if not message_ids:
        return
    try:
        await _with_retry(lambda: bot.delete_messages(chat_id=chat_id, message_ids=message_ids))
    except BadRequest:
        if len(message_ids) == 1:
            stats.ids_rejected += 1
            return
        midpoint = len(message_ids) // 2
        await _delete_chunk(bot, chat_id=chat_id, message_ids=message_ids[:midpoint], stats=stats)
        await _delete_chunk(bot, chat_id=chat_id, message_ids=message_ids[midpoint:], stats=stats)
    except Forbidden:
        raise
    except (TimedOut, NetworkError, OSError):
        stats.request_failures += 1
    else:
        # Telegram skips unknown IDs and doesn't return per-message results.
        stats.ids_accepted += len(message_ids)


async def purge_chat(
    bot: Any,
    *,
    chat_id: int,
    scan_depth: int,
    stats: EmergencyDeleteStats,
) -> None:
    """Create an upper-bound marker and sweep a guessed descending ID range."""

    stats.chats_total += 1
    try:
        marker = await _with_retry(lambda: bot.send_message(chat_id=chat_id, text=_MARKER_TEXT))
        upper = int(marker.message_id)
    except (BadRequest, Forbidden, TimedOut, NetworkError, OSError, TypeError, ValueError, OverflowError):
        stats.chats_failed += 1
        return

    stats.chats_marked += 1
    lower = 1 if scan_depth == 0 else max(1, upper - scan_depth + 1)
    high = upper
    try:
        while high >= lower:
            low = max(lower, high - _BATCH_SIZE + 1)
            await _delete_chunk(
                bot,
                chat_id=chat_id,
                message_ids=list(range(low, high + 1)),
                stats=stats,
            )
            high = low - 1
    except Forbidden:
        stats.chats_failed += 1


async def purge_recent_ranges(
    bot: Any,
    recipient_ids: Iterable[int],
    *,
    scan_depth: int,
) -> EmergencyDeleteStats:
    stats = EmergencyDeleteStats()
    for chat_id in recipient_ids:
        await purge_chat(bot, chat_id=int(chat_id), scan_depth=scan_depth, stats=stats)
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
        recipients = (
            sorted(set(user_id for user_id in args.chat_id if user_id > 0))
            if args.chat_id
            else known_recipient_ids(authorized_users_snapshot())
        )
        if not recipients:
            print("No recipient chat IDs found.", file=sys.stderr)
            return 1

        cancelled = asyncio.run(cancel_pending_broadcasts())

        async def run() -> EmergencyDeleteStats:
            async with Bot(token=BOT_TOKEN) as bot:
                return await purge_recent_ranges(bot, recipients, scan_depth=args.scan_depth)

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
        f"rejected_ids={stats.ids_rejected} request_failures={stats.request_failures}"
    )
    return 0 if stats.chats_marked else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "EmergencyDeleteStats",
    "cancel_pending_broadcasts",
    "known_recipient_ids",
    "main",
    "purge_chat",
    "purge_recent_ranges",
]
