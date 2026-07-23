"""Scheduled, outbox-backed Fail2Ban digest delivery."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

from telegram.ext import ContextTypes

from ...bot.guards import authorized_ids
from ...bot.ui import html_escape
from ...config import SERVERS, TZ, logger
from ...messaging.outbox import message_payload
from ...storage import (
    ImportantData,
    enqueue_important_outbox,
    get_fail2ban_cursor,
    make_outbox_event,
    update_important_data,
)
from .cursor import cursor_has_pending_delivery, read_fail2ban_increment
from .models import Fail2banEvent
from .views import build_fail2ban_digest_text


async def fail2ban_daily_digest(
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    admin_ids = authorized_ids(role_filter="admin")
    if not admin_ids:
        return
    semaphore = asyncio.Semaphore(3)

    async def process_server(server_key: str) -> None:
        server = SERVERS[server_key]
        if not server.fail2ban_enabled or cursor_has_pending_delivery(server_key):
            return
        async with semaphore:
            try:
                original_cursor = get_fail2ban_cursor(server_key)
                working_cursor = original_cursor
                all_events: list[Fail2banEvent] = []
                since = datetime.now(TZ) - timedelta(days=1)
                for _ in range(4):
                    events, next_cursor, batch_since, has_more = await read_fail2ban_increment(
                        server_key, working_cursor
                    )
                    all_events.extend(events)
                    since = min(since, batch_since)
                    working_cursor = next_cursor
                    if not has_more:
                        break
                if working_cursor is None:
                    return

                ban_events = [event for event in all_events if event.action in {"Ban", "Restore Ban"}]
                expected_cursor = original_cursor or {}
                if not ban_events:

                    def advance_without_delivery(config: ImportantData) -> bool:
                        current = config.fail2ban_cursors.get(server_key) or {}
                        if current != expected_cursor:
                            return False
                        config.fail2ban_cursors[server_key] = working_cursor
                        return True

                    advanced = await update_important_data(advance_without_delivery)
                    if not advanced:
                        logger.info(
                            "Fail2ban cursor changed concurrently; skip advance server=%s",
                            server_key,
                        )
                    return

                until = datetime.now(TZ)
                payload = f"🌍 <b>Сервер:</b> {html_escape(server.label)}\n" + build_fail2ban_digest_text(
                    all_events,
                    since=since,
                    until=until,
                )
                event = make_outbox_event(
                    kind="fail2ban_daily_digest",
                    recipient_ids=admin_ids,
                    payload=message_payload(payload),
                )
                event["completion"] = {
                    "type": "fail2ban_cursor",
                    "server_key": server_key,
                    "cursor": working_cursor,
                }

                def queue_digest(config: ImportantData) -> bool:
                    current = config.fail2ban_cursors.get(server_key) or {}
                    if current != expected_cursor:
                        return False
                    for pending in config.outbox.values():
                        if not isinstance(pending, dict):
                            continue
                        completion = pending.get("completion")
                        if (
                            isinstance(completion, dict)
                            and completion.get("type") == "fail2ban_cursor"
                            and completion.get("server_key") == server_key
                        ):
                            return False
                    enqueue_important_outbox(config, event)
                    return True

                queued = await update_important_data(queue_digest)
                if not queued:
                    logger.info(
                        "Fail2ban digest already queued or cursor changed server=%s",
                        server_key,
                    )
            except FileNotFoundError:
                logger.warning(
                    "fail2ban_daily_digest server=%s: log file not found",
                    server_key,
                )
            except PermissionError:
                logger.warning(
                    "fail2ban_daily_digest server=%s: permission denied",
                    server_key,
                )
            except Exception:
                logger.exception(
                    "fail2ban_daily_digest failed for server=%s",
                    server_key,
                )

    await asyncio.gather(*(process_server(server_key) for server_key in SERVERS))


__all__ = ["fail2ban_daily_digest"]
