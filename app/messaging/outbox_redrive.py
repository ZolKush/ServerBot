"""Explicit offline redrive for retained Telegram outbox dead letters."""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..storage import initialize_storage, mutate_outbox_event
from .outbox_state import DEAD_LETTER_STATUS


async def redrive_outbox_dead_letters(source: str, event_id: str) -> bool:
    """Make retained dead-letter recipients eligible for an explicit retry."""

    redriven = False

    def apply(event: dict[str, Any]) -> dict[str, Any]:
        nonlocal redriven
        recipients = event.get("recipients")
        if not isinstance(recipients, dict):
            return event
        now = datetime.now(timezone.utc).isoformat()
        for raw_state in recipients.values():
            if not isinstance(raw_state, dict) or raw_state.get("status") != DEAD_LETTER_STATUS:
                continue
            delivery_was_recorded = all(
                isinstance(raw_state.get(field), int) and raw_state[field] > 0
                for field in ("delivered_chat_id", "delivered_message_id")
            )
            raw_state.update(
                {
                    "status": ("delivered_pending_registration" if delivery_was_recorded else "pending"),
                    "attempts": 0,
                    "next_attempt_at": now,
                    "last_error": "",
                    "dead_lettered_at": "",
                }
            )
            redriven = True
        return event

    await mutate_outbox_event(source, event_id, apply)
    return redriven


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--source", choices=("user", "important"), required=True)
    parser.add_argument("--event-id", required=True)
    args = parser.parse_args(argv)
    try:
        initialize_storage(args.data_dir)
        changed = asyncio.run(redrive_outbox_dead_letters(args.source, args.event_id))
    except Exception as exc:
        detail = " ".join(str(exc).split()) or exc.__class__.__name__
        print(f"Outbox redrive failed: {detail}", file=sys.stderr)
        return 1
    if not changed:
        print("No dead-letter recipients found for the selected event.", file=sys.stderr)
        return 1
    print(f"Redriven outbox event: source={args.source} event_id={args.event_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main", "redrive_outbox_dead_letters"]
