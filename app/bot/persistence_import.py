"""Explicit, read-only-source migration of trusted legacy Telegram pickle state."""

from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
from typing import Any

from telegram.ext import ExtBot
from telegram.ext._picklepersistence import _BotUnpickler

from .persistence_sqlite import SQLitePersistence


def load_legacy_pickle(source: str | Path | bytes) -> dict[str, Any]:
    """Validate and load a trusted PTB image without modifying its source files.

    A backup must be selected explicitly. Immutable bytes let the migration
    capture inputs once. The temporary bot is a codec placeholder; no network runs.
    """
    payload = source if isinstance(source, bytes) else Path(source).read_bytes()
    try:
        placeholder = ExtBot("0:legacy-persistence-placeholder")
        data = _BotUnpickler(placeholder, io.BytesIO(payload)).load()
        if isinstance(data, dict):
            # PTB writes unused context categories as None before first access.
            for field in ("user_data", "chat_data", "conversations", "bot_data"):
                if field in data and data[field] is None:
                    data[field] = {}
        _validate(data)
        return {**data, "_codec_bot": placeholder}
    except Exception as exc:
        raise ValueError("Legacy Telegram persistence does not contain valid PTB state") from exc


def _validate(data: Any) -> None:
    if not isinstance(data, dict):
        raise ValueError("Telegram persistence must be a dictionary")
    for field in ("user_data", "chat_data", "conversations"):
        if field not in data or not isinstance(data[field], dict):
            raise ValueError(f"Invalid Telegram persistence field: {field}")
    if not isinstance(data.get("bot_data", {}), dict):
        raise ValueError("Invalid Telegram bot_data")
    for field in ("user_data", "chat_data"):
        for key, value in data[field].items():
            if not isinstance(key, int) or isinstance(key, bool) or not isinstance(value, dict):
                raise ValueError(f"Invalid Telegram {field} row")
    for name, conversations in data["conversations"].items():
        if not isinstance(name, str) or not isinstance(conversations, dict):
            raise ValueError("Invalid Telegram conversation handler")
        for key in conversations:
            if not isinstance(key, tuple) or not all(isinstance(item, (str, int)) for item in key):
                raise ValueError("Invalid Telegram conversation key")
    callback = data.get("callback_data")
    if (
        callback is not None
        and callback != {}
        and not (
            isinstance(callback, tuple)
            and len(callback) == 2
            and isinstance(callback[0], list)
            and isinstance(callback[1], dict)
        )
    ):
        raise ValueError("Invalid Telegram callback_data")


def import_legacy_pickle(database_path: str | Path, source: str | Path | bytes | dict[str, Any]) -> bool:
    """Import all Telegram rows atomically; caller must own the process lock.

    A previously imported database is unchanged. A database with other Telegram
    state is refused. A preloaded, validated image allows a dry-run and import to
    use precisely the same bytes even if the original file changes afterward.
    """
    data = source if isinstance(source, dict) else load_legacy_pickle(source)
    _validate(data)
    placeholder = data.get("_codec_bot")
    if not isinstance(placeholder, ExtBot):
        raise ValueError("Legacy state must come from load_legacy_pickle")
    persistence = SQLitePersistence(database_path)
    persistence.set_bot(placeholder)
    rows = [
        (kind, str(key), persistence._encode(value))
        for kind, field in (("user", "user_data"), ("chat", "chat_data"))
        for key, value in data[field].items()
    ]
    rows.append(("bot", "0", persistence._encode(data.get("bot_data", {}))))
    callback = data.get("callback_data")
    if callback:
        rows.append(("callback", "0", persistence._encode(callback)))
    conversations = [
        (name, json.dumps(key, separators=(",", ":"), ensure_ascii=True), persistence._encode(value))
        for name, states in data["conversations"].items()
        for key, value in states.items()
        if value is not None
    ]
    with contextlib.closing(persistence._connect()) as connection, connection:
        connection.execute("BEGIN IMMEDIATE")
        if connection.execute("SELECT 1 FROM telegram_migrations WHERE name='pickle-v1'").fetchone():
            return False
        if (
            connection.execute("SELECT 1 FROM telegram_data LIMIT 1").fetchone()
            or connection.execute("SELECT 1 FROM telegram_conversations LIMIT 1").fetchone()
        ):
            raise ValueError("Refusing to overwrite existing Telegram database state")
        connection.executemany("INSERT INTO telegram_data VALUES (?, ?, ?)", rows)
        connection.executemany("INSERT INTO telegram_conversations VALUES (?, ?, ?)", conversations)
        connection.execute("INSERT INTO telegram_migrations VALUES ('pickle-v1')")
    return True


__all__ = ["import_legacy_pickle", "load_legacy_pickle"]
