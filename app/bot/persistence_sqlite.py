"""Row-based PTB state in the application database, using PTB's bot-aware codec."""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import pickle  # nosec B403 -- trusted local state uses PTB's pinned bot-aware codec.
import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

from telegram.ext import BasePersistence
from telegram.ext._picklepersistence import _BotPickler, _BotUnpickler

T = TypeVar("T")
_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS telegram_data ("
    "kind TEXT NOT NULL, entity_key TEXT NOT NULL, payload BLOB NOT NULL, "
    "PRIMARY KEY (kind, entity_key))",
    "CREATE TABLE IF NOT EXISTS telegram_conversations ("
    "handler TEXT NOT NULL, conversation_key TEXT NOT NULL, payload BLOB NOT NULL, "
    "PRIMARY KEY (handler, conversation_key))",
    "CREATE TABLE IF NOT EXISTS telegram_migrations (name TEXT PRIMARY KEY)",
)


class SQLitePersistence(BasePersistence[dict, dict, dict]):
    """Persist each user/chat/conversation separately, without a mutable file cache.

    The pinned PTB codec preserves tuples, datetimes, Telegram objects and bot
    placeholders. Database contents, like the previous pickle, must be trusted.
    Schema creation occurs on first use, never merely on import or construction.
    """

    def __init__(self, database_path: str | Path, *, update_interval: float = 60) -> None:
        super().__init__(update_interval=update_interval)
        self.database_path = Path(database_path)

    def _connect(self) -> sqlite3.Connection:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.database_path, timeout=5)
        try:
            connection.execute("PRAGMA synchronous=EXTRA")
            connection.execute("PRAGMA foreign_keys=ON")
            with connection:
                for statement in _SCHEMA:
                    connection.execute(statement)
            with contextlib.suppress(OSError):
                self.database_path.chmod(0o600)
            return connection
        except BaseException:
            connection.close()
            raise

    def _encode(self, value: object) -> bytes:
        stream = io.BytesIO()
        _BotPickler(self.bot, stream, protocol=pickle.HIGHEST_PROTOCOL).dump(value)
        return stream.getvalue()

    def _decode(self, payload: bytes) -> Any:
        # PTB's serializer is required to restore the current bot into objects.
        return _BotUnpickler(self.bot, io.BytesIO(payload)).load()

    async def _run(self, operation: Callable[..., T], *args: Any) -> T:
        task = asyncio.create_task(asyncio.to_thread(operation, *args))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            # A commit already executing in a thread must finish before shutdown.
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    continue
            task.result()
            raise

    def _read_data(self, kind: str) -> dict[str, Any]:
        with contextlib.closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT entity_key, payload FROM telegram_data WHERE kind = ?", (kind,)
            ).fetchall()
        return {key: self._decode(payload) for key, payload in rows}

    def _write_data(self, kind: str, key: str, data: object) -> None:
        payload = self._encode(data)
        with contextlib.closing(self._connect()) as connection, connection:
            connection.execute(
                "INSERT INTO telegram_data(kind, entity_key, payload) VALUES (?, ?, ?) "
                "ON CONFLICT(kind, entity_key) DO UPDATE SET payload=excluded.payload",
                (kind, key, payload),
            )

    def _drop_data(self, kind: str, key: str) -> None:
        with contextlib.closing(self._connect()) as connection, connection:
            connection.execute("DELETE FROM telegram_data WHERE kind=? AND entity_key=?", (kind, key))

    def _read_conversations(self, name: str) -> dict[tuple, object]:
        with contextlib.closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT conversation_key, payload FROM telegram_conversations WHERE handler=?", (name,)
            ).fetchall()
        return {tuple(json.loads(key)): self._decode(payload) for key, payload in rows}

    def _write_conversation(self, name: str, key: tuple, state: object | None) -> None:
        encoded_key = json.dumps(key, separators=(",", ":"), ensure_ascii=True)
        payload = self._encode(state) if state is not None else None
        with contextlib.closing(self._connect()) as connection, connection:
            if state is None:
                connection.execute(
                    "DELETE FROM telegram_conversations WHERE handler=? AND conversation_key=?", (name, encoded_key)
                )
            else:
                connection.execute(
                    "INSERT INTO telegram_conversations(handler, conversation_key, payload) VALUES (?, ?, ?) "
                    "ON CONFLICT(handler, conversation_key) DO UPDATE SET payload=excluded.payload",
                    (name, encoded_key, payload),
                )

    async def get_user_data(self) -> dict[int, dict]:
        return {int(key): value for key, value in (await self._run(self._read_data, "user")).items()}

    async def get_chat_data(self) -> dict[int, dict]:
        return {int(key): value for key, value in (await self._run(self._read_data, "chat")).items()}

    async def get_bot_data(self) -> dict:
        return (await self._run(self._read_data, "bot")).get("0", {})

    async def get_callback_data(self) -> Any:
        return (await self._run(self._read_data, "callback")).get("0")

    async def get_conversations(self, name: str) -> dict[tuple, object]:
        return await self._run(self._read_conversations, name)

    async def update_user_data(self, user_id: int, data: dict) -> None:
        await self._run(self._write_data, "user", str(user_id), data)

    async def update_chat_data(self, chat_id: int, data: dict) -> None:
        await self._run(self._write_data, "chat", str(chat_id), data)

    async def update_bot_data(self, data: dict) -> None:
        await self._run(self._write_data, "bot", "0", data)

    async def update_callback_data(self, data: Any) -> None:
        await self._run(self._write_data, "callback", "0", data)

    async def update_conversation(self, name: str, key: tuple, new_state: object | None) -> None:
        await self._run(self._write_conversation, name, key, new_state)

    async def drop_user_data(self, user_id: int) -> None:
        await self._run(self._drop_data, "user", str(user_id))

    async def drop_chat_data(self, chat_id: int) -> None:
        await self._run(self._drop_data, "chat", str(chat_id))

    async def refresh_user_data(self, user_id: int, user_data: dict) -> None:
        """A single bot process owns the in-memory PTB context between updates."""
        del user_id, user_data

    async def refresh_chat_data(self, chat_id: int, chat_data: dict) -> None:
        """A single bot process owns the in-memory PTB context between updates."""
        del chat_id, chat_data

    async def refresh_bot_data(self, bot_data: dict) -> None:
        """A single bot process owns the in-memory PTB context between updates."""
        del bot_data

    async def flush(self) -> None:
        """Every update commits durably; Application.stop awaits those updates."""


def build_sqlite_persistence(database_path: str | Path) -> SQLitePersistence:
    return SQLitePersistence(database_path)


__all__ = ["SQLitePersistence", "build_sqlite_persistence"]
