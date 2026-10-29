"""Convert repository projections to individual SQLite entity rows.

Interpolated SQL identifiers are fixed schema constants; values use bindings.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from .errors import SchemaError
from .io import decode_json
from .sqlite_schema import DOCUMENT_TABLES, INDEX_FIELDS, INDEXED_STORES, MAPPING_TABLES


def _encode(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False, separators=(",", ":"))


def _decode(value: str) -> Any:
    return decode_json(value.encode("utf-8"), source="SQLite payload")


def read_store(connection: sqlite3.Connection, name: str, next_id: int | None) -> Any:
    if name == "audit.events":
        return [_decode(row[0]) for row in connection.execute("SELECT payload FROM audit_events ORDER BY position")]
    if name == "support.ticket_messages":
        histories: dict[str, list[Any]] = {
            row[0]: [] for row in connection.execute("SELECT ticket_key FROM ticket_histories ORDER BY rowid")
        }
        for ticket_key, payload in connection.execute(
            "SELECT ticket_key, payload FROM ticket_messages ORDER BY ticket_key, position"
        ):
            histories[ticket_key].append(_decode(payload))
        return histories
    if name in DOCUMENT_TABLES:
        table = DOCUMENT_TABLES[name]
        return {
            key: _decode(payload)
            for key, payload in connection.execute(f"SELECT field_name, payload FROM {table}")  # nosec B608
        }
    table = MAPPING_TABLES[name]
    result = {
        key: _decode(payload)
        for key, payload in connection.execute(f"SELECT record_key, payload FROM {table} ORDER BY rowid")  # nosec B608
    }
    if name == "messaging.outbox":
        for event_key, recipient_key, payload in connection.execute(
            "SELECT event_key, recipient_key, payload FROM outbox_recipients ORDER BY rowid"
        ):
            result[event_key]["recipients"][recipient_key] = _decode(payload)
    return {"next_id": next_id, "items": result} if name in INDEXED_STORES else result


def write_store(connection: sqlite3.Connection, name: str, data: Any) -> None:
    if name == "audit.events":
        connection.execute("DELETE FROM audit_events")
        connection.executemany(
            "INSERT INTO audit_events(position, payload) VALUES (?, ?)",
            [(index, _encode(event)) for index, event in enumerate(data)],
        )
        return
    if name == "support.ticket_messages":
        _write_histories(connection, data)
        return
    if name in DOCUMENT_TABLES:
        table = DOCUMENT_TABLES[name]
        _sync_rows(
            connection, table, "field_name", {key: (_encode(value),) for key, value in data.items()}, ("payload",)
        )
        return
    table = MAPPING_TABLES[name]
    records = data["items"] if name in INDEXED_STORES else data
    fields = INDEX_FIELDS.get(table, ())
    rows = {}
    recipients: dict[tuple[str, str], tuple[Any, ...]] = {}
    for key, value in records.items():
        payload = value
        if name == "messaging.outbox" and isinstance(value, dict) and "recipients" in value:
            payload = dict(value)
            raw_recipients = payload["recipients"]
            if not isinstance(raw_recipients, dict):
                raise SchemaError(f"outbox event {key} recipients must be an object")
            payload["recipients"] = {}
            for recipient_key, recipient in raw_recipients.items():
                if not isinstance(recipient, dict):
                    raise SchemaError(f"outbox event {key} recipient {recipient_key} must be an object")
                recipients[(key, recipient_key)] = (
                    recipient.get("status"),
                    recipient.get("next_attempt_at"),
                    _encode(recipient),
                )
        indexed = tuple(
            str(value[field]) if isinstance(value, dict) and value.get(field) is not None else None for field in fields
        )
        rows[key] = (_encode(payload), *indexed)
    _sync_rows(connection, table, "record_key", rows, ("payload", *fields))
    if name == "messaging.outbox":
        _write_recipients(connection, recipients)


def _sync_rows(
    connection: sqlite3.Connection,
    table: str,
    key_field: str,
    rows: dict[str, tuple[Any, ...]],
    fields: tuple[str, ...],
) -> None:
    """Write only changed rows while preserving stable keys and insertion order."""

    columns = ", ".join(fields)
    # All identifiers originate in sqlite_schema; entity keys and values use bindings.
    existing = {
        row[0]: tuple(row[1:])
        for row in connection.execute(f"SELECT {key_field}, {columns} FROM {table}")  # nosec B608
    }
    removed = set(existing) - set(rows)
    connection.executemany(f"DELETE FROM {table} WHERE {key_field}=?", [(key,) for key in removed])  # nosec B608
    updates = ", ".join(f"{field}=excluded.{field}" for field in fields)
    placeholders = ", ".join("?" for _ in range(len(fields) + 1))
    connection.executemany(
        f"INSERT INTO {table}({key_field}, {columns}) VALUES ({placeholders}) "  # nosec B608
        f"ON CONFLICT({key_field}) DO UPDATE SET {updates}",
        [(key, *values) for key, values in rows.items() if existing.get(key) != values],
    )


def _write_histories(connection: sqlite3.Connection, histories: dict[str, Any]) -> None:
    for messages in histories.values():
        if not isinstance(messages, list):
            raise SchemaError("ticket history must be an array")
    existing = {row[0] for row in connection.execute("SELECT ticket_key FROM ticket_histories")}
    connection.executemany(
        "DELETE FROM ticket_histories WHERE ticket_key=?", [(key,) for key in existing - set(histories)]
    )
    connection.executemany(
        "INSERT INTO ticket_histories(ticket_key) VALUES (?)", [(key,) for key in histories if key not in existing]
    )
    for key, messages in histories.items():
        old = [
            row[0]
            for row in connection.execute(
                "SELECT payload FROM ticket_messages WHERE ticket_key=? ORDER BY position", (key,)
            )
        ]
        encoded = [_encode(message) for message in messages]
        if encoded != old:
            connection.execute("DELETE FROM ticket_messages WHERE ticket_key=?", (key,))
            connection.executemany(
                "INSERT INTO ticket_messages(ticket_key, position, payload) VALUES (?, ?, ?)",
                [(key, index, payload) for index, payload in enumerate(encoded)],
            )


def _write_recipients(connection: sqlite3.Connection, recipients: dict[tuple[str, str], tuple[Any, ...]]) -> None:
    existing = {
        (row[0], row[1]): tuple(row[2:])
        for row in connection.execute(
            "SELECT event_key, recipient_key, status, next_attempt_at, payload FROM outbox_recipients"
        )
    }
    connection.executemany(
        "DELETE FROM outbox_recipients WHERE event_key=? AND recipient_key=?", list(set(existing) - set(recipients))
    )
    connection.executemany(
        "INSERT INTO outbox_recipients(event_key, recipient_key, status, next_attempt_at, payload) VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(event_key, recipient_key) DO UPDATE SET status=excluded.status, "
        "next_attempt_at=excluded.next_attempt_at, payload=excluded.payload",
        [(*key, *values) for key, values in recipients.items() if existing.get(key) != values],
    )


__all__ = ["read_store", "write_store"]
