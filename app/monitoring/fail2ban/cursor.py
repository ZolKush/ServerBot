"""Rotation-safe, byte-exact Fail2Ban cursor reader."""

from __future__ import annotations

import contextlib
import hashlib
from datetime import datetime, timedelta
from typing import Any

from ...config import FAIL2BAN_DIGEST_MAX_BYTES, TZ, server_monitoring_fingerprint
from ...storage import outbox_snapshot
from .models import Fail2banEvent, FileIdentity, FileIdentityChangedError
from .parser import parse_fail2ban_events
from .source import (
    file_identity,
    get_server,
    read_range,
    server_timezone,
    try_file_identity,
)


def identity_matches(cursor: dict[str, Any], identity: FileIdentity) -> bool:
    try:
        return int(cursor.get("device", -1)) == identity.device and int(cursor.get("inode", -1)) == identity.inode
    except (TypeError, ValueError):
        return False


def cursor_has_pending_delivery(server_key: str) -> bool:
    for source, event in outbox_snapshot():
        if source != "important":
            continue
        completion = event.get("completion")
        if (
            isinstance(completion, dict)
            and completion.get("type") == "fail2ban_cursor"
            and completion.get("server_key") == server_key
        ):
            return True
    return False


async def _read_fail2ban_increment_once(
    server_key: str,
    cursor: dict[str, Any] | None,
) -> tuple[list[Fail2banEvent], dict[str, Any], datetime, bool]:
    server = get_server(server_key)
    base_path = server.fail2ban_log_path
    current_identity = await file_identity(server_key, base_path)
    if current_identity is None:
        raise RuntimeError("stat fail2ban log unavailable")

    source_path = base_path
    source_identity = current_identity
    offset = 0
    carry = ""
    first_run = not cursor
    drop_prefix = False
    if cursor:
        source_path = str(cursor.get("path") or base_path)
        source_identity = await try_file_identity(server_key, source_path) or current_identity
        if not identity_matches(cursor, source_identity):
            rotated_path = base_path + ".1"
            rotated_identity = await try_file_identity(server_key, rotated_path)
            if rotated_identity and identity_matches(cursor, rotated_identity):
                source_path = rotated_path
                source_identity = rotated_identity
            else:
                source_path = base_path
                source_identity = current_identity
                offset = 0
        if identity_matches(cursor, source_identity):
            try:
                offset = max(0, int(cursor.get("offset", 0) or 0))
            except (TypeError, ValueError):
                offset = 0
            carry = str(cursor.get("carry") or "")[-8192:]
        if source_identity.size < offset:
            offset = 0
            carry = ""
    else:
        offset = max(0, current_identity.size - FAIL2BAN_DIGEST_MAX_BYTES)
        drop_prefix = offset > 0

    read_result = await read_range(
        server_key,
        source_path,
        offset,
        FAIL2BAN_DIGEST_MAX_BYTES,
        source_identity,
    )
    text = read_result.text
    consumed = read_result.consumed
    source_identity = read_result.identity
    if drop_prefix and "\n" in text:
        text = text.split("\n", 1)[1]
    elif drop_prefix:
        text = ""

    combined = carry + text
    if combined.endswith("\n"):
        complete_lines = combined.splitlines()
        next_carry = ""
    else:
        parts = combined.split("\n")
        complete_lines = parts[:-1]
        next_carry = parts[-1][-8192:] if parts else ""

    next_offset = offset + consumed
    next_path = source_path
    next_identity = source_identity
    has_more = next_offset < source_identity.size
    if source_path != base_path and next_offset >= source_identity.size:
        if next_carry:
            complete_lines.append(next_carry)
            next_carry = ""
        next_path = base_path
        next_identity = current_identity
        next_offset = 0
        has_more = current_identity.size > 0

    events = parse_fail2ban_events(
        complete_lines,
        timezone=server_timezone(server_key),
    )
    previous_fingerprints = [
        str(value) for value in ((cursor or {}).get("recent_fingerprints") or []) if isinstance(value, str)
    ][-200:]
    seen = set(previous_fingerprints)
    deduplicated: list[Fail2banEvent] = []
    new_fingerprints: list[str] = []
    for event in events:
        fingerprint = hashlib.sha256(event.raw.encode("utf-8", errors="replace")).hexdigest()[:24]
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        deduplicated.append(event)
        new_fingerprints.append(fingerprint)

    since = datetime.now(TZ) - timedelta(days=1)
    if cursor and cursor.get("updated_at"):
        with contextlib.suppress(TypeError, ValueError):
            since = datetime.fromisoformat(str(cursor["updated_at"])).astimezone(TZ)
    elif first_run:
        deduplicated = [event for event in deduplicated if event.ts.astimezone(TZ) >= since]

    next_cursor = {
        "path": next_path,
        "device": next_identity.device,
        "inode": next_identity.inode,
        "offset": next_offset,
        "carry": next_carry,
        "updated_at": datetime.now(TZ).isoformat(),
        "recent_fingerprints": [
            *previous_fingerprints[-100:],
            *new_fingerprints,
        ][-200:],
        "_config_fingerprint": server_monitoring_fingerprint(server),
    }
    return deduplicated, next_cursor, since, has_more


async def read_fail2ban_increment(
    server_key: str,
    cursor: dict[str, Any] | None,
) -> tuple[list[Fail2banEvent], dict[str, Any], datetime, bool]:
    """Read a stable increment, retrying path changes caused by logrotate."""

    last_error: FileIdentityChangedError | None = None
    for _ in range(3):
        try:
            return await _read_fail2ban_increment_once(server_key, cursor)
        except FileIdentityChangedError as exc:
            last_error = exc
    if last_error is None:  # pragma: no cover - the loop always executes
        raise RuntimeError("fail2ban read retry loop did not execute")
    raise last_error


__all__ = ["cursor_has_pending_delivery", "read_fail2ban_increment"]
