"""Strict, bounded JSON reads for operator-managed configuration files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

MAX_CONFIG_BYTES = 1_000_000


class JsonConfigError(RuntimeError):
    """A safe-to-display configuration read error."""


class _DuplicateKey(ValueError):
    pass


def _object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKey(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def load_json_object(path: str | Path, *, field_name: str) -> dict[str, Any]:
    """Read one UTF-8 JSON object while rejecting duplicates and oversized input."""

    config_path = Path(path)
    if not config_path.is_file():
        raise JsonConfigError(f"{field_name} not found or is not a regular file: {config_path}")
    try:
        with config_path.open("rb") as stream:
            payload = stream.read(MAX_CONFIG_BYTES + 1)
    except OSError as exc:
        raise JsonConfigError(f"cannot read {field_name} {config_path}: {exc}") from exc
    if len(payload) > MAX_CONFIG_BYTES:
        raise JsonConfigError(f"{field_name} is larger than {MAX_CONFIG_BYTES} bytes: {config_path}")
    try:
        text = payload.decode("utf-8")
        raw = json.loads(text, object_pairs_hook=_object_without_duplicates)
    except (UnicodeError, json.JSONDecodeError, _DuplicateKey) as exc:
        raise JsonConfigError(f"invalid {field_name} {config_path}: {exc}") from None
    if not isinstance(raw, dict):
        raise JsonConfigError(f"{field_name} root must be a JSON object: {config_path}")
    return raw


__all__ = ["JsonConfigError", "MAX_CONFIG_BYTES", "load_json_object"]
