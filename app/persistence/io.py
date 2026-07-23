"""Durable and strict filesystem primitives used by persistence code."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import uuid
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from .errors import DuplicateJsonKeyError, SchemaError


def secure_directory(path: Path) -> None:
    missing: list[Path] = []
    cursor = path
    while not cursor.exists() and cursor != cursor.parent:
        missing.append(cursor)
        cursor = cursor.parent
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if os.name != "nt":
        with contextlib.suppress(OSError):
            path.chmod(0o700)
        for created in reversed(missing):
            with contextlib.suppress(OSError):
                created.chmod(0o700)
            fsync_directory(created.parent)


def tighten_file_permissions(path: Path) -> None:
    if os.name != "nt":
        with contextlib.suppress(OSError):
            path.chmod(0o600)


def fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def encode_json(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")


def _reject_duplicate_keys(pairs: Iterable[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateJsonKeyError(f"duplicate JSON key: {key!r}")
        result[key] = value
    return result


def decode_json(payload: bytes, *, source: str = "JSON") -> Any:
    try:
        return json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda value: _reject_non_finite(value, source),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SchemaError(f"invalid {source}: {exc}") from exc


def _reject_non_finite(value: str, source: str) -> None:
    raise SchemaError(f"invalid {source}: non-finite number {value}")


def read_json(path: Path) -> Any:
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise SchemaError(f"cannot read {path}: {exc}") from exc
    return decode_json(payload, source=str(path))


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def write_bytes_durable(path: Path, payload: bytes, *, exclusive: bool = False) -> None:
    secure_directory(path.parent)
    flags = os.O_WRONLY | os.O_CREAT
    flags |= os.O_EXCL if exclusive else os.O_TRUNC
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        with contextlib.suppress(OSError):
            os.close(descriptor)
        raise
    tighten_file_permissions(path)
    fsync_directory(path.parent)


def write_atomic(path: Path, payload: bytes) -> None:
    secure_directory(path.parent)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        write_bytes_durable(temporary, payload, exclusive=True)
        replace_durable(temporary, path)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def replace_durable(source: Path, target: Path) -> None:
    secure_directory(target.parent)
    os.replace(source, target)
    tighten_file_permissions(target)
    fsync_directory(target.parent)


def resolve_inside(root: Path, relative_path: str) -> Path:
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise SchemaError(f"unsafe relative path: {relative_path!r}")
    resolved_root = root.resolve()
    resolved = (resolved_root / relative).resolve()
    if resolved != resolved_root and resolved_root not in resolved.parents:
        raise SchemaError(f"path escapes storage root: {relative_path!r}")
    return resolved


__all__ = [
    "decode_json",
    "encode_json",
    "fsync_directory",
    "read_json",
    "replace_durable",
    "resolve_inside",
    "secure_directory",
    "sha256_bytes",
    "sha256_file",
    "tighten_file_permissions",
    "write_atomic",
    "write_bytes_durable",
]
