"""Checksum-verified pre-migration backup creation."""

from __future__ import annotations

import contextlib
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..errors import MigrationError
from ..io import (
    encode_json,
    fsync_directory,
    read_json,
    secure_directory,
    sha256_bytes,
    sha256_file,
    write_bytes_durable,
)
from ..layout import BACKUP_SCHEMA_VERSION, STATE_LOCK_FILE, TRANSACTIONS_DIR

BACKUP_MANIFEST_FILE = "backup_manifest.json"


def create_verified_backup(
    data_root: Path,
    *,
    backup_root: Path,
    source_fingerprint: str,
) -> Path:
    source = data_root.resolve()
    destination_root = backup_root.resolve()
    if destination_root == source or source in destination_root.parents:
        raise MigrationError("backup root must be outside the source data directory")
    secure_directory(destination_root)
    final = destination_root / f"pre-split-v4-{source_fingerprint[:16]}"
    expected = _source_files(source)
    if final.exists():
        _validate_existing_backup(final, source_fingerprint, expected)
        return final

    incomplete = destination_root / f".{final.name}.incomplete-{uuid.uuid4().hex}"
    secure_directory(incomplete)
    try:
        for relative_path, metadata in expected.items():
            source_path = source / relative_path
            target_path = incomplete / relative_path
            payload = source_path.read_bytes()
            if sha256_bytes(payload) != metadata["sha256"]:
                raise MigrationError(f"source changed while backing up: {relative_path}")
            write_bytes_durable(target_path, payload, exclusive=True)
        confirmed = _source_files(source)
        if confirmed != expected:
            raise MigrationError("source data changed while the backup was being created")
        manifest = {
            "schema_version": BACKUP_SCHEMA_VERSION,
            "source_format": "monolithic-json-v4",
            "source_fingerprint": source_fingerprint,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "files": expected,
        }
        write_bytes_durable(
            incomplete / BACKUP_MANIFEST_FILE,
            encode_json(manifest),
            exclusive=True,
        )
        _validate_existing_backup(incomplete, source_fingerprint, expected)
        os.replace(incomplete, final)
        fsync_directory(destination_root)
        return final
    except BaseException:
        if incomplete.exists():
            shutil.rmtree(incomplete)
        raise


def _source_files(source: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for path in sorted(source.rglob("*")):
        if path.is_symlink():
            raise MigrationError(f"data backup refuses symbolic link: {path}")
        if not path.is_file():
            continue
        relative = path.relative_to(source)
        normalized = relative.as_posix()
        if normalized == STATE_LOCK_FILE or relative.parts[:1] == (TRANSACTIONS_DIR,):
            continue
        result[normalized] = {
            "size": path.stat().st_size,
            "sha256": sha256_file(path),
        }
    return result


def _validate_existing_backup(
    backup: Path,
    source_fingerprint: str,
    expected: dict[str, dict[str, Any]],
) -> None:
    manifest_path = backup / BACKUP_MANIFEST_FILE
    if not manifest_path.is_file():
        raise MigrationError(f"backup has no manifest: {backup}")
    raw = read_json(manifest_path)
    if not isinstance(raw, dict):
        raise MigrationError(f"backup manifest is invalid: {manifest_path}")
    required = {"schema_version", "source_format", "source_fingerprint", "created_at", "files"}
    if set(raw) != required or raw.get("schema_version") != BACKUP_SCHEMA_VERSION:
        raise MigrationError(f"backup manifest schema is invalid: {manifest_path}")
    if raw.get("source_fingerprint") != source_fingerprint or raw.get("files") != expected:
        raise MigrationError(f"existing backup does not match migration source: {backup}")
    for relative_path, metadata in expected.items():
        copied = backup / relative_path
        if not copied.is_file():
            raise MigrationError(f"backup is missing {relative_path}")
        if copied.stat().st_size != metadata["size"] or sha256_file(copied) != metadata["sha256"]:
            raise MigrationError(f"backup checksum mismatch: {relative_path}")


def remove_incomplete_backups(backup_root: Path) -> None:
    """Remove only abandoned migration-owned incomplete backup directories."""

    if not backup_root.exists():
        return
    for path in backup_root.iterdir():
        if path.is_dir() and path.name.startswith(".pre-split-v4-") and ".incomplete-" in path.name:
            with contextlib.suppress(OSError):
                shutil.rmtree(path)


__all__ = ["BACKUP_MANIFEST_FILE", "create_verified_backup", "remove_incomplete_backups"]
