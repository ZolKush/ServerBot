"""Explicit, offline JSON/pickle to SQLite migration with atomic publication."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sqlite3
import uuid
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from ..runtime.lock import InstanceAlreadyRunning, SingleInstanceLock
from .aggregate_mapping import important_data_from_uow, user_data_from_uow
from .errors import MigrationError, PersistenceError, StorageConflictError
from .io import fsync_directory, secure_directory, tighten_file_permissions
from .snapshots import BackendSnapshot, StoreSnapshot
from .sqlite_backend import SQLiteBackend
from .sqlite_import_source import backup_source, capture_source
from .unit_of_work import JsonUnitOfWork


class _CapturedBackend:
    """Run startup projections on captured input without filesystem writes."""

    def __init__(self, stores: dict[str, Any]) -> None:
        self._snapshot = BackendSnapshot(
            1,
            "import-validation",
            {name: StoreSnapshot(name, 1, value) for name, value in stores.items()},
            None,
        )

    def snapshot(self) -> BackendSnapshot:
        return self._snapshot

    def commit(self, *, base_revision: int, changes: Mapping[str, Any]) -> BackendSnapshot:
        raise StorageConflictError("captured migration input is read-only")


@dataclass(frozen=True)
class SQLiteMigrationReport:
    dry_run: bool
    already_migrated: bool
    database: str
    source_format: str
    source_fingerprint: str
    backup_path: str | None
    telegram_imported: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def migrate_to_sqlite(
    data_root: Path | str,
    *,
    dry_run: bool = False,
    ptb_path: Path | None = None,
    instance_lock: Path | None = None,
) -> SQLiteMigrationReport:
    """Publish the complete new DB once; leave all source bytes untouched.

    Dry runs perform no writes. Real migration holds the same process lock as
    the bot; pass its configured INSTANCE_LOCK_PATH when it is customized.
    """
    root = Path(data_root).resolve()
    if dry_run:
        return _migrate(root, dry_run=True, ptb_path=ptb_path)
    lock_path = instance_lock or root / "runtime/instance.lock"
    with SingleInstanceLock(lock_path):
        return _migrate(root, dry_run=False, ptb_path=ptb_path)


def _migrate(root: Path, *, dry_run: bool, ptb_path: Path | None) -> SQLiteMigrationReport:
    backend = SQLiteBackend(root)
    if backend.exists():
        snapshot = backend.inspect()
        metadata = snapshot.migration or {}
        if metadata.get("target_format") != "sqlite-v1":
            raise StorageConflictError("existing SQLite database was not created by this migration")
        return SQLiteMigrationReport(
            dry_run,
            True,
            str(backend.database_path),
            str(metadata["source_format"]),
            str(metadata["source_fingerprint"]),
            metadata.get("backup_path"),
            bool(metadata.get("telegram_imported")),
        )
    source = capture_source(root, ptb_path=ptb_path)
    with JsonUnitOfWork(_CapturedBackend(source.stores)) as uow:
        user_data_from_uow(uow)
        important_data_from_uow(uow)
    telegram_state = None
    if source.ptb_payload is not None:
        from ..bot.persistence_import import load_legacy_pickle

        telegram_state = load_legacy_pickle(source.ptb_payload)
    if dry_run:
        return SQLiteMigrationReport(
            True,
            False,
            str(backend.database_path),
            source.format,
            source.fingerprint,
            None,
            source.ptb_payload is not None,
        )
    backup = backup_source(root, source)
    secure_directory(root)
    temporary = root / f".maintbot-migrate-{uuid.uuid4().hex}.sqlite3"
    staging = SQLiteBackend(root, database_path=temporary)
    try:
        snapshot = staging.bootstrap(
            stores=source.stores,
            migration={
                "target_format": "sqlite-v1",
                "source_format": source.format,
                "source_fingerprint": source.fingerprint,
                "backup_path": str(backup),
                "telegram_imported": source.ptb_payload is not None,
            },
        )
        if any(snapshot.data(name) != value for name, value in source.stores.items()):
            raise MigrationError("SQLite application data differs from migration source")
        if telegram_state is not None:
            from ..bot.persistence_import import import_legacy_pickle

            import_legacy_pickle(temporary, telegram_state)
        confirmed = capture_source(root, ptb_path=ptb_path)
        if confirmed.fingerprint != source.fingerprint:
            raise MigrationError("source changed during migration; publication refused")
        staging.inspect()
        with temporary.open("rb+") as handle:
            os.fsync(handle.fileno())
        if backend.database_path.exists():
            raise StorageConflictError("SQLite target appeared during migration")
        os.replace(temporary, backend.database_path)
        tighten_file_permissions(backend.database_path)
        fsync_directory(root)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()
    return SQLiteMigrationReport(
        False,
        False,
        str(backend.database_path),
        source.format,
        source.fingerprint,
        str(backup),
        source.ptb_payload is not None,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Migrate offline split JSON v1 or monolithic v4 and trusted PTB pickle to SQLite."
    )
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--ptb-pickle", type=Path)
    parser.add_argument("--instance-lock", type=Path)
    args = parser.parse_args()
    try:
        report = migrate_to_sqlite(
            args.data_dir,
            dry_run=args.dry_run,
            ptb_path=args.ptb_pickle,
            instance_lock=args.instance_lock,
        )
    except (PersistenceError, InstanceAlreadyRunning, OSError, sqlite3.Error, ValueError, TypeError) as exc:
        parser.exit(2, f"migration refused: {exc}\n")
    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
