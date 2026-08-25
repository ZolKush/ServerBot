"""Orchestration for the explicit, one-shot v4-to-split migration."""

from __future__ import annotations

import copy
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..backend import BackendSnapshot, SplitJsonBackend
from ..errors import MigrationError, StorageConflictError
from ..io import resolve_inside, sha256_bytes, sha256_file
from ..layout import LAYOUT_FILE, PTB_TARGET_FILE, STORE_SPECS
from ..locking import StateLock
from .backup import create_verified_backup
from .v4 import V4Source, V4Transform, load_v4_source, transform_v4


@dataclass(frozen=True, slots=True)
class MigrationReport:
    dry_run: bool
    already_migrated: bool
    source_fingerprint: str
    backup_path: str | None
    store_counts: dict[str, int]
    outbox_collisions: dict[str, str]
    ptb_copied: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def migrate_v4_to_split(
    data_root: Path | str,
    *,
    dry_run: bool = False,
    backup_root: Path | str | None = None,
    user_path: Path | None = None,
    important_path: Path | None = None,
    ptb_path: Path | None = None,
    failpoint: Callable[[str], None] | None = None,
) -> MigrationReport:
    """Validate and migrate exact monolithic schema v4.

    The function is never called by package import or normal application
    startup. ``dry_run`` performs no filesystem writes, including lock files.
    """

    root = Path(data_root).resolve()
    _ensure_explicit_sources_are_backed_up(
        root,
        user_path=user_path,
        important_path=important_path,
        ptb_path=ptb_path,
    )
    backend = SplitJsonBackend(root, failpoint=failpoint)
    if dry_run:
        return _dry_run(
            root,
            backend,
            user_path=user_path,
            important_path=important_path,
            ptb_path=ptb_path,
        )

    with StateLock(root):
        backend.recover()
        if backend.exists():
            snapshot = backend.snapshot()
            source = _optional_source(
                root,
                user_path=user_path,
                important_path=important_path,
                ptb_path=ptb_path,
            )
            return _existing_report(snapshot, source, dry_run=False)

        source = load_v4_source(
            root,
            user_path=user_path,
            important_path=important_path,
            ptb_path=ptb_path,
        )
        transform = transform_v4(source)
        _ensure_targets_absent(root, source)
        resolved_backup_root = (
            Path(backup_root).resolve() if backup_root is not None else root.parent / f"{root.name}-backups"
        )
        backup = create_verified_backup(
            root,
            backup_root=resolved_backup_root,
            source_fingerprint=source.fingerprint,
        )
        confirmed_source = load_v4_source(
            root,
            user_path=user_path,
            important_path=important_path,
            ptb_path=ptb_path,
        )
        if confirmed_source.fingerprint != source.fingerprint:
            raise MigrationError("monolithic source changed after backup creation")
        migrated_at = datetime.now(timezone.utc).isoformat()
        metadata: dict[str, Any] = {
            "source_format": "monolithic-json-v4",
            "source_schema_version": 4,
            "source_fingerprint": source.fingerprint,
            "migrated_at": migrated_at,
            "backup_path": str(backup),
            "outbox_collisions": copy.deepcopy(transform.outbox_collisions),
            "ptb_sha256": sha256_bytes(source.ptb_payload) if source.ptb_payload is not None else None,
        }
        extra_files = {PTB_TARGET_FILE: source.ptb_payload} if source.ptb_payload is not None else None
        snapshot = backend.bootstrap(
            stores=transform.stores,
            migration=metadata,
            extra_files=extra_files,
        )
        _verify_published(root, snapshot, source, transform)
        return _report(
            source=source,
            transform=transform,
            backup_path=backup,
            dry_run=False,
            already_migrated=False,
        )


def _dry_run(
    root: Path,
    backend: SplitJsonBackend,
    *,
    user_path: Path | None,
    important_path: Path | None,
    ptb_path: Path | None,
) -> MigrationReport:
    if backend.exists():
        snapshot = backend.inspect()
        source = _optional_source(
            root,
            user_path=user_path,
            important_path=important_path,
            ptb_path=ptb_path,
        )
        return _existing_report(snapshot, source, dry_run=True)
    if backend.has_pending_transactions():
        raise StorageConflictError("dry-run found a pending transaction; run normal recovery first")
    source = load_v4_source(
        root,
        user_path=user_path,
        important_path=important_path,
        ptb_path=ptb_path,
    )
    transform = transform_v4(source)
    _ensure_targets_absent(root, source)
    return _report(
        source=source,
        transform=transform,
        backup_path=None,
        dry_run=True,
        already_migrated=False,
    )


def _optional_source(
    root: Path,
    *,
    user_path: Path | None,
    important_path: Path | None,
    ptb_path: Path | None,
) -> V4Source | None:
    selected_user = user_path or root / "user_data.json"
    selected_important = important_path or root / "important_data.json"
    if not selected_user.is_file() and not selected_important.is_file():
        return None
    if not selected_user.is_file() or not selected_important.is_file():
        raise MigrationError("only one monolithic v4 source file remains")
    return load_v4_source(
        root,
        user_path=user_path,
        important_path=important_path,
        ptb_path=ptb_path,
    )


def _ensure_explicit_sources_are_backed_up(
    root: Path,
    *,
    user_path: Path | None,
    important_path: Path | None,
    ptb_path: Path | None,
) -> None:
    for label, selected in (
        ("user_path", user_path),
        ("important_path", important_path),
        ("ptb_path", ptb_path),
    ):
        if selected is None:
            continue
        resolved = Path(selected).resolve()
        if root not in resolved.parents:
            raise MigrationError(
                f"explicit {label} must be inside DATA_DIR so the verified backup includes it: {resolved}"
            )


def _existing_report(
    snapshot: BackendSnapshot,
    source: V4Source | None,
    *,
    dry_run: bool,
) -> MigrationReport:
    metadata = snapshot.migration
    if not isinstance(metadata, dict) or metadata.get("source_format") != "monolithic-json-v4":
        raise StorageConflictError("existing split layout was not produced by the v4 migrator")
    fingerprint = metadata.get("source_fingerprint")
    if not isinstance(fingerprint, str) or len(fingerprint) != 64:
        raise StorageConflictError("existing split layout has invalid migration metadata")
    if source is not None and source.fingerprint != fingerprint:
        raise StorageConflictError("monolithic sources changed after split migration")
    collisions = metadata.get("outbox_collisions")
    if not isinstance(collisions, dict):
        raise StorageConflictError("existing split layout has invalid outbox migration metadata")
    backup_path = metadata.get("backup_path")
    if backup_path is not None and not isinstance(backup_path, str):
        raise StorageConflictError("existing split layout has invalid backup metadata")
    return MigrationReport(
        dry_run=dry_run,
        already_migrated=True,
        source_fingerprint=fingerprint,
        backup_path=backup_path,
        store_counts=_snapshot_counts(snapshot),
        outbox_collisions={str(key): str(value) for key, value in collisions.items()},
        ptb_copied=metadata.get("ptb_sha256") is not None,
    )


def _ensure_targets_absent(root: Path, source: V4Source) -> None:
    conflicts = [
        spec.relative_path for spec in STORE_SPECS.values() if resolve_inside(root, spec.relative_path).exists()
    ]
    layout = root / LAYOUT_FILE
    if layout.exists():
        conflicts.append(LAYOUT_FILE)
    if source.ptb_payload is not None and resolve_inside(root, PTB_TARGET_FILE).exists():
        conflicts.append(PTB_TARGET_FILE)
    if conflicts:
        raise StorageConflictError(f"split-layout targets already exist: {sorted(set(conflicts))}")


def _verify_published(
    root: Path,
    snapshot: BackendSnapshot,
    source: V4Source,
    transform: V4Transform,
) -> None:
    for name, expected in transform.stores.items():
        if snapshot.data(name) != expected:
            raise MigrationError(f"published store differs from transformation: {name}")
    if source.ptb_payload is not None:
        target = resolve_inside(root, PTB_TARGET_FILE)
        if not target.is_file() or sha256_file(target) != sha256_bytes(source.ptb_payload):
            raise MigrationError("PTB persistence was not copied byte-for-byte")


def _report(
    *,
    source: V4Source,
    transform: V4Transform,
    backup_path: Path | None,
    dry_run: bool,
    already_migrated: bool,
) -> MigrationReport:
    return MigrationReport(
        dry_run=dry_run,
        already_migrated=already_migrated,
        source_fingerprint=source.fingerprint,
        backup_path=str(backup_path) if backup_path is not None else None,
        store_counts={name: _data_count(data) for name, data in transform.stores.items()},
        outbox_collisions=copy.deepcopy(transform.outbox_collisions),
        ptb_copied=source.ptb_payload is not None,
    )


def _snapshot_counts(snapshot: BackendSnapshot) -> dict[str, int]:
    return {name: _data_count(store.data) for name, store in snapshot.stores.items()}


def _data_count(data: Any) -> int:
    if isinstance(data, list):
        return len(data)
    if isinstance(data, dict):
        items = data.get("items")
        if set(data) == {"next_id", "items"} and isinstance(items, dict):
            return len(items)
        return len(data)
    return 1


__all__ = ["MigrationReport", "migrate_v4_to_split"]
