"""Read-only capture and verified backup of the split JSON migration source."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import MigrationError, StorageConflictError
from .io import decode_json, encode_json, resolve_inside, sha256_bytes, write_bytes_durable
from .layout import LAYOUT_FILE, STORE_SPECS
from .schema import parse_layout_manifest, parse_store_document


@dataclass(frozen=True)
class ImportSource:
    format: str
    stores: dict[str, Any]
    files: dict[str, bytes]
    ptb_payload: bytes | None

    @property
    def fingerprint(self) -> str:
        return sha256_bytes(encode_json({name: sha256_bytes(data) for name, data in self.files.items()}))


def _source_path(root: Path, relative: str) -> Path:
    path = root / relative
    for part in (path, *path.parents):
        if part == root:
            break
        if part.is_symlink():
            raise MigrationError(f"migration refuses symbolic links: {relative}")
    return resolve_inside(root, relative)


def capture_source(root: Path, *, ptb_path: Path | None = None) -> ImportSource:
    """Capture exact bytes without creating directories, locks or journals."""
    transactions = _source_path(root, ".transactions")
    if transactions.exists() and any(transactions.iterdir()):
        raise StorageConflictError("pending JSON transaction found; recover with the old release first")
    manifest_path = _source_path(root, LAYOUT_FILE)
    if not manifest_path.is_file():
        raise MigrationError("split JSON layout is required; storage_layout.json is missing")
    if ptb_path is not None:
        selected = ptb_path.resolve()
        if not selected.is_file():
            raise MigrationError("--ptb-pickle must name an existing trusted file")
    else:
        candidates = [root / "telegram/persistence.pickle", root / "ptb_persistence"]
        present = [path for path in candidates if path.is_file()]
        if len(present) > 1:
            raise MigrationError("multiple Telegram pickle files found; select --ptb-pickle explicitly")
        backups = [
            path for path in (root / "telegram/persistence.pickle.bak", root / "ptb_persistence.bak") if path.is_file()
        ]
        if not present and len(backups) > 1:
            raise MigrationError("multiple Telegram backup files found; select --ptb-pickle explicitly")
        selected = present[0] if present else backups[0] if backups else root / "telegram/persistence.pickle"
    if root in selected.parents:
        selected = _source_path(root, selected.relative_to(root).as_posix())
    ptb_payload = selected.read_bytes() if selected.is_file() else None
    files = {LAYOUT_FILE: manifest_path.read_bytes()}
    manifest = parse_layout_manifest(decode_json(files[LAYOUT_FILE]), files[LAYOUT_FILE])
    stores = {}
    for name, spec in STORE_SPECS.items():
        files[spec.relative_path] = _source_path(root, spec.relative_path).read_bytes()
        document = parse_store_document(name, decode_json(files[spec.relative_path]), files[spec.relative_path])
        expected = manifest.stores[name]
        if document.sha256 != expected.sha256 or document.revision != expected.revision:
            raise MigrationError(f"source checksum/revision differs from manifest: {name}")
        stores[name] = document.data
    if ptb_payload is not None:
        name = (
            selected.relative_to(root).as_posix()
            if root in selected.parents
            else "_external/telegram/persistence.pickle"
        )
        files[name] = ptb_payload
    return ImportSource("split-json-v1", stores, files, ptb_payload)


def backup_source(root: Path, source: ImportSource) -> Path:
    """Keep a byte-for-byte backup inside DATA_DIR with a checksum manifest."""
    backup = root / "sqlite-backups" / source.fingerprint
    manifest = encode_json(
        {
            "source_format": source.format,
            "source_fingerprint": source.fingerprint,
            "files": {name: sha256_bytes(data) for name, data in source.files.items()},
        }
    )
    for name, payload in {**source.files, "backup_manifest.json": manifest}.items():
        target = _source_path(root, (backup / name).relative_to(root).as_posix())
        if target.exists():
            if not target.is_file() or target.read_bytes() != payload:
                raise MigrationError(f"existing backup differs from source: {target}")
        else:
            write_bytes_durable(target, payload, exclusive=True)
        if target.read_bytes() != payload:
            raise MigrationError(f"backup verification failed: {target}")
    return backup
