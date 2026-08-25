"""Crash-safe redo transactions for atomically publishing multiple files."""

from __future__ import annotations

import contextlib
import shutil
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .errors import PreparedTransactionError, RecoveryError, SchemaError, StorageConflictError
from .io import (
    encode_json,
    read_json,
    replace_durable,
    resolve_inside,
    secure_directory,
    sha256_bytes,
    sha256_file,
    write_atomic,
    write_bytes_durable,
)
from .layout import LAYOUT_FILE, TRANSACTION_SCHEMA_VERSION, TRANSACTIONS_DIR

_JOURNAL_FILE = "journal.json"
_JOURNAL_KEYS = {
    "schema_version",
    "transaction_id",
    "state",
    "created_at",
    "base_revision",
    "target_revision",
    "files",
}
_FILE_KEYS = {"target", "staged", "sha256", "manifest"}


@dataclass(frozen=True, slots=True)
class FileInstall:
    target: str
    staged: str
    sha256: str
    manifest: bool


@dataclass(frozen=True, slots=True)
class TransactionJournal:
    transaction_id: str
    state: str
    base_revision: int
    target_revision: int
    files: tuple[FileInstall, ...]
    created_at: str


class TransactionCoordinator:
    """Prepare, publish, and recover one global split-state transaction."""

    def __init__(
        self,
        data_root: Path,
        *,
        failpoint: Callable[[str], None] | None = None,
    ) -> None:
        self.data_root = data_root.resolve()
        self.transactions_root = self.data_root / TRANSACTIONS_DIR
        self.failpoint = failpoint

    def commit(
        self,
        payloads: Mapping[str, bytes],
        *,
        transaction_id: str | None = None,
        base_revision: int,
        target_revision: int,
    ) -> str:
        if LAYOUT_FILE not in payloads:
            raise SchemaError("transaction must publish the layout manifest")
        tx_id = transaction_id or uuid.uuid4().hex
        if not tx_id or any(char not in "0123456789abcdef-" for char in tx_id.lower()):
            raise SchemaError(f"invalid transaction id: {tx_id!r}")
        tx_root = self.transactions_root / tx_id
        if tx_root.exists():
            raise StorageConflictError(f"transaction directory already exists: {tx_root}")
        secure_directory(tx_root)
        prepared = False
        try:
            files: list[FileInstall] = []
            for relative_target, payload in sorted(payloads.items(), key=lambda item: item[0]):
                resolve_inside(self.data_root, relative_target)
                staged_relative = str(Path("staged") / relative_target).replace("\\", "/")
                staged_path = resolve_inside(tx_root, staged_relative)
                write_bytes_durable(staged_path, payload, exclusive=True)
                files.append(
                    FileInstall(
                        target=relative_target,
                        staged=staged_relative,
                        sha256=sha256_bytes(payload),
                        manifest=relative_target == LAYOUT_FILE,
                    )
                )
            journal = TransactionJournal(
                transaction_id=tx_id,
                state="PREPARED",
                base_revision=base_revision,
                target_revision=target_revision,
                files=tuple(files),
                created_at=datetime.now(timezone.utc).isoformat(),
            )
            write_bytes_durable(tx_root / _JOURNAL_FILE, _encode_journal(journal), exclusive=True)
            prepared = True
            try:
                self._hit("after_prepare")
                self._install(tx_root, journal)
            except Exception:
                # PREPARED is the durable commit point.  A normal, recoverable
                # filesystem error after it must not be reported as an aborted
                # mutation: retry redo once while the state lock is still held.
                # If redo still cannot finish, expose a distinct outcome so a
                # caller cannot safely interpret the operation as retryable.
                try:
                    self._install(tx_root, journal, recovering=True)
                except Exception as recovery_error:
                    raise PreparedTransactionError(tx_id) from recovery_error
            return tx_id
        finally:
            if not prepared:
                _remove_transaction_tree(tx_root)

    def recover_all(self) -> list[str]:
        transaction_dirs = self._transaction_directories()
        prepared: list[tuple[Path, TransactionJournal]] = []
        recovered: list[str] = []
        for tx_root in transaction_dirs:
            journal_path = tx_root / _JOURNAL_FILE
            if not journal_path.exists():
                _remove_transaction_tree(tx_root)
                continue
            journal = _parse_journal(read_json(journal_path))
            if journal.transaction_id != tx_root.name:
                raise RecoveryError(f"transaction id/path mismatch in {journal_path}")
            if journal.state == "COMMITTED":
                self._validate_committed_targets(journal)
                _remove_transaction_tree(tx_root)
                continue
            prepared.append((tx_root, journal))
        if len(prepared) > 1:
            ids = [journal.transaction_id for _, journal in prepared]
            raise RecoveryError(f"multiple prepared transactions require manual recovery: {ids}")
        if prepared:
            tx_root, journal = prepared[0]
            self._install(tx_root, journal, recovering=True)
            recovered.append(journal.transaction_id)
        with contextlib.suppress(OSError):
            self.transactions_root.rmdir()
        return recovered

    def validate_pending(self) -> list[str]:
        """Verify, without writes, that every pending transaction is recoverable."""

        transaction_dirs = self._transaction_directories()
        prepared: list[tuple[Path, TransactionJournal]] = []
        pending: list[str] = []
        for tx_root in transaction_dirs:
            journal_path = tx_root / _JOURNAL_FILE
            pending.append(tx_root.name)
            if not journal_path.exists():
                # Recovery can safely remove debris created before PREPARED.
                continue
            journal = _parse_journal(read_json(journal_path))
            if journal.transaction_id != tx_root.name:
                raise RecoveryError(f"transaction id/path mismatch in {journal_path}")
            if journal.state == "COMMITTED":
                self._validate_committed_targets(journal)
                continue
            prepared.append((tx_root, journal))
        if len(prepared) > 1:
            ids = [journal.transaction_id for _, journal in prepared]
            raise RecoveryError(f"multiple prepared transactions require manual recovery: {ids}")
        if prepared:
            self._preflight_install(*prepared[0], recovering=True)
        return pending

    def has_pending_transactions(self) -> bool:
        return bool(self._transaction_directories())

    def _transaction_directories(self) -> list[Path]:
        if not self.transactions_root.exists():
            return []
        try:
            return sorted(path for path in self.transactions_root.iterdir() if path.is_dir())
        except OSError as exc:
            raise RecoveryError(f"cannot inspect transaction directory {self.transactions_root}: {exc}") from exc

    def _install(
        self,
        tx_root: Path,
        journal: TransactionJournal,
        *,
        recovering: bool = False,
    ) -> None:
        installs = self._preflight_install(tx_root, journal, recovering=recovering)
        for item, target, staged in installs:
            replace_durable(staged, target)
            self._hit(f"after_install:{item.target}")
        self._hit("after_manifest")
        committed = TransactionJournal(
            transaction_id=journal.transaction_id,
            state="COMMITTED",
            base_revision=journal.base_revision,
            target_revision=journal.target_revision,
            files=journal.files,
            created_at=journal.created_at,
        )
        write_atomic(tx_root / _JOURNAL_FILE, _encode_journal(committed))
        self._hit("after_commit")
        _remove_transaction_tree(tx_root)
        with contextlib.suppress(OSError):
            self.transactions_root.rmdir()

    def _preflight_install(
        self,
        tx_root: Path,
        journal: TransactionJournal,
        *,
        recovering: bool,
    ) -> list[tuple[FileInstall, Path, Path]]:
        ordinary = [item for item in journal.files if not item.manifest]
        manifests = [item for item in journal.files if item.manifest]
        if len(manifests) != 1 or manifests[0].target != LAYOUT_FILE:
            raise RecoveryError("prepared transaction has no unique layout manifest")
        installs: list[tuple[FileInstall, Path, Path]] = []
        targets: set[Path] = set()
        staged_paths: set[Path] = set()
        for item in [*ordinary, *manifests]:
            target = resolve_inside(self.data_root, item.target)
            staged = resolve_inside(tx_root, item.staged)
            if target in targets:
                raise RecoveryError(f"transaction journal aliases target path: {item.target}")
            if staged in staged_paths:
                raise RecoveryError(f"transaction journal aliases staged path: {item.staged}")
            targets.add(target)
            staged_paths.add(staged)
            if _matches_checksum(target, item.sha256, label=item.target):
                continue
            if not _matches_checksum(staged, item.sha256, label=item.staged):
                phase = "recovery" if recovering else "commit"
                raise RecoveryError(
                    f"{phase} cannot install {item.target}: neither target nor staging has the expected content"
                )
            installs.append((item, target, staged))
        return installs

    def _validate_committed_targets(self, journal: TransactionJournal) -> None:
        manifests = [item for item in journal.files if item.manifest]
        if len(manifests) != 1 or manifests[0].target != LAYOUT_FILE:
            raise RecoveryError("committed transaction has no unique layout manifest")
        targets: set[Path] = set()
        for item in journal.files:
            target = resolve_inside(self.data_root, item.target)
            if target in targets:
                raise RecoveryError(f"transaction journal aliases target path: {item.target}")
            targets.add(target)
            if not _matches_checksum(target, item.sha256, label=item.target):
                raise RecoveryError(
                    f"committed transaction {journal.transaction_id} has an invalid target: {item.target}"
                )

    def _hit(self, name: str) -> None:
        if self.failpoint is not None:
            self.failpoint(name)


def _matches_checksum(path: Path, expected: str, *, label: str) -> bool:
    try:
        return path.is_file() and sha256_file(path) == expected
    except OSError as exc:
        raise RecoveryError(f"cannot verify transaction file {label}: {exc}") from exc


def _encode_journal(journal: TransactionJournal) -> bytes:
    return encode_json(
        {
            "schema_version": TRANSACTION_SCHEMA_VERSION,
            "transaction_id": journal.transaction_id,
            "state": journal.state,
            "created_at": journal.created_at,
            "base_revision": journal.base_revision,
            "target_revision": journal.target_revision,
            "files": [
                {
                    "target": item.target,
                    "staged": item.staged,
                    "sha256": item.sha256,
                    "manifest": item.manifest,
                }
                for item in journal.files
            ],
        }
    )


def _parse_journal(raw: Any) -> TransactionJournal:
    if not isinstance(raw, dict) or set(raw) != _JOURNAL_KEYS:
        raise RecoveryError("transaction journal has an invalid shape")
    if raw["schema_version"] != TRANSACTION_SCHEMA_VERSION:
        raise RecoveryError("transaction journal has an unsupported schema version")
    tx_id = raw["transaction_id"]
    state = raw["state"]
    created_at = raw["created_at"]
    if not isinstance(tx_id, str) or not tx_id:
        raise RecoveryError("transaction journal has no id")
    if state not in {"PREPARED", "COMMITTED"}:
        raise RecoveryError(f"transaction journal has invalid state: {state!r}")
    if not isinstance(created_at, str) or not created_at:
        raise RecoveryError("transaction journal has no creation time")
    base_revision = _journal_revision(raw["base_revision"], "base_revision")
    target_revision = _journal_revision(raw["target_revision"], "target_revision")
    raw_files = raw["files"]
    if not isinstance(raw_files, list) or not raw_files:
        raise RecoveryError("transaction journal has no files")
    files: list[FileInstall] = []
    targets: set[str] = set()
    staged_paths: set[str] = set()
    for raw_file in raw_files:
        if not isinstance(raw_file, dict) or set(raw_file) != _FILE_KEYS:
            raise RecoveryError("transaction journal contains an invalid file entry")
        target = raw_file["target"]
        staged = raw_file["staged"]
        digest = raw_file["sha256"]
        manifest = raw_file["manifest"]
        if not isinstance(target, str) or not isinstance(staged, str):
            raise RecoveryError("transaction journal paths must be strings")
        if target in targets:
            raise RecoveryError(f"transaction journal repeats target: {target}")
        targets.add(target)
        expected_staged = str(Path("staged") / target).replace("\\", "/")
        if staged != expected_staged:
            raise RecoveryError(f"transaction journal has a non-canonical staged path for {target}")
        if staged in staged_paths:
            raise RecoveryError(f"transaction journal repeats staged path: {staged}")
        staged_paths.add(staged)
        if not isinstance(digest, str) or len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise RecoveryError(f"invalid transaction checksum for {target}")
        if not isinstance(manifest, bool):
            raise RecoveryError("transaction manifest marker must be boolean")
        files.append(FileInstall(target, staged, digest, manifest))
    return TransactionJournal(tx_id, state, base_revision, target_revision, tuple(files), created_at)


def _journal_revision(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RecoveryError(f"transaction {label} must be a non-negative integer")
    return value


def _remove_transaction_tree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


__all__ = ["TransactionCoordinator", "TransactionJournal"]
