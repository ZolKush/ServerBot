"""Validated split JSON backend with optimistic revisions and startup recovery."""

from __future__ import annotations

import copy
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .errors import CommittedTransactionError, SchemaError, StorageConflictError
from .io import decode_json, resolve_inside
from .layout import LAYOUT_FILE, STORE_SPECS, default_store_data
from .locking import StateLock
from .schema import (
    LayoutManifest,
    StoreDocument,
    build_layout_manifest,
    build_store_document,
    parse_layout_manifest,
    parse_store_document,
)
from .snapshots import BackendSnapshot, StoreSnapshot
from .transaction import TransactionCoordinator

if TYPE_CHECKING:
    from .unit_of_work import JsonUnitOfWork


class SplitJsonBackend:
    """Filesystem implementation of the repository persistence boundary."""

    def __init__(
        self,
        data_root: Path | str,
        *,
        failpoint: Callable[[str], None] | None = None,
    ) -> None:
        self.data_root = Path(data_root).resolve()
        self._transactions = TransactionCoordinator(self.data_root, failpoint=failpoint)

    @property
    def layout_path(self) -> Path:
        return self.data_root / LAYOUT_FILE

    def exists(self) -> bool:
        return self.layout_path.is_file()

    def has_pending_transactions(self) -> bool:
        return self._transactions.has_pending_transactions()

    def recover(self) -> list[str]:
        with StateLock(self.data_root):
            return self._transactions.recover_all()

    def verify_recovery(self) -> list[str]:
        """Read-only validation that pending redo/cleanup can complete safely."""

        return self._transactions.validate_pending()

    def inspect(self) -> BackendSnapshot:
        """Read and verify a stable layout without recovery or data writes.

        The same process/file lock as commits prevents a check-then-load race,
        but pending journals are never applied. Runtime callers should use
        ``snapshot`` when recovery is permitted.
        """

        with StateLock(self.data_root):
            if self._transactions.has_pending_transactions():
                raise StorageConflictError("pending transaction requires recovery before read-only inspection")
            return self._load()

    def snapshot(self) -> BackendSnapshot:
        with StateLock(self.data_root):
            self._transactions.recover_all()
            return self._load()

    def bootstrap(
        self,
        *,
        stores: Mapping[str, Any] | None = None,
        migration: dict[str, Any] | None = None,
        extra_files: Mapping[str, bytes] | None = None,
        transaction_id: str | None = None,
    ) -> BackendSnapshot:
        """Create layout v1 once; refuse any partially existing target."""

        with StateLock(self.data_root):
            self._transactions.recover_all()
            if self.layout_path.exists():
                raise StorageConflictError(f"split layout already exists: {self.layout_path}")
            initial = default_store_data() if stores is None else copy.deepcopy(dict(stores))
            if set(initial) != set(STORE_SPECS):
                missing = sorted(set(STORE_SPECS) - set(initial))
                extra = sorted(set(initial) - set(STORE_SPECS))
                raise SchemaError(f"initial store set mismatch; missing={missing}, extra={extra}")
            conflicts = [
                spec.relative_path
                for spec in STORE_SPECS.values()
                if resolve_inside(self.data_root, spec.relative_path).exists()
            ]
            additional = dict(extra_files or {})
            reserved = {LAYOUT_FILE, *(spec.relative_path for spec in STORE_SPECS.values())}
            overlap = sorted(set(additional) & reserved)
            if overlap:
                raise SchemaError(f"extra files overlap reserved split-layout paths: {overlap}")
            for relative_path in additional:
                target = resolve_inside(self.data_root, relative_path)
                if target.exists():
                    conflicts.append(relative_path)
            if conflicts:
                raise StorageConflictError(f"split-layout targets already exist: {sorted(set(conflicts))}")

            tx_id = transaction_id or uuid.uuid4().hex
            documents = {name: build_store_document(name, 1, initial[name]) for name in STORE_SPECS}
            manifest = build_layout_manifest(
                revision=1,
                transaction_id=tx_id,
                documents=documents,
                migration=migration,
            )
            payloads = {STORE_SPECS[name].relative_path: document.payload for name, document in documents.items()}
            payloads.update(additional)
            payloads[LAYOUT_FILE] = manifest.payload
            self._transactions.commit(
                payloads,
                transaction_id=tx_id,
                base_revision=0,
                target_revision=1,
            )
            return self._load_committed(tx_id)

    def commit(
        self,
        *,
        base_revision: int,
        changes: Mapping[str, Any],
    ) -> BackendSnapshot:
        """Commit changed store payloads if the global base revision is current."""

        unknown = sorted(set(changes) - set(STORE_SPECS))
        if unknown:
            raise SchemaError(f"unknown stores in commit: {unknown}")
        with StateLock(self.data_root):
            self._transactions.recover_all()
            current = self._load()
            if current.revision != base_revision:
                raise StorageConflictError(
                    f"stale storage revision {base_revision}; current revision is {current.revision}"
                )
            effective: dict[str, Any] = {}
            for name, data in changes.items():
                STORE_SPECS[name].validate_data(data)
                if data != current.stores[name].data:
                    effective[name] = copy.deepcopy(data)
            if not effective:
                return current

            tx_id = uuid.uuid4().hex
            documents: dict[str, StoreDocument] = {}
            for name, snapshot in current.stores.items():
                if name in effective:
                    documents[name] = build_store_document(
                        name,
                        snapshot.revision + 1,
                        effective[name],
                    )
                else:
                    documents[name] = self._read_store_document(name)
            manifest = build_layout_manifest(
                revision=current.revision + 1,
                transaction_id=tx_id,
                documents=documents,
                migration=current.migration,
            )
            payloads = {STORE_SPECS[name].relative_path: documents[name].payload for name in effective}
            payloads[LAYOUT_FILE] = manifest.payload
            self._transactions.commit(
                payloads,
                transaction_id=tx_id,
                base_revision=current.revision,
                target_revision=current.revision + 1,
            )
            return self._load_committed(tx_id)

    def unit_of_work(self) -> JsonUnitOfWork:
        from .unit_of_work import JsonUnitOfWork

        return JsonUnitOfWork(self)

    def _load(self) -> BackendSnapshot:
        if not self.layout_path.is_file():
            raise StorageConflictError(f"split layout does not exist: {self.layout_path}")
        manifest_payload = self.layout_path.read_bytes()
        manifest = parse_layout_manifest(
            decode_json(manifest_payload, source=str(self.layout_path)),
            manifest_payload,
        )
        documents: dict[str, StoreDocument] = {}
        for name in STORE_SPECS:
            document = self._read_store_document(name)
            entry = manifest.stores[name]
            if document.sha256 != entry.sha256:
                raise SchemaError(f"{name} checksum differs from the layout manifest")
            if document.revision != entry.revision:
                raise SchemaError(f"{name} revision differs from the layout manifest")
            documents[name] = document
        return _snapshot_from_documents(manifest, documents)

    def _read_store_document(self, name: str) -> StoreDocument:
        spec = STORE_SPECS[name]
        path = resolve_inside(self.data_root, spec.relative_path)
        if not path.is_file():
            raise SchemaError(f"missing store document: {path}")
        payload = path.read_bytes()
        raw = decode_json(payload, source=str(path))
        return parse_store_document(name, raw, payload)

    def _load_committed(self, transaction_id: str) -> BackendSnapshot:
        try:
            return self._load()
        except Exception as exc:
            raise CommittedTransactionError(transaction_id) from exc


def _snapshot_from_documents(
    manifest: LayoutManifest,
    documents: Mapping[str, StoreDocument],
) -> BackendSnapshot:
    stores = {
        name: StoreSnapshot(name=name, revision=document.revision, data=copy.deepcopy(document.data))
        for name, document in documents.items()
    }
    return BackendSnapshot(
        revision=manifest.revision,
        transaction_id=manifest.transaction_id,
        stores=stores,
        migration=copy.deepcopy(manifest.migration),
    )


__all__ = ["BackendSnapshot", "SplitJsonBackend", "StoreSnapshot"]
