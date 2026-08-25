"""Repository composition and atomic multi-store commit boundary."""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Mapping
from types import TracebackType
from typing import Any, Protocol

from .errors import CommittedTransactionError, PreparedTransactionError
from .repositories import DocumentRepository, IndexedRepository, ListRepository, MappingRepository
from .snapshots import BackendSnapshot


class BackendProtocol(Protocol):
    def snapshot(self) -> BackendSnapshot: ...

    def commit(self, *, base_revision: int, changes: Mapping[str, Any]) -> BackendSnapshot: ...


class JsonUnitOfWork:
    """Mutable repository snapshot committed as one durable JSON transaction."""

    profiles: MappingRepository
    access: MappingRepository
    subscriptions: MappingRepository
    service_requests: IndexedRepository
    billing_settings: DocumentRepository
    help_and_contacts: DocumentRepository
    tickets: IndexedRepository
    ticket_messages: MappingRepository
    maintenance: DocumentRepository
    outbox: MappingRepository
    audit: ListRepository
    dns_cache: MappingRepository
    node_status_cache: MappingRepository
    docker_cache: MappingRepository
    tls_state: MappingRepository
    fail2ban_cursors: MappingRepository

    _MAPPING_STORES = {
        "profiles": "users.profiles",
        "access": "access.grants",
        "subscriptions": "subscriptions.accounts",
        "ticket_messages": "support.ticket_messages",
        "outbox": "messaging.outbox",
        "dns_cache": "monitoring.dns_cache",
        "node_status_cache": "monitoring.node_status_cache",
        "docker_cache": "monitoring.docker_cache",
        "tls_state": "monitoring.tls_state",
        "fail2ban_cursors": "monitoring.fail2ban_cursors",
    }
    _INDEXED_STORES = {
        "service_requests": "subscriptions.requests",
        "tickets": "support.tickets",
    }
    _DOCUMENT_STORES = {
        "billing_settings": "subscriptions.billing_settings",
        "help_and_contacts": "settings.help_and_contacts",
        "maintenance": "maintenance.state",
    }

    def __init__(self, backend: BackendProtocol) -> None:
        self.backend = backend
        self._snapshot: BackendSnapshot | None = None
        self._original: dict[str, Any] = {}
        self._entered = False
        self._committed = False
        self._rolled_back = False
        self._closed = False

    def __enter__(self) -> JsonUnitOfWork:
        if self._entered or self._closed:
            raise RuntimeError("Unit of Work cannot be entered more than once")
        self._open(self.backend.snapshot())
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        if not self._committed:
            self.rollback()
        self._closed = True

    async def __aenter__(self) -> JsonUnitOfWork:
        if self._entered or self._closed:
            raise RuntimeError("Unit of Work cannot be entered more than once")
        snapshot = await asyncio.to_thread(self.backend.snapshot)
        self._open(snapshot)
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        if not self._committed:
            self.rollback()
        self._closed = True

    def commit(self) -> None:
        snapshot = self._require_active()
        if self._rolled_back:
            raise RuntimeError("Unit of Work has already been rolled back")
        if self._committed:
            raise RuntimeError("Unit of Work has already been committed")
        exports = self._exports()
        changes = {store_name: value for store_name, value in exports.items() if value != self._original[store_name]}
        try:
            self._snapshot = self.backend.commit(base_revision=snapshot.revision, changes=changes)
        except (PreparedTransactionError, CommittedTransactionError):
            self._committed = True
            raise
        self._committed = True

    async def commit_async(self) -> None:
        snapshot = self._require_active()
        if self._rolled_back:
            raise RuntimeError("Unit of Work has already been rolled back")
        if self._committed:
            raise RuntimeError("Unit of Work has already been committed")
        exports = self._exports()
        changes = {store_name: value for store_name, value in exports.items() if value != self._original[store_name]}
        task = asyncio.create_task(
            asyncio.to_thread(
                self.backend.commit,
                base_revision=snapshot.revision,
                changes=changes,
            )
        )
        try:
            self._snapshot = await asyncio.shield(task)
        except asyncio.CancelledError:
            try:
                self._snapshot = await task
            except (PreparedTransactionError, CommittedTransactionError):
                self._committed = True
                raise
            self._committed = True
            raise
        except (PreparedTransactionError, CommittedTransactionError):
            self._committed = True
            raise
        else:
            self._committed = True

    def rollback(self) -> None:
        self._require_active()
        if self._committed:
            raise RuntimeError("a committed Unit of Work cannot be rolled back")
        self._rolled_back = True

    @property
    def base_revision(self) -> int:
        return self._require_active().revision

    @property
    def current_snapshot(self) -> BackendSnapshot:
        return self._require_active()

    def _open(self, snapshot: BackendSnapshot) -> None:
        self._snapshot = snapshot
        self._original = {name: snapshot.data(name) for name in snapshot.stores}
        for attribute, store_name in self._MAPPING_STORES.items():
            setattr(self, attribute, MappingRepository(self._original[store_name]))
        for attribute, store_name in self._INDEXED_STORES.items():
            setattr(self, attribute, IndexedRepository(self._original[store_name]))
        for attribute, store_name in self._DOCUMENT_STORES.items():
            setattr(self, attribute, DocumentRepository(self._original[store_name]))
        self.audit = ListRepository(self._original["audit.events"])
        self._entered = True

    def _exports(self) -> dict[str, Any]:
        exports: dict[str, Any] = {}
        for attribute, store_name in self._MAPPING_STORES.items():
            mapping_repository: MappingRepository = getattr(self, attribute)
            exports[store_name] = mapping_repository.export()
        for attribute, store_name in self._INDEXED_STORES.items():
            indexed_repository: IndexedRepository = getattr(self, attribute)
            exports[store_name] = indexed_repository.export()
        for attribute, store_name in self._DOCUMENT_STORES.items():
            document_repository: DocumentRepository = getattr(self, attribute)
            exports[store_name] = document_repository.export()
        exports["audit.events"] = self.audit.export()
        return copy.deepcopy(exports)

    def _require_active(self) -> BackendSnapshot:
        if not self._entered or self._snapshot is None or self._closed:
            raise RuntimeError("Unit of Work is not active")
        return self._snapshot


__all__ = ["JsonUnitOfWork"]
