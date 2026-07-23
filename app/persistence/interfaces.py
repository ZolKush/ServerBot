"""Backend-independent repository and Unit-of-Work protocols."""

from __future__ import annotations

from typing import Any, Protocol


class MappingRepositoryProtocol(Protocol):
    def get(self, key: str | int) -> Any | None: ...

    def require(self, key: str | int) -> Any: ...

    def put(self, key: str | int, value: Any) -> None: ...

    def remove(self, key: str | int) -> Any | None: ...

    def items(self) -> list[tuple[str, Any]]: ...


class IndexedRepositoryProtocol(MappingRepositoryProtocol, Protocol):
    @property
    def next_id(self) -> int: ...

    def allocate_id(self) -> int: ...


class AuditRepositoryProtocol(Protocol):
    def append(self, value: Any) -> None: ...

    def values(self) -> list[Any]: ...


class DocumentRepositoryProtocol(Protocol):
    def get(self, key: str, default: Any = None) -> Any: ...

    def set(self, key: str, value: Any) -> None: ...

    def replace(self, value: dict[str, Any]) -> None: ...


UserRepository = MappingRepositoryProtocol
AccessRepository = MappingRepositoryProtocol
SubscriptionRepository = MappingRepositoryProtocol
ServiceRequestRepository = IndexedRepositoryProtocol
TicketRepository = IndexedRepositoryProtocol
MaintenanceRepository = DocumentRepositoryProtocol
OutboxRepository = MappingRepositoryProtocol
MonitoringRepository = MappingRepositoryProtocol


class UnitOfWork(Protocol):
    profiles: UserRepository
    access: AccessRepository
    subscriptions: SubscriptionRepository
    service_requests: ServiceRequestRepository
    billing_settings: DocumentRepositoryProtocol
    help_and_contacts: DocumentRepositoryProtocol
    tickets: TicketRepository
    ticket_messages: MappingRepositoryProtocol
    maintenance: MaintenanceRepository
    outbox: OutboxRepository
    audit: AuditRepositoryProtocol
    dns_cache: MonitoringRepository
    node_status_cache: MonitoringRepository
    docker_cache: MonitoringRepository
    tls_state: MonitoringRepository
    fail2ban_cursors: MonitoringRepository

    def __enter__(self) -> UnitOfWork: ...

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...


__all__ = [
    "AccessRepository",
    "AuditRepositoryProtocol",
    "DocumentRepositoryProtocol",
    "IndexedRepositoryProtocol",
    "MappingRepositoryProtocol",
    "MonitoringRepository",
    "MaintenanceRepository",
    "OutboxRepository",
    "ServiceRequestRepository",
    "SubscriptionRepository",
    "TicketRepository",
    "UnitOfWork",
    "UserRepository",
]
