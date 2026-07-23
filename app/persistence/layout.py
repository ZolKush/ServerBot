"""Names, paths, and structural data contracts for split JSON layout v1."""

from __future__ import annotations

import copy
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from .errors import SchemaError

LAYOUT_SCHEMA_VERSION = 1
STORE_SCHEMA_VERSION = 1
TRANSACTION_SCHEMA_VERSION = 1
BACKUP_SCHEMA_VERSION = 1

LAYOUT_FILE = "storage_layout.json"
TRANSACTIONS_DIR = ".transactions"
STATE_LOCK_FILE = "runtime/state.lock"
PTB_TARGET_FILE = "telegram/persistence.pickle"


def _mapping_default() -> dict[str, Any]:
    return {}


def _sequence_default() -> list[Any]:
    return []


def _indexed_default() -> dict[str, Any]:
    return {"next_id": 0, "items": {}}


def _maintenance_default() -> dict[str, Any]:
    return {"active": {}, "scheduled": {}}


def _require_mapping(value: Any, label: str) -> None:
    if not isinstance(value, dict):
        raise SchemaError(f"{label} must be a JSON object")


def _require_sequence(value: Any, label: str) -> None:
    if not isinstance(value, list):
        raise SchemaError(f"{label} must be a JSON array")


def _require_indexed(value: Any, label: str) -> None:
    _require_mapping(value, label)
    if set(value) != {"next_id", "items"}:
        raise SchemaError(f"{label} must contain exactly next_id and items")
    next_id = value["next_id"]
    if isinstance(next_id, bool) or not isinstance(next_id, int) or next_id < 0:
        raise SchemaError(f"{label}.next_id must be a non-negative integer")
    _require_mapping(value["items"], f"{label}.items")


def _require_maintenance(value: Any, label: str) -> None:
    _require_mapping(value, label)
    if set(value) != {"active", "scheduled"}:
        raise SchemaError(f"{label} must contain exactly active and scheduled")
    _require_mapping(value["active"], f"{label}.active")
    _require_mapping(value["scheduled"], f"{label}.scheduled")


@dataclass(frozen=True, slots=True)
class StoreSpec:
    """Static contract for one independently versioned JSON store."""

    name: str
    relative_path: str
    default_factory: Callable[[], Any]
    validator: Callable[[Any, str], None]

    def default_data(self) -> Any:
        value = self.default_factory()
        self.validate_data(value)
        return copy.deepcopy(value)

    def validate_data(self, value: Any) -> None:
        self.validator(value, self.name)


_STORE_SPECS = (
    StoreSpec("users.profiles", "users/profiles.json", _mapping_default, _require_mapping),
    StoreSpec("access.grants", "access/grants.json", _mapping_default, _require_mapping),
    StoreSpec("subscriptions.accounts", "subscriptions/accounts.json", _mapping_default, _require_mapping),
    StoreSpec("subscriptions.requests", "subscriptions/requests.json", _indexed_default, _require_indexed),
    StoreSpec(
        "subscriptions.billing_settings",
        "subscriptions/billing_settings.json",
        _mapping_default,
        _require_mapping,
    ),
    StoreSpec("settings.help_and_contacts", "settings/help_and_contacts.json", _mapping_default, _require_mapping),
    StoreSpec("support.tickets", "support/tickets.json", _indexed_default, _require_indexed),
    StoreSpec("support.ticket_messages", "support/ticket_messages.json", _mapping_default, _require_mapping),
    StoreSpec("maintenance.state", "maintenance/state.json", _maintenance_default, _require_maintenance),
    StoreSpec("messaging.outbox", "messaging/outbox.json", _mapping_default, _require_mapping),
    StoreSpec("audit.events", "audit/events.json", _sequence_default, _require_sequence),
    StoreSpec("monitoring.dns_cache", "monitoring/dns_cache.json", _mapping_default, _require_mapping),
    StoreSpec(
        "monitoring.node_status_cache",
        "monitoring/node_status_cache.json",
        _mapping_default,
        _require_mapping,
    ),
    StoreSpec("monitoring.docker_cache", "monitoring/docker_cache.json", _mapping_default, _require_mapping),
    StoreSpec("monitoring.tls_state", "monitoring/tls_state.json", _mapping_default, _require_mapping),
    StoreSpec(
        "monitoring.fail2ban_cursors",
        "monitoring/fail2ban_cursors.json",
        _mapping_default,
        _require_mapping,
    ),
)

STORE_SPECS: Mapping[str, StoreSpec] = MappingProxyType({spec.name: spec for spec in _STORE_SPECS})
STORE_PATHS: Mapping[str, StoreSpec] = MappingProxyType({spec.relative_path: spec for spec in _STORE_SPECS})


def default_store_data() -> dict[str, Any]:
    """Return independent default values for every v1 store."""

    return {name: spec.default_data() for name, spec in STORE_SPECS.items()}


__all__ = [
    "BACKUP_SCHEMA_VERSION",
    "LAYOUT_FILE",
    "LAYOUT_SCHEMA_VERSION",
    "PTB_TARGET_FILE",
    "STATE_LOCK_FILE",
    "STORE_PATHS",
    "STORE_SCHEMA_VERSION",
    "STORE_SPECS",
    "TRANSACTIONS_DIR",
    "TRANSACTION_SCHEMA_VERSION",
    "StoreSpec",
    "default_store_data",
]
