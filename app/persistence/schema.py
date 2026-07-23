"""Serialization and validation for store documents and the layout manifest."""

from __future__ import annotations

import copy
import math
from dataclasses import dataclass
from typing import Any

from .errors import SchemaError
from .io import encode_json, sha256_bytes
from .layout import LAYOUT_SCHEMA_VERSION, STORE_SCHEMA_VERSION, STORE_SPECS

STORE_DOCUMENT_KEYS = {"schema_version", "revision", "store", "data"}
LAYOUT_KEYS = {
    "schema_version",
    "layout",
    "revision",
    "transaction_id",
    "stores",
    "migration",
}
STORE_ENTRY_KEYS = {"path", "schema_version", "revision", "sha256"}


def _non_negative_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SchemaError(f"{label} must be a non-negative integer")
    return value


def _non_empty_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SchemaError(f"{label} must be a non-empty string")
    return value


@dataclass(frozen=True, slots=True)
class StoreDocument:
    name: str
    revision: int
    data: Any
    payload: bytes
    sha256: str


@dataclass(frozen=True, slots=True)
class StoreManifestEntry:
    path: str
    schema_version: int
    revision: int
    sha256: str


@dataclass(frozen=True, slots=True)
class LayoutManifest:
    revision: int
    transaction_id: str
    stores: dict[str, StoreManifestEntry]
    migration: dict[str, Any] | None
    payload: bytes
    sha256: str


def build_store_document(name: str, revision: int, data: Any) -> StoreDocument:
    spec = STORE_SPECS.get(name)
    if spec is None:
        raise SchemaError(f"unknown store: {name!r}")
    revision = _non_negative_int(revision, f"{name}.revision")
    spec.validate_data(data)
    _validate_json_value(data, f"{name}.data")
    document = {
        "schema_version": STORE_SCHEMA_VERSION,
        "revision": revision,
        "store": name,
        "data": copy.deepcopy(data),
    }
    payload = encode_json(document)
    return StoreDocument(name, revision, copy.deepcopy(data), payload, sha256_bytes(payload))


def parse_store_document(name: str, raw: Any, payload: bytes) -> StoreDocument:
    spec = STORE_SPECS.get(name)
    if spec is None:
        raise SchemaError(f"unknown store: {name!r}")
    if not isinstance(raw, dict) or set(raw) != STORE_DOCUMENT_KEYS:
        raise SchemaError(f"{name} document must contain exactly {sorted(STORE_DOCUMENT_KEYS)}")
    if raw["schema_version"] != STORE_SCHEMA_VERSION:
        raise SchemaError(f"{name} has schema_version={raw['schema_version']!r}; expected {STORE_SCHEMA_VERSION}")
    if raw["store"] != name:
        raise SchemaError(f"{name} document declares store={raw['store']!r}")
    revision = _non_negative_int(raw["revision"], f"{name}.revision")
    spec.validate_data(raw["data"])
    return StoreDocument(name, revision, copy.deepcopy(raw["data"]), payload, sha256_bytes(payload))


def build_layout_manifest(
    *,
    revision: int,
    transaction_id: str,
    documents: dict[str, StoreDocument],
    migration: dict[str, Any] | None,
) -> LayoutManifest:
    revision = _non_negative_int(revision, "layout.revision")
    transaction_id = _non_empty_string(transaction_id, "layout.transaction_id")
    if set(documents) != set(STORE_SPECS):
        missing = sorted(set(STORE_SPECS) - set(documents))
        extra = sorted(set(documents) - set(STORE_SPECS))
        raise SchemaError(f"layout store set mismatch; missing={missing}, extra={extra}")
    stores: dict[str, StoreManifestEntry] = {}
    raw_stores: dict[str, dict[str, Any]] = {}
    for name, spec in STORE_SPECS.items():
        document = documents[name]
        entry = StoreManifestEntry(
            path=spec.relative_path,
            schema_version=STORE_SCHEMA_VERSION,
            revision=document.revision,
            sha256=document.sha256,
        )
        stores[name] = entry
        raw_stores[name] = {
            "path": entry.path,
            "schema_version": entry.schema_version,
            "revision": entry.revision,
            "sha256": entry.sha256,
        }
    if migration is not None and not isinstance(migration, dict):
        raise SchemaError("layout.migration must be an object or null")
    _validate_json_value(migration, "layout.migration")
    raw = {
        "schema_version": LAYOUT_SCHEMA_VERSION,
        "layout": "split-json",
        "revision": revision,
        "transaction_id": transaction_id,
        "stores": raw_stores,
        "migration": copy.deepcopy(migration),
    }
    payload = encode_json(raw)
    return LayoutManifest(
        revision,
        transaction_id,
        stores,
        copy.deepcopy(migration),
        payload,
        sha256_bytes(payload),
    )


def _validate_json_value(value: Any, label: str) -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise SchemaError(f"{label} contains a non-finite number")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{label}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise SchemaError(f"{label} contains a non-string object key")
            _validate_json_value(item, f"{label}.{key}")
        return
    raise SchemaError(f"{label} contains non-JSON value of type {type(value).__name__}")


def parse_layout_manifest(raw: Any, payload: bytes) -> LayoutManifest:
    if not isinstance(raw, dict) or set(raw) != LAYOUT_KEYS:
        raise SchemaError(f"layout manifest must contain exactly {sorted(LAYOUT_KEYS)}")
    if raw["schema_version"] != LAYOUT_SCHEMA_VERSION:
        raise SchemaError(f"layout has schema_version={raw['schema_version']!r}; expected {LAYOUT_SCHEMA_VERSION}")
    if raw["layout"] != "split-json":
        raise SchemaError(f"unsupported layout kind: {raw['layout']!r}")
    revision = _non_negative_int(raw["revision"], "layout.revision")
    transaction_id = _non_empty_string(raw["transaction_id"], "layout.transaction_id")
    migration = raw["migration"]
    if migration is not None and not isinstance(migration, dict):
        raise SchemaError("layout.migration must be an object or null")
    raw_stores = raw["stores"]
    if not isinstance(raw_stores, dict) or set(raw_stores) != set(STORE_SPECS):
        raise SchemaError("layout manifest has an unexpected store set")
    stores: dict[str, StoreManifestEntry] = {}
    for name, spec in STORE_SPECS.items():
        raw_entry = raw_stores[name]
        if not isinstance(raw_entry, dict) or set(raw_entry) != STORE_ENTRY_KEYS:
            raise SchemaError(f"layout entry {name} has an invalid shape")
        if raw_entry["path"] != spec.relative_path:
            raise SchemaError(f"layout entry {name} points to {raw_entry['path']!r}")
        if raw_entry["schema_version"] != STORE_SCHEMA_VERSION:
            raise SchemaError(f"layout entry {name} has an unsupported schema version")
        entry_revision = _non_negative_int(raw_entry["revision"], f"layout.stores.{name}.revision")
        entry_hash = raw_entry["sha256"]
        if (
            not isinstance(entry_hash, str)
            or len(entry_hash) != 64
            or any(char not in "0123456789abcdef" for char in entry_hash)
        ):
            raise SchemaError(f"layout.stores.{name}.sha256 is invalid")
        stores[name] = StoreManifestEntry(
            path=spec.relative_path,
            schema_version=STORE_SCHEMA_VERSION,
            revision=entry_revision,
            sha256=entry_hash,
        )
    return LayoutManifest(
        revision,
        transaction_id,
        stores,
        copy.deepcopy(migration),
        payload,
        sha256_bytes(payload),
    )


__all__ = [
    "LayoutManifest",
    "StoreDocument",
    "StoreManifestEntry",
    "build_layout_manifest",
    "build_store_document",
    "parse_layout_manifest",
    "parse_store_document",
]
