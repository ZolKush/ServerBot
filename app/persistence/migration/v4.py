"""Strict, loss-detecting transformation from monolithic schema v4."""

from __future__ import annotations

import copy
import hashlib
from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..aggregate_fields import (
    ACCESS_FIELDS,
    BILLING_FIELDS,
    HELP_FIELDS,
    KNOWN_USER_FIELDS,
    OUTBOX_ORIGIN_FIELD,
    PROFILE_FIELDS,
    SUBSCRIPTION_FIELDS,
)
from ..errors import MigrationError
from ..io import decode_json
from ..layout import STORE_SPECS

USER_TOP_LEVEL_KEYS = {
    "schema_version",
    "authorized_users",
    "outbox",
    "request_seq",
    "service_requests",
    "product_settings",
    "audit_log",
}
IMPORTANT_TOP_LEVEL_KEYS = {
    "schema_version",
    "tickets_seq",
    "tickets",
    "maintenance",
    "scheduled_maintenance",
    "dns_status",
    "daily_node_status",
    "outbox",
    "fail2ban_cursors",
    "tls_certificates",
    "docker_status",
}


@dataclass(frozen=True, slots=True)
class V4Source:
    user_path: Path
    important_path: Path
    user_payload: bytes
    important_payload: bytes
    user_data: dict[str, Any]
    important_data: dict[str, Any]
    ptb_path: Path | None
    ptb_payload: bytes | None
    fingerprint: str


@dataclass(frozen=True, slots=True)
class V4Transform:
    stores: dict[str, Any]
    outbox_collisions: dict[str, str]


def load_v4_source(
    data_root: Path,
    *,
    user_path: Path | None = None,
    important_path: Path | None = None,
    ptb_path: Path | None = None,
) -> V4Source:
    user_source = (user_path or data_root / "user_data.json").resolve()
    important_source = (important_path or data_root / "important_data.json").resolve()
    if not user_source.is_file() or not important_source.is_file():
        raise MigrationError("both user_data.json and important_data.json are required")
    user_payload = user_source.read_bytes()
    important_payload = important_source.read_bytes()
    user_data = _root_object(decode_json(user_payload, source=str(user_source)), "user_data")
    important_data = _root_object(
        decode_json(important_payload, source=str(important_source)),
        "important_data",
    )
    _validate_root(user_data, USER_TOP_LEVEL_KEYS, "user_data")
    _validate_root(important_data, IMPORTANT_TOP_LEVEL_KEYS, "important_data")

    selected_ptb = ptb_path if ptb_path is not None else data_root / "ptb_persistence"
    selected_ptb = selected_ptb.resolve()
    if ptb_path is not None and not selected_ptb.is_file():
        raise MigrationError(f"explicit PTB persistence file does not exist: {selected_ptb}")
    ptb_payload = selected_ptb.read_bytes() if selected_ptb.is_file() else None
    actual_ptb_path = selected_ptb if ptb_payload is not None else None
    fingerprint = _source_fingerprint(user_payload, important_payload, ptb_payload)
    return V4Source(
        user_path=user_source,
        important_path=important_source,
        user_payload=user_payload,
        important_payload=important_payload,
        user_data=user_data,
        important_data=important_data,
        ptb_path=actual_ptb_path,
        ptb_payload=ptb_payload,
        fingerprint=fingerprint,
    )


def transform_v4(source: V4Source) -> V4Transform:
    users = _mapping(source.user_data["authorized_users"], "authorized_users")
    profiles: dict[str, Any] = {}
    grants: dict[str, Any] = {}
    accounts: dict[str, Any] = {}
    for raw_user_id, raw_meta in users.items():
        meta = _mapping(raw_meta, f"authorized_users.{raw_user_id}")
        unknown = sorted(set(meta) - KNOWN_USER_FIELDS)
        if unknown:
            raise MigrationError(f"authorized_users.{raw_user_id} has unknown fields: {unknown}")
        user_id = meta.get("user_id")
        if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
            raise MigrationError(f"authorized_users.{raw_user_id}.user_id must be positive")
        if str(user_id) != str(raw_user_id):
            raise MigrationError(f"authorized_users key/id mismatch for {raw_user_id!r}")
        access_state = meta.get("access_state")
        enabled = meta.get("enabled")
        if not isinstance(enabled, bool) or enabled != (access_state == "approved"):
            raise MigrationError(f"authorized_users.{raw_user_id}.enabled is inconsistent with access_state")
        profiles[str(user_id)] = _select_fields(meta, PROFILE_FIELDS)
        grants[str(user_id)] = _select_fields(meta, ACCESS_FIELDS)
        accounts[str(user_id)] = _select_fields(meta, SUBSCRIPTION_FIELDS)
    owners = sum(1 for grant in grants.values() if grant.get("role") == "admin" and grant.get("admin_level") == "owner")
    if owners > 1:
        raise MigrationError("authorized_users contains multiple service owners")

    product_settings = _mapping(source.user_data["product_settings"], "product_settings")
    unknown_settings = sorted(set(product_settings) - BILLING_FIELDS - HELP_FIELDS)
    missing_settings = sorted((BILLING_FIELDS | HELP_FIELDS) - set(product_settings))
    if unknown_settings or missing_settings:
        raise MigrationError(f"product_settings field mismatch; missing={missing_settings}, unknown={unknown_settings}")

    user_outbox = _validated_outbox(source.user_data["outbox"], "user_data.outbox")
    important_outbox = _validated_outbox(source.important_data["outbox"], "important_data.outbox")
    merged_outbox, collisions = _merge_outboxes(user_outbox, important_outbox)
    ticket_items, ticket_messages = _split_tickets(source.important_data["tickets"])

    stores: dict[str, Any] = {
        "users.profiles": profiles,
        "access.grants": grants,
        "subscriptions.accounts": accounts,
        "subscriptions.requests": {
            "next_id": _non_negative_int(source.user_data["request_seq"], "request_seq"),
            "items": copy.deepcopy(_mapping(source.user_data["service_requests"], "service_requests")),
        },
        "subscriptions.billing_settings": _select_fields(product_settings, BILLING_FIELDS),
        "settings.help_and_contacts": _select_fields(product_settings, HELP_FIELDS),
        "support.tickets": {
            "next_id": _non_negative_int(source.important_data["tickets_seq"], "tickets_seq"),
            "items": ticket_items,
        },
        "support.ticket_messages": ticket_messages,
        "maintenance.state": {
            "active": copy.deepcopy(_mapping(source.important_data["maintenance"], "maintenance")),
            "scheduled": copy.deepcopy(
                _mapping(source.important_data["scheduled_maintenance"], "scheduled_maintenance")
            ),
        },
        "messaging.outbox": merged_outbox,
        "audit.events": copy.deepcopy(_sequence(source.user_data["audit_log"], "audit_log")),
        "monitoring.dns_cache": copy.deepcopy(_mapping(source.important_data["dns_status"], "dns_status")),
        "monitoring.node_status_cache": copy.deepcopy(
            _mapping(source.important_data["daily_node_status"], "daily_node_status")
        ),
        "monitoring.docker_cache": _convert_docker(source.important_data["docker_status"]),
        "monitoring.tls_state": copy.deepcopy(_mapping(source.important_data["tls_certificates"], "tls_certificates")),
        "monitoring.fail2ban_cursors": copy.deepcopy(
            _mapping(source.important_data["fail2ban_cursors"], "fail2ban_cursors")
        ),
    }
    if set(stores) != set(STORE_SPECS):
        raise MigrationError("internal migration store set mismatch")
    for name, data in stores.items():
        STORE_SPECS[name].validate_data(data)
    return V4Transform(stores=stores, outbox_collisions=collisions)


def _validate_root(raw: dict[str, Any], expected_keys: set[str], label: str) -> None:
    if raw.get("schema_version") != 4:
        raise MigrationError(f"{label} schema_version must be exactly 4")
    missing = sorted(expected_keys - set(raw))
    unknown = sorted(set(raw) - expected_keys)
    if missing or unknown:
        raise MigrationError(f"{label} field mismatch; missing={missing}, unknown={unknown}")


def _root_object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise MigrationError(f"{label} must be a JSON object")
    return value


def _mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise MigrationError(f"{label} must be a JSON object")
    return value


def _sequence(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise MigrationError(f"{label} must be a JSON array")
    return value


def _non_negative_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MigrationError(f"{label} must be a non-negative integer")
    return value


def _select_fields(source: dict[str, Any], fields: Collection[str]) -> dict[str, Any]:
    return {key: copy.deepcopy(source[key]) for key in sorted(fields) if key in source}


def _validated_outbox(value: Any, label: str) -> dict[str, Any]:
    outbox = _mapping(value, label)
    result: dict[str, Any] = {}
    for raw_id, raw_event in outbox.items():
        event_id = str(raw_id)
        event = _mapping(raw_event, f"{label}.{event_id}")
        if event.get("id") != event_id:
            raise MigrationError(f"{label}.{event_id} id does not match its key")
        result[event_id] = copy.deepcopy(event)
    return result


def _merge_outboxes(
    user_outbox: dict[str, Any],
    important_outbox: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, str]]:
    merged = copy.deepcopy(user_outbox)
    for event in merged.values():
        event[OUTBOX_ORIGIN_FIELD] = "user"
    collisions: dict[str, str] = {}
    for event_id, event in important_outbox.items():
        target_id = event_id
        if target_id in merged:
            suffix = 1
            target_id = f"{event_id}~important"
            while target_id in merged:
                suffix += 1
                target_id = f"{event_id}~important-{suffix}"
            collisions[event_id] = target_id
        migrated = copy.deepcopy(event)
        migrated["id"] = target_id
        migrated[OUTBOX_ORIGIN_FIELD] = "important"
        merged[target_id] = migrated
    return merged, collisions


def _split_tickets(value: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    tickets = _mapping(value, "tickets")
    ticket_items: dict[str, Any] = {}
    messages: dict[str, Any] = {}
    for raw_id, raw_ticket in tickets.items():
        ticket_id = str(raw_id)
        ticket = copy.deepcopy(_mapping(raw_ticket, f"tickets.{ticket_id}"))
        declared_id = ticket.get("id")
        if isinstance(declared_id, bool) or not isinstance(declared_id, int) or str(declared_id) != ticket_id:
            raise MigrationError(f"tickets.{ticket_id} id does not match its key")
        raw_messages = _sequence(ticket.pop("messages", None), f"tickets.{ticket_id}.messages")
        for index, raw_message in enumerate(raw_messages):
            _mapping(raw_message, f"tickets.{ticket_id}.messages[{index}]")
        ticket_items[ticket_id] = ticket
        messages[ticket_id] = copy.deepcopy(raw_messages)
    return ticket_items, messages


def _convert_docker(value: Any) -> dict[str, Any]:
    servers = _mapping(value, "docker_status")
    result: dict[str, Any] = {}
    for server_key, raw_status in servers.items():
        status = _mapping(raw_status, f"docker_status.{server_key}")
        if set(status) != {"updated_at", "containers"}:
            raise MigrationError(f"docker_status.{server_key} has an unexpected shape")
        raw_containers = _sequence(status["containers"], f"docker_status.{server_key}.containers")
        containers: list[dict[str, Any]] = []
        for index, raw_container in enumerate(raw_containers):
            if not isinstance(raw_container, list) or len(raw_container) != 4:
                raise MigrationError(f"docker_status.{server_key}.containers[{index}] must be a four-item array")
            name, running, status_text, restarts = raw_container
            if not isinstance(name, str) or not isinstance(running, bool) or not isinstance(status_text, str):
                raise MigrationError(f"docker_status.{server_key}.containers[{index}] has invalid types")
            if not isinstance(restarts, str):
                raise MigrationError(f"docker_status.{server_key}.containers[{index}].restarts must be text")
            containers.append(
                {
                    "name": name,
                    "running": running,
                    "status": status_text,
                    "restarts": restarts,
                }
            )
        result[str(server_key)] = {
            "updated_at": copy.deepcopy(status["updated_at"]),
            "containers": containers,
        }
    return result


def _source_fingerprint(user_payload: bytes, important_payload: bytes, ptb_payload: bytes | None) -> str:
    digest = hashlib.sha256()
    for label, payload in (
        ("user_data.json", user_payload),
        ("important_data.json", important_payload),
        ("ptb_persistence", ptb_payload),
    ):
        encoded_label = label.encode("utf-8")
        digest.update(len(encoded_label).to_bytes(4, "big"))
        digest.update(encoded_label)
        marker = b"\x01" if payload is not None else b"\x00"
        digest.update(marker)
        if payload is not None:
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
    return digest.hexdigest()


__all__ = ["V4Source", "V4Transform", "load_v4_source", "transform_v4"]
