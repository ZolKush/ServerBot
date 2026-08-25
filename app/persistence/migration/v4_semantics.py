"""Strict semantic validation and canonicalization for legacy schema v4."""

from __future__ import annotations

import copy
from typing import Any

from ..errors import MigrationError
from ..normalization import (
    OUTBOX_RECIPIENT_STATUSES,
    SERVICE_REQUEST_KINDS,
    SERVICE_REQUEST_STATUSES,
    normalize_audit_log,
    normalize_outbox,
    normalize_review_messages,
    normalize_service_requests,
    normalize_tls_certificates,
)

_REQUEST_TEXT_LIMITS = {
    "created_at": 500,
    "updated_at": 500,
    "comment": 3200,
    "claimed_at": 500,
    "reviewed_at": 500,
    "target_end_at": 500,
    "payment_reported_at": 500,
    "decision_reason": 500,
}
_OUTBOX_FIELDS = {
    "id",
    "kind",
    "created_at",
    "payload",
    "recipients",
    "completion",
    "allow_blocked_delivery",
}
_RECIPIENT_FIELDS = {
    "status",
    "attempts",
    "part_index",
    "next_attempt_at",
    "last_error",
    "delivered_at",
    "delivered_chat_id",
    "delivered_message_id",
    "dead_lettered_at",
}
_AUDIT_FIELDS = {
    "ts",
    "action",
    "actor_id",
    "actor_public",
    "actor_internal",
    "target_user_id",
    "details",
}
_TLS_FIELDS = {
    "domain",
    "port",
    "primary_port",
    "fallback_ports",
    "effective_port",
    "servers",
    "status",
    "checked_at",
    "not_before",
    "not_after",
    "fingerprint",
    "issuer",
    "error",
    "last_attempt_at",
    "last_success_at",
    "used_fallback",
    "attempt_errors",
    "hostname_valid",
    "trust_valid",
    "remaining_seconds",
    "notified_fingerprint",
    "notified_levels",
}
_TLS_STATUSES = {"valid", "unavailable", "ok", "expiring", "expired", "invalid", "error", "unknown"}


def canonicalize_service_requests(value: Any) -> dict[str, dict[str, Any]]:
    records = _mapping(value, "service_requests")
    result: dict[str, dict[str, Any]] = {}
    for raw_key, raw_record in records.items():
        key = str(raw_key)
        record = _mapping(raw_record, f"service_requests.{key}")
        _positive_integer(record.get("id"), f"service_requests.{key}.id")
        _positive_integer(record.get("user_id"), f"service_requests.{key}.user_id")
        if record.get("kind") not in SERVICE_REQUEST_KINDS:
            raise MigrationError(f"service_requests.{key}.kind is unsupported")
        if record.get("status") not in SERVICE_REQUEST_STATUSES:
            raise MigrationError(f"service_requests.{key}.status is unsupported")
        created_at = record.get("created_at")
        if not isinstance(created_at, str) or not created_at.strip() or len(created_at) > 500:
            raise MigrationError(f"service_requests.{key}.created_at must be non-empty text")
        obsolete = sorted({"used_app", "used_application"} & set(record))
        if obsolete:
            raise MigrationError(f"service_requests.{key} has ambiguous legacy fields: {obsolete}")
        _validate_review_messages(record.get("review_messages"), key)
        resume_status = record.get("resume_status")
        if resume_status not in {None, "", "pending", "payment_reported"}:
            raise MigrationError(f"service_requests.{key}.resume_status is unsupported")
        normalized = normalize_service_requests({key: record})
        if set(normalized) != {key}:
            raise MigrationError(f"service_requests.{key} cannot be represented canonically")
        canonical = normalized[key]
        for field, limit in _REQUEST_TEXT_LIMITS.items():
            raw_text = record.get(field)
            if (
                field in record
                and raw_text not in (None, "")
                and (not isinstance(raw_text, str) or len(raw_text) > limit or canonical[field] != raw_text)
            ):
                raise MigrationError(f"service_requests.{key}.{field} is not canonical text")
        lost = sorted(set(record) - set(canonical))
        if lost:
            raise MigrationError(f"service_requests.{key} has fields with no current owner: {lost}")
        result[key] = canonical
    return result


def canonicalize_outbox(value: Any, label: str) -> dict[str, dict[str, Any]]:
    events = _mapping(value, label)
    result: dict[str, dict[str, Any]] = {}
    for raw_key, raw_event in events.items():
        event_id = str(raw_key)
        event = _mapping(raw_event, f"{label}.{event_id}")
        unknown = sorted(set(event) - _OUTBOX_FIELDS)
        if unknown:
            raise MigrationError(f"{label}.{event_id} has ambiguous fields: {unknown}")
        if event.get("id") != event_id or not event_id.strip():
            raise MigrationError(f"{label}.{event_id} id does not match its key")
        if not isinstance(event.get("kind"), str) or not event["kind"] or len(event["kind"]) > 100:
            raise MigrationError(f"{label}.{event_id}.kind must be non-empty text")
        if not isinstance(event.get("created_at"), str) or not event["created_at"]:
            raise MigrationError(f"{label}.{event_id}.created_at must be non-empty text")
        _mapping(event.get("payload"), f"{label}.{event_id}.payload")
        if "completion" in event and not isinstance(event["completion"], dict):
            raise MigrationError(f"{label}.{event_id}.completion must be an object")
        if "allow_blocked_delivery" in event and not isinstance(event["allow_blocked_delivery"], bool):
            raise MigrationError(f"{label}.{event_id}.allow_blocked_delivery must be boolean")
        recipients = _mapping(event.get("recipients"), f"{label}.{event_id}.recipients")
        if not recipients:
            raise MigrationError(f"{label}.{event_id} has no recipients")
        canonical_recipients: set[str] = set()
        for raw_uid, raw_state in recipients.items():
            uid = _positive_integer_from_key(raw_uid, f"{label}.{event_id}.recipients")
            uid_key = str(uid)
            if uid_key in canonical_recipients:
                raise MigrationError(f"{label}.{event_id} has colliding recipient id {uid}")
            canonical_recipients.add(uid_key)
            state = _mapping(raw_state, f"{label}.{event_id}.recipients.{raw_uid}")
            unknown_state = sorted(set(state) - _RECIPIENT_FIELDS)
            if unknown_state:
                raise MigrationError(f"{label}.{event_id}.recipients.{raw_uid} has ambiguous fields: {unknown_state}")
            if "status" in state and state["status"] not in OUTBOX_RECIPIENT_STATUSES:
                raise MigrationError(f"{label}.{event_id}.recipients.{raw_uid}.status is unsupported")
            for field in ("attempts", "part_index"):
                if field in state and (
                    isinstance(state[field], bool) or not isinstance(state[field], int) or state[field] < 0
                ):
                    raise MigrationError(f"{label}.{event_id}.recipients.{raw_uid}.{field} must be non-negative")
            for field in ("next_attempt_at", "last_error", "delivered_at", "dead_lettered_at"):
                if field in state and not isinstance(state[field], str):
                    raise MigrationError(f"{label}.{event_id}.recipients.{raw_uid}.{field} must be text")
            if len(state.get("last_error", "")) > 500:
                raise MigrationError(f"{label}.{event_id}.recipients.{raw_uid}.last_error is too long")
            for field in ("delivered_chat_id", "delivered_message_id"):
                identifier = state.get(field)
                if identifier is not None and (
                    isinstance(identifier, bool) or not isinstance(identifier, int) or identifier <= 0
                ):
                    raise MigrationError(
                        f"{label}.{event_id}.recipients.{raw_uid}.{field} must be a positive integer or null"
                    )
        normalized = normalize_outbox({event_id: event})
        if set(normalized) != {event_id}:
            raise MigrationError(f"{label}.{event_id} cannot be represented canonically")
        canonical = normalized[event_id]
        if set(canonical["recipients"]) != canonical_recipients:
            raise MigrationError(f"{label}.{event_id} loses recipients during canonicalization")
        result[event_id] = canonical
    return result


def canonicalize_audit_log(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise MigrationError("audit_log must be a JSON array")
    if len(value) > 2000:
        raise MigrationError("audit_log exceeds the lossless 2000-entry limit")
    for index, raw_item in enumerate(value):
        item = _mapping(raw_item, f"audit_log[{index}]")
        unknown = sorted(set(item) - _AUDIT_FIELDS)
        if unknown:
            raise MigrationError(f"audit_log[{index}] has ambiguous fields: {unknown}")
        for field, limit in (("ts", 80), ("action", 100)):
            text = item.get(field)
            if not isinstance(text, str) or not text.strip() or len(text) > limit or text.strip() != text:
                raise MigrationError(f"audit_log[{index}].{field} must be canonical non-empty text")
        for field in ("actor_id", "target_user_id"):
            identifier = item.get(field)
            if identifier is not None and (isinstance(identifier, bool) or not isinstance(identifier, int)):
                raise MigrationError(f"audit_log[{index}].{field} must be an integer or null")
        if "details" in item and not isinstance(item["details"], dict):
            raise MigrationError(f"audit_log[{index}].details must be an object")
        for field, limit in (("actor_public", 160), ("actor_internal", 240)):
            text = item.get(field)
            if (
                field in item
                and text not in (None, "")
                and (not isinstance(text, str) or len(text) > limit or text.strip() != text)
            ):
                raise MigrationError(f"audit_log[{index}].{field} must be canonical text")
    normalized = normalize_audit_log(value)
    if len(normalized) != len(value):
        raise MigrationError("audit_log loses entries during canonicalization")
    return normalized


def canonicalize_tls_certificates(value: Any) -> dict[str, dict[str, Any]]:
    certificates = _mapping(value, "tls_certificates")
    result: dict[str, dict[str, Any]] = {}
    for raw_key, raw_item in certificates.items():
        source_key = str(raw_key)
        domain, primary_port = _tls_identity(source_key)
        item = copy.deepcopy(_mapping(raw_item, f"tls_certificates.{source_key}"))
        unknown = sorted(set(item) - _TLS_FIELDS)
        if unknown:
            raise MigrationError(f"tls_certificates.{source_key} has ambiguous fields: {unknown}")
        declared_domain = item.get("domain")
        if declared_domain is not None and (
            not isinstance(declared_domain, str) or _canonical_domain(declared_domain) != domain
        ):
            raise MigrationError(f"tls_certificates.{source_key}.domain conflicts with its key")
        if "primary_port" in item and _port(item["primary_port"], source_key, "primary_port") != primary_port:
            raise MigrationError(f"tls_certificates.{source_key}.primary_port conflicts with its key")
        for field in ("port", "effective_port"):
            if field in item:
                _port(item[field], source_key, field)
        if "port" in item and "effective_port" in item and item["port"] != item["effective_port"]:
            raise MigrationError(f"tls_certificates.{source_key} has ambiguous effective port fields")
        status = item.get("status")
        if status is not None and (not isinstance(status, str) or status not in _TLS_STATUSES):
            raise MigrationError(f"tls_certificates.{source_key}.status is unsupported")
        item["domain"] = domain
        item["primary_port"] = primary_port
        item.setdefault("port", primary_port)
        normalized = normalize_tls_certificates({source_key: item})
        if len(normalized) != 1:
            raise MigrationError(f"tls_certificates.{source_key} cannot be represented canonically")
        canonical_key, canonical = next(iter(normalized.items()))
        for field in ("fallback_ports", "servers", "attempt_errors", "notified_levels"):
            if field in item and (not isinstance(item[field], list) or canonical[field] != item[field]):
                raise MigrationError(f"tls_certificates.{source_key}.{field} is not canonical")
        for field in ("used_fallback", "hostname_valid", "trust_valid"):
            if field in item and not isinstance(item[field], bool):
                raise MigrationError(f"tls_certificates.{source_key}.{field} must be boolean")
        if "remaining_seconds" in item and (
            isinstance(item["remaining_seconds"], bool) or not isinstance(item["remaining_seconds"], int)
        ):
            raise MigrationError(f"tls_certificates.{source_key}.remaining_seconds must be an integer")
        for field in (
            "checked_at",
            "not_before",
            "not_after",
            "fingerprint",
            "issuer",
            "error",
            "last_attempt_at",
            "last_success_at",
            "notified_fingerprint",
        ):
            raw_text = item.get(field)
            if field in item and raw_text not in (None, "") and canonical[field] != raw_text:
                raise MigrationError(f"tls_certificates.{source_key}.{field} is not canonical text")
        if canonical_key in result:
            raise MigrationError(f"tls_certificates contains colliding canonical key {canonical_key}")
        result[canonical_key] = canonical
    return result


def _validate_review_messages(value: Any, request_key: str) -> None:
    if value is None:
        return
    reviews = _mapping(value, f"service_requests.{request_key}.review_messages")
    expected_refs = 0
    canonical_admins: set[str] = set()
    for raw_admin, raw_refs in reviews.items():
        admin_id = _positive_integer_from_key(raw_admin, f"service_requests.{request_key}.review_messages")
        canonical_admin = str(admin_id)
        if canonical_admin in canonical_admins:
            raise MigrationError(f"service_requests.{request_key}.review_messages has colliding admin ids")
        canonical_admins.add(canonical_admin)
        refs = raw_refs if isinstance(raw_refs, list) else [raw_refs]
        expected_refs += len(refs)
    normalized = normalize_review_messages(reviews)
    actual_refs = sum(len(refs) for refs in normalized.values())
    if set(normalized) != canonical_admins or actual_refs != expected_refs:
        raise MigrationError(f"service_requests.{request_key}.review_messages loses references")


def _tls_identity(raw_key: str) -> tuple[str, int]:
    raw_domain, separator, raw_port = raw_key.rpartition(":")
    if not separator or not raw_domain or not raw_port.isdecimal():
        raise MigrationError(f"tls_certificates key must be domain:port, got {raw_key!r}")
    domain = _canonical_domain(raw_domain)
    return domain, _port(int(raw_port), raw_key, "key port")


def _canonical_domain(value: str) -> str:
    domain = value.strip().lower().rstrip(".")
    if not domain or len(domain) > 253:
        raise MigrationError(f"invalid TLS domain: {value!r}")
    return domain


def _port(value: Any, key: str, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535:
        raise MigrationError(f"tls_certificates.{key}.{field} must be an integer in 1..65535")
    return value


def _positive_integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise MigrationError(f"{label} must be a positive integer")
    return value


def _positive_integer_from_key(value: Any, label: str) -> int:
    text = str(value)
    try:
        parsed = int(text)
    except (TypeError, ValueError, OverflowError) as exc:
        raise MigrationError(f"{label} key must be a positive integer: {value!r}") from exc
    if parsed <= 0 or text != str(parsed):
        raise MigrationError(f"{label} key must be a canonical positive integer: {value!r}")
    return parsed


def _mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise MigrationError(f"{label} must be a JSON object")
    return value


__all__ = [
    "canonicalize_audit_log",
    "canonicalize_outbox",
    "canonicalize_service_requests",
    "canonicalize_tls_certificates",
]
