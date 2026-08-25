from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from app.persistence import MigrationError, SplitJsonBackend
from app.persistence.aggregate_mapping import (
    apply_important_data,
    apply_user_data,
    important_data_from_uow,
    user_data_from_uow,
)
from app.persistence.migration import load_v4_source, migrate_v4_to_split, transform_v4

from .fixtures import write_v4_source


def test_v4_tls_key_identity_is_canonicalized(tmp_path: Path) -> None:
    root = tmp_path / "data"
    write_v4_source(root)

    transformed = transform_v4(load_v4_source(root))

    assert transformed.stores["monitoring.tls_state"] == {
        "example.com:443": {
            "domain": "example.com",
            "port": 443,
            "primary_port": 443,
            "fallback_ports": [],
            "effective_port": 443,
            "servers": [],
            "status": "ok",
            "checked_at": None,
            "not_before": None,
            "not_after": None,
            "fingerprint": None,
            "issuer": None,
            "error": None,
            "last_attempt_at": None,
            "last_success_at": None,
            "used_fallback": False,
            "attempt_errors": [],
            "hostname_valid": False,
            "trust_valid": False,
            "remaining_seconds": 0,
            "notified_fingerprint": None,
            "notified_levels": [],
        }
    }


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("user_id", 0, "user_id must be a positive integer"),
        ("kind", "unknown", "kind is unsupported"),
        ("status", "unknown", "status is unsupported"),
        ("created_at", "", "created_at must be non-empty text"),
        ("used_app", True, "ambiguous legacy fields"),
    ],
)
def test_v4_rejects_semantically_invalid_service_request(
    tmp_path: Path,
    field: str,
    value: Any,
    message: str,
) -> None:
    root = tmp_path / "data"
    write_v4_source(root)
    document = _read_json(root / "user_data.json")
    document["service_requests"]["3"][field] = value
    _write_json(root / "user_data.json", document)

    with pytest.raises(MigrationError, match=message):
        migrate_v4_to_split(root, dry_run=True)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda event: event["recipients"]["1"].update(status="retrying"), "status is unsupported"),
        (lambda event: event.update(allow_blocked_delivery="false"), "must be boolean"),
        (lambda event: event["recipients"].update({"01": {}}), "canonical positive integer"),
        (
            lambda event: event["recipients"]["1"].update(delivered_chat_id=0),
            "positive integer or null",
        ),
    ],
)
def test_v4_rejects_semantically_invalid_outbox(
    tmp_path: Path,
    mutation,
    message: str,
) -> None:
    root = tmp_path / "data"
    write_v4_source(root)
    document = _read_json(root / "user_data.json")
    mutation(document["outbox"]["shared"])
    _write_json(root / "user_data.json", document)

    with pytest.raises(MigrationError, match=message):
        migrate_v4_to_split(root, dry_run=True)


def test_v4_rejects_audit_log_that_would_be_truncated(tmp_path: Path) -> None:
    root = tmp_path / "data"
    write_v4_source(root)
    document = _read_json(root / "user_data.json")
    entry = document["audit_log"][0]
    document["audit_log"] = [copy.deepcopy(entry) for _ in range(2001)]
    _write_json(root / "user_data.json", document)

    with pytest.raises(MigrationError, match="lossless 2000-entry limit"):
        migrate_v4_to_split(root, dry_run=True)


def test_v4_rejects_ambiguous_audit_fields(tmp_path: Path) -> None:
    root = tmp_path / "data"
    write_v4_source(root)
    document = _read_json(root / "user_data.json")
    document["audit_log"][0]["legacy_actor_name"] = "ambiguous"
    _write_json(root / "user_data.json", document)

    with pytest.raises(MigrationError, match="ambiguous fields"):
        migrate_v4_to_split(root, dry_run=True)


@pytest.mark.parametrize(
    ("key", "item", "message"),
    [
        ("example.com", {"status": "ok"}, "key must be domain:port"),
        (
            "example.com:443",
            {"domain": "other.example", "status": "ok"},
            "domain conflicts with its key",
        ),
        (
            "example.com:443",
            {"status": "ok", "failure_kind": "transport"},
            "ambiguous fields",
        ),
    ],
)
def test_v4_rejects_ambiguous_tls_identity_or_fields(
    tmp_path: Path,
    key: str,
    item: dict[str, Any],
    message: str,
) -> None:
    root = tmp_path / "data"
    write_v4_source(root)
    document = _read_json(root / "important_data.json")
    document["tls_certificates"] = {key: item}
    _write_json(root / "important_data.json", document)

    with pytest.raises(MigrationError, match=message):
        migrate_v4_to_split(root, dry_run=True)


def test_v4_migration_is_stable_across_aggregate_round_trip(tmp_path: Path) -> None:
    root = tmp_path / "data"
    write_v4_source(root)
    user_source = _read_json(root / "user_data.json")
    recipient = user_source["outbox"]["shared"]["recipients"]["1"]
    recipient.update(
        {
            "status": "dead_letter",
            "delivered_chat_id": 1001,
            "delivered_message_id": 2002,
            "dead_lettered_at": "2026-01-02T00:00:00+00:00",
        }
    )
    _write_json(root / "user_data.json", user_source)
    migrate_v4_to_split(root, backup_root=tmp_path / "backups")
    backend = SplitJsonBackend(root)
    before = backend.snapshot()
    migrated_recipient = before.data("messaging.outbox")["shared"]["recipients"]["1"]
    assert migrated_recipient["status"] == "dead_letter"
    assert migrated_recipient["delivered_chat_id"] == 1001
    assert migrated_recipient["delivered_message_id"] == 2002
    assert migrated_recipient["dead_lettered_at"] == "2026-01-02T00:00:00+00:00"

    with backend.unit_of_work() as uow:
        user = user_data_from_uow(uow)
        important = important_data_from_uow(uow)
        apply_user_data(uow, user)
        apply_important_data(uow, important)
        uow.commit()

    after = backend.snapshot()
    changed = {
        name: {"before": before.data(name), "after": after.data(name)}
        for name in after.stores
        if after.data(name) != before.data(name)
    }
    assert changed == {}
    assert after.revision == before.revision


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
