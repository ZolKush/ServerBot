from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from app.persistence import MigrationError, SchemaError, SplitJsonBackend, StorageConflictError
from app.persistence.layout import PTB_TARGET_FILE, STORE_SPECS
from app.persistence.migration import load_v4_source, migrate_v4_to_split, transform_v4
from app.persistence.migration.backup import BACKUP_MANIFEST_FILE

from .fixtures import clone_payload, write_v4_source


def test_v4_transform_splits_every_domain_without_references(tmp_path: Path) -> None:
    root = tmp_path / "data"
    user, important = write_v4_source(root)

    transformed = transform_v4(load_v4_source(root))

    assert set(transformed.stores) == set(STORE_SPECS)
    profile = transformed.stores["users.profiles"]["42"]
    grant = transformed.stores["access.grants"]["42"]
    account = transformed.stores["subscriptions.accounts"]["42"]
    assert profile["username"] == user["authorized_users"]["42"]["username"]
    assert grant["access_state"] == "approved"
    assert account["connection_url"] == user["authorized_users"]["42"]["connection_url"]
    assert "enabled" not in profile
    assert "enabled" not in grant
    assert "enabled" not in account
    assert "service_tier" not in profile
    assert "username" not in account

    tickets = transformed.stores["support.tickets"]
    messages = transformed.stores["support.ticket_messages"]
    assert "messages" not in tickets["items"]["7"]
    assert messages["7"] == important["tickets"]["7"]["messages"]
    assert tickets["items"]["7"]["user_id"] == 9001
    assert "9001" not in transformed.stores["users.profiles"]

    docker = transformed.stores["monitoring.docker_cache"]["main"]["containers"]
    assert docker == [
        {
            "name": "api",
            "running": True,
            "status": "Up (healthy)",
            "restarts": "2",
        }
    ]
    assert transformed.outbox_collisions == {"shared": "shared~important"}
    assert transformed.stores["messaging.outbox"]["shared"]["kind"] == "user"
    assert transformed.stores["messaging.outbox"]["shared~important"]["kind"] == "important"
    assert transformed.stores["messaging.outbox"]["shared~important"]["id"] == "shared~important"


def test_dry_run_is_strictly_read_only(tmp_path: Path) -> None:
    root = tmp_path / "data"
    write_v4_source(root)
    before = _tree_hashes(root)
    backup_root = tmp_path / "backups"

    report = migrate_v4_to_split(root, dry_run=True, backup_root=backup_root)

    assert report.dry_run is True
    assert report.already_migrated is False
    assert report.backup_path is None
    assert report.store_counts["users.profiles"] == 1
    assert _tree_hashes(root) == before
    assert not backup_root.exists()
    assert not (root / "runtime" / "state.lock").exists()


def test_one_shot_migration_backs_up_and_publishes_verified_layout(tmp_path: Path) -> None:
    root = tmp_path / "data"
    backup_root = tmp_path / "backups"
    user, important = write_v4_source(root)
    original_ptb = (root / "ptb_persistence").read_bytes()

    report = migrate_v4_to_split(root, backup_root=backup_root)

    assert report.dry_run is False
    assert report.already_migrated is False
    assert report.ptb_copied is True
    assert report.outbox_collisions == {"shared": "shared~important"}
    snapshot = SplitJsonBackend(root).snapshot()
    assert snapshot.revision == 1
    assert snapshot.data("subscriptions.requests")["items"] == user["service_requests"]
    assert snapshot.data("monitoring.tls_state") == important["tls_certificates"]
    assert (root / PTB_TARGET_FILE).read_bytes() == original_ptb

    backup = Path(report.backup_path or "")
    assert backup.is_dir()
    assert (backup / "user_data.json").read_bytes() == (root / "user_data.json").read_bytes()
    assert (backup / "important_data.json").read_bytes() == (root / "important_data.json").read_bytes()
    assert (backup / "ptb_persistence").read_bytes() == original_ptb
    assert (backup / "important_data.fail2ban_state.local.json").is_file()
    assert (backup / BACKUP_MANIFEST_FILE).is_file()
    assert not any(
        path.name.startswith("important_data.fail2ban_state")
        for spec in STORE_SPECS.values()
        for path in [(root / spec.relative_path)]
    )


def test_successful_migration_is_idempotent(tmp_path: Path) -> None:
    root = tmp_path / "data"
    write_v4_source(root)
    first = migrate_v4_to_split(root, backup_root=tmp_path / "backups")
    revision = SplitJsonBackend(root).snapshot().revision

    second = migrate_v4_to_split(root, backup_root=tmp_path / "backups")

    assert second.already_migrated is True
    assert second.source_fingerprint == first.source_fingerprint
    assert second.backup_path == first.backup_path
    assert SplitJsonBackend(root).snapshot().revision == revision


def test_migration_recovers_crash_and_becomes_idempotent(tmp_path: Path) -> None:
    root = tmp_path / "data"
    write_v4_source(root)

    class Crash(BaseException):
        pass

    def crash(name: str) -> None:
        if name == "after_install:subscriptions/accounts.json":
            raise Crash

    with pytest.raises(Crash):
        migrate_v4_to_split(root, backup_root=tmp_path / "backups", failpoint=crash)

    report = migrate_v4_to_split(root, backup_root=tmp_path / "backups")
    assert report.already_migrated is True
    assert SplitJsonBackend(root).snapshot().data("users.profiles")["42"]["user_id"] == 42


def test_changed_monolithic_source_after_migration_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "data"
    write_v4_source(root)
    migrate_v4_to_split(root, backup_root=tmp_path / "backups")
    user = json.loads((root / "user_data.json").read_text(encoding="utf-8"))
    user["request_seq"] += 1
    (root / "user_data.json").write_text(json.dumps(user), encoding="utf-8")

    with pytest.raises(StorageConflictError, match="changed after split migration"):
        migrate_v4_to_split(root, backup_root=tmp_path / "backups")


def test_migrator_rejects_any_schema_except_exact_v4(tmp_path: Path) -> None:
    root = tmp_path / "data"
    write_v4_source(root)
    user = json.loads((root / "user_data.json").read_text(encoding="utf-8"))
    user["schema_version"] = 3
    (root / "user_data.json").write_text(json.dumps(user), encoding="utf-8")

    with pytest.raises(MigrationError, match="exactly 4"):
        migrate_v4_to_split(root, dry_run=True)
    assert not (root / "storage_layout.json").exists()


def test_unknown_user_field_is_rejected_instead_of_dropped(tmp_path: Path) -> None:
    root = tmp_path / "data"
    write_v4_source(root)
    user = json.loads((root / "user_data.json").read_text(encoding="utf-8"))
    user["authorized_users"]["42"]["future_field"] = "must-not-be-lost"
    (root / "user_data.json").write_text(json.dumps(user), encoding="utf-8")

    with pytest.raises(MigrationError, match="unknown fields"):
        migrate_v4_to_split(root, dry_run=True)


def test_inconsistent_derived_enabled_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "data"
    write_v4_source(root)
    user = json.loads((root / "user_data.json").read_text(encoding="utf-8"))
    user["authorized_users"]["42"]["enabled"] = False
    (root / "user_data.json").write_text(json.dumps(user), encoding="utf-8")

    with pytest.raises(MigrationError, match="inconsistent with access_state"):
        migrate_v4_to_split(root, dry_run=True)


def test_multiple_service_owners_are_rejected_by_migration(tmp_path: Path) -> None:
    root = tmp_path / "data"
    write_v4_source(root)
    user = json.loads((root / "user_data.json").read_text(encoding="utf-8"))
    first = user["authorized_users"]["42"]
    first.update({"role": "admin", "admin_level": "owner", "access_state": "approved", "enabled": True})
    second = dict(first)
    second["user_id"] = 43
    user["authorized_users"]["43"] = second
    (root / "user_data.json").write_text(json.dumps(user), encoding="utf-8")

    with pytest.raises(MigrationError, match="multiple service owners"):
        migrate_v4_to_split(root, dry_run=True)


def test_duplicate_json_key_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _, important = write_v4_source(root)
    (root / "user_data.json").write_text(
        '{"schema_version":4,"schema_version":4}',
        encoding="utf-8",
    )

    with pytest.raises(SchemaError, match="duplicate JSON key"):
        migrate_v4_to_split(root, dry_run=True)
    assert important["schema_version"] == 4


def test_partial_split_target_is_a_hard_conflict(tmp_path: Path) -> None:
    root = tmp_path / "data"
    write_v4_source(root)
    target = root / STORE_SPECS["users.profiles"].relative_path
    target.parent.mkdir(parents=True)
    target.write_text("{}\n", encoding="utf-8")

    with pytest.raises(StorageConflictError, match="targets already exist"):
        migrate_v4_to_split(root, dry_run=True)


def test_transform_does_not_mutate_source_objects(tmp_path: Path) -> None:
    root = tmp_path / "data"
    user, important = write_v4_source(root)
    original_user = clone_payload(user)
    original_important = clone_payload(important)
    source = load_v4_source(root)

    transform_v4(source)

    assert source.user_data == original_user
    assert source.important_data == original_important


def _tree_hashes(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file()
    }
