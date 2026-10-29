from __future__ import annotations

import pickle
import sqlite3
from pathlib import Path

import pytest

from app.persistence import MigrationError, SchemaError, SplitJsonBackend, SQLiteBackend, StorageConflictError
from app.persistence.layout import default_store_data
from app.persistence.migrate_sqlite import migrate_to_sqlite
from app.runtime.lock import InstanceAlreadyRunning, SingleInstanceLock
from tests.persistence.fixtures import write_v4_source


def _files(root: Path) -> dict[str, bytes]:
    return {path.relative_to(root).as_posix(): path.read_bytes() for path in root.rglob("*") if path.is_file()}


def _pickle() -> bytes:
    return pickle.dumps(
        {
            "user_data": {42: {"pending_reply": 3}},
            "chat_data": {},
            "bot_data": {},
            "callback_data": None,
            "conversations": {"support": {(42, 42): 8}},
        }
    )


def test_split_sqlite_migration_dry_run_backup_restart_and_idempotence(tmp_path: Path) -> None:
    root = tmp_path / "data"
    stores = default_store_data()
    stores["users.profiles"] = {"42": {"user_id": 42, "nickname": "Example"}}
    stores["access.grants"] = {"42": {"role": "user", "access_state": "approved"}}
    stores["subscriptions.accounts"] = {"42": {"service_tier": "basic"}}
    stores["support.tickets"] = {"next_id": 7, "items": {"7": {"id": 7, "user_id": 42}}}
    SplitJsonBackend(root).bootstrap(stores=stores)
    ptb = root / "telegram/persistence.pickle"
    ptb.parent.mkdir()
    ptb.write_bytes(_pickle())
    before = _files(root)

    dry_run = migrate_to_sqlite(root, dry_run=True)
    assert dry_run.dry_run and dry_run.telegram_imported
    assert _files(root) == before

    report = migrate_to_sqlite(root)
    assert not report.already_migrated
    snapshot = SQLiteBackend(root).inspect()
    assert {name: snapshot.data(name) for name in stores} == stores
    for name, payload in before.items():
        if name != "runtime/state.lock":
            assert (root / name).read_bytes() == payload
            assert (Path(report.backup_path) / name).read_bytes() == payload
    with sqlite3.connect(report.database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM telegram_conversations").fetchone()[0] == 1
    with SQLiteBackend(root).unit_of_work() as uow:
        uow.profiles.put(43, {"user_id": 43})
        uow.access.put(43, {"role": "user", "access_state": "approved"})
        uow.subscriptions.put(43, {"service_tier": "basic"})
        uow.commit()
    assert migrate_to_sqlite(root).already_migrated
    assert SQLiteBackend(root).inspect().data("users.profiles")["43"] == {"user_id": 43}


def test_monolithic_v4_import_goes_directly_to_sqlite(tmp_path: Path) -> None:
    root = tmp_path / "data"
    write_v4_source(root)
    (root / "ptb_persistence").write_bytes(_pickle())
    report = migrate_to_sqlite(root)
    assert report.source_format == "monolithic-json-v4"
    assert SQLiteBackend(root).inspect().data("users.profiles")["42"]["nickname"] == "Example"
    assert not (root / "storage_layout.json").exists()


def test_configured_external_telegram_pickle_is_backed_up_and_imported(tmp_path: Path) -> None:
    root = tmp_path / "data"
    SplitJsonBackend(root).bootstrap()
    ptb = tmp_path / "custom-telegram.pickle"
    ptb.write_bytes(_pickle())
    report = migrate_to_sqlite(root, ptb_path=ptb)
    assert report.telegram_imported
    assert (Path(report.backup_path) / "_external/telegram/persistence.pickle").read_bytes() == _pickle()
    assert ptb.read_bytes() == _pickle()


def test_invalid_pickle_never_publishes_an_application_only_database(tmp_path: Path) -> None:
    root = tmp_path / "data"
    SplitJsonBackend(root).bootstrap()
    path = root / "telegram/persistence.pickle"
    path.parent.mkdir()
    path.write_bytes(b"invalid")
    with pytest.raises(ValueError):
        migrate_to_sqlite(root)
    assert not SQLiteBackend(root).exists()


@pytest.mark.parametrize("dry_run", [True, False])
def test_incomplete_user_projections_are_rejected_before_backup(tmp_path: Path, dry_run: bool) -> None:
    stores = default_store_data()
    stores["users.profiles"] = {"42": {"user_id": 42}}
    SplitJsonBackend(tmp_path).bootstrap(stores=stores)
    with pytest.raises(SchemaError, match="access.grants.42"):
        migrate_to_sqlite(tmp_path, dry_run=dry_run)
    assert not SQLiteBackend(tmp_path).exists()
    assert not (tmp_path / "sqlite-backups").exists()


def test_sqlite_migration_respects_running_bot_lock(tmp_path: Path) -> None:
    SplitJsonBackend(tmp_path).bootstrap()
    with SingleInstanceLock(tmp_path / "runtime/instance.lock"), pytest.raises(InstanceAlreadyRunning):
        migrate_to_sqlite(tmp_path)
    assert not SQLiteBackend(tmp_path).exists()


def test_migration_refuses_fresh_unrelated_sqlite_database(tmp_path: Path) -> None:
    SQLiteBackend(tmp_path).bootstrap()
    with pytest.raises(StorageConflictError, match="not created by this migration"):
        migrate_to_sqlite(tmp_path)


def test_pending_legacy_bootstrap_is_not_bypassed_when_manifest_is_missing(tmp_path: Path) -> None:
    write_v4_source(tmp_path / "data")
    pending = tmp_path / "data/.transactions/pending"
    pending.mkdir(parents=True)
    with pytest.raises(StorageConflictError, match="pending JSON transaction"):
        migrate_to_sqlite(tmp_path / "data", dry_run=True)
    assert not SQLiteBackend(tmp_path / "data").exists()


def test_source_change_during_backup_does_not_publish(tmp_path: Path, monkeypatch) -> None:
    from app.persistence import migrate_sqlite

    SplitJsonBackend(tmp_path).bootstrap()
    original = migrate_sqlite.backup_source

    def change_source(root, source):
        backup = original(root, source)
        with SplitJsonBackend(root).unit_of_work() as uow:
            uow.profiles.put(42, {"user_id": 42})
            uow.commit()
        return backup

    monkeypatch.setattr(migrate_sqlite, "backup_source", change_source)
    with pytest.raises(MigrationError, match="source changed"):
        migrate_to_sqlite(tmp_path)
    assert not SQLiteBackend(tmp_path).exists()


def test_migration_refuses_a_source_directory_symlink(tmp_path: Path) -> None:
    root = tmp_path / "data"
    SplitJsonBackend(root).bootstrap()
    users = root / "users"
    outside = tmp_path / "outside-users"
    users.rename(outside)
    try:
        users.symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("directory symlinks require platform support")
    with pytest.raises(MigrationError, match="symbolic links"):
        migrate_to_sqlite(root, dry_run=True)
    assert not SQLiteBackend(root).exists()
