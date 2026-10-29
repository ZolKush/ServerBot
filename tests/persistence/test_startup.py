from __future__ import annotations

import json
import os
import pickle
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from app.persistence import PersistenceError, SplitJsonBackend, SQLiteBackend, StorageConflictError
from app.persistence.startup import prepare_storage
from app.runtime.lock import InstanceAlreadyRunning, SingleInstanceLock
from tests.persistence.fixtures import write_v4_source


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


def _write_pickle(path: Path) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _pickle()
    path.write_bytes(payload)
    return payload


def test_fresh_container_storage_is_initialized_once(tmp_path: Path) -> None:
    (tmp_path / "conf").mkdir()
    (tmp_path / "conf/bot.json").write_text("{}", encoding="utf-8")

    first = prepare_storage(tmp_path)
    second = prepare_storage(tmp_path)

    assert first["action"] == "initialized"
    assert second["action"] == "existing"
    assert first["revision"] == second["revision"] == 1
    assert SQLiteBackend(tmp_path).inspect().data("users.profiles") == {}
    assert not (tmp_path / "sqlite-backups").exists()


def test_split_source_and_dialogs_migrate_once_without_reimport_on_restart(tmp_path: Path) -> None:
    SplitJsonBackend(tmp_path).bootstrap()
    source_path = tmp_path / "users/profiles.json"
    source_bytes = source_path.read_bytes()
    pickle_path = tmp_path / "telegram/persistence.pickle"
    pickle_bytes = _write_pickle(pickle_path)

    first = prepare_storage(tmp_path)

    assert first["action"] == "migrated"
    assert first["telegram_imported"] is True
    assert source_path.read_bytes() == source_bytes
    assert pickle_path.read_bytes() == pickle_bytes
    assert (Path(first["backup_path"]) / "telegram/persistence.pickle").read_bytes() == pickle_bytes
    with sqlite3.connect(first["database"]) as connection:
        assert connection.execute("SELECT COUNT(*) FROM telegram_conversations").fetchone() == (1,)
    with SQLiteBackend(tmp_path).unit_of_work() as uow:
        uow.profiles.put(42, {"user_id": 42, "nickname": "New database value"})
        uow.access.put(42, {"role": "user", "access_state": "approved"})
        uow.subscriptions.put(42, {"service_tier": "basic"})
        uow.commit()
    source_path.write_text("broken old source", encoding="utf-8")
    pickle_path.write_bytes(b"broken old pickle")

    restarted = prepare_storage(tmp_path, ptb_path="missing-obsolete-pickle")

    assert restarted["action"] == "existing"
    assert SQLiteBackend(tmp_path).inspect().data("users.profiles")["42"]["nickname"] == "New database value"


def test_monolithic_source_and_dialogs_migrate_directly_on_startup(tmp_path: Path) -> None:
    root = tmp_path / "data"
    write_v4_source(root)
    _write_pickle(root / "ptb_persistence")

    report = prepare_storage(root)

    assert report["action"] == "migrated"
    assert report["source_format"] == "monolithic-json-v4"
    assert report["telegram_imported"] is True
    assert SQLiteBackend(root).inspect().data("users.profiles")["42"]["nickname"] == "Example"


def test_prepared_pickle_is_selected_when_both_legacy_primaries_exist(tmp_path: Path) -> None:
    SplitJsonBackend(tmp_path).bootstrap()
    _write_pickle(tmp_path / "telegram/persistence.pickle")
    _write_pickle(tmp_path / "ptb_persistence")
    prepared_bytes = _write_pickle(tmp_path / "telegram-migration.pickle")

    report = prepare_storage(tmp_path)

    assert report["action"] == "migrated"
    assert report["telegram_imported"] is True
    assert (Path(report["backup_path"]) / "telegram-migration.pickle").read_bytes() == prepared_bytes


def test_explicit_pickle_still_wins_over_the_prepared_pickle(tmp_path: Path) -> None:
    SplitJsonBackend(tmp_path).bootstrap()
    (tmp_path / "telegram-migration.pickle").write_bytes(b"obsolete prepared state")
    explicit_bytes = _write_pickle(tmp_path / "selected-state.pickle")

    report = prepare_storage(tmp_path, ptb_path="selected-state.pickle")

    assert report["telegram_imported"] is True
    assert (Path(report["backup_path"]) / "selected-state.pickle").read_bytes() == explicit_bytes


@pytest.mark.parametrize("source_name", ["users/profiles.json", "telegram/persistence.pickle", "ptb_persistence.bak"])
def test_partial_legacy_source_does_not_become_a_fresh_database(tmp_path: Path, source_name: str) -> None:
    source = tmp_path / source_name
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(b"partial")

    with pytest.raises(StorageConflictError, match="legacy data found"):
        prepare_storage(tmp_path)

    assert source.read_bytes() == b"partial"
    assert not SQLiteBackend(tmp_path).exists()


def test_bad_pickle_and_pending_journal_stop_startup_without_a_database(tmp_path: Path) -> None:
    SplitJsonBackend(tmp_path).bootstrap()
    path = tmp_path / "telegram/persistence.pickle"
    _write_pickle(path)
    path.write_bytes(b"invalid")
    with pytest.raises(ValueError, match="valid PTB state"):
        prepare_storage(tmp_path)
    assert not SQLiteBackend(tmp_path).exists()

    (tmp_path / ".transactions/pending").mkdir(parents=True)
    with pytest.raises(StorageConflictError, match="pending JSON transaction"):
        prepare_storage(tmp_path)
    assert not SQLiteBackend(tmp_path).exists()


def test_corrupt_sqlite_is_not_replaced_by_legacy_data(tmp_path: Path) -> None:
    SplitJsonBackend(tmp_path).bootstrap()
    database = SQLiteBackend(tmp_path).database_path
    database.write_bytes(b"invalid SQLite")

    with pytest.raises((PersistenceError, sqlite3.DatabaseError)):
        prepare_storage(tmp_path)

    assert database.read_bytes() == b"invalid SQLite"


@pytest.mark.parametrize("legacy", [False, True])
def test_startup_write_paths_respect_the_process_lock(tmp_path: Path, legacy: bool) -> None:
    if legacy:
        SplitJsonBackend(tmp_path).bootstrap()
    with SingleInstanceLock(tmp_path / "runtime/instance.lock"), pytest.raises(InstanceAlreadyRunning):
        prepare_storage(tmp_path)
    assert not SQLiteBackend(tmp_path).exists()


def test_container_startup_cli_accepts_relative_pickle_environment_path(tmp_path: Path) -> None:
    SplitJsonBackend(tmp_path).bootstrap()
    _write_pickle(tmp_path / "custom-state.pickle")
    environment = os.environ.copy()
    environment.update({"MAINTBOT_PTB_PICKLE": "custom-state.pickle", "PYTHONUTF8": "1"})

    result = subprocess.run(
        [sys.executable, "-m", "app.persistence.startup", "--data-dir", str(tmp_path)],
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
        timeout=20,
    )

    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["action"] == "migrated"
    assert report["telegram_imported"] is True


def test_explicit_pickle_without_application_source_is_not_silently_ignored(tmp_path: Path) -> None:
    _write_pickle(tmp_path / "custom-state.pickle")
    with pytest.raises(PersistenceError, match="both user_data.json and important_data.json"):
        prepare_storage(tmp_path, ptb_path="custom-state.pickle")
    assert not SQLiteBackend(tmp_path).exists()
