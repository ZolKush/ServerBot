from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path

import pytest

from app.persistence import RecoveryError, SchemaError, SplitJsonBackend, StorageConflictError
from app.persistence.layout import STORE_SPECS, TRANSACTIONS_DIR


class SimulatedCrash(BaseException):
    pass


def test_bootstrap_creates_valid_complete_layout(tmp_path: Path) -> None:
    backend = SplitJsonBackend(tmp_path / "data")

    snapshot = backend.bootstrap()

    assert snapshot.revision == 1
    assert set(snapshot.stores) == set(STORE_SPECS)
    assert all(store.revision == 1 for store in snapshot.stores.values())
    assert backend.snapshot() == snapshot
    manifest = json.loads((backend.layout_path).read_text(encoding="utf-8"))
    assert manifest["layout"] == "split-json"
    assert manifest["revision"] == 1
    assert set(manifest["stores"]) == set(STORE_SPECS)


def test_unit_of_work_commits_multiple_stores_once(tmp_path: Path) -> None:
    backend = SplitJsonBackend(tmp_path / "data")
    backend.bootstrap()

    with backend.unit_of_work() as uow:
        uow.profiles.put(42, {"user_id": 42, "username": "example"})
        uow.access.put(42, {"access_state": "approved"})
        request_id = uow.service_requests.allocate_id()
        uow.service_requests.put(request_id, {"id": request_id, "user_id": 42})
        uow.audit.append({"action": "created", "target_user_id": 42})
        uow.commit()

    snapshot = backend.snapshot()
    assert snapshot.revision == 2
    assert snapshot.data("users.profiles")["42"]["user_id"] == 42
    assert snapshot.data("access.grants")["42"]["access_state"] == "approved"
    assert snapshot.data("subscriptions.requests") == {
        "next_id": 1,
        "items": {"1": {"id": 1, "user_id": 42}},
    }
    assert snapshot.data("audit.events") == [{"action": "created", "target_user_id": 42}]
    assert snapshot.stores["users.profiles"].revision == 2
    assert snapshot.stores["monitoring.dns_cache"].revision == 1


def test_unit_of_work_without_commit_rolls_back(tmp_path: Path) -> None:
    backend = SplitJsonBackend(tmp_path / "data")
    backend.bootstrap()

    with backend.unit_of_work() as uow:
        uow.profiles.put(42, {"user_id": 42})

    assert backend.snapshot().data("users.profiles") == {}
    assert backend.snapshot().revision == 1


@pytest.mark.asyncio
async def test_cancelled_async_commit_finishes_atomically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = SplitJsonBackend(tmp_path / "data")
    backend.bootstrap()
    original_commit = backend.commit
    started = threading.Event()
    release = threading.Event()

    def delayed_commit(**kwargs):
        started.set()
        assert release.wait(timeout=5)
        return original_commit(**kwargs)

    monkeypatch.setattr(backend, "commit", delayed_commit)
    async with backend.unit_of_work() as uow:
        uow.profiles.put(42, {"user_id": 42})
        task = asyncio.create_task(uow.commit_async())
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert backend.snapshot().data("users.profiles") == {"42": {"user_id": 42}}


def test_stale_unit_of_work_is_rejected(tmp_path: Path) -> None:
    backend = SplitJsonBackend(tmp_path / "data")
    backend.bootstrap()
    first = backend.unit_of_work()
    second = backend.unit_of_work()
    first.__enter__()
    second.__enter__()
    try:
        first.profiles.put(1, {"user_id": 1})
        first.commit()
        second.profiles.put(2, {"user_id": 2})
        with pytest.raises(StorageConflictError, match="stale storage revision"):
            second.commit()
    finally:
        first.__exit__(None, None, None)
        second.__exit__(None, None, None)


def test_recovery_finishes_bootstrap_after_process_crash(tmp_path: Path) -> None:
    root = tmp_path / "data"

    def crash(name: str) -> None:
        if name == "after_install:users/profiles.json":
            raise SimulatedCrash

    with pytest.raises(SimulatedCrash):
        SplitJsonBackend(root, failpoint=crash).bootstrap()

    assert not (root / "storage_layout.json").exists()
    recovered = SplitJsonBackend(root).snapshot()
    assert recovered.revision == 1
    assert set(recovered.stores) == set(STORE_SPECS)
    assert not (root / TRANSACTIONS_DIR).exists()


def test_recovery_finishes_multi_store_commit_after_crash(tmp_path: Path) -> None:
    root = tmp_path / "data"
    normal = SplitJsonBackend(root)
    normal.bootstrap()

    def crash(name: str) -> None:
        if name == "after_install:messaging/outbox.json":
            raise SimulatedCrash

    crashing = SplitJsonBackend(root, failpoint=crash)
    with crashing.unit_of_work() as uow:
        uow.profiles.put(42, {"user_id": 42})
        uow.outbox.put("event", {"id": "event"})
        with pytest.raises(SimulatedCrash):
            uow.commit()

    recovered = SplitJsonBackend(root).snapshot()
    assert recovered.revision == 2
    assert recovered.data("users.profiles") == {"42": {"user_id": 42}}
    assert recovered.data("messaging.outbox") == {"event": {"id": "event"}}


def test_recovery_refuses_missing_staging_and_target(tmp_path: Path) -> None:
    root = tmp_path / "data"

    def crash(name: str) -> None:
        if name == "after_prepare":
            raise SimulatedCrash

    with pytest.raises(SimulatedCrash):
        SplitJsonBackend(root, failpoint=crash).bootstrap()

    transactions = list((root / TRANSACTIONS_DIR).iterdir())
    assert len(transactions) == 1
    missing = transactions[0] / "staged" / "users" / "profiles.json"
    missing.unlink()
    with pytest.raises(RecoveryError, match="neither target nor staging"):
        SplitJsonBackend(root).recover()


def test_recovery_removes_only_unprepared_transaction_debris(tmp_path: Path) -> None:
    root = tmp_path / "data"
    backend = SplitJsonBackend(root)
    backend.bootstrap()
    debris = root / TRANSACTIONS_DIR / "orphan" / "staged"
    debris.mkdir(parents=True)
    (debris / "unused").write_text("partial", encoding="utf-8")

    snapshot = backend.snapshot()

    assert snapshot.revision == 1
    assert not (root / TRANSACTIONS_DIR).exists()


def test_checksum_tampering_is_detected(tmp_path: Path) -> None:
    backend = SplitJsonBackend(tmp_path / "data")
    backend.bootstrap()
    path = backend.data_root / STORE_SPECS["users.profiles"].relative_path
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["data"]["42"] = {"user_id": 42}
    path.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(SchemaError, match="checksum differs"):
        backend.snapshot()


def test_bootstrap_refuses_partial_target(tmp_path: Path) -> None:
    root = tmp_path / "data"
    target = root / STORE_SPECS["users.profiles"].relative_path
    target.parent.mkdir(parents=True)
    target.write_text("{}\n", encoding="utf-8")

    with pytest.raises(StorageConflictError, match="targets already exist"):
        SplitJsonBackend(root).bootstrap()


def test_bootstrap_refuses_extra_file_overlapping_store(tmp_path: Path) -> None:
    root = tmp_path / "data"
    profiles_path = STORE_SPECS["users.profiles"].relative_path

    with pytest.raises(SchemaError, match="overlap reserved"):
        SplitJsonBackend(root).bootstrap(extra_files={profiles_path: b"not-a-store"})


def test_commit_refuses_non_json_values_before_writing(tmp_path: Path) -> None:
    backend = SplitJsonBackend(tmp_path / "data")
    backend.bootstrap()

    with backend.unit_of_work() as uow:
        uow.profiles.put(42, {"bad": {1, 2, 3}})
        with pytest.raises(SchemaError, match="non-JSON value"):
            uow.commit()

    assert backend.snapshot().revision == 1
