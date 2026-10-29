from __future__ import annotations

import asyncio
import contextlib
import multiprocessing
import os
import sqlite3
import threading
from pathlib import Path

import pytest

from app.persistence.errors import CommittedTransactionError, SchemaError, StorageConflictError
from app.persistence.layout import STORE_SPECS, default_store_data
from app.persistence.sqlite_backend import SQLiteBackend


def test_construction_and_missing_inspection_create_no_files(tmp_path: Path) -> None:
    root = tmp_path / "missing"
    backend = SQLiteBackend(root)
    assert not backend.exists()
    with pytest.raises(StorageConflictError, match="does not exist"):
        backend.inspect()
    assert not root.exists()


def test_bootstrap_creates_only_database_and_repeated_bootstrap_preserves_it(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path)
    before = backend.bootstrap()
    assert before.revision == 1
    assert {name: before.data(name) for name in STORE_SPECS} == default_store_data()
    assert not list(tmp_path.rglob("*.json"))
    with pytest.raises(StorageConflictError, match="already exists"):
        backend.bootstrap()
    assert backend.snapshot() == before


def test_invalid_bootstrap_leaves_no_database(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path)
    stores = default_store_data()
    stores["users.profiles"] = {"1": {"bad": {1, 2}}}
    with pytest.raises(SchemaError, match="non-JSON"):
        backend.bootstrap(stores=stores)
    assert not backend.exists()


def test_bootstrap_preserves_explicit_migration_metadata_and_alternate_path(tmp_path: Path) -> None:
    target = tmp_path / ".migration.sqlite3"
    backend = SQLiteBackend(tmp_path, database_path=target)
    result = backend.bootstrap(migration={"source": "split-json"}, transaction_id="migration-1")
    assert backend.database_path == target
    assert result.migration == {"source": "split-json"}
    assert result.transaction_id == "migration-1"


def test_multi_entity_unit_of_work_is_one_revision_and_uses_relational_rows(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path)
    backend.bootstrap()
    with backend.unit_of_work() as uow:
        uow.profiles.put(42, {"user_id": 42, "username": "example"})
        uow.access.put(42, {"role": "user", "access_state": "approved"})
        request_id = uow.service_requests.allocate_id()
        uow.service_requests.put(request_id, {"id": request_id, "user_id": 42, "status": "pending"})
        ticket_id = uow.tickets.allocate_id()
        uow.tickets.put(ticket_id, {"id": ticket_id, "user_id": 42, "status": "open"})
        uow.ticket_messages.put(ticket_id, [{"text": "Первое"}, {"text": "Второе"}])
        uow.ticket_messages.put("empty", [])
        uow.audit.append({"action": "created"})
        uow.billing_settings.replace({"standard_price_rub": 300})
        uow.outbox.put(
            "event-1",
            {
                "id": "event-1",
                "origin": "user",
                "recipients": {
                    "42": {"status": "pending", "attempts": 2, "next_attempt_at": "2026-01-01"},
                    "43": {"status": "delivered", "attempts": 1},
                },
            },
        )
        uow.commit()
    result = SQLiteBackend(tmp_path).inspect()
    assert result.revision == 2
    assert result.stores["users.profiles"].revision == 2
    assert result.stores["monitoring.dns_cache"].revision == 1
    assert result.data("support.ticket_messages") == {"1": [{"text": "Первое"}, {"text": "Второе"}], "empty": []}
    assert result.data("subscriptions.requests")["next_id"] == 1
    assert result.data("subscriptions.billing_settings") == {"standard_price_rub": 300}
    assert result.data("messaging.outbox")["event-1"]["recipients"]["42"]["attempts"] == 2
    with sqlite3.connect(backend.database_path) as connection:
        assert connection.execute("SELECT user_id, status FROM subscription_requests").fetchall() == [("42", "pending")]
        assert connection.execute("SELECT COUNT(*) FROM ticket_messages").fetchone() == (2,)
        assert connection.execute("SELECT COUNT(*) FROM outbox_recipients").fetchone() == (2,)
        assert '"recipients":{}' in connection.execute("SELECT payload FROM outbox_events").fetchone()[0]


def test_noop_rollback_and_stale_revision(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path)
    backend.bootstrap()
    with backend.unit_of_work() as uow:
        uow.profiles.put(42, {"user_id": 42})
    assert backend.snapshot().revision == 1
    assert backend.commit(base_revision=1, changes={}).revision == 1
    first = backend.snapshot()
    backend.commit(base_revision=1, changes={"users.profiles": {"42": {"user_id": 42}}})
    with pytest.raises(StorageConflictError, match="stale storage revision"):
        backend.commit(base_revision=first.revision, changes={"access.grants": {"42": {"role": "admin"}}})
    assert backend.snapshot().data("access.grants") == {}


def test_failure_mid_transaction_rolls_back_all_tables(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path)
    before = backend.bootstrap()

    def fail(name: str) -> None:
        if name == "after_store:messaging.outbox":
            raise OSError("injected failure")

    failing = SQLiteBackend(tmp_path, failpoint=fail)
    with pytest.raises(OSError, match="injected"):
        failing.commit(
            base_revision=1,
            changes={
                "users.profiles": {"42": {"user_id": 42}},
                "messaging.outbox": {"event": {"id": "event", "recipients": {"42": {"status": "pending"}}}},
            },
        )
    assert backend.snapshot() == before


def test_failure_after_commit_reports_non_retryable_outcome(tmp_path: Path) -> None:
    SQLiteBackend(tmp_path).bootstrap()

    def fail(name: str) -> None:
        if name == "after_commit":
            raise OSError("lost response")

    backend = SQLiteBackend(tmp_path, failpoint=fail)
    with backend.unit_of_work() as uow:
        uow.profiles.put(42, {"user_id": 42})
        with pytest.raises(CommittedTransactionError) as captured:
            uow.commit()
        with pytest.raises(RuntimeError, match="committed Unit of Work"):
            uow.rollback()
    result = backend.snapshot()
    assert result.revision == 2
    assert result.transaction_id == captured.value.transaction_id


def test_deleting_event_cascades_recipient_state_and_histories_update(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path)
    backend.bootstrap()
    backend.commit(
        base_revision=1,
        changes={
            "messaging.outbox": {"a": {"recipients": {"1": {"status": "pending"}}}},
            "support.ticket_messages": {"1": [{"text": "a"}], "2": []},
        },
    )
    backend.commit(
        base_revision=2,
        changes={
            "messaging.outbox": {},
            "support.ticket_messages": {"1": [{"text": "edited"}]},
        },
    )
    assert backend.snapshot().data("support.ticket_messages") == {"1": [{"text": "edited"}]}
    with sqlite3.connect(backend.database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM outbox_recipients").fetchone() == (0,)
        assert connection.execute("SELECT ticket_key FROM ticket_histories").fetchall() == [("1",)]


def test_invalid_update_rejects_entire_transaction(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path)
    before = backend.bootstrap()
    with pytest.raises(SchemaError, match="non-finite"):
        backend.commit(base_revision=1, changes={"users.profiles": {"42": {"cpu": float("nan")}}})
    with pytest.raises(SchemaError, match="unknown stores"):
        backend.commit(base_revision=1, changes={"unknown": {}})
    assert backend.snapshot() == before


def test_inspection_is_read_only_and_rejects_unknown_schema(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path)
    backend.bootstrap()
    before = {path.name: path.read_bytes() for path in tmp_path.iterdir() if path.is_file()}
    backend.inspect()
    backend.verify_recovery()
    assert {path.name: path.read_bytes() for path in tmp_path.iterdir() if path.is_file()} == before
    with sqlite3.connect(backend.database_path) as connection:
        connection.execute("UPDATE storage_metadata SET schema_version=99")
    with pytest.raises(SchemaError, match="unsupported"):
        backend.snapshot()


def test_writer_lock_wait_is_bounded(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path, busy_timeout=0.01)
    backend.bootstrap()
    with sqlite3.connect(backend.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            backend.commit(base_revision=1, changes={"users.profiles": {"1": {}}})


@pytest.mark.asyncio
async def test_cancelled_async_commit_waits_for_atomic_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend = SQLiteBackend(tmp_path)
    backend.bootstrap()
    original = backend.commit
    started = threading.Event()
    release = threading.Event()

    def delayed_commit(**kwargs):
        started.set()
        assert release.wait(timeout=5)
        return original(**kwargs)

    monkeypatch.setattr(backend, "commit", delayed_commit)
    async with backend.unit_of_work() as uow:
        uow.profiles.put(42, {"user_id": 42})
        task = asyncio.create_task(uow.commit_async())
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert uow.current_snapshot.revision == 2
    assert backend.snapshot().data("users.profiles") == {"42": {"user_id": 42}}


def _crash_before_commit(root: str) -> None:
    def crash(name: str) -> None:
        if name == "before_commit":
            os._exit(23)

    backend = SQLiteBackend(root, failpoint=crash)
    backend.commit(
        base_revision=1,
        changes={
            "users.profiles": {str(index): {"blob": "x" * 10000} for index in range(500)},
            "messaging.outbox": {"event": {"recipients": {"42": {"status": "pending"}}}},
        },
    )


def test_process_crash_recovers_previous_complete_revision(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path)
    before = backend.bootstrap()
    process = multiprocessing.get_context("spawn").Process(target=_crash_before_commit, args=(str(tmp_path),))
    process.start()
    process.join(timeout=15)
    try:
        assert process.exitcode == 23
    finally:
        if process.is_alive():
            process.kill()
            process.join(timeout=5)
    # Preflight may reject a hot journal, but must never recover it or write.
    damaged = {path.name: path.read_bytes() for path in tmp_path.iterdir() if path.is_file()}
    with contextlib.suppress(SchemaError, sqlite3.DatabaseError):
        backend.inspect()
    assert {path.name: path.read_bytes() for path in tmp_path.iterdir() if path.is_file()} == damaged
    assert backend.snapshot() == before
