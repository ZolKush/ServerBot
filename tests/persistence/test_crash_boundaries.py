from pathlib import Path

import pytest

from app.config.checks import _check_split_storage
from app.persistence import SplitJsonBackend, io, transaction
from app.persistence.layout import TRANSACTIONS_DIR


class SimulatedCrash(BaseException):
    pass


def test_partial_prepared_journal_is_unpublished_debris(tmp_path, monkeypatch):
    backend = SplitJsonBackend(tmp_path / "data")
    backend.bootstrap()
    original_write = io.write_bytes_durable

    def interrupted_write(path, payload, *, exclusive=False):
        if "journal.json" in path.name:
            path.write_bytes(b'{"schema_version":')
            raise SimulatedCrash
        return original_write(path, payload, exclusive=exclusive)

    with monkeypatch.context() as crash:
        crash.setattr(io, "write_bytes_durable", interrupted_write)
        crash.setattr(transaction, "write_bytes_durable", interrupted_write)
        # A real process crash cannot execute the coordinator's finally block.
        crash.setattr(transaction, "_remove_transaction_tree", lambda _path: None)
        with backend.unit_of_work() as uow:
            uow.profiles.put(42, {"user_id": 42})
            with pytest.raises(SimulatedCrash):
                uow.commit()

    recovered = SplitJsonBackend(backend.data_root).snapshot()
    assert recovered.revision == 1
    assert recovered.data("users.profiles") == {}


def test_transaction_cleanup_syncs_the_parent_directory(tmp_path, monkeypatch):
    transaction_dir = tmp_path / TRANSACTIONS_DIR / "abc123"
    transaction_dir.mkdir(parents=True)
    (transaction_dir / "journal.json").write_text("{}", encoding="utf-8")
    synced = []
    monkeypatch.setattr(transaction, "fsync_directory", synced.append, raising=False)

    transaction._remove_transaction_tree(transaction_dir)

    assert not transaction_dir.exists()
    assert transaction_dir.parent in synced


def test_preflight_allows_recoverable_bootstrap_without_manifest(tmp_path: Path):
    def crash(name):
        if name == "after_install:users/profiles.json":
            raise SimulatedCrash

    with pytest.raises(SimulatedCrash):
        SplitJsonBackend(tmp_path, failpoint=crash).bootstrap()
    assert not (tmp_path / "storage_layout.json").exists()

    assert _check_split_storage(str(tmp_path)) == []
    assert not (tmp_path / "storage_layout.json").exists()  # Preflight is read-only.
    assert SplitJsonBackend(tmp_path).snapshot().revision == 1
