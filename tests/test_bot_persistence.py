from __future__ import annotations

from pathlib import Path

import pytest
from telegram.ext import ExtBot

from app.bot import persistence as persistence_module
from app.bot.persistence import AtomicPicklePersistence, build_atomic_persistence

_TEST_TOKEN = "123456:TEST_TOKEN_NOT_USED_BY_TESTS_ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def _persistence(path: Path) -> AtomicPicklePersistence:
    persistence = build_atomic_persistence(path)
    persistence.set_bot(ExtBot(_TEST_TOKEN))
    return persistence


@pytest.mark.asyncio
async def test_atomic_persistence_recovers_previous_snapshot_after_interrupted_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "telegram" / "persistence.pickle"
    persistence = _persistence(path)
    await persistence.update_bot_data({"version": 1})

    real_replace = persistence_module._replace_file
    injected = False

    def fail_new_image_once(source: Path, destination: Path) -> None:
        nonlocal injected
        if not injected and destination == path and source.suffix == ".tmp":
            injected = True
            raise OSError("injected replacement failure")
        real_replace(source, destination)

    monkeypatch.setattr(persistence_module, "_replace_file", fail_new_image_once)

    with pytest.raises(OSError, match="injected replacement failure"):
        await persistence.update_bot_data({"version": 2})

    restarted = _persistence(path)
    assert await restarted.get_bot_data() == {"version": 1}
    assert not list(path.parent.glob(f".{path.name}.*.tmp"))


@pytest.mark.asyncio
async def test_atomic_persistence_restores_backup_when_primary_is_corrupt(tmp_path: Path) -> None:
    path = tmp_path / "telegram" / "persistence.pickle"
    persistence = _persistence(path)
    await persistence.update_bot_data({"version": 1})
    await persistence.update_bot_data({"version": 2})

    backup = path.with_name(path.name + ".bak")
    assert backup.is_file()
    path.write_bytes(b"interrupted pickle")

    restarted = _persistence(path)
    assert await restarted.get_bot_data() == {"version": 1}

    restarted_again = _persistence(path)
    assert await restarted_again.get_bot_data() == {"version": 1}
