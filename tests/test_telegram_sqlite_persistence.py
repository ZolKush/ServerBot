from __future__ import annotations

import asyncio
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

import pytest
from telegram import User
from telegram.ext import ExtBot, PicklePersistence

from app.bot.persistence_import import import_legacy_pickle, load_legacy_pickle
from app.bot.persistence_sqlite import SQLitePersistence


def _persistence(path: Path, token: str = "123:LOCAL_ONLY") -> SQLitePersistence:
    persistence = SQLitePersistence(path)
    persistence.set_bot(ExtBot(token))
    return persistence


def test_constructor_has_no_filesystem_side_effects(tmp_path):
    target = tmp_path / "absent" / "maintbot.sqlite3"
    SQLitePersistence(target)
    assert not target.parent.exists()


@pytest.mark.asyncio
async def test_context_conversations_and_bot_references_survive_restart(tmp_path):
    target = tmp_path / "maintbot.sqlite3"
    persistence = _persistence(target)
    user = User(id=123, first_name="Пользователь", is_bot=False)
    user.set_bot(persistence.bot)
    now = datetime.now(timezone.utc)
    callback = ([("button", 1.0, {"id": (123, 456)})], {"query": "button"})

    await persistence.update_user_data(123, {"user": user, "at": now, "scope": ("server", 1)})
    await persistence.update_chat_data(-456, {"panel": 7})
    await persistence.update_bot_data({"navigation": {"123": [1, 2]}})
    await persistence.update_callback_data(callback)
    await persistence.update_conversation("purchase", (-456, 123), 0)
    await persistence.update_conversation("purchase", ("text", 123), "reply")
    await persistence.flush()

    restarted = _persistence(target, "456:NEW_BOT")
    users = await restarted.get_user_data()
    assert users[123]["at"] == now
    assert users[123]["scope"] == ("server", 1)
    assert users[123]["user"].get_bot() is restarted.bot
    assert await restarted.get_chat_data() == {-456: {"panel": 7}}
    assert await restarted.get_bot_data() == {"navigation": {"123": [1, 2]}}
    assert await restarted.get_callback_data() == callback
    assert await restarted.get_conversations("purchase") == {(-456, 123): 0, ("text", 123): "reply"}
    assert await restarted.get_conversations("missing") == {}

    # Reads return detached objects; unrelated rows survive another row's update.
    users[123]["scope"] = "changed"
    await restarted.update_user_data(456, {"other": True})
    assert (await restarted.get_user_data())[123]["scope"] == ("server", 1)
    with sqlite3.connect(target) as connection:
        assert connection.execute("SELECT count(*) FROM telegram_data WHERE kind='user'").fetchone() == (2,)


@pytest.mark.asyncio
async def test_deletion_and_zero_conversation_state_persist(tmp_path):
    target = tmp_path / "maintbot.sqlite3"
    persistence = _persistence(target)
    await persistence.update_user_data(1, {"active": True})
    await persistence.update_chat_data(2, {"active": True})
    await persistence.update_conversation("ticket", (2, 1), 0)
    await persistence.update_conversation("other", (2, 1), 3)
    assert await persistence.get_conversations("ticket") == {(2, 1): 0}
    await persistence.update_conversation("ticket", (2, 1), None)
    await persistence.drop_user_data(1)
    await persistence.drop_chat_data(2)
    restarted = _persistence(target)
    assert await restarted.get_user_data() == {}
    assert await restarted.get_chat_data() == {}
    assert await restarted.get_conversations("ticket") == {}
    assert await restarted.get_conversations("other") == {(2, 1): 3}


@pytest.mark.asyncio
async def test_failed_sql_update_preserves_previous_committed_row(tmp_path):
    target = tmp_path / "maintbot.sqlite3"
    persistence = _persistence(target)
    await persistence.update_user_data(1, {"value": "previous"})
    with sqlite3.connect(target) as connection:
        connection.execute(
            "CREATE TRIGGER reject_telegram_update BEFORE UPDATE ON telegram_data "
            "BEGIN SELECT RAISE(ABORT, 'test rollback'); END"
        )
    with pytest.raises(sqlite3.IntegrityError, match="test rollback"):
        await persistence.update_user_data(1, {"value": "new"})
    assert await _persistence(target).get_user_data() == {1: {"value": "previous"}}


@pytest.mark.asyncio
async def test_shutdown_cancellation_waits_for_executing_commit(tmp_path, monkeypatch):
    target = tmp_path / "maintbot.sqlite3"
    persistence = _persistence(target)
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    original = SQLitePersistence._write_data

    def delayed_write(self, *args):
        started.set()
        assert release.wait(5)
        original(self, *args)
        finished.set()

    monkeypatch.setattr(SQLitePersistence, "_write_data", delayed_write)
    task = asyncio.create_task(persistence.update_user_data(1, {"committed": True}))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()
    assert await _persistence(target).get_user_data() == {1: {"committed": True}}


@pytest.mark.asyncio
async def test_explicit_pickle_import_preserves_source_and_bot_placeholders(tmp_path):
    source = tmp_path / "legacy.pickle"
    legacy = PicklePersistence(source)
    legacy.set_bot(ExtBot("123:LEGACY"))
    user = User(id=1, first_name="Legacy", is_bot=False)
    user.set_bot(legacy.bot)
    await legacy.update_user_data(1, {"user": user})
    await legacy.update_chat_data(2, {"reply": 3})
    await legacy.update_bot_data({"navigation": {"1": [4]}})
    await legacy.update_conversation("ticket", (2, 1), 0)
    await legacy.flush()
    original_bytes = source.read_bytes()
    target = tmp_path / "maintbot.sqlite3"

    validated = load_legacy_pickle(source)
    assert not target.exists()
    assert import_legacy_pickle(target, validated)
    assert not import_legacy_pickle(target, validated)
    assert source.read_bytes() == original_bytes
    persistence = _persistence(target)
    assert (await persistence.get_user_data())[1]["user"].get_bot() is persistence.bot
    assert await persistence.get_chat_data() == {2: {"reply": 3}}
    assert await persistence.get_bot_data() == {"navigation": {"1": [4]}}
    assert await persistence.get_conversations("ticket") == {(2, 1): 0}


@pytest.mark.asyncio
async def test_import_validates_explicit_backup_and_refuses_existing_state(tmp_path):
    source = tmp_path / "legacy.pickle"
    legacy = PicklePersistence(source)
    legacy.set_bot(ExtBot("123:LEGACY"))
    await legacy.update_user_data(1, {"legacy": True})
    backup = source.with_name(source.name + ".bak")
    backup.write_bytes(source.read_bytes())
    source.write_bytes(b"interrupted write")
    target = tmp_path / "maintbot.sqlite3"
    persistence = _persistence(target)
    await persistence.update_user_data(2, {"existing": True})
    with pytest.raises(ValueError, match="valid PTB state"):
        load_legacy_pickle(source)
    validated = load_legacy_pickle(backup.read_bytes())
    with pytest.raises(ValueError, match="overwrite"):
        import_legacy_pickle(target, validated)
    assert await persistence.get_user_data() == {2: {"existing": True}}
    assert source.read_bytes() == b"interrupted write"
    assert backup.is_file()
