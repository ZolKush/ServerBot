from __future__ import annotations

import base64
import io
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from app.config.servers import ServerTarget
from app.monitoring.fail2ban import cursor as fail2ban_cursor
from app.monitoring.fail2ban import local as fail2ban_local
from app.monitoring.fail2ban import source as fail2ban_source
from app.monitoring.fail2ban.models import FileIdentity, FileIdentityChangedError
from app.monitoring.remote import fail2ban as remote_fail2ban

_TEST_BASE = datetime.now(ZoneInfo("Europe/Moscow")) - timedelta(minutes=10)


def _line(minute: int, ip: str) -> str:
    timestamp = _TEST_BASE + timedelta(minutes=minute)
    return f"{timestamp:%Y-%m-%d %H:%M:%S},000 fail2ban.actions [123]: NOTICE [sshd] Ban {ip}\n"


@pytest.mark.asyncio
async def test_cursor_drains_rotated_file_before_new_log(tmp_path: Path, monkeypatch) -> None:
    log = tmp_path / "fail2ban.log"
    log.write_text(_line(0, "192.0.2.1"), encoding="utf-8")
    server = ServerTarget(
        key="test",
        label="Test",
        flag="",
        mode="local",
        expected_a_ip="",
        check_a_domains=[],
        monitor_containers=[],
        fail2ban_log_path=str(log),
        fail2ban_timezone="Europe/Moscow",
    )
    monkeypatch.setattr(fail2ban_source, "SERVERS", {"test": server})

    events, cursor, _since, has_more = await fail2ban_cursor.read_fail2ban_increment("test", None)
    assert [event.ip for event in events] == ["192.0.2.1"]
    assert has_more is False

    with log.open("a", encoding="utf-8") as handle:
        handle.write(_line(1, "192.0.2.2"))
    log.replace(tmp_path / "fail2ban.log.1")
    log.write_text(_line(2, "192.0.2.3"), encoding="utf-8")

    rotated_events, next_cursor, _since, has_more = await fail2ban_cursor.read_fail2ban_increment("test", cursor)
    assert [event.ip for event in rotated_events] == ["192.0.2.2"]
    assert next_cursor["path"] == str(log)
    assert next_cursor["offset"] == 0
    assert has_more is True

    new_events, final_cursor, _since, has_more = await fail2ban_cursor.read_fail2ban_increment("test", next_cursor)
    assert [event.ip for event in new_events] == ["192.0.2.3"]
    assert final_cursor["offset"] == log.stat().st_size
    assert has_more is False


@pytest.mark.asyncio
async def test_local_range_opens_before_fstat_and_reads_the_same_handle(tmp_path: Path, monkeypatch) -> None:
    log = tmp_path / "fail2ban.log"
    old_text = _line(0, "192.0.2.10")
    operations: list[str] = []

    class OpenLog(io.BytesIO):
        def fileno(self) -> int:
            return 123

        def read(self, size: int = -1) -> bytes:
            operations.append("read")
            return super().read(size)

    def open_log(_path: Path, _mode: str) -> OpenLog:
        operations.append("open")
        return OpenLog(old_text.encode("utf-8"))

    def fstat(_descriptor: int) -> SimpleNamespace:
        operations.append("fstat")
        return SimpleNamespace(
            st_size=len(old_text.encode("utf-8")),
            st_mtime=_TEST_BASE.timestamp(),
            st_dev=7,
            st_ino=42,
        )

    monkeypatch.setattr(Path, "open", open_log)
    monkeypatch.setattr(fail2ban_local.os, "fstat", fstat)

    result = await fail2ban_local.read_text_range_with_sudo_async(str(log), 0, 1_000_000)

    assert operations == ["open", "fstat", "read", "fstat"]
    assert result.text == old_text
    assert result.identity.device == 7
    assert result.identity.inode == 42


@pytest.mark.asyncio
async def test_cursor_retries_rotation_between_identity_and_open(tmp_path: Path, monkeypatch) -> None:
    log = tmp_path / "fail2ban.log"
    rotated = tmp_path / "fail2ban.log.1"
    log.write_text(_line(0, "192.0.2.20"), encoding="utf-8")
    server = ServerTarget(
        key="test",
        label="Test",
        flag="",
        mode="local",
        expected_a_ip="",
        check_a_domains=[],
        monitor_containers=[],
        fail2ban_log_path=str(log),
        fail2ban_timezone="Europe/Moscow",
    )
    monkeypatch.setattr(fail2ban_source, "SERVERS", {"test": server})
    _events, cursor, _since, _has_more = await fail2ban_cursor.read_fail2ban_increment("test", None)
    with log.open("a", encoding="utf-8") as handle:
        handle.write(_line(1, "192.0.2.21"))

    original_read_range = fail2ban_cursor.read_range
    rotated_once = False

    async def rotate_before_open(server_key, path, offset, limit, expected_identity):
        nonlocal rotated_once
        if not rotated_once and path == str(log):
            rotated_once = True
            log.replace(rotated)
            log.write_text(_line(2, "192.0.2.22"), encoding="utf-8")
        return await original_read_range(server_key, path, offset, limit, expected_identity)

    monkeypatch.setattr(fail2ban_cursor, "read_range", rotate_before_open)

    events, next_cursor, _since, has_more = await fail2ban_cursor.read_fail2ban_increment("test", cursor)

    assert [event.ip for event in events] == ["192.0.2.21"]
    assert next_cursor["path"] == str(log)
    assert next_cursor["offset"] == 0
    assert has_more is True


@pytest.mark.asyncio
async def test_remote_range_refuses_log_rotated_during_read(monkeypatch) -> None:
    before = FileIdentity(size=100, mtime=_TEST_BASE, device=1, inode=10)
    after = FileIdentity(size=20, mtime=_TEST_BASE, device=1, inode=11)
    identities = iter((before, after))

    async def fake_identity(_target: str, _path: str) -> FileIdentity:
        return next(identities)

    async def fake_ssh_run_shell(_target: str, _command: str, **_kwargs):
        return 0, base64.b64encode(b"new log bytes").decode("ascii"), ""

    monkeypatch.setattr(remote_fail2ban, "remote_fail2ban_identity", fake_identity)
    monkeypatch.setattr(remote_fail2ban, "ssh_run_shell", fake_ssh_run_shell)

    with pytest.raises(FileIdentityChangedError, match="changed during read"):
        await remote_fail2ban.remote_read_text_range(
            "maintbot@example.com",
            "/var/log/fail2ban.log",
            0,
            1024,
            expected_identity=before,
        )
