from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from app.config.servers import ServerTarget
from app.monitoring.fail2ban import cursor as fail2ban_cursor
from app.monitoring.fail2ban import source as fail2ban_source

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
