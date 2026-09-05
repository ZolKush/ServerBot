from __future__ import annotations

import io
import json
import subprocess
import sys
from unittest.mock import AsyncMock

import pytest

from app import config, storage
from app.runtime.lock import SingleInstanceLock
from tools import emergency_delete_recent as cli


@pytest.fixture
def runtime(monkeypatch, isolated_storage, tmp_path):
    monkeypatch.setattr(config, "DATA_DIR", storage.storage_data_dir())
    monkeypatch.setattr(config, "INSTANCE_LOCK_PATH", tmp_path / "instance.lock")
    monkeypatch.setattr(cli, "configure_logging", lambda **kwargs: None)
    bot = AsyncMock()
    bot.__aenter__.return_value = bot
    bot.delete_messages.return_value = True
    monkeypatch.setattr(cli, "Bot", lambda **kwargs: bot)
    return bot


BASE = ["--min-message-id", "10", "--max-message-id", "12", "--chat-id", "123"]


def test_default_dry_run_does_not_connect_or_cancel(runtime, monkeypatch, capsys):
    cancel = AsyncMock()
    monkeypatch.setattr(cli, "cancel_pending_broadcasts", cancel)
    assert cli.main([*BASE, "--cancel-pending-broadcasts"]) == 0
    runtime.__aenter__.assert_not_awaited()
    cancel.assert_not_awaited()
    assert "DRY_RUN" in capsys.readouterr().out


def test_execute_reports_unknown_deleted_count_and_never_sends_markers(runtime, tmp_path):
    report = tmp_path / "cleanup.jsonl"
    assert cli.main([*BASE, "--execute", "--report", str(report)]) == 0
    runtime.delete_messages.assert_awaited_once_with(chat_id=123, message_ids=[10, 11, 12])
    runtime.send_message.assert_not_awaited()
    records = [json.loads(line) for line in report.read_text(encoding="utf-8").splitlines()]
    assert records[-1]["event"] == "SUMMARY"
    assert records[-1]["accepted_ids"] == 3
    assert records[-1]["deleted_count"] is None


def test_partial_failure_has_nonzero_exit_and_identifies_chat(runtime, capsys):
    from telegram.error import TimedOut

    runtime.delete_messages.side_effect = TimedOut()
    assert cli.main([*BASE, "--execute", "--attempts", "1"]) == 1
    summary = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert summary["incomplete_chat_ids"] == [123]
    assert summary["unresolved_ids"] == 3


def test_cancellation_is_opt_in_and_runs_in_same_event_loop(runtime, monkeypatch):
    cancel = AsyncMock(return_value=2)
    monkeypatch.setattr(cli, "cancel_pending_broadcasts", cancel)
    assert cli.main([*BASE, "--execute"]) == 0
    cancel.assert_not_awaited()
    assert cli.main([*BASE, "--execute", "--cancel-pending-broadcasts"]) == 0
    cancel.assert_awaited_once()


def test_all_chats_uses_known_profiles(runtime, monkeypatch):
    monkeypatch.setattr(storage, "authorized_users_snapshot", lambda: {"10": {}, "20": {"enabled": False}})
    assert cli.main([*BASE[:4], "--all-chats", "--execute"]) == 0
    assert [call.kwargs["chat_id"] for call in runtime.delete_messages.await_args_list] == [10, 20]


def test_report_is_not_overwritten(runtime, tmp_path):
    report = tmp_path / "existing.jsonl"
    report.write_text("keep", encoding="utf-8")
    assert cli.main([*BASE, "--execute", "--report", str(report)]) == 1
    assert report.read_text(encoding="utf-8") == "keep"
    runtime.delete_messages.assert_not_awaited()


def test_process_lock_prevents_storage_initialization(runtime, monkeypatch):
    initialize = AsyncMock()
    monkeypatch.setattr(storage, "initialize_storage", initialize)
    with SingleInstanceLock(config.INSTANCE_LOCK_PATH):
        assert cli.main([*BASE, "--execute"]) == 75
    initialize.assert_not_called()
    runtime.__aenter__.assert_not_awaited()


def test_permission_error_has_actionable_message(runtime, monkeypatch, capsys):
    def denied(self):
        raise PermissionError("/run/maintbot")

    monkeypatch.setattr(SingleInstanceLock, "acquire", denied)
    assert cli.main(BASE) == 1
    assert "README" in capsys.readouterr().out


def test_interrupt_releases_lock_and_is_reported(runtime, capsys):
    runtime.delete_messages.side_effect = KeyboardInterrupt()
    assert cli.main([*BASE, "--execute"]) == 130
    assert "INTERRUPTED" in capsys.readouterr().out
    with SingleInstanceLock(config.INSTANCE_LOCK_PATH):
        pass


def test_reporter_redacts_secrets_in_console_and_file(capsys):
    stream = io.StringIO()
    secret = 'fake"secret\\token'
    cli.Reporter(stream, (secret,))("ERROR", reason=f"url/{secret}/deleteMessages")
    assert secret not in json.loads(stream.getvalue())["reason"]
    assert "[REDACTED]" in capsys.readouterr().out


@pytest.mark.parametrize(
    "args",
    [
        [],
        [*BASE, "--batch-size", "101"],
        [*BASE, "--attempts", "0"],
        [*BASE, "--max-message-id", "9"],
        [*BASE, "--min-message-id", "-1"],
        [*BASE, "--max-message-id", "2147483648"],
        [*BASE, "--all-chats"],
        [*BASE, "--execute", "--dry-run"],
        [*BASE, "--scan-depth", "2"],
    ],
)
def test_invalid_or_legacy_arguments_rejected_before_runtime(args):
    with pytest.raises(SystemExit) as exc:
        cli.main(args)
    assert exc.value.code == 2


def test_help_and_import_do_not_load_runtime_configuration(tmp_path):
    import os

    env = dict(os.environ, MAINTBOT_CONFIG_DIR=str(tmp_path / "missing"))
    commands = [
        ["-m", "tools.emergency_delete_recent", "--help"],
        ["-c", "import sys; import tools.emergency_delete_recent; assert 'app.config' not in sys.modules"],
    ]
    for args in commands:
        result = subprocess.run([sys.executable, *args], env=env, capture_output=True, timeout=30, check=False)
        assert result.returncode == 0, result.stderr
