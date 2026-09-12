import errno
import json
import logging

import pytest

from app.runtime import lock as instance_lock
from app.runtime.logging import JsonLogFormatter, SecretRedactingFormatter


def test_failed_pid_write_closes_the_locked_file(tmp_path, monkeypatch):
    opened = []
    original_fdopen = instance_lock.os.fdopen

    def track_fdopen(*args, **kwargs):
        stream = original_fdopen(*args, **kwargs)
        opened.append(stream)
        return stream

    def fail_sync(_fd):
        raise OSError("disk unavailable")

    monkeypatch.setattr(instance_lock.os, "fdopen", track_fdopen)
    monkeypatch.setattr(instance_lock.os, "fsync", fail_sync)
    lock = instance_lock.SingleInstanceLock(tmp_path / "process.lock")
    try:
        with pytest.raises(OSError, match="disk unavailable"):
            lock.acquire()
        assert opened[0].closed
    finally:
        for stream in opened:
            stream.close()


def test_corrupt_pid_metadata_does_not_hide_contention(tmp_path, monkeypatch):
    path = tmp_path / "process.lock"
    path.write_bytes(b"0\xff\xff")

    def held(_stream):
        raise OSError(errno.EACCES, "already locked")

    monkeypatch.setattr(instance_lock.SingleInstanceLock, "_lock_file", staticmethod(held))
    with pytest.raises(instance_lock.InstanceAlreadyRunning):
        instance_lock.SingleInstanceLock(path).acquire()


def test_json_extra_fields_are_redacted_recursively():
    secret = 'long-"password\\value'
    record = logging.LogRecord("maint-bot", logging.INFO, __file__, 1, "ok", (), None)
    record.source = {"nested": [secret], secret: "value"}
    rendered = JsonLogFormatter(secrets=[secret]).format(record)
    payload = json.loads(rendered)
    assert payload["source"] == {"nested": ["[REDACTED]"], "[REDACTED]": "value"}


def test_overlapping_secrets_are_redacted_longest_first():
    record = logging.LogRecord("maint-bot", logging.INFO, __file__, 1, "password-with-suffix", (), None)
    formatter = SecretRedactingFormatter("%(message)s", secrets=["password", "password-with-suffix"])
    assert formatter.format(record) == "[REDACTED]"
