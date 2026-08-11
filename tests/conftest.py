"""Изолирует тесты от реальных секретов и JSON-файлов проекта."""

from __future__ import annotations

import atexit
import os
import shutil
import tempfile
from pathlib import Path

import pytest

_TEST_ROOT = Path(tempfile.mkdtemp(prefix="maintbot-tests-"))
atexit.register(shutil.rmtree, _TEST_ROOT, True)
_TEST_INVENTORY = _TEST_ROOT / "servers.toml"
_TEST_INVENTORY.write_text(
    """version = 1

[servers.local]
label = "Local server"
flag = ""

[servers.local.connection]
transport = "local"

[servers.local.monitoring]
source = "system"
""",
    encoding="utf-8",
)

os.environ.update(
    {
        "ENV_PATH": str(_TEST_ROOT / "missing.env"),
        "SECRETS_ENV_PATH": str(_TEST_ROOT / "missing.secrets"),
        "BOT_TOKEN": "123456:TEST_TOKEN_NOT_USED_BY_TESTS_ABCDEFGHIJKLMNOPQRSTUVWXYZ",
        "ADMIN_PASSWORD": "test-admin-password-that-is-never-used",
        "OWNER_PASSWORD": "test-owner-password-that-is-never-used",
        "DATA_DIR": str(_TEST_ROOT / "data"),
        "INSTANCE_LOCK_PATH": str(_TEST_ROOT / "maintbot.lock"),
        "PTB_PERSISTENCE_PATH": str(_TEST_ROOT / "data" / "telegram" / "persistence.pickle"),
        "PTB_TIMEDELTA": "1",
        "SERVER_INVENTORY_FILE": str(_TEST_INVENTORY),
    }
)


@pytest.fixture
def isolated_storage(tmp_path: Path):
    """Give feature tests a fresh split-layout v1 without legacy JSON globals."""
    from app import storage

    storage.initialize_empty_storage_for_tests(tmp_path / "data")
    yield
