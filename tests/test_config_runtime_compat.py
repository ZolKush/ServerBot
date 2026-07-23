from __future__ import annotations

import importlib
import logging
import sys
from types import ModuleType

from app import config, launcher
from app.config import checks
from app.config.schema import AppSettings
from app.config.servers import ServerTarget
from app.config_check import validate_configuration


def test_configuration_exports_share_canonical_objects() -> None:
    assert config.ServerTarget is ServerTarget
    assert isinstance(config.SETTINGS, AppSettings)
    assert config.BASE_DIR.name == "app"
    assert validate_configuration is checks.validate_configuration


def test_importing_config_does_not_reconfigure_root_logging() -> None:
    root = logging.getLogger()
    previous_handlers = list(root.handlers)
    previous_level = root.level
    marker = logging.NullHandler()
    root.handlers[:] = [marker]
    try:
        importlib.reload(config)
        assert root.handlers == [marker]
    finally:
        root.handlers[:] = previous_handlers
        root.setLevel(previous_level)


def test_launcher_configures_logging_before_lock_and_application_import(monkeypatch) -> None:
    events: list[str] = []

    class FakeLock:
        def __init__(self, _path: str) -> None:
            events.append("lock-created")

        def acquire(self) -> None:
            events.append("lock-acquired")

        def release(self) -> None:
            events.append("lock-released")

    fake_main = ModuleType("app.main")

    def fake_run_application(*, instance_lock: FakeLock) -> None:
        assert isinstance(instance_lock, FakeLock)
        events.append("application")

    fake_main.run_application = fake_run_application  # type: ignore[attr-defined]
    fake_storage = ModuleType("app.storage")

    def fake_initialize_storage(_data_dir: str) -> None:
        assert events[-1] == "lock-acquired"
        events.append("storage")

    fake_storage.initialize_storage = fake_initialize_storage  # type: ignore[attr-defined]
    monkeypatch.setattr(launcher, "configure_logging", lambda **_kwargs: events.append("logging"))
    monkeypatch.setattr(launcher, "SingleInstanceLock", FakeLock)
    monkeypatch.setitem(sys.modules, "app.main", fake_main)
    monkeypatch.setitem(sys.modules, "app.storage", fake_storage)

    launcher.main()

    assert events == [
        "logging",
        "lock-created",
        "lock-acquired",
        "storage",
        "application",
        "lock-released",
    ]
