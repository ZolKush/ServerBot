from __future__ import annotations

import importlib
import json
import logging
import sys
from pathlib import Path
from types import ModuleType

import pytest

from app import config, launcher
from app.config import checks
from app.config.json_files import JsonConfigError
from app.config.locations import ROOT_DIR, _resolve_bootstrap_path
from app.config.schema import AppSettings, load_app_settings
from app.config.servers import ServerTarget
from app.config_check import validate_configuration


def test_configuration_exports_share_canonical_objects(monkeypatch) -> None:
    assert config.ServerTarget is ServerTarget
    assert isinstance(config.SETTINGS, AppSettings)
    assert config.BASE_DIR.name == "app"

    delegated_result = ["delegated validation result"]
    monkeypatch.setattr(checks, "validate_configuration", lambda: delegated_result)

    assert validate_configuration() is delegated_result


def test_relative_bootstrap_paths_are_resolved_from_project_root() -> None:
    assert _resolve_bootstrap_path("private/secrets.env", ROOT_DIR / "unused") == ROOT_DIR / "private/secrets.env"


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


def test_bot_settings_are_loaded_from_one_strict_json_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "bot.json"
    document = json.loads(config.BOT_CONFIG_FILE.read_text(encoding="utf-8"))
    document["DATA_DIR"] = "configured/data"
    path.write_text(json.dumps(document), encoding="utf-8")
    monkeypatch.setenv("DATA_DIR", "must-not-override-file")

    settings = load_app_settings(path)

    assert settings.DATA_DIR == "configured/data"
    assert settings.TZ == "Europe/Moscow"


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ('{"version": 1, "version": 1}', "duplicate JSON key"),
        ('{"version": 2}', "missing keys"),
        ("[1, 2, 3]", "root must be a JSON object"),
    ],
)
def test_bot_settings_reject_partial_or_ambiguous_json(tmp_path: Path, payload: str, message: str) -> None:
    path = tmp_path / "bot.json"
    path.write_text(payload, encoding="utf-8")

    with pytest.raises(JsonConfigError, match=message):
        load_app_settings(path)


@pytest.mark.parametrize(
    ("field", "value", "message"), [("UNKNOWN", True, "Extra inputs"), ("version", 2, "Input should be 1")]
)
def test_bot_settings_reject_unknown_keys_or_version(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    document = json.loads(config.BOT_CONFIG_FILE.read_text(encoding="utf-8"))
    document[field] = value
    path = tmp_path / "bot.json"
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(JsonConfigError, match=message):
        load_app_settings(path)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("LOG_JSON", "false"),
        ("SUBPROC_SHORT_TIMEOUT", "3"),
        ("DNS_RESOLVERS", "1.1.1.1,8.8.8.8"),
        ("REMNAWAVE_HIDDEN_UUIDS", [1]),
    ],
)
def test_bot_settings_reject_coerced_json_types(tmp_path: Path, field: str, value: object) -> None:
    document = json.loads(config.BOT_CONFIG_FILE.read_text(encoding="utf-8"))
    document[field] = value
    path = tmp_path / "bot.json"
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(JsonConfigError):
        load_app_settings(path)


def test_launcher_configures_logging_before_lock_and_application_import(monkeypatch) -> None:
    events: list[str] = []

    class FakeLock:
        def __init__(self, _path: str) -> None:
            events.append("lock-created")

        def acquire(self) -> None:
            events.append("lock-acquired")

        def release(self) -> None:
            events.append("lock-released")

    fake_application = ModuleType("app.bot.application")

    def fake_run_application(*, instance_lock: FakeLock) -> None:
        assert isinstance(instance_lock, FakeLock)
        events.append("application")

    fake_application.run_application = fake_run_application  # type: ignore[attr-defined]
    fake_storage = ModuleType("app.storage")

    def fake_initialize_storage(_data_dir: str) -> None:
        assert events[-1] == "lock-acquired"
        events.append("storage")

    fake_storage.initialize_storage = fake_initialize_storage  # type: ignore[attr-defined]
    monkeypatch.setattr(launcher, "configure_logging", lambda **_kwargs: events.append("logging"))
    monkeypatch.setattr(launcher, "SingleInstanceLock", FakeLock)
    monkeypatch.setitem(sys.modules, "app.bot.application", fake_application)
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


def test_main_entrypoint_delegates_to_canonical_launcher(monkeypatch) -> None:
    from app import main as main_module

    events: list[str] = []

    def fail_if_direct_runner_is_used(**_kwargs) -> None:
        raise AssertionError("app.main bypassed the canonical launcher")

    monkeypatch.setattr(main_module, "run_application", fail_if_direct_runner_is_used)
    monkeypatch.setattr(launcher, "main", lambda: events.append("launcher"))

    main_module.main()

    assert events == ["launcher"]
