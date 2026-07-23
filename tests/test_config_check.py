from __future__ import annotations

from pathlib import Path

from app import config
from app.config.checks import _check_split_storage
from app.config.servers import ServerTarget
from app.config_check import _check_json_object, validate_configuration
from app.persistence.backend import SplitJsonBackend
from app.persistence.layout import default_store_data


def test_config_check_rejects_malformed_or_non_object_json(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_text("{broken", encoding="utf-8")
    assert _check_json_object(str(path), "STATE")

    path.write_text("[]", encoding="utf-8")
    assert "корнем JSON должен быть объект" in _check_json_object(str(path), "STATE")[0]

    path.write_text("{}", encoding="utf-8")
    assert _check_json_object(str(path), "STATE") == []


def test_config_check_rejects_insecure_ssh_host_key_mode(monkeypatch) -> None:
    server = ServerTarget(
        key="remote",
        label="Remote",
        flag="",
        mode="ssh",
        expected_a_ip="",
        check_a_domains=[],
        monitor_containers=[],
        fail2ban_log_path="/var/log/fail2ban.log",
        ssh_target="maintbot@example.com",
    )
    monkeypatch.setattr(config, "SERVERS", {"remote": server})
    monkeypatch.setattr(config, "SSH_STRICT_HOST_KEY_CHECKING", "no")

    errors = validate_configuration()

    assert "SSH_STRICT_HOST_KEY_CHECKING должен быть yes для удалённых серверов" in errors


def test_config_check_requires_migrated_split_layout(tmp_path: Path) -> None:
    errors = _check_split_storage(str(tmp_path))

    assert any("split-layout v1 не найден" in error for error in errors)
    assert any("app.persistence.migration" in error for error in errors)


def test_config_check_rejects_multiple_owners_across_split_access_store(tmp_path: Path) -> None:
    stores = default_store_data()
    stores["access.grants"] = {
        "1": {"role": "admin", "admin_level": "owner"},
        "2": {"role": "admin", "admin_level": "owner"},
    }
    SplitJsonBackend(tmp_path).bootstrap(stores=stores)

    errors = _check_split_storage(str(tmp_path))

    assert any("несколько руководителей" in error for error in errors)
