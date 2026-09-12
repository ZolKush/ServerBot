from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from app.config.json_files import JsonConfigError, load_json_object
from app.config.schema import AppSettings
from app.config.validators import validate_ssh_target
from tools import migrate_config_layout as layout
from tools import migrate_runtime_env as env
from tools import migrate_server_inventory as inventory


@pytest.mark.parametrize("value", ["NaN", "Infinity", "1e999", "-1e999"])
def test_nonfinite_json_is_rejected(tmp_path, value):
    path = tmp_path / "bot.json"
    path.write_text('{"version":1,"value":' + value + "}", encoding="utf-8")
    with pytest.raises(JsonConfigError):
        load_json_object(path, field_name="test config")


@pytest.mark.parametrize("version", [True, 1.0])
def test_json_version_is_an_integer_not_a_coerced_literal(tmp_path, version):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"version": version}), encoding="utf-8")
    with pytest.raises(JsonConfigError):
        load_json_object(path, field_name="test config")


@pytest.mark.parametrize("target", ["user@", "one@two@host", "host:bad", "host:", "[bad]:22", ":22", "user@-option"])
def test_malformed_ssh_targets_are_rejected(target):
    with pytest.raises(ValueError):
        validate_ssh_target(target)


@pytest.mark.parametrize("url", ["https://example.test:bad/", "https://exa mple.test/", "https://example.test/\x00"])
def test_invalid_metrics_url_fails_preflight(url):
    with pytest.raises(ValidationError):
        AppSettings(REMNAWAVE_METRICS_URL=url)


def test_nested_toml_identity_cannot_override_the_validated_key(tmp_path):
    source = tmp_path / "inventory.toml"
    source.write_text('version=1\n[servers.safe]\nkey="nested/name"\nlabel="test"\n', encoding="utf-8")
    with pytest.raises(layout.ConfigMigrationError):
        layout.load_toml_servers(source)


def test_layout_validates_all_keys_before_creating_any_output(tmp_path):
    with pytest.raises(layout.ConfigMigrationError):
        layout.write_layout(tmp_path / "conf", {"version": 1}, [{"version": 1, "key": "nested/name"}])
    assert not (tmp_path / "conf").exists()
    assert not list(tmp_path.glob(".conf.*.tmp"))


def test_single_remote_legacy_uuid_is_preserved():
    uuid = "00000000-0000-0000-0000-000000000001"
    servers = inventory.migrate(
        {"REMOTE_SERVER_SSH_TARGET": "user@host", "REMOTE_SERVER_REMNAWAVE_UUID": uuid}, fallbacks={}
    )
    assert servers[1]["node_uuid"] == uuid
    assert servers[1]["monitoring_source"] == "remnawave"


def test_env_writer_syncs_before_success(tmp_path, monkeypatch):
    synced = []
    monkeypatch.setattr(env.os, "fsync", synced.append)
    env._exclusive_write(tmp_path / "example.env", "TOKEN=synthetic", mode=0o600)
    assert synced
