from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from app.config.inventory import InventoryError, load_inventory_document
from app.config.servers import load_servers
from tools.migrate_server_inventory import _write_exclusive, migrate, render

ROOT = Path(__file__).resolve().parents[1]


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "servers.toml"
    path.write_text(text, encoding="utf-8")
    return path


def test_inventory_keeps_each_server_and_tls_policy_together(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """version = 1

[servers.nl]
label = "Netherlands"
flag = "NL"
[servers.nl.connection]
transport = "ssh"
target = "maintbot@example.com:1606"
[servers.nl.monitoring]
source = "remnawave"
node_uuid = "00000000-0000-0000-0000-000000000001"
[servers.nl.dns]
expected_a_ip = "192.0.2.10"
[[servers.nl.domains]]
host = "ZERONET-MONITOR.EMBEDDEDCONTROLSINC.COM."
checks = ["dns", "tls"]
tls_primary_port = 443
tls_fallback_ports = [8443]
[servers.nl.docker]
containers = ["remnanode", "remnawave-nginx"]
[servers.nl.fail2ban]
enabled = true
log_path = "/var/log/fail2ban.log"
timezone = "Europe/Amsterdam"
""",
    )

    servers = load_servers(path, timezone_name="Europe/Moscow")

    assert list(servers) == ["nl"]
    server = servers["nl"]
    assert server.ssh_target == "maintbot@example.com:1606"
    assert server.monitoring_source == "remnawave"
    assert server.check_a_domains == ["zeronet-monitor.embeddedcontrolsinc.com"]
    assert server.monitor_containers == ["remnanode", "remnawave-nginx"]
    assert server.tls_endpoints[0].primary_port == 443
    assert server.tls_endpoints[0].fallback_ports == (8443,)


@pytest.mark.parametrize(
    ("fragment", "message"),
    [
        ("tls_fallback_ports = [443]", "must not repeat"),
        ("tls_fallback_ports = [70000]", "range 1..65535"),
        ('unknown_option = "typo"', "Extra inputs"),
    ],
)
def test_inventory_rejects_unsafe_or_unknown_tls_options(tmp_path: Path, fragment: str, message: str) -> None:
    path = _write(
        tmp_path,
        f"""version = 1
[servers.main]
label = "Main"
[servers.main.connection]
transport = "local"
[[servers.main.domains]]
host = "example.com"
checks = ["tls"]
tls_primary_port = 443
{fragment}
""",
    )

    with pytest.raises(InventoryError, match=message):
        load_inventory_document(path)


def test_inventory_requires_uuid_for_remnawave_source(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """version = 1
[servers.main]
label = "Main"
[servers.main.connection]
transport = "local"
[servers.main.monitoring]
source = "remnawave"
""",
    )

    with pytest.raises(InventoryError, match="node_uuid is required"):
        load_inventory_document(path)


def test_one_shot_migration_renders_parseable_inventory_with_explicit_fallback(tmp_path: Path) -> None:
    legacy = {
        "TZ": "Europe/Moscow",
        "LOCAL_SERVER_CODE": "main",
        "LOCAL_SERVER_LABEL": "Main",
        "CHECK_A_DOMAINS": "main.example.com",
        "REMOTE_SERVER_ENABLED": "true",
        "REMOTE_SERVER_SSH_TARGETS": "maintbot@nl.example:1606",
        "REMOTE_SERVER_CODES": "nl",
        "REMOTE_SERVER_LABELS": "Netherlands",
        "REMOTE_SERVER_FLAGS": "NL",
        "REMOTE_SERVER_DOMAINS": "zeronet-monitor.embeddedcontrolsinc.com",
        "REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER": "remnanode,remnawave-nginx",
    }
    servers = migrate(
        legacy,
        fallbacks={"zeronet-monitor.embeddedcontrolsinc.com": [8443]},
    )
    path = _write(tmp_path, render(servers))

    loaded = load_servers(path, timezone_name="Europe/Moscow")

    assert list(loaded) == ["main", "nl"]
    assert loaded["nl"].tls_endpoints[0].fallback_ports == (8443,)


def test_one_shot_migration_never_copies_secrets_to_inventory() -> None:
    marker = "SECRET-MUST-NOT-BE-RENDERED"
    servers = migrate(
        {
            "BOT_TOKEN": marker,
            "REMNAWAVE_METRICS_PASS": marker,
            "LOCAL_SERVER_CODE": "main",
            "LOCAL_SERVER_LABEL": "Main",
        },
        fallbacks={},
    )

    assert marker not in render(servers)


def test_migration_cli_creates_private_inventory_once(tmp_path: Path) -> None:
    source = tmp_path / "legacy.env"
    source.write_text("LOCAL_SERVER_CODE=main\nLOCAL_SERVER_LABEL=Main\n", encoding="utf-8")
    output = tmp_path / "private" / "servers.toml"
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"

    result = subprocess.run(
        [
            sys.executable,
            "tools/migrate_server_inventory.py",
            "--env",
            str(source),
            "--output",
            str(output),
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "[servers.main]" in output.read_text(encoding="utf-8")
    if os.name != "nt":
        assert stat.S_IMODE(output.stat().st_mode) == 0o600


def test_migration_cli_never_overwrites_existing_inventory(tmp_path: Path) -> None:
    source = tmp_path / "legacy.env"
    source.write_text("LOCAL_SERVER_CODE=main\n", encoding="utf-8")
    output = tmp_path / "servers.toml"
    marker = "existing inventory must stay unchanged\n"
    output.write_text(marker, encoding="utf-8")
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"

    result = subprocess.run(
        [
            sys.executable,
            "tools/migrate_server_inventory.py",
            "--env",
            str(source),
            "--output",
            str(output),
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )

    assert result.returncode == 1
    assert output.read_text(encoding="utf-8") == marker
    assert "Ошибка миграции: выходной файл уже существует" in result.stderr
    assert "Traceback" not in result.stderr


def test_exclusive_inventory_write_removes_partial_file_on_failure(tmp_path: Path, monkeypatch) -> None:
    output = tmp_path / "servers.toml"

    def fail_sync(_descriptor: int) -> None:
        raise OSError("simulated sync failure")

    monkeypatch.setattr(os, "fsync", fail_sync)

    with pytest.raises(OSError, match="simulated sync failure"):
        _write_exclusive(output, "version = 1\n")

    assert not output.exists()
