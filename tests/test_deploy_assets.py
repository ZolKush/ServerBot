from __future__ import annotations

from pathlib import Path

from app.config.inventory import load_inventory_document

ROOT = Path(__file__).resolve().parents[1]


def test_systemd_unit_uses_hardened_launcher_and_single_instance_exit() -> None:
    unit = (ROOT / "deploy" / "maintbot.service").read_text(encoding="utf-8")

    required = {
        "ExecCondition=/opt/maintbot/.venv/bin/python -m app.config_check",
        "ExecStart=/opt/maintbot/.venv/bin/python -m app.launcher",
        "RestartPreventExitStatus=75",
        "SuccessExitStatus=75",
        "ProtectSystem=strict",
        "ProtectHome=true",
        "PrivateTmp=true",
        "UMask=0077",
        "KillMode=control-group",
        "ReadWritePaths=/opt/maintbot/data",
    }
    assert required.issubset(set(unit.splitlines()))
    assert "StandardOutput=append:" not in unit


def test_sudoers_only_allows_the_validating_helper() -> None:
    sudoers = (ROOT / "deploy" / "maintbot-sudoers").read_text(encoding="utf-8")

    assert "NOPASSWD: ALL" not in sudoers
    assert "maintbot ALL=(root) NOPASSWD: /usr/local/libexec/maintbot-helper *" in sudoers


def test_privileged_helper_is_valid_python() -> None:
    helper = ROOT / "deploy" / "maintbot-helper"
    compile(helper.read_text(encoding="utf-8"), str(helper), "exec")


def test_server_inventory_example_is_valid_and_documents_tls_fallback() -> None:
    inventory = load_inventory_document(ROOT / "deploy" / "servers.toml.example")

    assert list(inventory.servers) == ["main", "nl"]
    zeronet = next(item for item in inventory.servers["nl"].domains if item.host.startswith("zeronet-monitor"))
    assert zeronet.tls_primary_port == 443
    assert zeronet.tls_fallback_ports == [8443]
