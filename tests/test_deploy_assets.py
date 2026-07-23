from __future__ import annotations

from pathlib import Path

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
