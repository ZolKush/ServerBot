from __future__ import annotations

import json
import re
import runpy
from ipaddress import ip_address, ip_network
from pathlib import Path

import pytest

from app.config.inventory import load_inventory_directory

ROOT = Path(__file__).resolve().parents[1]


def test_ci_covers_supported_python_versions_and_has_read_only_permissions() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert 'python-version: ["3.10", "3.14"]' in workflow
    assert "uses: actions/checkout@v6" in workflow
    assert "uses: actions/setup-python@v6" in workflow
    assert re.search(r"(?m)^permissions:\n  contents: read$", workflow)
    assert not re.search(r"(?m)^\s+[\w-]+:\s+write\s*$", workflow)


def test_systemd_unit_uses_hardened_launcher_and_single_instance_exit() -> None:
    unit = (ROOT / "deploy" / "maintbot.service").read_text(encoding="utf-8")

    required = {
        "Environment=MAINTBOT_CONFIG_DIR=/opt/maintbot/data/conf",
        "ExecStartPre=/opt/maintbot/.venv/bin/python -m app.config_check",
        "ExecStart=/opt/maintbot/.venv/bin/python -m app.launcher",
        "RestartPreventExitStatus=75",
        "SuccessExitStatus=75",
        "ProtectSystem=strict",
        "ProtectHome=true",
        "PrivateTmp=true",
        "UMask=0077",
        "KillMode=control-group",
        "ReadWritePaths=/opt/maintbot/data",
        "ReadOnlyPaths=-/opt/maintbot/data/conf",
    }
    assert required.issubset(set(unit.splitlines()))
    assert "ExecCondition=" not in unit
    assert "StandardOutput=append:" not in unit


def test_sudoers_only_allows_the_validating_helper() -> None:
    sudoers = (ROOT / "deploy" / "maintbot-sudoers").read_text(encoding="utf-8")

    assert "NOPASSWD: ALL" not in sudoers
    assert "Cmnd_Alias MAINTBOT_READONLY_HELPER = /usr/local/libexec/maintbot-helper *" in sudoers
    assert "maintbot ALL=(root) NOPASSWD: NOSETENV: MAINTBOT_READONLY_HELPER" in sudoers
    assert "/usr/bin/docker" not in sudoers


def test_privileged_helper_is_valid_python() -> None:
    helper = ROOT / "deploy" / "maintbot-helper"
    compile(helper.read_text(encoding="utf-8"), str(helper), "exec")


def _helper_namespace() -> dict[str, object]:
    return runpy.run_path(str(ROOT / "deploy" / "maintbot-helper"))


def test_docker_allowlist_example_contains_only_valid_exact_names() -> None:
    namespace = _helper_namespace()
    parse_container_names = namespace["parse_container_names"]
    lines = [
        line.strip()
        for line in (ROOT / "deploy" / "docker-containers.example").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]

    assert parse_container_names(lines) == {"remnawave", "remnawave-db", "remnanode", "remnawave-nginx"}


def test_privileged_helper_rejects_unlisted_container() -> None:
    checked_container_name = _helper_namespace()["checked_container_name"]

    assert checked_container_name("allowed", allowed={"allowed"}) == "allowed"
    with pytest.raises(SystemExit) as captured:
        checked_container_name("other", allowed={"allowed"})

    assert captured.value.code == 77


def test_privileged_docker_commands_are_allowlisted_and_inspect_is_redacted() -> None:
    namespace = _helper_namespace()
    docker_ps_argv = namespace["docker_ps_argv"]
    docker_inspect_argv = namespace["docker_inspect_argv"]
    docker_logs_argv = namespace["docker_logs_argv"]
    allowed = {"api", "worker"}

    ps_argv = docker_ps_argv("/usr/bin/docker", allowed=allowed)
    assert "name=^/api$" in ps_argv
    assert "name=^/worker$" in ps_argv

    inspect_argv = docker_inspect_argv("/usr/bin/docker", "api", allowed=allowed)
    inspect_format = inspect_argv[inspect_argv.index("--format") + 1]
    assert ".Config.Image" in inspect_format
    assert ".Config.Env" not in inspect_format
    assert ".Mounts" not in inspect_format
    assert ".State.Health" not in inspect_format
    assert ".State.Error" not in inspect_format
    assert isinstance(json.loads(re.sub(r"\{\{json [^}]+\}\}", "null", inspect_format)), list)

    assert docker_logs_argv("/usr/bin/docker", "worker", "25", allowed=allowed) == [
        "/usr/bin/docker",
        "logs",
        "--tail",
        "25",
        "worker",
    ]


def test_server_inventory_examples_are_valid_and_document_tls_fallback() -> None:
    inventory = load_inventory_directory(ROOT / "examples" / "conf" / "servers")

    assert list(inventory) == ["local", "remote"]
    fallback = next(item for item in inventory["remote"].domains if item.host == "tls-fallback.example.com")
    assert fallback.tls_primary_port == 443
    assert fallback.tls_fallback_ports == [8443]


def test_configuration_examples_use_only_reserved_network_identifiers() -> None:
    example_dir = ROOT / "examples" / "conf" / "servers"
    documentation_networks = tuple(ip_network(cidr) for cidr in ("192.0.2.0/24", "198.51.100.0/24", "203.0.113.0/24"))

    assert {path.name for path in example_dir.glob("*.json")} == {
        "local-example.json",
        "remote-example.json",
    }
    for path in example_dir.glob("*.json"):
        document = json.loads(path.read_text(encoding="utf-8"))
        assert document["key"] in {"local", "remote"}
        assert document["label"].startswith("Example ")
        target = document["connection"]["target"]
        if target:
            target_host = target.rsplit("@", 1)[-1].rsplit(":", 1)[0]
            assert target_host.endswith(".example")

        expected_ip = ip_address(document["dns"]["expected_a_ip"])
        assert any(expected_ip in network for network in documentation_networks)
        assert all(domain["host"].endswith(".example.com") for domain in document["domains"])
