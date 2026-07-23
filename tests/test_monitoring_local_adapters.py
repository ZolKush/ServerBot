from __future__ import annotations

import json
from zoneinfo import ZoneInfo

import pytest

from app.monitoring.docker import local as docker_local
from app.monitoring.docker import models as docker_models
from app.monitoring.docker.handlers import docker_list_menu
from app.monitoring.fail2ban import local as fail2ban_local
from app.monitoring.fail2ban import parser as fail2ban_parser
from app.monitoring.fail2ban.handlers import f2b_menu_cb
from app.monitoring.fail2ban.jobs import fail2ban_daily_digest
from app.monitoring.remote.docker import remote_docker_containers
from app.monitoring.remote.transport import ssh_run_shell
from app.monitoring.status.handlers import cmd_health
from app.monitoring.status.jobs import docker_status_refresh
from app.monitoring.system import dns, metrics, ufw


def test_monitoring_entrypoints_are_canonical_modules() -> None:
    assert dns.resolve_a_record.__module__ == "app.monitoring.system.dns"
    assert metrics.check_uptime.__module__ == "app.monitoring.system.metrics"
    assert ufw.ufw_status_basic.__module__ == "app.monitoring.system.ufw"
    assert fail2ban_parser.parse_fail2ban_events.__module__ == "app.monitoring.fail2ban.parser"
    assert fail2ban_local.tail_text_file.__module__ == "app.monitoring.fail2ban.local"
    assert docker_local.docker_containers.__module__ == "app.monitoring.docker.local"
    assert docker_models.docker_status_is_running.__module__ == "app.monitoring.docker.models"
    assert docker_list_menu.__module__ == "app.monitoring.docker.handlers"
    assert f2b_menu_cb.__module__ == "app.monitoring.fail2ban.handlers"
    assert fail2ban_daily_digest.__module__ == "app.monitoring.fail2ban.jobs"
    assert cmd_health.__module__ == "app.monitoring.status.handlers"
    assert docker_status_refresh.__module__ == "app.monitoring.status.jobs"
    assert remote_docker_containers.__module__ == "app.monitoring.remote.docker"
    assert ssh_run_shell.__module__ == "app.monitoring.remote.transport"


@pytest.mark.asyncio
async def test_invalid_dns_name_returns_without_network_lookup() -> None:
    assert await dns.resolve_a_record("invalid name.example") == []


def test_system_metric_parsers_preserve_display_contract() -> None:
    assert metrics._fmt_bytes_binary(0) == "0 B"
    assert metrics._fmt_bytes_binary(1536) == "1.5 KiB"
    assert metrics._parse_uptime_p("up 2 days, 3 hours, 4 minutes") == "2 д 3 ч 4 м"


def test_ufw_parser_splits_rules_and_removes_duplicates() -> None:
    output = """\
Status: active

To                         Action      From
--                         ------      ----
22/tcp                     ALLOW IN    192.0.2.10
22/tcp                     ALLOW IN    192.0.2.10
23/tcp                     DENY IN     Anywhere
25/tcp                     REJECT IN   198.51.100.7
"""

    assert ufw._parse_ufw_status(output) == "active"
    assert ufw._parse_ufw_rules(output) == (
        ["22/tcp <- 192.0.2.10"],
        ["23/tcp"],
        ["25/tcp <- 198.51.100.7"],
    )


def test_fail2ban_parser_preserves_actions_addresses_and_timezone() -> None:
    events = fail2ban_parser.parse_fail2ban_events(
        [
            "2026-07-23 12:00:00,123 fail2ban.actions [10]: NOTICE [sshd] Ban 192.0.2.10",
            "2026-07-23 12:01:00 fail2ban.actions [10]: NOTICE [sshd] Restore Ban 2001:db8::1",
            "not a fail2ban event",
        ],
        timezone=ZoneInfo("UTC"),
    )

    assert [(event.action, event.ip) for event in events] == [
        ("Ban", "192.0.2.10"),
        ("Restore Ban", "2001:db8::1"),
    ]
    assert all(event.ts.tzinfo == ZoneInfo("UTC") for event in events)


def test_fail2ban_tail_reader_returns_requested_lines(tmp_path) -> None:
    path = tmp_path / "fail2ban.log"
    path.write_text("one\ntwo\nthree\nfour\n", encoding="utf-8")

    assert fail2ban_local.tail_text_file(str(path), 2) == "three\nfour"


def test_docker_validation_and_inspect_parser_preserve_contract() -> None:
    payload = json.dumps(
        [
            {
                "Config": {"Image": "example/image:latest"},
                "State": {
                    "Status": "running",
                    "Running": True,
                    "StartedAt": "2026-07-23T10:00:00Z",
                    "FinishedAt": "0001-01-01T00:00:00Z",
                    "ExitCode": 0,
                    "Error": "",
                    "Health": {"Status": "healthy"},
                },
                "RestartCount": 2,
                "NetworkSettings": {
                    "Ports": {
                        "443/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8443"}],
                    },
                },
            },
        ],
    )

    summary = docker_local._parse_docker_inspect_json("api", payload)
    assert docker_models.is_valid_container_name("api-1")
    assert not docker_models.is_valid_container_name("../api")
    assert docker_models.docker_status_is_running("Up 2 hours (healthy)")
    assert not docker_models.docker_status_is_running("Up 2 hours (Paused)")
    assert "Container: api" in summary
    assert "Image: example/image:latest" in summary
    assert "RestartCount: 2" in summary
    assert "443/tcp→127.0.0.1:8443" in summary
