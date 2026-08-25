from __future__ import annotations

from dataclasses import replace

import pytest

from app.config import ServerTarget, TLSEndpoint, server_monitoring_fingerprint
from app.monitoring.status.reconciliation import reconcile_configured_servers
from app.storage import update_important_data


def _target(*, ssh_target: str = "maintbot@live.example") -> ServerTarget:
    return ServerTarget(
        key="live",
        label="Live",
        flag="",
        mode="ssh",
        expected_a_ip="192.0.2.10",
        check_a_domains=["shared.example"],
        monitor_containers=["app"],
        fail2ban_log_path="/var/log/fail2ban.log",
        ssh_target=ssh_target,
        tls_endpoints=(TLSEndpoint("shared.example", 443, (8443,)),),
    )


async def _seed_monitoring_state(fingerprint: str) -> None:
    def apply(aggregate) -> None:
        aggregate.dns_status = {
            "live": {"ok": True, "_config_fingerprint": fingerprint},
            "removed": {"ok": False, "_config_fingerprint": "old"},
        }
        aggregate.daily_node_status = {"removed": {"disk": "old", "_config_fingerprint": "old"}}
        aggregate.docker_status = {"removed": {"containers": [], "_config_fingerprint": "old"}}
        aggregate.fail2ban_cursors = {"removed": {"offset": 10, "_config_fingerprint": "old"}}
        aggregate.tls_certificates = {
            "shared.example:443": {
                "domain": "shared.example",
                "primary_port": 443,
                "fallback_ports": [8443],
                "servers": ["live"],
            },
            "old.example:443": {
                "domain": "old.example",
                "primary_port": 443,
                "fallback_ports": [],
                "servers": ["removed"],
            },
        }

    await update_important_data(apply)


@pytest.mark.asyncio
async def test_removed_servers_are_pruned_from_persisted_monitoring_state(isolated_storage) -> None:
    server = _target()
    fingerprint = server_monitoring_fingerprint(server)
    await _seed_monitoring_state(fingerprint)

    removed = await reconcile_configured_servers({"live": server})

    snapshot = await update_important_data(lambda aggregate: aggregate)
    assert snapshot.dns_status == {"live": {"ok": True, "_config_fingerprint": fingerprint}}
    assert snapshot.daily_node_status == {}
    assert snapshot.docker_status == {}
    assert snapshot.fail2ban_cursors == {}
    assert set(snapshot.tls_certificates) == {"shared.example:443"}
    assert sum(removed.values()) == 5


@pytest.mark.asyncio
async def test_reused_server_key_invalidates_cache_when_connection_changes(isolated_storage) -> None:
    old_server = _target()
    await _seed_monitoring_state(server_monitoring_fingerprint(old_server))
    new_server = replace(old_server, ssh_target="maintbot@replacement.example")

    removed = await reconcile_configured_servers({"live": new_server})

    snapshot = await update_important_data(lambda aggregate: aggregate)
    assert snapshot.dns_status == {}
    assert removed["dns_status"] == 2
