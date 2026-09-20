from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from app import config, storage
from app.config.servers import ServerTarget, TLSEndpoint
from app.monitoring.tls import policy, service


def _server(key, *domains):
    return ServerTarget(
        key=key,
        label=key.title(),
        flag="",
        mode="local",
        expected_a_ip="",
        check_a_domains=[],
        monitor_containers=[],
        fail2ban_log_path="/var/log/fail2ban.log",
        tls_endpoints=tuple(TLSEndpoint(domain) for domain in domains),
    )


def _certificate(domain, port, servers, *, fingerprint="new"):
    now = datetime.now(timezone.utc)
    return {
        "domain": domain,
        "port": port,
        "primary_port": port,
        "servers": list(servers),
        "status": "ok",
        "checked_at": now.isoformat(),
        "not_after": (now + timedelta(days=60)).isoformat(),
        "fingerprint": fingerprint,
        "hostname_valid": True,
        "trust_valid": True,
        "remaining_seconds": 60 * 86400,
        "error": None,
        "failure_kind": "",
    }


@pytest.fixture
def tls_monitoring(isolated_storage, monkeypatch):
    monkeypatch.setattr(service, "_TLS_REFRESH_LOCK", asyncio.Lock())

    def configure(*servers):
        inventory = {server.key: server for server in servers}
        monkeypatch.setattr(config, "SERVERS", inventory)
        monkeypatch.setattr(policy, "SERVERS", inventory)
        check = AsyncMock(side_effect=_certificate)
        monkeypatch.setattr(policy, "check_tls_endpoint", check)
        return check

    return configure


@pytest.mark.asyncio
async def test_manual_tls_refresh_only_checks_selected_server_and_preserves_other_certificates(tls_monitoring):
    selected = _server("main", "main.example", "extra.example")
    other = _server("other", "other.example")
    check = tls_monitoring(selected, other)
    original = _certificate("other.example", 443, ["other"], fingerprint="other-original")
    original.update(notified_fingerprint="other-original", notified_levels=["expiring"])
    await storage.set_tls_certificates_snapshot({"other.example:443": original})
    saved_other = storage.tls_certificates_snapshot()["other.example:443"]

    await service.refresh_tls_certificates(source="manual", server_key="main")

    assert {call.args[0] for call in check.await_args_list} == {"main.example", "extra.example"}
    assert check.await_count == 2
    saved = storage.tls_certificates_snapshot()
    assert set(saved) == {"main.example:443", "extra.example:443", "other.example:443"}
    assert saved["other.example:443"] == saved_other
    assert saved["main.example:443"]["fingerprint"] == "new"


@pytest.mark.asyncio
async def test_manual_tls_refresh_keeps_every_server_binding_for_a_shared_endpoint(tls_monitoring):
    check = tls_monitoring(_server("main", "shared.example"), _server("other", "shared.example"))
    await storage.set_tls_certificates_snapshot(
        {"shared.example:443": _certificate("shared.example", 443, ["main", "other"], fingerprint="old")}
    )

    await service.refresh_tls_certificates(source="manual", server_key="main")

    check.assert_awaited_once_with("shared.example", 443, ["main", "other"])
    saved = storage.tls_certificates_snapshot()["shared.example:443"]
    assert saved["servers"] == ["main", "other"]
    assert service.tls_snapshot_for_server("main")[0]["fingerprint"] == "new"
    assert service.tls_snapshot_for_server("other")[0]["fingerprint"] == "new"


@pytest.mark.asyncio
@pytest.mark.parametrize("server_key", ["empty", "unknown"])
async def test_manual_tls_refresh_without_endpoints_does_not_expand_scope_or_clear_storage(tls_monitoring, server_key):
    check = tls_monitoring(_server("main", "main.example"), _server("empty"))
    await storage.set_tls_certificates_snapshot(
        {"main.example:443": _certificate("main.example", 443, ["main"], fingerprint="original")}
    )
    before = storage.tls_certificates_snapshot()

    await service.refresh_tls_certificates(source="manual", server_key=server_key)

    check.assert_not_awaited()
    assert storage.tls_certificates_snapshot() == before


@pytest.mark.asyncio
async def test_scheduled_tls_refresh_cannot_overwrite_a_newer_manual_result(tls_monitoring):
    check = tls_monitoring(_server("main", "main.example"))
    scheduled_started = asyncio.Event()
    release_scheduled = asyncio.Event()
    manual_requested = asyncio.Event()
    attempts = 0

    async def check_in_order(domain, port, servers):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            scheduled_started.set()
            await release_scheduled.wait()
            return _certificate(domain, port, servers, fingerprint="scheduled-old")
        assert storage.tls_certificates_snapshot()["main.example:443"]["fingerprint"] == "scheduled-old"
        return _certificate(domain, port, servers, fingerprint="manual-new")

    async def request_manual():
        manual_requested.set()
        return await service.refresh_tls_certificates(source="manual", server_key="main")

    check.side_effect = check_in_order
    scheduled = asyncio.create_task(service.refresh_tls_certificates(source="scheduled"))
    manual = None
    try:
        await asyncio.wait_for(scheduled_started.wait(), timeout=2)
        manual = asyncio.create_task(request_manual())
        await asyncio.wait_for(manual_requested.wait(), timeout=2)
        assert check.await_count == 1
        release_scheduled.set()
        await asyncio.wait_for(asyncio.gather(scheduled, manual), timeout=2)
    finally:
        release_scheduled.set()
        pending = [task for task in (scheduled, manual) if task is not None and not task.done()]
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    assert check.await_count == 2
    assert storage.tls_certificates_snapshot()["main.example:443"]["fingerprint"] == "manual-new"
