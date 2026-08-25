from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from app.config.servers import ServerTarget
from app.monitoring.remnawave.models import MetricsSnapshot, NodeMetrics
from app.monitoring.status import handlers as status_handlers
from app.monitoring.status import ssh as status_ssh
from app.monitoring.status.keyboards import status_actions_keyboard


def _callbacks(markup) -> list[str]:
    return [button.callback_data or "" for row in markup.inline_keyboard for button in row]


def test_status_has_one_refresh_and_hides_ssh_fallback_while_monitoring_is_healthy() -> None:
    callbacks = _callbacks(status_actions_keyboard(True, "nl", show_ssh_fallback=False))

    assert callbacks.count("status:refresh:nl") == 1
    assert not any("dnsrefresh" in callback for callback in callbacks)
    assert not any("tlsrefresh" in callback for callback in callbacks)
    assert not any("sshfallback" in callback for callback in callbacks)
    assert "docker:list:nl" in callbacks
    assert "tls:list:nl" in callbacks


def test_status_shows_only_one_ssh_fallback_when_primary_monitoring_failed() -> None:
    callbacks = _callbacks(status_actions_keyboard(True, "nl", show_ssh_fallback=True))

    assert [callback for callback in callbacks if "ssh" in callback] == ["status:sshfallback:nl"]


def _server() -> ServerTarget:
    return ServerTarget(
        key="nl",
        label="Example remote server",
        flag="🇳🇱",
        mode="ssh",
        expected_a_ip="",
        check_a_domains=[],
        monitor_containers=[],
        fail2ban_log_path="/var/log/fail2ban.log",
        ssh_target="maintbot@example.com",
        remnawave_uuid="00000000-0000-0000-0000-000000000001",
        monitoring_source="remnawave",
    )


def _node(*, status: int | None, complete: bool) -> NodeMetrics:
    return NodeMetrics(
        uuid="00000000-0000-0000-0000-000000000001",
        status=status,
        online_users=0,
        uptime_s=100 if complete else None,
        mem_total=1024 if complete else None,
        mem_free=512 if complete else None,
        cpu_count=1,
        network_rx_per_sec=0,
        network_tx_per_sec=0,
    )


@pytest.mark.asyncio
async def test_ssh_fallback_guard_distinguishes_offline_from_technical_failure(monkeypatch) -> None:
    server = _server()
    snapshots = iter(
        [
            MetricsSnapshot(nodes={server.remnawave_uuid: _node(status=0, complete=False)}),
            MetricsSnapshot(error="HTTP 503"),
            MetricsSnapshot(nodes={server.remnawave_uuid: _node(status=1, complete=False)}),
        ]
    )

    async def _metrics(*, force_refresh: bool = False) -> MetricsSnapshot:
        assert force_refresh is True
        return next(snapshots)

    monkeypatch.setattr(status_ssh, "get_metrics_snapshot", _metrics)

    assert await status_ssh.primary_monitoring_failed(server, force_refresh=True) is False
    assert await status_ssh.primary_monitoring_failed(server, force_refresh=True) is True
    assert await status_ssh.primary_monitoring_failed(server, force_refresh=True) is True


@pytest.mark.asyncio
async def test_unified_refresh_updates_dns_and_forces_primary_metrics_once(monkeypatch) -> None:
    server = _server()
    calls = {"dns": 0, "metrics": 0, "saved": 0, "invalidated": 0}

    async def _dns(_server):
        calls["dns"] += 1
        return {"ok": 1, "bad": 0, "unknown": 0, "total": 1}

    async def _metrics(*, force_refresh: bool = False):
        assert force_refresh is True
        calls["metrics"] += 1
        return MetricsSnapshot(nodes={server.remnawave_uuid: _node(status=1, complete=True)})

    async def _save(_server_key, _payload):
        calls["saved"] += 1

    def _invalidate(_server_key):
        calls["invalidated"] += 1

    async def _message(_update, *, server_key: str):
        assert server_key == server.key
        return "status", None

    class Query:
        async def answer(self, *_args, **_kwargs):
            return None

        async def edit_message_text(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(status_handlers, "get_server_target", lambda _key: server)
    monkeypatch.setattr(status_handlers, "build_dns_status_payload_live", _dns)
    monkeypatch.setattr(status_handlers, "get_metrics_snapshot", _metrics)
    monkeypatch.setattr(status_handlers, "set_dns_status_cache", _save)
    monkeypatch.setattr(status_handlers, "invalidate_status_cache", _invalidate)
    monkeypatch.setattr(status_handlers, "build_status_message", _message)

    await status_handlers._refresh_status_screen(SimpleNamespace(callback_query=Query()), server_key=server.key)

    assert calls == {"dns": 1, "metrics": 1, "saved": 1, "invalidated": 1}


@pytest.mark.asyncio
async def test_unified_refresh_cancels_all_parallel_work_with_handler(monkeypatch) -> None:
    server = _server()
    dns_started = asyncio.Event()
    metrics_started = asyncio.Event()
    dns_cancelled = asyncio.Event()
    metrics_cancelled = asyncio.Event()

    async def _blocked(started: asyncio.Event, cancelled: asyncio.Event):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async def _dns(_server):
        return await _blocked(dns_started, dns_cancelled)

    async def _metrics(*, force_refresh: bool = False):
        assert force_refresh is True
        return await _blocked(metrics_started, metrics_cancelled)

    class Query:
        async def answer(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(status_handlers, "get_server_target", lambda _key: server)
    monkeypatch.setattr(status_handlers, "build_dns_status_payload_live", _dns)
    monkeypatch.setattr(status_handlers, "get_metrics_snapshot", _metrics)

    task = asyncio.create_task(
        status_handlers._refresh_status_screen(SimpleNamespace(callback_query=Query()), server_key=server.key)
    )
    await asyncio.gather(dns_started.wait(), metrics_started.wait())
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert dns_cancelled.is_set()
    assert metrics_cancelled.is_set()
