from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app import config, storage
from app.config import ServerTarget
from app.monitoring.docker import local as local_docker
from app.monitoring.remnawave.models import MetricsSnapshot, NodeMetrics
from app.monitoring.remote import docker as remote_docker
from app.monitoring.remote import status as remote_status
from app.monitoring.status import cache, collectors, common, handlers, keyboards, presenter

_REMOTE_STATUS = """__MBOT_SEC_UPTIME__
172800.00 0
__MBOT_SEC_MEMINFO__
MemTotal: 2097152 kB
MemAvailable: 1048576 kB
__MBOT_SEC_DF__
Filesystem 1B-blocks Used Available Use% Mounted on
/dev/test 10737418240 2147483648 8589934592 20% /
__MBOT_SEC_UFW__
Status: active
"""


def _server(**changes):
    target = ServerTarget(
        key="test",
        label="Test server",
        flag="",
        mode="ssh",
        expected_a_ip="",
        check_a_domains=[],
        monitor_containers=["api"],
        fail2ban_log_path="/var/log/fail2ban.log",
        ssh_target="maintbot@test.example",
    )
    return replace(target, **changes)


@pytest.fixture
def monitoring(isolated_storage, monkeypatch):
    monkeypatch.setattr(cache, "_STATUS_CACHE", {})
    monkeypatch.setattr(cache, "_STATUS_LOCKS", {})
    monkeypatch.setattr(handlers, "_REFRESH_LOCKS", {})
    monkeypatch.setattr(collectors, "is_admin", lambda update: True)
    monkeypatch.setattr(collectors, "check_uptime", AsyncMock(return_value="2 days"))
    monkeypatch.setattr(collectors, "meminfo", AsyncMock(return_value="1 / 2 GiB"))
    monkeypatch.setattr(collectors, "disk_root", AsyncMock(return_value="2 / 10 GiB"))
    monkeypatch.setattr(collectors, "ufw_summary_for_admin", AsyncMock(return_value=("active", [], [], [])))

    def configure(*targets):
        inventory = {target.key: target for target in targets}
        for module in (config, common, handlers, keyboards, presenter):
            monkeypatch.setattr(module, "SERVERS", inventory)
        dns = AsyncMock(return_value={"total": 0, "ok": 0, "bad": 0, "unknown": 0, "details": []})
        ssh_status = AsyncMock(return_value=(0, _REMOTE_STATUS, ""))
        ssh_docker = AsyncMock(return_value=(0, "api|Up 2 days\n__MBOT_DOCKER_OK__|1\n", ""))
        docker_process = AsyncMock(return_value=(0, "api|Up 2 days\n", ""))
        metrics = AsyncMock(
            return_value=MetricsSnapshot(
                nodes={
                    target.remnawave_uuid: NodeMetrics(
                        uuid=target.remnawave_uuid,
                        status=1,
                        online_users=3,
                        uptime_s=172800,
                        mem_total=2 * 1024**3,
                        mem_free=1024**3,
                        cpu_count=2,
                        network_rx_per_sec=0,
                        network_tx_per_sec=0,
                    )
                    for target in targets
                    if target.remnawave_uuid
                }
            )
        )
        monkeypatch.setattr(handlers, "build_dns_status_payload_live", dns)
        monkeypatch.setattr(handlers, "get_metrics_snapshot", metrics)
        monkeypatch.setattr(collectors, "get_metrics_snapshot", metrics)
        monkeypatch.setattr(remote_status, "ssh_run_shell", ssh_status)
        monkeypatch.setattr(remote_docker, "ssh_run_shell", ssh_docker)
        monkeypatch.setattr(local_docker, "run_exec", docker_process)
        query = SimpleNamespace(answer=AsyncMock(), edit_message_text=AsyncMock())
        return SimpleNamespace(
            update=SimpleNamespace(callback_query=query),
            dns=dns,
            ssh_status=ssh_status,
            ssh_docker=ssh_docker,
            docker_process=docker_process,
            metrics=metrics,
        )

    return configure


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["local", "ssh"])
@pytest.mark.parametrize("source", ["system", "remnawave"])
async def test_refresh_recovers_cached_container_error(monitoring, mode, source):
    target = _server(mode=mode, monitoring_source=source, remnawave_uuid="node" if source == "remnawave" else "")
    env = monitoring(target)
    await storage.set_docker_status_cache(target.key, {"containers": [["api", False, "ssh ошибка: timeout", "-"]]})
    before, _ = await presenter.build_status_message(env.update, target.key)
    assert "Docker ⚠️ н/д" in before

    await handlers._refresh_status_screen(env.update, server_key=target.key)

    saved = storage.get_docker_status_cache(target.key)
    assert saved["containers"] == [["api", True, "Up 2 days", "-"]]
    text = env.update.callback_query.edit_message_text.call_args.args[0]
    assert "Docker 🟢 1/1" in text
    assert "ssh ошибка" not in text
    assert "Контейнеры — проблемы" not in text
    if mode == "ssh":
        env.ssh_docker.assert_awaited_once()
        env.docker_process.assert_not_awaited()
    else:
        env.docker_process.assert_awaited_once()
        env.ssh_docker.assert_not_awaited()
    if source == "remnawave":
        env.metrics.assert_any_await(force_refresh=True)
        env.ssh_status.assert_not_awaited()
    else:
        env.metrics.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("dns_fails", [False, True])
async def test_refresh_replaces_cached_ssh_failure_even_when_dns_fails(monitoring, dns_fails):
    target = _server()
    env = monitoring(target)
    env.ssh_status.return_value = (255, "", "Connection timed out")
    await storage.set_docker_status_cache(target.key, {"containers": [["api", False, "ssh ошибка", "-"]]})
    before = await collectors.build_status_snapshot(env.update, target)
    assert (before.uptime_text, before.memory_raw, before.disk_raw, before.ufw_state) == ("н/д",) * 4
    env.ssh_status.return_value = (0, _REMOTE_STATUS, "")
    if dns_fails:
        env.dns.side_effect = RuntimeError("resolver failed")

    await handlers._refresh_status_screen(env.update, server_key=target.key)

    after = await collectors.build_status_snapshot(env.update, target)
    assert after is not before
    assert all(value != "н/д" for value in (after.uptime_text, after.memory_raw, after.disk_raw))
    assert after.ufw_state == "active"
    assert after.containers[0].is_up
    assert env.ssh_status.await_count == 2
    text = env.update.callback_query.edit_message_text.call_args.args[0]
    assert "Docker 🟢 1/1" in text
    if dns_fails:
        assert "DNS: RuntimeError" in text


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["local", "ssh"])
async def test_refresh_replaces_healthy_containers_on_exception_and_next_click_recovers(monitoring, mode):
    target = _server(mode=mode)
    env = monitoring(target)
    await storage.set_docker_status_cache(target.key, {"containers": [["api", True, "Up 1 day", "-"]]})
    before, _ = await presenter.build_status_message(env.update, target.key)
    assert "Docker 🟢 1/1" in before
    transport = env.ssh_docker if mode == "ssh" else env.docker_process
    transport.side_effect = OSError("connection lost")

    await handlers._refresh_status_screen(env.update, server_key=target.key)

    failed = storage.get_docker_status_cache(target.key)["containers"]
    assert failed[0][0] == "api"
    assert failed[0][1] is False
    assert "ошибка" in failed[0][2]
    text = env.update.callback_query.edit_message_text.call_args.args[0]
    assert "Docker ⚠️ н/д" in text
    assert "Docker 🟢 1/1" not in text

    transport.side_effect = None
    await handlers._refresh_status_screen(env.update, server_key=target.key)

    assert storage.get_docker_status_cache(target.key)["containers"] == [["api", True, "Up 2 days", "-"]]
    text = env.update.callback_query.edit_message_text.call_args.args[0]
    assert "Docker 🟢 1/1" in text
    assert "connection lost" not in text


@pytest.mark.asyncio
async def test_refresh_only_changes_selected_server(monitoring):
    selected = _server()
    other = _server(key="other", label="Other server", ssh_target="maintbot@other.example")
    env = monitoring(selected, other)
    for target in (selected, other):
        await storage.set_docker_status_cache(target.key, {"containers": [["api", False, "ssh ошибка", "-"]]})
    other_snapshot = await collectors.build_status_snapshot(env.update, other)
    other_cache = storage.get_docker_status_cache(other.key)
    env.ssh_status.reset_mock()

    await handlers._refresh_status_screen(env.update, server_key=selected.key)

    assert storage.get_docker_status_cache(selected.key)["containers"][0][1] is True
    assert storage.get_docker_status_cache(other.key) == other_cache
    assert await collectors.build_status_snapshot(env.update, other) is other_snapshot
    env.dns.assert_awaited_once_with(selected)
    env.ssh_docker.assert_awaited_once()
    env.ssh_status.assert_awaited_once()
    assert env.ssh_docker.await_args.args[0] == selected.ssh_target
    assert env.ssh_status.await_args.args[0] == selected.ssh_target
