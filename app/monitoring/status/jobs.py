"""Scheduled refresh jobs for Docker, DNS and node disk/UFW status."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime

from telegram.ext import ContextTypes

from ...config import BOT_MODE, SERVERS, TZ, ServerTarget, logger
from ...monitoring.docker.local import docker_containers
from ...monitoring.remote.docker import remote_docker_containers
from ...storage import (
    set_daily_node_status_cache,
    set_dns_status_cache,
    set_docker_status_cache,
)
from .cache import docker_failure_rows, invalidate_status_cache
from .common import exc_brief
from .dns import build_dns_status_payload_live
from .ssh import collect_disk_ufw

DOCKER_STATUS_REFRESH_INTERVAL_SEC = 6 * 60 * 60
DOCKER_STATUS_STARTUP_DELAY_SEC = 15


async def docker_status_refresh(
    _context: ContextTypes.DEFAULT_TYPE,
) -> None:
    """Refresh the persistent Docker inventory used by every status screen."""
    semaphore = asyncio.Semaphore(4)

    async def refresh(server: ServerTarget) -> None:
        async with semaphore:
            started = time.monotonic()
            try:
                containers = (
                    await remote_docker_containers(
                        server.ssh_target,
                        server.monitor_containers,
                    )
                    if server.mode == "ssh"
                    else await docker_containers(server.monitor_containers)
                )
            except Exception as exc:
                logger.exception(
                    "Docker status refresh failed for server=%s",
                    server.key,
                )
                containers = docker_failure_rows(
                    server,
                    f"ошибка: {exc_brief(exc)}",
                )
            payload = {
                "updated_at": datetime.now(TZ).isoformat(),
                "containers": [
                    [str(name), bool(is_up), str(status), str(restarts)] for name, is_up, status, restarts in containers
                ],
            }
            try:
                await set_docker_status_cache(server.key, payload)
            except Exception:
                logger.exception(
                    "Docker status cache write failed for server=%s",
                    server.key,
                )
                return
            invalidate_status_cache(server.key)
            problem_count = sum(
                1
                for _, is_up, status, _ in containers
                if not is_up or "unhealthy" in str(status).lower() or str(status).lower() == "не найден"
            )
            logger.info(
                "Docker status refreshed source=scheduled server=%s containers=%s problems=%s duration_ms=%s",
                server.key,
                len(containers),
                problem_count,
                round((time.monotonic() - started) * 1000),
                extra={
                    "action": "docker_refresh",
                    "source": "scheduled",
                    "server_key": server.key,
                    "total": len(containers),
                    "problems": problem_count,
                    "duration_ms": round((time.monotonic() - started) * 1000),
                },
            )

    await asyncio.gather(*(refresh(server) for server in SERVERS.values()))


async def dns_daily_refresh(
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    job_name = str(getattr(getattr(context, "job", None), "name", "") or "")
    source = "startup" if "startup" in job_name else "scheduled"
    semaphore = asyncio.Semaphore(4)

    async def refresh(server: ServerTarget) -> None:
        async with semaphore:
            started = time.monotonic()
            try:
                payload = await build_dns_status_payload_live(server)
                await set_dns_status_cache(server.key, payload)
                invalidate_status_cache(server.key)
                logger.info(
                    "DNS status refreshed source=%s server=%s ok=%s bad=%s unknown=%s total=%s duration_ms=%s",
                    source,
                    server.key,
                    payload.get("ok"),
                    payload.get("bad"),
                    payload.get("unknown"),
                    payload.get("total"),
                    round((time.monotonic() - started) * 1000),
                    extra={
                        "action": "dns_refresh",
                        "source": source,
                        "server_key": server.key,
                        "total": payload.get("total"),
                        "ok": payload.get("ok"),
                        "duration_ms": round((time.monotonic() - started) * 1000),
                    },
                )
            except Exception:
                logger.exception(
                    "DNS status refresh failed for server=%s",
                    server.key,
                )

    await asyncio.gather(*(refresh(server) for server in SERVERS.values()))


async def daily_node_status_refresh(
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    if BOT_MODE != "mixed":
        return

    async def refresh(server: ServerTarget) -> None:
        started = time.monotonic()
        try:
            payload = await collect_disk_ufw(server, admin_mode=True)
            if payload.get("ok"):
                await set_daily_node_status_cache(server.key, payload)
                invalidate_status_cache(server.key)
            logger.info(
                "Daily node status refreshed source=scheduled server=%s ok=%s duration_ms=%s",
                server.key,
                payload.get("ok"),
                round((time.monotonic() - started) * 1000),
                extra={
                    "action": "daily_node_status_refresh",
                    "source": "scheduled",
                    "server_key": server.key,
                    "ok": bool(payload.get("ok")),
                    "duration_ms": round((time.monotonic() - started) * 1000),
                },
            )
        except Exception:
            logger.exception(
                "Daily node status refresh failed for server=%s",
                server.key,
            )

    await asyncio.gather(*(refresh(server) for server in SERVERS.values()))


__all__ = [
    "DOCKER_STATUS_REFRESH_INTERVAL_SEC",
    "DOCKER_STATUS_STARTUP_DELAY_SEC",
    "daily_node_status_refresh",
    "dns_daily_refresh",
    "docker_status_refresh",
]
