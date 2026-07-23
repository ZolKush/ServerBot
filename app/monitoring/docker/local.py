"""Local Docker CLI adapter."""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence

from ...config import DOCKER_BIN, PRIVILEGED_HELPER_BIN, SUBPROC_MEDIUM_TIMEOUT, SUDO_BIN
from ...runtime.process import run_exec
from .models import ContainerRecord, ContainerSnapshot, docker_status_is_running

logger = logging.getLogger("maint-bot")

_REPORTED_MISSING: set[str] = set()


def _docker_cmds(*arguments: str) -> list[list[str]]:
    binaries: list[str] = []
    for candidate in (DOCKER_BIN, "/usr/bin/docker", "docker"):
        if candidate and candidate not in binaries:
            binaries.append(candidate)

    commands = [[binary, *arguments] for binary in binaries]
    if SUDO_BIN and PRIVILEGED_HELPER_BIN:
        helper_arguments: list[str] | None = None
        if arguments[:1] == ("ps",):
            helper_arguments = ["docker-ps"]
        elif len(arguments) == 2 and arguments[0] == "inspect":
            helper_arguments = ["docker-inspect", arguments[1]]
        elif len(arguments) == 4 and arguments[:2] == ("logs", "--tail"):
            helper_arguments = ["docker-logs", arguments[3], arguments[2]]
        if helper_arguments:
            commands.append(
                [SUDO_BIN, "-n", PRIVILEGED_HELPER_BIN, *helper_arguments],
            )
    return commands


def _parse_docker_inspect_json(name: str, output: str) -> str:
    try:
        payload = json.loads(output)
        if not isinstance(payload, list) or not payload:
            return "inspect: пустой ответ"
        container = payload[0]
        image = ((container.get("Config") or {}).get("Image")) or "-"
        state = container.get("State") or {}
        status = state.get("Status") or "-"
        running = state.get("Running")
        started = state.get("StartedAt") or "-"
        finished = state.get("FinishedAt") or "-"
        exit_code = state.get("ExitCode")
        error = (state.get("Error") or "").strip() or "-"
        health = ((state.get("Health") or {}).get("Status")) or "-"
        restart_count = container.get("RestartCount")
        ports = ((container.get("NetworkSettings") or {}).get("Ports")) or {}
        port_items: list[str] = []
        if isinstance(ports, dict):
            for container_port, bindings in ports.items():
                if bindings is None:
                    port_items.append(f"{container_port}→-")
                elif isinstance(bindings, list) and bindings:
                    binding = bindings[0]
                    port_items.append(
                        f"{container_port}→{binding.get('HostIp', '')}:{binding.get('HostPort', '')}",
                    )
                else:
                    port_items.append(str(container_port))
        lines = [
            f"Container: {name}",
            f"Image: {image}",
            f"State: {status} (running={running})",
            f"Health: {health}",
        ]
        if restart_count is not None:
            lines.append(f"RestartCount: {restart_count}")
        if exit_code is not None:
            lines.append(f"ExitCode: {exit_code}")
        lines.append(f"StartedAt: {started}")
        if status not in ("running", "up"):
            lines.append(f"FinishedAt: {finished}")
        if error != "-":
            lines.append(f"Error: {error}")
        if port_items:
            lines.append("Ports: " + ", ".join(port_items))
        return "\n".join(lines)
    except Exception as exc:
        return f"inspect parse error: {exc}"


async def _run_first_success(commands: list[list[str]]) -> tuple[int, str, str]:
    result = (127, "", "")
    for command in commands:
        result = await run_exec(command, timeout=SUBPROC_MEDIUM_TIMEOUT)
        if result[0] == 0:
            break
    return result


async def docker_containers(names: Sequence[str]) -> list[ContainerRecord]:
    return_code, stdout, _ = await _run_first_success(
        _docker_cmds("ps", "-a", "--format", "{{.Names}}|{{.Status}}"),
    )
    if return_code != 0:
        configured = [name for name in names if name]
        unavailable = [ContainerSnapshot(name, False, "docker недоступен").as_record() for name in configured]
        return unavailable or [
            ContainerSnapshot("Docker API", False, "docker недоступен").as_record(),
        ]

    statuses: dict[str, str] = {}
    for line in stdout.splitlines():
        parts = line.split("|", 1)
        if len(parts) == 2:
            statuses[parts[0].strip()] = parts[1].strip()

    snapshots = [
        ContainerSnapshot(
            name=container_name,
            running=docker_status_is_running(status),
            status=status,
        )
        for container_name, status in statuses.items()
    ]
    for name in names:
        if name not in statuses:
            if name not in _REPORTED_MISSING:
                logger.warning(
                    "Container '%s' from MONITOR_CONTAINERS not found in docker ps output",
                    name,
                )
                _REPORTED_MISSING.add(name)
            snapshots.append(ContainerSnapshot(name, False, "не найден"))
        else:
            _REPORTED_MISSING.discard(name)
    return [snapshot.as_record() for snapshot in snapshots]


async def docker_inspect_summary(name: str) -> str:
    return_code, stdout, stderr = await _run_first_success(
        _docker_cmds("inspect", name),
    )
    if return_code != 0:
        error = stderr.strip() or stdout.strip() or "н/д"
        return f"docker inspect error: {error}"
    return _parse_docker_inspect_json(name, stdout)


async def docker_logs_tail(name: str, tail: int) -> str:
    return_code, stdout, stderr = await _run_first_success(
        _docker_cmds("logs", "--tail", str(tail), name),
    )
    if return_code != 0:
        error = stderr.strip() or stdout.strip() or "н/д"
        return f"docker logs error: {error}"
    # Docker writes the container's stderr stream to stderr. Preserve both
    # streams even though their exact interleaving is unavailable here.
    parts = [part for part in (stdout, stderr) if part.strip()]
    return "\n".join(parts) if parts else stdout


__all__ = ["docker_containers", "docker_inspect_summary", "docker_logs_tail"]
