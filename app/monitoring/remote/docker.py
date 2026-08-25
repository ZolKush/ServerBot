"""Remote Docker monitoring operations."""

from __future__ import annotations

import shlex
from collections.abc import Sequence

from ...config import (
    DOCKER_BIN,
    PRIVILEGED_HELPER_BIN,
    SUBPROC_MEDIUM_TIMEOUT,
    SUDO_BIN,
)
from ..docker.local import _parse_docker_inspect_json
from ..docker.models import docker_status_is_running
from ..docker.presentation import normalize_docker_status
from .transport import ssh_run_exec, ssh_run_shell


def _docker_candidates() -> list[str]:
    result: list[str] = []
    for candidate in (DOCKER_BIN, "/usr/bin/docker", "docker"):
        if candidate and candidate not in result:
            result.append(candidate)
    return result


async def remote_docker_containers(
    ssh_target: str,
    names: Sequence[str],
) -> list[tuple[str, bool, str, str]]:
    """Read the complete remote Docker inventory with one fixed SSH command."""
    name_list = [name for name in names if name]
    docker_loop = " ".join(shlex.quote(candidate) for candidate in _docker_candidates()) or '""'
    lines = [
        "PATH=/usr/sbin:/usr/bin:/sbin:/bin:$PATH; export PATH",
        "LC_ALL=C; LANG=C; export LC_ALL LANG",
        "_docker_done=0",
        f"for d in {docker_loop}; do",
        '  [ -n "$d" ] || continue',
        '  if command -v "$d" >/dev/null 2>&1 || [ -x "$d" ]; then',
        "    if \"$d\" ps -a --format '{{.Names}}|{{.Status}}' 2>/dev/null; then",
        "      _docker_done=1; break",
        "    fi",
    ]
    if SUDO_BIN and PRIVILEGED_HELPER_BIN:
        lines.extend(
            [
                f"    if {shlex.quote(SUDO_BIN)} -n {shlex.quote(PRIVILEGED_HELPER_BIN)} docker-ps 2>/dev/null; then",
                "      _docker_done=1; break",
                "    fi",
            ]
        )
    lines.extend(["  fi", "done", 'printf "__MBOT_DOCKER_OK__|%s\\n" "$_docker_done"'])
    return_code, stdout, stderr = await ssh_run_shell(
        ssh_target,
        "\n".join(lines),
        timeout=SUBPROC_MEDIUM_TIMEOUT + 4,
    )
    if return_code != 0:
        detail = normalize_docker_status(stderr or return_code)
        status = normalize_docker_status(f"ssh ошибка: {detail}")
        return [(name, False, status, "-") for name in name_list] or [
            ("Docker API", False, status, "-"),
        ]

    statuses: dict[str, str] = {}
    docker_ok = False
    for line in stdout.splitlines():
        parts = line.split("|", 1)
        if len(parts) != 2:
            continue
        key, value = parts[0].strip(), parts[1].strip()
        if key == "__MBOT_DOCKER_OK__":
            docker_ok = value == "1"
        elif key:
            statuses[key] = value
    if not docker_ok:
        return [(name, False, "docker недоступен", "-") for name in name_list] or [
            ("Docker API", False, "docker недоступен", "-"),
        ]
    result = [(name, docker_status_is_running(status), status, "-") for name, status in statuses.items()]
    result.extend((name, False, "не найден", "-") for name in name_list if name not in statuses)
    return result


async def remote_docker_inspect_summary(ssh_target: str, name: str) -> str:
    commands = [[docker_bin, "inspect", name] for docker_bin in _docker_candidates()]
    if SUDO_BIN and PRIVILEGED_HELPER_BIN:
        commands.append(
            [SUDO_BIN, "-n", PRIVILEGED_HELPER_BIN, "docker-inspect", name],
        )
    return_code, stdout, stderr = 127, "", "docker inspect unavailable"
    for command in commands:
        return_code, stdout, stderr = await ssh_run_exec(
            ssh_target,
            command,
            timeout=SUBPROC_MEDIUM_TIMEOUT,
        )
        if return_code == 0:
            break
    if return_code != 0:
        return f"docker inspect error: {stderr.strip() or stdout.strip() or 'н/д'}"
    return _parse_docker_inspect_json(name, stdout)


async def remote_docker_logs_tail(ssh_target: str, name: str, tail: int) -> str:
    return_code, stdout, stderr = 127, "", "docker logs unavailable"
    for docker_bin in _docker_candidates():
        arguments = [docker_bin, "logs", "--tail", str(int(tail)), name]
        command = " ".join(shlex.quote(str(argument)) for argument in arguments)
        return_code, stdout, stderr = await ssh_run_shell(
            ssh_target,
            command + " 2>&1",
            timeout=SUBPROC_MEDIUM_TIMEOUT,
        )
        if return_code == 0:
            break
    if return_code != 0 and SUDO_BIN and PRIVILEGED_HELPER_BIN:
        arguments = [
            SUDO_BIN,
            "-n",
            PRIVILEGED_HELPER_BIN,
            "docker-logs",
            name,
            str(int(tail)),
        ]
        command = " ".join(shlex.quote(str(argument)) for argument in arguments)
        return_code, stdout, stderr = await ssh_run_shell(
            ssh_target,
            command + " 2>&1",
            timeout=SUBPROC_MEDIUM_TIMEOUT,
        )
    if return_code != 0:
        return f"docker logs error: {stderr.strip() or stdout.strip() or 'н/д'}"
    return stdout


__all__ = [
    "remote_docker_containers",
    "remote_docker_inspect_summary",
    "remote_docker_logs_tail",
]
