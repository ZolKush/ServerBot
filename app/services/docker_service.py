import json
import logging
import re
from collections.abc import Sequence

from ..config import DOCKER_BIN, PRIVILEGED_HELPER_BIN, SUBPROC_MEDIUM_TIMEOUT, SUDO_BIN
from .system_process import run_exec

logger = logging.getLogger("maint-bot")

_CONTAINER_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.\-]{0,62}$")
_REPORTED_MISSING: set[str] = set()


def is_valid_container_name(name: str) -> bool:
    nm = (name or "").strip()
    return bool(nm and _CONTAINER_NAME_RE.fullmatch(nm))


def _docker_cmds(*args: str) -> list[list[str]]:
    bases: list[str] = []
    for candidate in [DOCKER_BIN, "/usr/bin/docker", "docker"]:
        if candidate and candidate not in bases:
            bases.append(candidate)

    commands: list[list[str]] = []
    for base in bases:
        commands.append([base, *args])
    if SUDO_BIN and PRIVILEGED_HELPER_BIN:
        helper_args: list[str] | None = None
        if args[:1] == ("ps",):
            helper_args = ["docker-ps"]
        elif len(args) == 2 and args[0] == "inspect":
            helper_args = ["docker-inspect", args[1]]
        elif len(args) == 4 and args[0:2] == ("logs", "--tail"):
            helper_args = ["docker-logs", args[3], args[2]]
        if helper_args:
            commands.append([SUDO_BIN, "-n", PRIVILEGED_HELPER_BIN, *helper_args])
    return commands


def _parse_docker_inspect_json(name: str, out: str) -> str:
    try:
        data = json.loads(out)
        if not isinstance(data, list) or not data:
            return "inspect: пустой ответ"
        c = data[0]
        image = ((c.get("Config") or {}).get("Image")) or "-"
        state = c.get("State") or {}
        status = state.get("Status") or "-"
        running = state.get("Running")
        started = state.get("StartedAt") or "-"
        finished = state.get("FinishedAt") or "-"
        exit_code = state.get("ExitCode")
        error = (state.get("Error") or "").strip() or "-"
        health = ((state.get("Health") or {}).get("Status")) or "-"
        restart_count = c.get("RestartCount")
        ports = ((c.get("NetworkSettings") or {}).get("Ports")) or {}
        port_items: list[str] = []
        if isinstance(ports, dict):
            for k, v in ports.items():
                if v is None:
                    port_items.append(f"{k}→-")
                elif isinstance(v, list) and v:
                    b = v[0]
                    port_items.append(f"{k}→{b.get('HostIp', '')}:{b.get('HostPort', '')}")
                else:
                    port_items.append(f"{k}")
        lines: list[str] = [
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
        if error and error != "-":
            lines.append(f"Error: {error}")
        if port_items:
            lines.append("Ports: " + ", ".join(port_items))
        return "\n".join(lines)
    except Exception as e:
        return f"inspect parse error: {e}"


async def docker_containers(names: Sequence[str]) -> list[tuple[str, bool, str, str]]:
    rc, out, _ = 127, "", ""
    for cmd in _docker_cmds("ps", "-a", "--format", "{{.Names}}|{{.Status}}"):
        rc, out, _ = await run_exec(cmd, timeout=SUBPROC_MEDIUM_TIMEOUT)
        if rc == 0:
            break
    info: dict[str, str] = {}
    if rc != 0:
        return [(n, False, "docker недоступен", "-") for n in names]
    for line in out.splitlines():
        parts = line.split("|", 1)
        if len(parts) == 2:
            info[parts[0].strip()] = parts[1].strip()

    result: list[tuple[str, bool, str, str]] = []
    for n in names:
        st = info.get(n)
        if st is None:
            if n not in _REPORTED_MISSING:
                logger.warning("Container '%s' from MONITOR_CONTAINERS not found in docker ps output", n)
                _REPORTED_MISSING.add(n)
            result.append((n, False, "не найден", "-"))
        else:
            _REPORTED_MISSING.discard(n)
            up = st.lower().startswith("up")
            result.append((n, up, st, "-"))
    return result


async def docker_inspect_summary(name: str) -> str:
    rc, out, err = 127, "", "docker inspect unavailable"
    for cmd in _docker_cmds("inspect", name):
        rc, out, err = await run_exec(cmd, timeout=SUBPROC_MEDIUM_TIMEOUT)
        if rc == 0:
            break
    if rc != 0:
        return f"docker inspect error: {err.strip() or out.strip() or 'н/д'}"
    return _parse_docker_inspect_json(name, out)


async def docker_logs_tail(name: str, tail: int) -> str:
    rc, out, err = 127, "", "docker logs unavailable"
    for cmd in _docker_cmds("logs", "--tail", str(tail), name):
        rc, out, err = await run_exec(cmd, timeout=SUBPROC_MEDIUM_TIMEOUT)
        if rc == 0:
            break
    if rc != 0:
        return f"docker logs error: {err.strip() or out.strip() or 'н/д'}"
    # docker logs пишет stderr-поток контейнера в stderr — объединяем оба
    # потока (хронология между ними теряется, но логи не пропадают).
    parts = [p for p in (out, err) if p.strip()]
    return "\n".join(parts) if parts else out
