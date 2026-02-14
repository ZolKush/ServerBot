import json
import re
from typing import Dict, List, Sequence, Tuple

from ..config import DOCKER_BIN, MONITOR_CONTAINER_SET, SUBPROC_MEDIUM_TIMEOUT, SUBPROC_SHORT_TIMEOUT
from .system_service import run_exec

_CONTAINER_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.\-]{0,62}$")


def is_allowed_container(name: str) -> bool:
    nm = (name or "").strip()
    return bool(nm and _CONTAINER_NAME_RE.fullmatch(nm) and nm in MONITOR_CONTAINER_SET)


async def docker_containers(names: Sequence[str]) -> List[Tuple[str, bool, str, str]]:
    rc, _, _ = await run_exec([DOCKER_BIN, "info"], timeout=SUBPROC_SHORT_TIMEOUT)
    if rc != 0:
        return [(n, False, "docker недоступен", "-") for n in names]

    rc, out, _ = await run_exec([DOCKER_BIN, "ps", "-a", "--format", "{{.Names}}|{{.Status}}"], timeout=SUBPROC_MEDIUM_TIMEOUT)
    if rc != 0:
        return [(n, False, "ошибка docker ps", "-") for n in names]

    info: Dict[str, str] = {}
    for line in out.splitlines():
        parts = line.split("|", 1)
        if len(parts) == 2:
            info[parts[0].strip()] = parts[1].strip()

    rc2, out2, _ = await run_exec([DOCKER_BIN, "ps", "-a", "--format", "{{.Names}}|{{.RestartCount}}"], timeout=SUBPROC_MEDIUM_TIMEOUT)
    restarts: Dict[str, str] = {}
    if rc2 == 0:
        for ln in out2.splitlines():
            p = ln.split("|", 1)
            if len(p) == 2:
                restarts[p[0].strip()] = p[1].strip()

    result: List[Tuple[str, bool, str, str]] = []
    for n in names:
        st = info.get(n)
        if st is None:
            result.append((n, False, "не найден", restarts.get(n, "-")))
        else:
            up = st.lower().startswith("up")
            result.append((n, up, st, restarts.get(n, "-")))
    return result


async def docker_inspect_summary(name: str) -> str:
    rc, out, err = await run_exec([DOCKER_BIN, "inspect", name], timeout=SUBPROC_MEDIUM_TIMEOUT)
    if rc != 0:
        return f"docker inspect error: {err.strip() or out.strip() or 'н/д'}"
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
        port_items: List[str] = []
        if isinstance(ports, dict):
            for k, v in ports.items():
                if v is None:
                    port_items.append(f"{k}→-")
                elif isinstance(v, list) and v:
                    b = v[0]
                    host_ip = b.get("HostIp", "")
                    host_port = b.get("HostPort", "")
                    port_items.append(f"{k}→{host_ip}:{host_port}")
                else:
                    port_items.append(f"{k}")

        lines: List[str] = []
        lines.append(f"Container: {name}")
        lines.append(f"Image: {image}")
        lines.append(f"State: {status} (running={running})")
        lines.append(f"Health: {health}")
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


async def docker_logs_tail(name: str, tail: int) -> str:
    rc, out, err = await run_exec([DOCKER_BIN, "logs", "--tail", str(tail), name], timeout=SUBPROC_MEDIUM_TIMEOUT)
    if rc != 0:
        return f"docker logs error: {err.strip() or out.strip() or 'н/д'}"
    return out
