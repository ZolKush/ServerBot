"""Remote node status collection over the shared SSH transport."""

from __future__ import annotations

import re
import shlex
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta

from ...config import (
    DOCKER_BIN,
    PRIVILEGED_HELPER_BIN,
    SUBPROC_MEDIUM_TIMEOUT,
    SUBPROC_SHORT_TIMEOUT,
    SUDO_BIN,
    UFW_BIN,
)
from ..docker.models import docker_status_is_running
from ..system.metrics import _fmt_bytes_binary
from ..system.ufw import _parse_ufw_rules, _parse_ufw_status
from .transport import ssh_run_shell

_SEC_UPTIME = "__MBOT_SEC_UPTIME__"
_SEC_MEMINFO = "__MBOT_SEC_MEMINFO__"
_SEC_DF = "__MBOT_SEC_DF__"
_SEC_UFW = "__MBOT_SEC_UFW__"
_SEC_DOCKER_STATUS = "__MBOT_SEC_DOCKER_STATUS__"


@dataclass(frozen=True)
class RemoteStatusBundle:
    ok: bool
    error: str = ""
    uptime: str = "н/д"
    memory: str = "н/д"
    disk: str = "н/д"
    containers: list[tuple[str, bool, str, str]] = field(default_factory=list)
    ufw_status: str = "н/д"
    ufw_allow: list[str] = field(default_factory=list)
    ufw_deny: list[str] = field(default_factory=list)
    ufw_reject: list[str] = field(default_factory=list)

    def values(
        self,
    ) -> tuple[
        str,
        str,
        str,
        list[tuple[str, bool, str, str]],
        str,
        list[str],
        list[str],
        list[str],
    ]:
        return (
            self.uptime,
            self.memory,
            self.disk,
            self.containers,
            self.ufw_status,
            self.ufw_allow,
            self.ufw_deny,
            self.ufw_reject,
        )


def _split_sections(text: str) -> dict[str, str]:
    markers = {
        _SEC_UPTIME,
        _SEC_MEMINFO,
        _SEC_DF,
        _SEC_UFW,
        _SEC_DOCKER_STATUS,
    }
    current: str | None = None
    sections: dict[str, list[str]] = {}
    for line in (text or "").splitlines():
        if line in markers:
            current = line
            sections.setdefault(current, [])
            continue
        if current:
            sections[current].append(line)
    return {key: "\n".join(lines).strip("\n") for key, lines in sections.items()}


def _parse_uptime_from_proc(raw: str) -> str:
    try:
        seconds = int(float((raw or "").split()[0]))
    except Exception:
        return "н/д"
    uptime = timedelta(seconds=seconds)
    days = uptime.days
    hours, remainder = divmod(uptime.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days} д")
    if hours:
        parts.append(f"{hours} ч")
    if minutes or not parts:
        parts.append(f"{minutes} м")
    return " ".join(parts)


def _parse_meminfo_text(raw: str) -> str:
    try:
        values: dict[str, int] = {}
        for line in (raw or "").splitlines():
            match = re.match(r"^(\w+):\s+(\d+)\s+kB$", line.strip())
            if match:
                values[match.group(1)] = int(match.group(2))
        total_kib = values.get("MemTotal", 0)
        available_kib = values.get("MemAvailable", values.get("MemFree", 0))
        used_kib = max(total_kib - available_kib, 0)
        return f"{int(round(used_kib / 1024.0))} / {int(round(total_kib / 1024.0))} MiB"
    except Exception:
        return "н/д"


def _parse_df_bytes_text(raw: str) -> str:
    try:
        lines = [line for line in (raw or "").splitlines() if line.strip()]
        if len(lines) < 2:
            return "н/д"
        fields = re.split(r"\s+", lines[1].strip())
        if len(fields) < 6:
            return "н/д"
        total, used, free = int(fields[1]), int(fields[2]), int(fields[3])
        percentage = int(round((used / total) * 100)) if total else 0
        return (
            f"{_fmt_bytes_binary(used)} / {_fmt_bytes_binary(total)} "
            f"(avail {_fmt_bytes_binary(free)}, {percentage}%) mount /"
        )
    except Exception:
        return "н/д"


def _unique_candidates(*values: str) -> list[str]:
    result: list[str] = []
    for value in values:
        if value and value not in result:
            result.append(value)
    return result


def _docker_shell_block(candidates: list[str]) -> str:
    candidate_list = " ".join(shlex.quote(value) for value in candidates) or '""'
    lines = [
        "_docker_done=0",
        f"for d in {candidate_list}; do",
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
    return "\n".join(lines)


def _ufw_shell_block(candidates: list[str]) -> str:
    candidate_list = " ".join(shlex.quote(value) for value in candidates) or '""'
    lines = [
        f"for u in {candidate_list}; do",
        '  [ -n "$u" ] || continue',
        '  if command -v "$u" >/dev/null 2>&1 || [ -x "$u" ]; then',
        '    if "$u" status 2>/dev/null; then break; fi',
    ]
    if SUDO_BIN and PRIVILEGED_HELPER_BIN:
        lines.append(
            f"    if {shlex.quote(SUDO_BIN)} -n {shlex.quote(PRIVILEGED_HELPER_BIN)} ufw-status 2>/dev/null; then break; fi"
        )
    lines.extend(["  fi", "done"])
    return "\n".join(lines)


def _parse_remote_containers(
    section: str,
    names: list[str],
) -> list[tuple[str, bool, str, str]]:
    statuses: dict[str, str] = {}
    docker_ok = False
    for line in section.splitlines():
        parts = line.split("|", 1)
        if len(parts) != 2:
            continue
        key, value = parts[0].strip(), parts[1].strip()
        if key == "__MBOT_DOCKER_OK__":
            docker_ok = value == "1"
        elif key:
            statuses[key] = value
    if not docker_ok:
        return [(name, False, "docker недоступен", "-") for name in names] or [
            ("Docker API", False, "docker недоступен", "-"),
        ]
    result = [(name, docker_status_is_running(status), status, "-") for name, status in statuses.items()]
    result.extend((name, False, "не найден", "-") for name in names if name not in statuses)
    return result


async def remote_status_bundle(
    ssh_target: str,
    names: Sequence[str],
    *,
    admin_mode: bool,
    include_docker: bool = True,
) -> RemoteStatusBundle:
    name_list = [name for name in names if name]
    docker_block = _docker_shell_block(
        _unique_candidates(DOCKER_BIN, "/usr/bin/docker", "docker"),
    )
    ufw_block = _ufw_shell_block(
        _unique_candidates(UFW_BIN, "/usr/sbin/ufw", "ufw"),
    )
    docker_section = f"echo {_SEC_DOCKER_STATUS}\n{docker_block}" if include_docker else ""
    shell = f"""
PATH=/usr/sbin:/usr/bin:/sbin:/bin:$PATH; export PATH
LC_ALL=C; LANG=C; export LC_ALL LANG
echo {_SEC_UPTIME}
cat /proc/uptime 2>/dev/null || true
echo {_SEC_MEMINFO}
cat /proc/meminfo 2>/dev/null || true
echo {_SEC_DF}
df -B1 / 2>/dev/null || true
echo {_SEC_UFW}
{ufw_block}
{docker_section}
""".strip()
    return_code, stdout, stderr = await ssh_run_shell(
        ssh_target,
        shell,
        timeout=max(SUBPROC_MEDIUM_TIMEOUT, SUBPROC_SHORT_TIMEOUT) + 4,
    )
    if return_code != 0:
        error = (stderr or "").strip() or f"SSH завершился с кодом {return_code}"
        containers = []
        if include_docker:
            containers = [(name, False, "ssh ошибка", "-") for name in name_list]
            if not containers:
                containers = [("Docker API", False, "ssh ошибка", "-")]
        return RemoteStatusBundle(ok=False, error=error, containers=containers)

    sections = _split_sections(stdout)
    uptime = _parse_uptime_from_proc(sections.get(_SEC_UPTIME, "") or "") or "н/д"
    memory = _parse_meminfo_text(sections.get(_SEC_MEMINFO, "") or "") or "н/д"
    disk = _parse_df_bytes_text(sections.get(_SEC_DF, "") or "") or "н/д"
    ufw_output = sections.get(_SEC_UFW, "") or ""
    ufw_status = _parse_ufw_status(ufw_output)
    allow, deny, reject = _parse_ufw_rules(ufw_output) if admin_mode else ([], [], [])
    containers = (
        _parse_remote_containers(
            sections.get(_SEC_DOCKER_STATUS, "") or "",
            name_list,
        )
        if include_docker
        else []
    )
    return RemoteStatusBundle(
        ok=True,
        uptime=uptime,
        memory=memory,
        disk=disk,
        containers=containers,
        ufw_status=ufw_status,
        ufw_allow=allow,
        ufw_deny=deny,
        ufw_reject=reject,
    )


__all__ = ["RemoteStatusBundle", "remote_status_bundle"]
