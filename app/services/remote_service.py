import base64
import binascii
import re
import shlex
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from ..config import (
    DOCKER_BIN,
    PRIVILEGED_HELPER_BIN,
    SSH_BIN,
    SSH_IDENTITY_FILE,
    SSH_KNOWN_HOSTS_FILE,
    SSH_STRICT_HOST_KEY_CHECKING,
    SUBPROC_MEDIUM_TIMEOUT,
    SUBPROC_SHORT_TIMEOUT,
    SUDO_BIN,
    TZ,
    UFW_BIN,
    logger,
)
from .docker_service import _parse_docker_inspect_json, docker_status_is_running
from .system_fail2ban import Fail2banEvent, FileIdentity, parse_fail2ban_events
from .system_metrics import _fmt_bytes_binary
from .system_process import run_exec
from .system_ufw import _parse_ufw_rules, _parse_ufw_status

_OUT_BEGIN = "__MBOT_OUT_BEGIN_43e1f3c4__"
_OUT_END = "__MBOT_OUT_END_43e1f3c4__"
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

    def values(self) -> tuple[str, str, str, list[tuple[str, bool, str, str]], str, list[str], list[str], list[str]]:
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


def _extract_wrapped_stdout(text: str) -> str:
    raw = text or ""
    start = raw.find(_OUT_BEGIN)
    if start < 0:
        return raw
    start = raw.find("\n", start)
    if start < 0:
        return ""
    start += 1
    end = raw.find(_OUT_END, start)
    if end < 0:
        return raw[start:]
    payload = raw[start:end]
    return payload[:-1] if payload.endswith("\n") else payload


def _split_sections(text: str) -> dict[str, str]:
    lines = (text or "").splitlines()
    markers = {
        _SEC_UPTIME,
        _SEC_MEMINFO,
        _SEC_DF,
        _SEC_UFW,
        _SEC_DOCKER_STATUS,
    }
    cur = None
    buf: dict[str, list[str]] = {}
    for ln in lines:
        if ln in markers:
            cur = ln
            buf.setdefault(cur, [])
            continue
        if cur:
            buf[cur].append(ln)
    return {k: "\n".join(v).strip("\n") for k, v in buf.items()}


def _split_ssh_target(target: str) -> tuple[str, int | None]:
    tgt = (target or "").strip()
    if not tgt:
        return tgt, None

    user_prefix, host_part = tgt.rsplit("@", 1) if "@" in tgt else ("", tgt)
    prefix = f"{user_prefix}@" if user_prefix else ""
    if host_part.startswith("["):
        closing = host_part.find("]")
        if closing < 0:
            raise ValueError("invalid bracketed IPv6 SSH target")
        host = prefix + host_part[1:closing]
        suffix = host_part[closing + 1 :]
        if not suffix:
            return host, None
        if not suffix.startswith(":") or not suffix[1:].isdigit():
            raise ValueError("invalid SSH target port")
        port = int(suffix[1:])
    else:
        # Two or more colons mean an unbracketed IPv6 literal. Its final numeric
        # component is part of the address, never an implicit SSH port.
        if host_part.count(":") != 1:
            return tgt, None
        host_name, port_text = host_part.rsplit(":", 1)
        if not port_text.isdigit():
            return tgt, None
        host = prefix + host_name
        port = int(port_text)
    if not 1 <= port <= 65535:
        raise ValueError("ssh port out of range")
    return host, port


async def ssh_run_shell(
    target: str,
    command: str,
    timeout: int,
    *,
    max_output_bytes: int | None = None,
) -> tuple[int, str, str]:
    tgt = (target or "").strip()
    if not tgt:
        return 127, "", "ssh target is not configured"
    try:
        ssh_host, ssh_port = _split_ssh_target(tgt)
    except ValueError as e:
        return 127, "", str(e)
    wrapped = (
        "PATH=/usr/sbin:/usr/bin:/sbin:/bin:$PATH; LANG=C; LC_ALL=C; export PATH LANG LC_ALL; "
        f"printf '%s\\n' {shlex.quote(_OUT_BEGIN)}; "
        f"{command}; "
        "rc=$?; "
        f"printf '\\n%s\\n' {shlex.quote(_OUT_END)}; "
        "exit $rc"
    )
    args = [
        SSH_BIN,
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={max(2, min(timeout, 10))}",
        "-o",
        "LogLevel=ERROR",
        "-o",
        f"StrictHostKeyChecking={SSH_STRICT_HOST_KEY_CHECKING}",
    ]
    if SSH_KNOWN_HOSTS_FILE:
        args.extend(["-o", f"UserKnownHostsFile={SSH_KNOWN_HOSTS_FILE}"])
    if SSH_IDENTITY_FILE:
        args.extend(["-o", "IdentitiesOnly=yes", "-i", SSH_IDENTITY_FILE])
    if ssh_port is not None:
        args.extend(["-p", str(ssh_port)])
    # ssh склеивает аргументы пробелами без quoting, поэтому команду для
    # удалённого sh -c нужно экранировать одной строкой — иначе её разберёт
    # логин-шелл пользователя (и сломается, если это не POSIX-шелл).
    args.append(ssh_host)
    args.append("sh -c " + shlex.quote(wrapped))
    run_kwargs = {"max_output_bytes": max_output_bytes} if max_output_bytes is not None else {}
    rc, out, err = await run_exec(args, timeout=max(timeout + 2, 5), **run_kwargs)
    return rc, _extract_wrapped_stdout(out), err


async def ssh_run_exec(target: str, argv: Sequence[str], timeout: int) -> tuple[int, str, str]:
    cmd = " ".join(shlex.quote(str(a)) for a in argv if str(a))
    return await ssh_run_shell(target, cmd, timeout)


def _parse_uptime_from_proc(raw: str) -> str:
    try:
        seconds = int(float((raw or "").split()[0]))
    except Exception:
        return "н/д"
    td = timedelta(seconds=seconds)
    days = td.days
    hours, rem = divmod(td.seconds, 3600)
    minutes, _ = divmod(rem, 60)
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
        kv: dict[str, int] = {}
        for line in (raw or "").splitlines():
            m = re.match(r"^(\w+):\s+(\d+)\s+kB$", line.strip())
            if m:
                kv[m.group(1)] = int(m.group(2))
        mem_total_kb = kv.get("MemTotal", 0)
        mem_avail_kb = kv.get("MemAvailable", kv.get("MemFree", 0))
        mem_used_kb = max(mem_total_kb - mem_avail_kb, 0)
        used_mib = int(round(mem_used_kb / 1024.0))
        total_mib = int(round(mem_total_kb / 1024.0))
        return f"{used_mib} / {total_mib} MiB"
    except Exception:
        return "н/д"


def _parse_df_bytes_text(raw: str) -> str:
    try:
        lines = [ln for ln in (raw or "").splitlines() if ln.strip()]
        if len(lines) < 2:
            return "н/д"
        parts = re.split(r"\s+", lines[1].strip())
        if len(parts) < 6:
            return "н/д"
        total = int(parts[1])
        used = int(parts[2])
        free = int(parts[3])
        usep = int(round((used / total) * 100)) if total else 0
        return (
            f"{_fmt_bytes_binary(used)} / {_fmt_bytes_binary(total)} (avail {_fmt_bytes_binary(free)}, {usep}%) mount /"
        )
    except Exception:
        return "н/д"


async def remote_status_bundle(
    ssh_target: str,
    names: Sequence[str],
    *,
    admin_mode: bool,
) -> RemoteStatusBundle:
    name_list = [n for n in names if n]
    docker_candidates: list[str] = []
    for cand in [DOCKER_BIN, "/usr/bin/docker", "docker"]:
        if cand and cand not in docker_candidates:
            docker_candidates.append(cand)
    ufw_candidates: list[str] = []
    for cand in [UFW_BIN, "/usr/sbin/ufw", "ufw"]:
        if cand and cand not in ufw_candidates:
            ufw_candidates.append(cand)

    sudo_quoted = shlex.quote(SUDO_BIN) if SUDO_BIN else ""
    helper_quoted = shlex.quote(PRIVILEGED_HELPER_BIN) if PRIVILEGED_HELPER_BIN else ""
    docker_for_loop = " ".join(shlex.quote(c) for c in docker_candidates) or '""'
    ufw_for_loop = " ".join(shlex.quote(c) for c in ufw_candidates) or '""'

    docker_block_lines: list[str] = [
        "_docker_done=0",
        f"for d in {docker_for_loop}; do",
        '  [ -n "$d" ] || continue',
        '  if command -v "$d" >/dev/null 2>&1 || [ -x "$d" ]; then',
        "    if \"$d\" ps -a --format '{{.Names}}|{{.Status}}' 2>/dev/null; then",
        "      _docker_done=1; break",
        "    fi",
    ]
    if sudo_quoted and helper_quoted:
        docker_block_lines.extend(
            [
                f"    if {sudo_quoted} -n {helper_quoted} docker-ps 2>/dev/null; then",
                "      _docker_done=1; break",
                "    fi",
            ]
        )
    docker_block_lines.extend(["  fi", "done", 'printf "__MBOT_DOCKER_OK__|%s\\n" "$_docker_done"'])
    docker_block = "\n".join(docker_block_lines)

    ufw_block_lines: list[str] = [
        f"for u in {ufw_for_loop}; do",
        '  [ -n "$u" ] || continue',
        '  if command -v "$u" >/dev/null 2>&1 || [ -x "$u" ]; then',
        '    if "$u" status 2>/dev/null; then break; fi',
    ]
    if sudo_quoted and helper_quoted:
        ufw_block_lines.append(f"    if {sudo_quoted} -n {helper_quoted} ufw-status 2>/dev/null; then break; fi")
    ufw_block_lines.extend(["  fi", "done"])
    ufw_block = "\n".join(ufw_block_lines)

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
echo {_SEC_DOCKER_STATUS}
{docker_block}
""".strip()
    rc, out, err = await ssh_run_shell(
        ssh_target,
        shell,
        timeout=max(SUBPROC_MEDIUM_TIMEOUT, SUBPROC_SHORT_TIMEOUT) + 4,
    )
    if rc != 0:
        error = (err or "").strip() or f"SSH завершился с кодом {rc}"
        return RemoteStatusBundle(
            ok=False,
            error=error,
            containers=[(n, False, "ssh ошибка", "-") for n in name_list] or [("Docker API", False, "ssh ошибка", "-")],
        )
    sec = _split_sections(out)
    up = _parse_uptime_from_proc(sec.get(_SEC_UPTIME, "") or "") or "н/д"
    mem = _parse_meminfo_text(sec.get(_SEC_MEMINFO, "") or "") or "н/д"
    disk = _parse_df_bytes_text(sec.get(_SEC_DF, "") or "") or "н/д"

    ufw_out = sec.get(_SEC_UFW, "") or ""
    ufw_status = _parse_ufw_status(ufw_out)
    if admin_mode:
        allow, deny, reject = _parse_ufw_rules(ufw_out)
    else:
        allow, deny, reject = [], [], []

    info: dict[str, str] = {}
    docker_ok = False
    for line in (sec.get(_SEC_DOCKER_STATUS, "") or "").splitlines():
        p = line.split("|", 1)
        if len(p) == 2:
            key, value = p[0].strip(), p[1].strip()
            if key == "__MBOT_DOCKER_OK__":
                docker_ok = value == "1"
            elif key:
                info[key] = value
    cont: list[tuple[str, bool, str, str]] = []
    if not docker_ok:
        cont = [(n, False, "docker недоступен", "-") for n in name_list] or [
            ("Docker API", False, "docker недоступен", "-")
        ]
    else:
        cont.extend((name, docker_status_is_running(status), status, "-") for name, status in info.items())
        for n in name_list:
            if n not in info:
                cont.append((n, False, "не найден", "-"))
    return RemoteStatusBundle(
        ok=True,
        uptime=up,
        memory=mem,
        disk=disk,
        containers=cont,
        ufw_status=ufw_status,
        ufw_allow=allow,
        ufw_deny=deny,
        ufw_reject=reject,
    )


async def remote_docker_containers(
    ssh_target: str,
    names: Sequence[str],
) -> list[tuple[str, bool, str, str]]:
    """Read the complete remote Docker inventory with one fixed SSH command."""

    name_list = [name for name in names if name]
    docker_candidates: list[str] = []
    for candidate in [DOCKER_BIN, "/usr/bin/docker", "docker"]:
        if candidate and candidate not in docker_candidates:
            docker_candidates.append(candidate)
    docker_for_loop = " ".join(shlex.quote(candidate) for candidate in docker_candidates) or '""'
    lines = [
        "PATH=/usr/sbin:/usr/bin:/sbin:/bin:$PATH; export PATH",
        "LC_ALL=C; LANG=C; export LC_ALL LANG",
        "_docker_done=0",
        f"for d in {docker_for_loop}; do",
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
    rc, out, err = await ssh_run_shell(
        ssh_target,
        "\n".join(lines),
        timeout=SUBPROC_MEDIUM_TIMEOUT + 4,
    )
    if rc != 0:
        status = f"ssh ошибка: {(err or '').strip() or rc}"
        return [(name, False, status, "-") for name in name_list] or [("Docker API", False, status, "-")]

    info: dict[str, str] = {}
    docker_ok = False
    for line in out.splitlines():
        parts = line.split("|", 1)
        if len(parts) != 2:
            continue
        key, value = parts[0].strip(), parts[1].strip()
        if key == "__MBOT_DOCKER_OK__":
            docker_ok = value == "1"
        elif key:
            info[key] = value
    if not docker_ok:
        return [(name, False, "docker недоступен", "-") for name in name_list] or [
            ("Docker API", False, "docker недоступен", "-")
        ]
    result = [(name, docker_status_is_running(status), status, "-") for name, status in info.items()]
    result.extend((name, False, "не найден", "-") for name in name_list if name not in info)
    return result


async def remote_docker_inspect_summary(ssh_target: str, name: str) -> str:
    cmds: list[list[str]] = []
    for docker_bin in [x for x in [DOCKER_BIN, "/usr/bin/docker", "docker"] if x]:
        cmds.append([docker_bin, "inspect", name])
    if SUDO_BIN and PRIVILEGED_HELPER_BIN:
        cmds.append([SUDO_BIN, "-n", PRIVILEGED_HELPER_BIN, "docker-inspect", name])
    rc, out, err = 127, "", "docker inspect unavailable"
    for cmd in cmds:
        rc, out, err = await ssh_run_exec(ssh_target, cmd, timeout=SUBPROC_MEDIUM_TIMEOUT)
        if rc == 0:
            break
    if rc != 0:
        return f"docker inspect error: {err.strip() or out.strip() or 'н/д'}"
    return _parse_docker_inspect_json(name, out)


async def remote_docker_logs_tail(ssh_target: str, name: str, tail: int) -> str:
    rc, out, err = 127, "", "docker logs unavailable"
    for docker_bin in [x for x in [DOCKER_BIN, "/usr/bin/docker", "docker"] if x]:
        # docker logs пишет stderr-поток контейнера в stderr — объединяем,
        # иначе часть логов теряется.
        base = " ".join(shlex.quote(str(a)) for a in [docker_bin, "logs", "--tail", str(int(tail)), name])
        rc, out, err = await ssh_run_shell(ssh_target, base + " 2>&1", timeout=SUBPROC_MEDIUM_TIMEOUT)
        if rc == 0:
            break
    if rc != 0 and SUDO_BIN and PRIVILEGED_HELPER_BIN:
        base_sudo = " ".join(
            shlex.quote(str(a)) for a in [SUDO_BIN, "-n", PRIVILEGED_HELPER_BIN, "docker-logs", name, str(int(tail))]
        )
        rc, out, err = await ssh_run_shell(ssh_target, base_sudo + " 2>&1", timeout=SUBPROC_MEDIUM_TIMEOUT)
    if rc != 0:
        return f"docker logs error: {err.strip() or out.strip() or 'н/д'}"
    return out


async def remote_tail_text_file(
    ssh_target: str,
    path: str,
    n_lines: int,
    max_bytes: int = 2_000_000,
) -> str:
    n = max(1, min(int(n_lines), 50_000))
    byte_limit = max(1, min(int(max_bytes), 3_000_000))
    quoted = shlex.quote(path)
    sudo_bin = shlex.quote(SUDO_BIN or "sudo")
    helper_bin = shlex.quote(PRIVILEGED_HELPER_BIN)
    cmd = (
        f"if [ ! -e {quoted} ]; then printf '__FNF__'; "
        f"elif [ -r {quoted} ]; then tail -n {n} {quoted} | tail -c {byte_limit}; "
        f"else {sudo_bin} -n {helper_bin} file-tail {quoted} {n} {byte_limit} "
        "2>/dev/null || printf '__PERM__'; fi"
    )
    rc, out, err = await ssh_run_shell(
        ssh_target,
        cmd,
        timeout=SUBPROC_MEDIUM_TIMEOUT,
        max_output_bytes=byte_limit + 8192,
    )
    if out.startswith("__FNF__"):
        raise FileNotFoundError(path)
    if out.startswith("__PERM__"):
        raise PermissionError(path)
    if rc != 0 and (err or "").strip():
        raise RuntimeError(err.strip())
    return out


async def remote_fail2ban_stat(ssh_target: str, path: str) -> tuple[int, datetime] | None:
    identity = await remote_fail2ban_identity(ssh_target, path)
    return (identity.size, identity.mtime) if identity else None


async def remote_fail2ban_identity(ssh_target: str, path: str) -> FileIdentity | None:
    quoted = shlex.quote(path)
    sudo_bin = shlex.quote(SUDO_BIN or "sudo")
    helper_bin = shlex.quote(PRIVILEGED_HELPER_BIN)
    for cmd in (
        f"stat -c '%s|%Y|%d|%i' {quoted} 2>/dev/null || true",
        f"{sudo_bin} -n {helper_bin} file-stat {quoted} 2>/dev/null || true",
    ):
        rc, out, _ = await ssh_run_shell(ssh_target, cmd, timeout=SUBPROC_SHORT_TIMEOUT)
        if rc != 0 or not out.strip():
            continue
        try:
            parts = out.strip().split("|")
            size_s, mtime_s = parts[0], parts[1]
            return FileIdentity(
                size=int(size_s),
                mtime=datetime.fromtimestamp(int(mtime_s), tz=TZ),
                device=int(parts[2]) if len(parts) > 2 else 0,
                inode=int(parts[3]) if len(parts) > 3 else 0,
            )
        except Exception:
            logger.debug("remote_fail2ban_stat parse failed for %s", ssh_target)
            return None
    return None


async def remote_read_text_range(
    ssh_target: str,
    path: str,
    offset: int,
    max_bytes: int,
) -> tuple[str, int]:
    offset = max(0, int(offset))
    limit = max(1, min(int(max_bytes), 3_000_000))
    quoted = shlex.quote(path)
    sudo_bin = shlex.quote(SUDO_BIN or "sudo")
    helper_bin = shlex.quote(PRIVILEGED_HELPER_BIN)
    # base64 keeps the SSH stdout wrapper from changing trailing newlines and
    # therefore preserves the exact byte cursor.
    direct = f"tail -c +{offset + 1} -- {quoted} | head -c {limit}"
    privileged = f"{sudo_bin} -n {helper_bin} file-read {quoted} {offset} {limit}"
    command = (
        f"if [ ! -e {quoted} ]; then printf '__FNF__'; "
        f"elif [ -r {quoted} ]; then ({direct}) | base64 | tr -d '\\n'; "
        f"elif _mbot_data=$({privileged} 2>/dev/null); then printf '%s' \"$_mbot_data\"; "
        "else exit 77; fi"
    )
    encoded_limit = ((limit + 2) // 3) * 4 + 8192
    rc, out, err = await ssh_run_shell(
        ssh_target,
        command,
        timeout=SUBPROC_MEDIUM_TIMEOUT + 4,
        max_output_bytes=encoded_limit,
    )
    if out.startswith("__FNF__"):
        raise FileNotFoundError(path)
    if rc != 0:
        raise RuntimeError((err or "remote file read failed").strip())
    try:
        data = base64.b64decode(out.strip(), validate=True) if out.strip() else b""
    except (binascii.Error, ValueError) as exc:
        raise RuntimeError("invalid base64 from remote file reader") from exc
    if len(data) > limit:
        raise RuntimeError("remote file reader exceeded requested byte limit")
    return data.decode("utf-8", errors="replace"), len(data)


async def remote_fail2ban_events(
    ssh_target: str,
    path: str,
    n_lines: int,
    *,
    timezone: ZoneInfo = TZ,
    max_bytes: int = 2_000_000,
) -> list[Fail2banEvent]:
    raw = await remote_tail_text_file(ssh_target, path=path, n_lines=n_lines, max_bytes=max_bytes)
    return parse_fail2ban_events(raw.splitlines(), timezone=timezone)
