import json
import re
import shlex
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

from ..config import DOCKER_BIN, SSH_BIN, SUBPROC_MEDIUM_TIMEOUT, SUBPROC_SHORT_TIMEOUT, SUDO_BIN, TZ, UFW_BIN, logger
from .system_service import Fail2banEvent, _fmt_bytes_binary, _parse_ufw_rules, parse_fail2ban_events, run_exec

_OUT_BEGIN = "__MBOT_OUT_BEGIN_43e1f3c4__"
_OUT_END = "__MBOT_OUT_END_43e1f3c4__"
_SEC_UPTIME = "__MBOT_SEC_UPTIME__"
_SEC_MEMINFO = "__MBOT_SEC_MEMINFO__"
_SEC_DF = "__MBOT_SEC_DF__"
_SEC_UFW = "__MBOT_SEC_UFW__"
_SEC_DOCKER_STATUS = "__MBOT_SEC_DOCKER_STATUS__"
_SEC_DOCKER_RESTARTS = "__MBOT_SEC_DOCKER_RESTARTS__"


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


def _split_sections(text: str) -> Dict[str, str]:
    lines = (text or "").splitlines()
    markers = {
        _SEC_UPTIME,
        _SEC_MEMINFO,
        _SEC_DF,
        _SEC_UFW,
        _SEC_DOCKER_STATUS,
        _SEC_DOCKER_RESTARTS,
    }
    cur = None
    buf: Dict[str, List[str]] = {}
    for ln in lines:
        if ln in markers:
            cur = ln
            buf.setdefault(cur, [])
            continue
        if cur:
            buf[cur].append(ln)
    return {k: "\n".join(v).strip("\n") for k, v in buf.items()}


async def ssh_run_shell(target: str, command: str, timeout: int) -> Tuple[int, str, str]:
    tgt = (target or "").strip()
    if not tgt:
        return 127, "", "ssh target is not configured"
    wrapped = (
        "PATH=/usr/sbin:/usr/bin:/sbin:/bin:$PATH; export PATH; "
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
        tgt,
        "sh",
        "-c",
        wrapped,
    ]
    rc, out, err = await run_exec(args, timeout=max(timeout + 2, 5))
    return rc, _extract_wrapped_stdout(out), err


async def ssh_run_exec(target: str, argv: Sequence[str], timeout: int) -> Tuple[int, str, str]:
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
    parts: List[str] = []
    if days:
        parts.append(f"{days} д")
    if hours:
        parts.append(f"{hours} ч")
    if minutes or not parts:
        parts.append(f"{minutes} м")
    return " ".join(parts)


def _parse_meminfo_text(raw: str) -> str:
    try:
        kv: Dict[str, int] = {}
        for line in (raw or "").splitlines():
            m = re.match(r"^(\w+):\s+(\d+)\s+kB$", line.strip())
            if m:
                kv[m.group(1)] = int(m.group(2))
        mem_total_kb = kv.get("MemTotal", 0)
        mem_avail_kb = kv.get("MemAvailable", kv.get("MemFree", 0))
        mem_used_kb = max(mem_total_kb - mem_avail_kb, 0)
        sw_total_kb = kv.get("SwapTotal", 0)
        sw_free_kb = kv.get("SwapFree", 0)
        sw_used_kb = max(sw_total_kb - sw_free_kb, 0)

        def kb_to_mib(x: int) -> int:
            return int(round(x / 1024.0))

        mem_s = f"{kb_to_mib(mem_used_kb)} / {kb_to_mib(mem_total_kb)} MiB (avail {kb_to_mib(mem_avail_kb)} MiB)"
        sw_s = f"{kb_to_mib(sw_used_kb)} / {kb_to_mib(sw_total_kb)} MiB" if sw_total_kb else "н/д"
        return f"RAM: {mem_s}; Swap: {sw_s}"
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
        return f"{_fmt_bytes_binary(used)} / {_fmt_bytes_binary(total)} (avail {_fmt_bytes_binary(free)}, {usep}%) mount /"
    except Exception:
        return "н/д"


async def remote_check_uptime(ssh_target: str) -> str:
    rc, out, _ = await ssh_run_shell(ssh_target, "cat /proc/uptime 2>/dev/null || true", timeout=SUBPROC_SHORT_TIMEOUT)
    if rc == 0 and out.strip():
        parsed = _parse_uptime_from_proc(out)
        if parsed != "н/д":
            return parsed
    rc, out, _ = await ssh_run_exec(ssh_target, ["uptime", "-p"], timeout=SUBPROC_SHORT_TIMEOUT)
    return out.strip() if rc == 0 and out.strip() else "н/д"


async def remote_meminfo(ssh_target: str) -> str:
    rc, out, _ = await ssh_run_shell(ssh_target, "cat /proc/meminfo 2>/dev/null || true", timeout=SUBPROC_SHORT_TIMEOUT)
    if rc == 0 and out.strip():
        parsed = _parse_meminfo_text(out)
        if parsed != "н/д":
            return parsed
    rc, out, _ = await ssh_run_exec(ssh_target, ["free", "-m"], timeout=SUBPROC_SHORT_TIMEOUT)
    if rc != 0:
        return "н/д"
    lines = out.splitlines()
    if len(lines) < 2:
        return "н/д"
    mem = re.split(r"\s+", lines[1].strip())
    swp = re.split(r"\s+", lines[2].strip()) if len(lines) > 2 else []
    try:
        mem_total = int(mem[1])
        mem_used = int(mem[2])
        mem_free = int(mem[3])
        mem_s = f"{mem_used} / {mem_total} MiB (free {mem_free} MiB)"
    except Exception:
        mem_s = "н/д"
    try:
        if swp and swp[0].lower().startswith("swap"):
            sw_total = int(swp[1])
            sw_used = int(swp[2])
            sw_s = f"{sw_used} / {sw_total} MiB"
        else:
            sw_s = "н/д"
    except Exception:
        sw_s = "н/д"
    return f"RAM: {mem_s}; Swap: {sw_s}"


async def remote_disk_root(ssh_target: str) -> str:
    rc, out, _ = await ssh_run_exec(ssh_target, ["df", "-B1", "/"], timeout=SUBPROC_SHORT_TIMEOUT)
    if rc == 0 and out.strip():
        parsed = _parse_df_bytes_text(out)
        if parsed != "н/д":
            return parsed
    rc, out, _ = await ssh_run_exec(ssh_target, ["df", "-h", "/"], timeout=SUBPROC_SHORT_TIMEOUT)
    if rc != 0:
        return "н/д"
    lines = out.splitlines()
    if len(lines) < 2:
        return "н/д"
    parts = re.split(r"\s+", lines[1].strip())
    if len(parts) >= 6:
        size, used, avail, usep, mnt = parts[1], parts[2], parts[3], parts[4], parts[5]
        return f"{used} / {size} (avail {avail}, {usep}) mount {mnt}"
    return "н/д"


async def remote_ufw_status_basic(ssh_target: str) -> str:
    ufw_candidates = [UFW_BIN, "/usr/sbin/ufw", "ufw"]
    cmds: List[str] = []
    for ufw_bin in ufw_candidates:
        if ufw_bin and f"{ufw_bin} status" not in cmds:
            cmds.append(f"{ufw_bin} status")
        if ufw_bin and SUDO_BIN:
            sudo_cmd = f"{SUDO_BIN} -n {ufw_bin} status"
            if sudo_cmd not in cmds:
                cmds.append(sudo_cmd)
    for cmd in cmds:
        rc, out, _ = await ssh_run_shell(ssh_target, cmd, timeout=SUBPROC_SHORT_TIMEOUT)
        if rc == 0 and out.strip():
            first = (out.strip().splitlines()[:1] or [""])[0].lower()
            if "active" in first:
                return "active"
            if "inactive" in first:
                return "inactive"
    return "н/д"


async def remote_ufw_summary_for_admin(ssh_target: str) -> Tuple[str, List[str], List[str], List[str]]:
    ufw_candidates = [UFW_BIN, "/usr/sbin/ufw", "ufw"]
    cmds: List[str] = []
    for ufw_bin in ufw_candidates:
        if ufw_bin and f"{ufw_bin} status" not in cmds:
            cmds.append(f"{ufw_bin} status")
        if ufw_bin and SUDO_BIN:
            sudo_cmd = f"{SUDO_BIN} -n {ufw_bin} status"
            if sudo_cmd not in cmds:
                cmds.append(sudo_cmd)
    for cmd in cmds:
        rc, out, _ = await ssh_run_shell(ssh_target, cmd, timeout=SUBPROC_SHORT_TIMEOUT)
        if rc == 0 and out.strip():
            first = (out.strip().splitlines()[:1] or [""])[0].lower()
            status = "active" if "active" in first else ("inactive" if "inactive" in first else "н/д")
            allow, deny, reject = _parse_ufw_rules(out)
            return status, allow, deny, reject
    return "н/д", [], [], []


async def remote_status_bundle(
    ssh_target: str,
    names: Sequence[str],
    *,
    admin_mode: bool,
) -> Tuple[str, str, str, List[Tuple[str, bool, str, str]], str, List[str], List[str], List[str]]:
    name_list = [n for n in names if n]
    shell = f"""
PATH=/usr/sbin:/usr/bin:/sbin:/bin:$PATH; export PATH
echo {_SEC_UPTIME}
cat /proc/uptime 2>/dev/null || true
echo {_SEC_MEMINFO}
cat /proc/meminfo 2>/dev/null || true
echo {_SEC_DF}
df -B1 / 2>/dev/null || true
echo {_SEC_UFW}
ufw_out=""
for u in {shlex.quote(UFW_BIN)} /usr/sbin/ufw ufw; do
  [ -n "$u" ] || continue
  if command -v "$u" >/dev/null 2>&1 || [ -x "$u" ]; then
    ufw_out=$("$u" status 2>/dev/null) && break
    if [ -n {shlex.quote(SUDO_BIN)} ]; then
      ufw_out=$({shlex.quote(SUDO_BIN)} -n "$u" status 2>/dev/null) && break
    fi
  fi
done
printf "%s\\n" "$ufw_out"
echo {_SEC_DOCKER_STATUS}
docker_cmd=""
for d in {shlex.quote(DOCKER_BIN)} /usr/bin/docker docker; do
  [ -n "$d" ] || continue
  if command -v "$d" >/dev/null 2>&1 || [ -x "$d" ]; then
    "$d" ps -a --format '{{{{.Names}}}}|{{{{.Status}}}}' >/dev/null 2>&1 && docker_cmd="$d" && break
    if [ -n {shlex.quote(SUDO_BIN)} ] && {shlex.quote(SUDO_BIN)} -n "$d" ps -a --format '{{{{.Names}}}}|{{{{.Status}}}}' >/dev/null 2>&1; then
      docker_cmd="{shlex.quote(SUDO_BIN)} -n $d"
      break
    fi
  fi
done
if [ -n "$docker_cmd" ]; then
  sh -c "$docker_cmd ps -a --format '{{{{.Names}}}}|{{{{.Status}}}}' 2>/dev/null" || true
fi
echo {_SEC_DOCKER_RESTARTS}
if [ -n "$docker_cmd" ]; then
  sh -c "$docker_cmd ps -a --format '{{{{.Names}}}}|{{{{.RestartCount}}}}' 2>/dev/null" || true
fi
""".strip()
    rc, out, _ = await ssh_run_shell(ssh_target, shell, timeout=max(SUBPROC_MEDIUM_TIMEOUT, SUBPROC_SHORT_TIMEOUT) + 4)
    if rc != 0 and not out.strip():
        return "н/д", "н/д", "н/д", [(n, False, "ssh ошибка", "-") for n in name_list], "н/д", [], [], []

    sec = _split_sections(out)
    up = _parse_uptime_from_proc(sec.get(_SEC_UPTIME, "") or "") or "н/д"
    if up == "н/д":
        up = "н/д"
    mem = _parse_meminfo_text(sec.get(_SEC_MEMINFO, "") or "") or "н/д"
    disk = _parse_df_bytes_text(sec.get(_SEC_DF, "") or "") or "н/д"

    ufw_out = sec.get(_SEC_UFW, "") or ""
    first = (ufw_out.strip().splitlines()[:1] or [""])[0].lower()
    ufw_status = "active" if "active" in first else ("inactive" if "inactive" in first else "н/д")
    if admin_mode:
        allow, deny, reject = _parse_ufw_rules(ufw_out)
    else:
        allow, deny, reject = [], [], []

    info: Dict[str, str] = {}
    for line in (sec.get(_SEC_DOCKER_STATUS, "") or "").splitlines():
        p = line.split("|", 1)
        if len(p) == 2:
            info[p[0].strip()] = p[1].strip()
    restarts: Dict[str, str] = {}
    for line in (sec.get(_SEC_DOCKER_RESTARTS, "") or "").splitlines():
        p = line.split("|", 1)
        if len(p) == 2:
            restarts[p[0].strip()] = p[1].strip()

    cont: List[Tuple[str, bool, str, str]] = []
    if not info and not restarts:
        cont = [(n, False, "docker недоступен", "-") for n in name_list]
    else:
        for n in name_list:
            st = info.get(n)
            if st is None:
                cont.append((n, False, "не найден", restarts.get(n, "-")))
            else:
                cont.append((n, st.lower().startswith("up"), st, restarts.get(n, "-")))
    return up, mem, disk, cont, ufw_status, allow, deny, reject


async def remote_docker_containers(ssh_target: str, names: Sequence[str]) -> List[Tuple[str, bool, str, str]]:
    docker_candidates = [DOCKER_BIN, "/usr/bin/docker", "docker"]
    docker_prefix: List[str] = []
    rc = 127
    for docker_bin in [x for x in docker_candidates if x]:
        rc, _, _ = await ssh_run_exec(ssh_target, [docker_bin, "info"], timeout=SUBPROC_SHORT_TIMEOUT)
        if rc == 0:
            docker_prefix = [docker_bin]
            break
        if SUDO_BIN:
            rc, _, _ = await ssh_run_exec(
                ssh_target,
                [SUDO_BIN, "-n", docker_bin, "info"],
                timeout=SUBPROC_SHORT_TIMEOUT,
            )
            if rc == 0:
                docker_prefix = [SUDO_BIN, "-n", docker_bin]
                break
    if rc != 0:
        return [(n, False, "docker недоступен", "-") for n in names]

    rc, out, _ = await ssh_run_exec(
        ssh_target,
        [*docker_prefix, "ps", "-a", "--format", "{{.Names}}|{{.Status}}"],
        timeout=SUBPROC_MEDIUM_TIMEOUT,
    )
    if rc != 0:
        return [(n, False, "ошибка docker ps", "-") for n in names]
    info: Dict[str, str] = {}
    for line in out.splitlines():
        parts = line.split("|", 1)
        if len(parts) == 2:
            info[parts[0].strip()] = parts[1].strip()

    rc2, out2, _ = await ssh_run_exec(
        ssh_target,
        [*docker_prefix, "ps", "-a", "--format", "{{.Names}}|{{.RestartCount}}"],
        timeout=SUBPROC_MEDIUM_TIMEOUT,
    )
    restarts: Dict[str, str] = {}
    if rc2 == 0:
        for ln in out2.splitlines():
            parts = ln.split("|", 1)
            if len(parts) == 2:
                restarts[parts[0].strip()] = parts[1].strip()

    result: List[Tuple[str, bool, str, str]] = []
    for n in names:
        st = info.get(n)
        if st is None:
            result.append((n, False, "не найден", restarts.get(n, "-")))
        else:
            result.append((n, st.lower().startswith("up"), st, restarts.get(n, "-")))
    return result


async def remote_docker_inspect_summary(ssh_target: str, name: str) -> str:
    cmds: List[List[str]] = []
    for docker_bin in [x for x in [DOCKER_BIN, "/usr/bin/docker", "docker"] if x]:
        cmds.append([docker_bin, "inspect", name])
        if SUDO_BIN:
            cmds.append([SUDO_BIN, "-n", docker_bin, "inspect", name])
    rc, out, err = 127, "", "docker inspect unavailable"
    for cmd in cmds:
        rc, out, err = await ssh_run_exec(ssh_target, cmd, timeout=SUBPROC_MEDIUM_TIMEOUT)
        if rc == 0:
            break
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
                    port_items.append(f"{k}->-")
                elif isinstance(v, list) and v:
                    b = v[0]
                    port_items.append(f"{k}->{b.get('HostIp', '')}:{b.get('HostPort', '')}")
                else:
                    port_items.append(f"{k}")
        lines: List[str] = [
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


async def remote_docker_logs_tail(ssh_target: str, name: str, tail: int) -> str:
    rc, out, err = 127, "", "docker logs unavailable"
    for docker_bin in [x for x in [DOCKER_BIN, "/usr/bin/docker", "docker"] if x]:
        rc, out, err = await ssh_run_exec(
            ssh_target,
            [docker_bin, "logs", "--tail", str(int(tail)), name],
            timeout=SUBPROC_MEDIUM_TIMEOUT,
        )
        if rc == 0:
            break
        if SUDO_BIN:
            rc, out, err = await ssh_run_exec(
                ssh_target,
                [SUDO_BIN, "-n", docker_bin, "logs", "--tail", str(int(tail)), name],
                timeout=SUBPROC_MEDIUM_TIMEOUT,
            )
            if rc == 0:
                break
    if rc != 0:
        return f"docker logs error: {err.strip() or out.strip() or 'н/д'}"
    return out


async def remote_resolve_a_record_system(ssh_target: str, domain: str) -> List[str]:
    dom = (domain or "").strip()
    if not dom:
        return []
    rc, out, _ = await ssh_run_shell(
        ssh_target,
        f"getent ahostsv4 {shlex.quote(dom)} 2>/dev/null || true",
        timeout=SUBPROC_SHORT_TIMEOUT,
    )
    if rc != 0 and not out:
        return []
    ips: List[str] = []
    for ln in out.splitlines():
        parts = ln.split()
        if not parts:
            continue
        ip = parts[0].strip()
        if re.fullmatch(r"(?:\d{1,3}\.){3}\d{1,3}", ip) and ip not in ips:
            ips.append(ip)
    return ips


async def remote_tail_text_file(ssh_target: str, path: str, n_lines: int) -> str:
    n = max(1, min(int(n_lines), 10000))
    quoted = shlex.quote(path)
    cmd = (
        f"if [ ! -e {quoted} ]; then printf '__FNF__'; "
        f"elif [ -r {quoted} ]; then tail -n {n} {quoted}; "
        f"else {SUDO_BIN} -n tail -n {n} {quoted} 2>/dev/null || printf '__PERM__'; fi"
    )
    rc, out, err = await ssh_run_shell(ssh_target, cmd, timeout=SUBPROC_MEDIUM_TIMEOUT)
    if out.startswith("__FNF__"):
        raise FileNotFoundError(path)
    if out.startswith("__PERM__"):
        raise PermissionError(path)
    if rc != 0 and (err or "").strip():
        raise RuntimeError(err.strip())
    return out


async def remote_fail2ban_stat(ssh_target: str, path: str) -> Optional[Tuple[int, datetime]]:
    quoted = shlex.quote(path)
    for cmd in (
        f"stat -c '%s|%Y' {quoted} 2>/dev/null || true",
        f"{SUDO_BIN} -n stat -c '%s|%Y' {quoted} 2>/dev/null || true",
    ):
        rc, out, _ = await ssh_run_shell(ssh_target, cmd, timeout=SUBPROC_SHORT_TIMEOUT)
        if rc != 0 or not out.strip():
            continue
        try:
            size_s, mtime_s = out.strip().split("|", 1)
            return int(size_s), datetime.fromtimestamp(int(mtime_s), tz=TZ)
        except Exception:
            logger.debug("remote_fail2ban_stat parse failed for %s", ssh_target)
            return None
    return None


async def remote_fail2ban_events_last_day(ssh_target: str, path: str) -> List[Fail2banEvent]:
    raw = await remote_tail_text_file(ssh_target, path=path, n_lines=20000)
    events = parse_fail2ban_events(raw.splitlines())
    until = datetime.now(tz=TZ)
    since = until - timedelta(days=1)
    return [e for e in events if since <= e.ts <= until]
