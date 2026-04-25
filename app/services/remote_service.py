import re
import shlex
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

from ..config import DOCKER_BIN, SSH_BIN, SUBPROC_MEDIUM_TIMEOUT, SUBPROC_SHORT_TIMEOUT, SUDO_BIN, TZ, UFW_BIN, logger
from .docker_service import _parse_docker_inspect_json
from .system_service import Fail2banEvent, _fmt_bytes_binary, _parse_ufw_rules, parse_fail2ban_events, run_exec

_OUT_BEGIN = "__MBOT_OUT_BEGIN_43e1f3c4__"
_OUT_END = "__MBOT_OUT_END_43e1f3c4__"
_SEC_UPTIME = "__MBOT_SEC_UPTIME__"
_SEC_MEMINFO = "__MBOT_SEC_MEMINFO__"
_SEC_DF = "__MBOT_SEC_DF__"
_SEC_UFW = "__MBOT_SEC_UFW__"
_SEC_DOCKER_STATUS = "__MBOT_SEC_DOCKER_STATUS__"
_SSH_TARGET_WITH_PORT_RE = re.compile(r"^(?P<host>[A-Za-z0-9_.@\-\[\]:]+):(?P<port>\d{1,5})$")


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


def _split_ssh_target(target: str) -> Tuple[str, Optional[int]]:
    tgt = (target or "").strip()
    m = _SSH_TARGET_WITH_PORT_RE.fullmatch(tgt)
    if not m:
        return tgt, None
    host = m.group("host")
    port = int(m.group("port"))
    if not 1 <= port <= 65535:
        raise ValueError("ssh port out of range")
    return host, port


async def ssh_run_shell(target: str, command: str, timeout: int) -> Tuple[int, str, str]:
    tgt = (target or "").strip()
    if not tgt:
        return 127, "", "ssh target is not configured"
    try:
        ssh_host, ssh_port = _split_ssh_target(tgt)
    except ValueError as e:
        return 127, "", str(e)
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
    ]
    if ssh_port is not None:
        args.extend(["-p", str(ssh_port)])
    args.extend([ssh_host, "sh", "-c", wrapped])
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
""".strip()
    rc, out, _ = await ssh_run_shell(ssh_target, shell, timeout=max(SUBPROC_MEDIUM_TIMEOUT, SUBPROC_SHORT_TIMEOUT) + 4)
    if rc != 0 and not out.strip():
        return "н/д", "н/д", "н/д", [(n, False, "ssh ошибка", "-") for n in name_list], "н/д", [], [], []
    if rc != 0:
        logger.debug("remote_status_bundle: rc=%s for %s, partial output present", rc, ssh_target)

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
    cont: List[Tuple[str, bool, str, str]] = []
    if not info:
        cont = [(n, False, "docker недоступен", "-") for n in name_list]
    else:
        for n in name_list:
            st = info.get(n)
            if st is None:
                cont.append((n, False, "не найден", "-"))
            else:
                cont.append((n, st.lower().startswith("up"), st, "-"))
    return up, mem, disk, cont, ufw_status, allow, deny, reject


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
    return _parse_docker_inspect_json(name, out)


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
    return out if out.strip() else err


async def remote_tail_text_file(ssh_target: str, path: str, n_lines: int) -> str:
    n = max(1, min(int(n_lines), 10000))
    quoted = shlex.quote(path)
    sudo_bin = shlex.quote(SUDO_BIN or "sudo")
    cmd = (
        f"if [ ! -e {quoted} ]; then printf '__FNF__'; "
        f"elif [ -r {quoted} ]; then tail -n {n} {quoted}; "
        f"else {sudo_bin} -n tail -n {n} {quoted} 2>/dev/null || printf '__PERM__'; fi"
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
    sudo_bin = shlex.quote(SUDO_BIN or "sudo")
    for cmd in (
        f"stat -c '%s|%Y' {quoted} 2>/dev/null || true",
        f"{sudo_bin} -n stat -c '%s|%Y' {quoted} 2>/dev/null || true",
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
