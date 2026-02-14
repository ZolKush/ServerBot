import asyncio
import json
import os
import re
import shutil
import socket
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import aiofiles

from ..config import (
    PING_BIN,
    SUBPROC_MEDIUM_TIMEOUT,
    SUBPROC_SHORT_TIMEOUT,
    TZ,
    UFW_BIN,
    SUDO_BIN,
    logger,
)

try:
    import aiodns  # type: ignore
except Exception:  # pragma: no cover - fallback when aiodns is missing
    aiodns = None


async def run_exec(args: Sequence[str], timeout: int) -> Tuple[int, str, str]:
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.DEVNULL,
        )
    except FileNotFoundError:
        return 127, "", f"command not found: {args[0]}"
    except Exception as e:
        return 127, "", f"spawn error: {e}"

    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except Exception:
            pass
        try:
            await proc.wait()
        except Exception:
            pass
        return 124, "", "timeout"

    return (
        int(proc.returncode or 0),
        (out or b"").decode(errors="ignore"),
        (err or b"").decode(errors="ignore"),
    )


# ============================================================
#                       METRICS
# ============================================================

def _fmt_bytes_binary(n: int) -> str:
    if n < 0:
        return "0 B"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    v = float(n)
    idx = 0
    while v >= 1024 and idx < len(units) - 1:
        v /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(v)} {units[idx]}"
    return f"{v:.1f} {units[idx]}"


async def check_uptime() -> str:
    try:
        raw = await asyncio.to_thread(Path("/proc/uptime").read_text, encoding="utf-8")
        seconds = int(float(raw.split()[0]))
    except Exception:
        rc, out, _ = await run_exec(["uptime", "-p"], timeout=SUBPROC_SHORT_TIMEOUT)
        return out.strip() if rc == 0 else "н/д"

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


async def loadavg() -> str:
    try:
        raw = await asyncio.to_thread(Path("/proc/loadavg").read_text, encoding="utf-8")
        parts = raw.strip().split()
        return f"{parts[0]} / {parts[1]} / {parts[2]}" if len(parts) >= 3 else "н/д"
    except Exception:
        return "н/д"


async def meminfo() -> str:
    try:
        raw = await asyncio.to_thread(Path("/proc/meminfo").read_text, encoding="utf-8")
        kv: Dict[str, int] = {}
        for line in raw.splitlines():
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
        rc, out, _ = await run_exec(["free", "-m"], timeout=SUBPROC_SHORT_TIMEOUT)
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


async def disk_root() -> str:
    try:
        usage = await asyncio.to_thread(shutil.disk_usage, "/")
        total = usage.total
        used = usage.used
        free = usage.free

        usep = int(round((used / total) * 100)) if total else 0
        return f"{_fmt_bytes_binary(used)} / {_fmt_bytes_binary(total)} (avail {_fmt_bytes_binary(free)}, {usep}%) mount /"
    except Exception:
        rc, out, _ = await run_exec(["df", "-h", "/"], timeout=SUBPROC_SHORT_TIMEOUT)
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


# ============================================================
#                       FIREWALL (UFW)
# ============================================================

def _ufw_candidates() -> List[List[str]]:
    bases: List[str] = []
    for b in [UFW_BIN, "ufw"]:
        if b and b not in bases:
            bases.append(b)
    cmds: List[List[str]] = []
    for b in bases:
        cmds.append([b, "status"])
        if SUDO_BIN:
            cmds.append([SUDO_BIN, "-n", b, "status"])
    return cmds


async def ufw_status_basic() -> str:
    candidates = _ufw_candidates()

    out = ""
    for args in candidates:
        rc, o, _ = await run_exec(args, timeout=SUBPROC_SHORT_TIMEOUT)
        if rc == 0 and (o or "").strip():
            out = o
            break

    if not out:
        return "н/д"

    first = (out.strip().splitlines()[:1] or [""])[0].lower()
    if "active" in first:
        return "active"
    if "inactive" in first:
        return "inactive"
    return "н/д"


def _parse_ufw_rules(out: str) -> Tuple[List[str], List[str], List[str]]:
    allow: List[str] = []
    deny: List[str] = []
    reject: List[str] = []

    lines = [ln.rstrip() for ln in (out or "").splitlines()]
    if not lines:
        return allow, deny, reject

    start_idx = 0
    for i, ln in enumerate(lines[:10]):
        if re.search(r"\bTo\b", ln) and re.search(r"\bAction\b", ln):
            start_idx = i + 1
            break

    for ln in lines[start_idx:]:
        if not ln.strip():
            continue

        parts = [p.strip() for p in re.split(r"\s{2,}", ln.strip()) if p.strip()]
        if len(parts) < 2:
            continue
        to, action = parts[0], parts[1].upper()
        src = parts[2] if len(parts) > 2 else ""
        item = to.strip()
        if not item:
            continue
        if src and src.lower() not in {"anywhere", "anywhere (v6)"}:
            item = f"{item} <- {src}"
        if action.startswith("ALLOW"):
            allow.append(item)
        elif action.startswith("DENY"):
            deny.append(item)
        elif action.startswith("REJECT"):
            reject.append(item)

    def uniq(xs: List[str]) -> List[str]:
        seen: set[str] = set()
        outl: List[str] = []
        for x in xs:
            if x not in seen:
                seen.add(x)
                outl.append(x)
        return outl

    return uniq(allow), uniq(deny), uniq(reject)


async def ufw_summary_for_admin() -> Tuple[str, List[str], List[str], List[str]]:
    candidates = _ufw_candidates()

    out = ""
    for args in candidates:
        rc, o, _ = await run_exec(args, timeout=SUBPROC_SHORT_TIMEOUT)
        if rc == 0 and (o or "").strip():
            out = o
            break

    if not out:
        return "н/д", [], [], []

    status = "н/д"
    first = (out.strip().splitlines()[:1] or [""])[0].lower()
    if "active" in first:
        status = "active"
    elif "inactive" in first:
        status = "inactive"

    allow, deny, reject = _parse_ufw_rules(out)
    return status, allow, deny, reject


# ============================================================
#                       NETWORK / DNS
# ============================================================

_HOST_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9.\-]{0,252}$")


async def ping_host(host: str, count: int, timeout_sec: int) -> Tuple[bool, Optional[float]]:
    if not host or not _HOST_RE.fullmatch(host):
        return False, None

    args = [PING_BIN, "-c", str(max(1, count)), "-W", str(max(1, timeout_sec)), host]
    rc, out, _ = await run_exec(args, timeout=max(2, timeout_sec * (count + 2)))
    if rc != 0:
        return False, None

    rtt = None
    for line in out.splitlines():
        if "rtt min/avg/max" in line or "round-trip min/avg/max" in line:
            try:
                part = line.split("=")[1].strip().split(" ")[0]
                rtt = float(part.split("/")[1])
            except Exception:
                rtt = None
            break
    return True, rtt


async def resolve_a_record(domain: str, resolver: Optional[str] = None, timeout: float = 2.0) -> List[str]:
    dom = (domain or "").strip()
    if not dom or not _HOST_RE.fullmatch(dom):
        return []

    if aiodns is not None:
        try:
            if resolver:
                res = aiodns.DNSResolver(nameservers=[resolver], timeout=timeout)
            else:
                res = aiodns.DNSResolver(timeout=timeout)
            ans = await res.query(dom, "A")
            ips = [a.host for a in ans if getattr(a, "host", None)]
            return list(dict.fromkeys(ips))
        except Exception:
            pass

    try:
        infos = await asyncio.get_running_loop().getaddrinfo(dom, None, family=socket.AF_INET)
        ips: List[str] = []
        for info in infos:
            addr = info[4]
            if addr and addr[0] not in ips:
                ips.append(addr[0])
        return ips
    except Exception:
        return []


# ============================================================
#                       FAIL2BAN (READ-ONLY)
# ============================================================

F2B_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d{3})?)\s+"
    r"(?P<logger>\S+)\s+\[\d+\]:\s+"
    r"(?P<level>[A-Z]+)\s+\[(?P<jail>[^\]]+)\]\s+"
    r"(?P<msg>.+?)\s*$"
)

F2B_IP_RE = re.compile(r"(?P<ip>\b(?:\d{1,3}\.){3}\d{1,3}\b|\b[0-9a-fA-F:]{2,}\b)")


def _f2b_parse_time(ts: str) -> Optional[datetime]:
    ts = (ts or "").strip()
    if not ts:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S,%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(ts, fmt)
            return dt.replace(tzinfo=TZ)
        except Exception:
            continue
    return None


async def load_json_file(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        async with aiofiles.open(p, "r", encoding="utf-8") as f:
            raw = await f.read()
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


async def save_json_file(path: str, data: Dict[str, Any]) -> None:
    try:
        p = Path(path)
        await asyncio.to_thread(p.parent.mkdir, parents=True, exist_ok=True)
        payload = json.dumps(data, ensure_ascii=False, indent=2)
        tmp = p.with_suffix(p.suffix + ".tmp")
        async with aiofiles.open(tmp, "w", encoding="utf-8") as f:
            await f.write(payload)
        await asyncio.to_thread(tmp.replace, p)
    except Exception as e:
        logger.warning("Не удалось сохранить %s: %s", path, e)


def tail_text_file(path: str, n_lines: int, max_bytes: int = 2_000_000) -> str:
    n_lines = max(1, min(int(n_lines), 10000))
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)

    with p.open("rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()

        read_size = min(size, max_bytes)
        f.seek(-read_size, os.SEEK_END)
        buf = f.read(read_size)

    text = buf.decode("utf-8", errors="replace")
    lines = text.splitlines()
    tail = lines[-n_lines:] if len(lines) > n_lines else lines
    return "\n".join(tail)


async def tail_text_file_async(path: str, n_lines: int, max_bytes: int = 2_000_000) -> str:
    return await asyncio.to_thread(tail_text_file, path, n_lines, max_bytes)


def _read_fail2ban_chunk_sync(
    log_path: str,
    state: Dict[str, Any],
    max_read_bytes: int,
) -> Tuple[List[str], Optional[Dict[str, Any]]]:
    p = Path(log_path)
    if not p.exists():
        return [], None

    try:
        stat = p.stat()
        inode = int(getattr(stat, "st_ino", 0) or 0)
        size = int(stat.st_size)
    except Exception:
        return [], None

    offset = int(state.get("offset", 0) or 0)
    last_inode = int(state.get("inode", 0) or 0)

    if last_inode and inode and inode != last_inode:
        offset = 0

    if offset > size:
        offset = 0

    if size - offset > max_read_bytes:
        offset = max(0, size - max_read_bytes)

    lines_out: List[str] = []
    try:
        with p.open("rb") as f:
            f.seek(offset, os.SEEK_SET)
            chunk = f.read(max_read_bytes)
            new_offset = f.tell()
        text = chunk.decode("utf-8", errors="replace")
        lines_out = text.splitlines()
    except Exception as e:
        logger.warning("fail2ban log read error (%s): %s", log_path, e)
        return [], None

    new_state = {
        "inode": inode,
        "offset": new_offset,
        "updated_at": datetime.now(tz=TZ).isoformat(),
    }
    return lines_out, new_state


FAIL2BAN_STATE_LOCK = asyncio.Lock()


async def read_fail2ban_new_lines_async(
    log_path: str,
    state_path: str,
    max_read_bytes: int = 5_000_000,
) -> List[str]:
    async with FAIL2BAN_STATE_LOCK:
        state = await load_json_file(state_path)
        lines_out, new_state = await asyncio.to_thread(
            _read_fail2ban_chunk_sync,
            log_path,
            state,
            max_read_bytes,
        )
        if new_state is not None:
            await save_json_file(state_path, new_state)
        return lines_out


@dataclass(frozen=True)
class Fail2banEvent:
    ts: datetime
    jail: str
    action: str
    ip: Optional[str]
    raw: str


def parse_fail2ban_events(lines_in: Iterable[str]) -> List[Fail2banEvent]:
    out: List[Fail2banEvent] = []
    for raw in lines_in:
        m = F2B_LINE_RE.match(raw or "")
        if not m:
            continue
        ts = _f2b_parse_time(m.group("ts"))
        if not ts:
            continue
        msg = (m.group("msg") or "").strip()
        action = "-"
        if "Ban" in msg:
            action = "Ban"
        elif "Unban" in msg:
            action = "Unban"
        elif "Restore Ban" in msg:
            action = "Restore Ban"
        elif "Jail started" in msg:
            action = "Jail started"
        elif "Jail stopped" in msg:
            action = "Jail stopped"
        ip_match = F2B_IP_RE.search(msg)
        ip = ip_match.group("ip") if ip_match else None
        out.append(Fail2banEvent(ts=ts, jail=m.group("jail"), action=action, ip=ip, raw=raw))
    return out
