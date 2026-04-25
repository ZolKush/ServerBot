import asyncio
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import aiofiles

from ..config import SUBPROC_MEDIUM_TIMEOUT, SUBPROC_SHORT_TIMEOUT, SUDO_BIN, TZ, logger
from .system_process import run_exec

F2B_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d{3})?)\s+"
    r"(?P<logger>\S+)\s+\[\d+\]:\s+"
    r"(?P<level>[A-Z]+)\s+\[(?P<jail>[^\]]+)\]\s+"
    r"(?P<msg>.+?)\s*$"
)
F2B_IP_RE = re.compile(
    r"(?P<ip>"
    r"\b(?:\d{1,3}\.){3}\d{1,3}\b"              # IPv4
    r"|"
    r"(?:[0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}"  # IPv6 (requires ≥2 colon-separated groups)
    r")"
)


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


async def tail_text_file_with_sudo_async(path: str, n_lines: int, max_bytes: int = 2_000_000) -> str:
    try:
        return await tail_text_file_async(path, n_lines, max_bytes)
    except PermissionError:
        if not SUDO_BIN:
            raise
        n = max(1, min(int(n_lines), 10000))
        rc, out, err = await run_exec([SUDO_BIN, "-n", "tail", "-n", str(n), path], timeout=SUBPROC_MEDIUM_TIMEOUT)
        if rc == 0:
            return out
        err_text = (err or out or "").strip().lower()
        if "no such file" in err_text or "cannot open" in err_text or "cannot access" in err_text:
            raise FileNotFoundError(path)
        raise PermissionError(path)


async def fail2ban_stat_with_sudo_async(path: str) -> Optional[Tuple[int, datetime]]:
    p = Path(path)
    try:
        st = await asyncio.to_thread(p.stat)
        return st.st_size, datetime.fromtimestamp(st.st_mtime, tz=TZ)
    except FileNotFoundError:
        raise
    except PermissionError:
        pass
    except Exception:
        return None

    if not SUDO_BIN:
        raise PermissionError(path)

    rc, out, err = await run_exec([SUDO_BIN, "-n", "stat", "-c", "%s|%Y", path], timeout=SUBPROC_SHORT_TIMEOUT)
    if rc != 0:
        err_text = (err or out or "").strip().lower()
        if "no such file" in err_text or "cannot stat" in err_text:
            raise FileNotFoundError(path)
        raise PermissionError(path)
    try:
        size_s, mtime_s = out.strip().split("|", 1)
        return int(size_s), datetime.fromtimestamp(int(mtime_s), tz=TZ)
    except Exception:
        logger.debug("fail2ban_stat_with_sudo_async parse failed for %s", path)
        return None


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
        if "Restore Ban" in msg:
            action = "Restore Ban"
        elif "Unban" in msg:
            action = "Unban"
        elif "Ban" in msg:
            action = "Ban"
        elif "Jail started" in msg:
            action = "Jail started"
        elif "Jail stopped" in msg:
            action = "Jail stopped"
        ip_match = F2B_IP_RE.search(msg)
        ip = ip_match.group("ip") if ip_match else None
        out.append(Fail2banEvent(ts=ts, jail=m.group("jail"), action=action, ip=ip, raw=raw))
    return out
