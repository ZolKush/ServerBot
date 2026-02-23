import asyncio
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import aiofiles

from ..config import TZ, logger

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
    lines_out, new_state = await read_fail2ban_new_lines_with_state_async(log_path, state_path, max_read_bytes)
    if new_state is not None:
        await save_json_file(state_path, new_state)
    return lines_out


async def read_fail2ban_new_lines_with_state_async(
    log_path: str,
    state_path: str,
    max_read_bytes: int = 5_000_000,
) -> Tuple[List[str], Optional[Dict[str, Any]]]:
    async with FAIL2BAN_STATE_LOCK:
        state = await load_json_file(state_path)
        return await asyncio.to_thread(_read_fail2ban_chunk_sync, log_path, state, max_read_bytes)


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
