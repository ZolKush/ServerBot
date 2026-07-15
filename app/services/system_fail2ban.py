import asyncio
import base64
import binascii
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from ..config import PRIVILEGED_HELPER_BIN, SUBPROC_MEDIUM_TIMEOUT, SUBPROC_SHORT_TIMEOUT, SUDO_BIN, TZ, logger
from .system_process import run_exec

F2B_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d{3})?)\s+"
    r"(?P<logger>\S+)\s+\[\d+\]:\s+"
    r"(?P<level>[A-Z]+)\s+\[(?P<jail>[^\]]+)\]\s+"
    r"(?P<msg>.+?)\s*$"
)
F2B_IP_RE = re.compile(
    r"(?P<ip>"
    r"\b(?:\d{1,3}\.){3}\d{1,3}\b"  # IPv4
    r"|"
    r"(?:[0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}"  # IPv6 (requires ≥2 colon-separated groups)
    r")"
)


def _f2b_parse_time(ts: str, tz: ZoneInfo = TZ) -> datetime | None:
    ts = (ts or "").strip()
    if not ts:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S,%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(ts, fmt)
            return dt.replace(tzinfo=tz)
        except ValueError:
            continue
    return None


def tail_text_file(path: str, n_lines: int, max_bytes: int = 2_000_000) -> str:
    n_lines = max(1, min(int(n_lines), 50_000))
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
        if not SUDO_BIN or not PRIVILEGED_HELPER_BIN:
            raise
        n = max(1, min(int(n_lines), 50_000))
        byte_limit = max(1, min(int(max_bytes), 3_000_000))
        rc, out, err = await run_exec(
            [SUDO_BIN, "-n", PRIVILEGED_HELPER_BIN, "file-tail", path, str(n), str(byte_limit)],
            timeout=SUBPROC_MEDIUM_TIMEOUT,
            max_output_bytes=byte_limit + 4096,
        )
        if rc == 0:
            return out
        err_text = (err or out or "").strip().lower()
        if "no such file" in err_text or "cannot open" in err_text or "cannot access" in err_text:
            raise FileNotFoundError(path) from None
        raise PermissionError(path) from None


async def fail2ban_stat_with_sudo_async(path: str) -> tuple[int, datetime] | None:
    identity = await fail2ban_identity_with_sudo_async(path)
    return (identity.size, identity.mtime) if identity else None


@dataclass(frozen=True)
class FileIdentity:
    size: int
    mtime: datetime
    device: int
    inode: int


async def fail2ban_identity_with_sudo_async(path: str) -> FileIdentity | None:
    p = Path(path)
    try:
        st = await asyncio.to_thread(p.stat)
        return FileIdentity(
            size=st.st_size,
            mtime=datetime.fromtimestamp(st.st_mtime, tz=TZ),
            device=st.st_dev,
            inode=st.st_ino,
        )
    except FileNotFoundError:
        raise
    except PermissionError:
        pass
    except Exception:
        return None

    if not SUDO_BIN or not PRIVILEGED_HELPER_BIN:
        raise PermissionError(path)

    rc, out, err = await run_exec(
        [SUDO_BIN, "-n", PRIVILEGED_HELPER_BIN, "file-stat", path],
        timeout=SUBPROC_SHORT_TIMEOUT,
    )
    if rc != 0:
        err_text = (err or out or "").strip().lower()
        if "no such file" in err_text or "cannot stat" in err_text:
            raise FileNotFoundError(path)
        raise PermissionError(path)
    try:
        parts = out.strip().split("|")
        size_s, mtime_s = parts[0], parts[1]
        device_s = parts[2] if len(parts) > 2 else "0"
        inode_s = parts[3] if len(parts) > 3 else "0"
        return FileIdentity(
            size=int(size_s),
            mtime=datetime.fromtimestamp(int(mtime_s), tz=TZ),
            device=int(device_s),
            inode=int(inode_s),
        )
    except Exception:
        logger.debug("fail2ban_stat_with_sudo_async parse failed for %s", path)
        return None


async def read_text_range_with_sudo_async(path: str, offset: int, max_bytes: int) -> tuple[str, int]:
    offset = max(0, int(offset))
    limit = max(1, min(int(max_bytes), 3_000_000))

    def _read() -> tuple[str, int]:
        with Path(path).open("rb") as handle:
            handle.seek(offset)
            data = handle.read(limit)
        return data.decode("utf-8", errors="replace"), len(data)

    try:
        return await asyncio.to_thread(_read)
    except PermissionError:
        if not SUDO_BIN or not PRIVILEGED_HELPER_BIN:
            raise
    rc, out, err = await run_exec(
        [SUDO_BIN, "-n", PRIVILEGED_HELPER_BIN, "file-read", path, str(offset), str(limit)],
        timeout=SUBPROC_MEDIUM_TIMEOUT,
        max_output_bytes=((limit + 2) // 3) * 4 + 4096,
    )
    if rc != 0:
        error = (err or out or "").lower()
        if "no such file" in error:
            raise FileNotFoundError(path)
        raise PermissionError(path)
    try:
        data = base64.b64decode(out.strip(), validate=True) if out.strip() else b""
    except (binascii.Error, ValueError) as exc:
        raise RuntimeError("invalid base64 from privileged file reader") from exc
    if len(data) > limit:
        raise RuntimeError("privileged file reader exceeded requested byte limit")
    return data.decode("utf-8", errors="replace"), len(data)


@dataclass(frozen=True)
class Fail2banEvent:
    ts: datetime
    jail: str
    action: str
    ip: str | None
    raw: str


def parse_fail2ban_events(lines_in: Iterable[str], *, timezone: ZoneInfo = TZ) -> list[Fail2banEvent]:
    out: list[Fail2banEvent] = []
    for raw in lines_in:
        m = F2B_LINE_RE.match(raw or "")
        if not m:
            continue
        ts = _f2b_parse_time(m.group("ts"), timezone)
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
