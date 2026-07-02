import asyncio
import re
import shutil
from datetime import timedelta
from pathlib import Path

from ..config import SUBPROC_SHORT_TIMEOUT
from .system_process import run_exec


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


def _parse_uptime_p(text: str) -> str:
    t = (text or "").strip().lower()
    if t.startswith("up "):
        t = t[3:]
    days = hours = minutes = 0
    for part in re.split(r",\s*", t):
        m = re.match(r"(\d+)\s+(day|days|hour|hours|minute|minutes)", part.strip())
        if not m:
            continue
        n, unit = int(m.group(1)), m.group(2)
        if unit.startswith("day"):
            days = n
        elif unit.startswith("hour"):
            hours = n
        elif unit.startswith("minute"):
            minutes = n
    parts: list[str] = []
    if days:
        parts.append(f"{days} д")
    if hours:
        parts.append(f"{hours} ч")
    if minutes or not parts:
        parts.append(f"{minutes} м")
    return " ".join(parts)


async def check_uptime() -> str:
    try:
        raw = await asyncio.to_thread(Path("/proc/uptime").read_text, encoding="utf-8")
        seconds = int(float(raw.split()[0]))
    except Exception:
        rc, out, _ = await run_exec(["uptime", "-p"], timeout=SUBPROC_SHORT_TIMEOUT)
        if rc != 0:
            return "н/д"
        return _parse_uptime_p(out.strip()) or "н/д"

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


async def meminfo() -> str:
    try:
        raw = await asyncio.to_thread(Path("/proc/meminfo").read_text, encoding="utf-8")
        kv: dict[str, int] = {}
        for line in raw.splitlines():
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
        rc, out, _ = await run_exec(["free", "-m"], timeout=SUBPROC_SHORT_TIMEOUT)
        if rc != 0:
            return "н/д"
        lines = out.splitlines()
        if len(lines) < 2:
            return "н/д"
        mem = re.split(r"\s+", lines[1].strip())
        try:
            mem_total = int(mem[1])
            mem_used = int(mem[2])
            return f"{mem_used} / {mem_total} MiB"
        except Exception:
            return "н/д"


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
            size_s, used_s, avail_s, usep_s, mnt_s = parts[1], parts[2], parts[3], parts[4], parts[5]
            return f"{used_s} / {size_s} (avail {avail_s}, {usep_s}) mount {mnt_s}"
        return "н/д"
