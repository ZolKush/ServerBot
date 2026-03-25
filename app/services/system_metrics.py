import asyncio
import re
import shutil
from datetime import timedelta
from pathlib import Path
from typing import Dict, List

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
