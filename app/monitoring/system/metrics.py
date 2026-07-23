"""Local uptime, memory and root-filesystem metrics."""

from __future__ import annotations

import asyncio
import re
import shutil
from datetime import timedelta
from pathlib import Path

from ...config import SUBPROC_SHORT_TIMEOUT
from ...runtime.process import run_exec


def _fmt_bytes_binary(size: int) -> str:
    if size < 0:
        return "0 B"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(size)
    unit_index = 0
    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024.0
        unit_index += 1
    if unit_index == 0:
        return f"{int(value)} {units[unit_index]}"
    return f"{value:.1f} {units[unit_index]}"


def _parse_uptime_p(text: str) -> str:
    normalized = (text or "").strip().lower()
    if normalized.startswith("up "):
        normalized = normalized[3:]
    days = hours = minutes = 0
    for part in re.split(r",\s*", normalized):
        match = re.match(r"(\d+)\s+(day|days|hour|hours|minute|minutes)", part.strip())
        if not match:
            continue
        value, unit = int(match.group(1)), match.group(2)
        if unit.startswith("day"):
            days = value
        elif unit.startswith("hour"):
            hours = value
        elif unit.startswith("minute"):
            minutes = value
    parts: list[str] = []
    if days:
        parts.append(f"{days} д")
    if hours:
        parts.append(f"{hours} ч")
    if minutes or not parts:
        parts.append(f"{minutes} м")
    return " ".join(parts)


def _format_uptime(seconds: int) -> str:
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


async def check_uptime() -> str:
    try:
        raw = await asyncio.to_thread(
            Path("/proc/uptime").read_text,
            encoding="utf-8",
        )
        return _format_uptime(int(float(raw.split()[0])))
    except Exception:
        return_code, stdout, _ = await run_exec(
            ["uptime", "-p"],
            timeout=SUBPROC_SHORT_TIMEOUT,
        )
        if return_code != 0:
            return "н/д"
        return _parse_uptime_p(stdout.strip()) or "н/д"


async def meminfo() -> str:
    try:
        raw = await asyncio.to_thread(
            Path("/proc/meminfo").read_text,
            encoding="utf-8",
        )
        values: dict[str, int] = {}
        for line in raw.splitlines():
            match = re.match(r"^(\w+):\s+(\d+)\s+kB$", line.strip())
            if match:
                values[match.group(1)] = int(match.group(2))
        total_kib = values.get("MemTotal", 0)
        available_kib = values.get("MemAvailable", values.get("MemFree", 0))
        used_kib = max(total_kib - available_kib, 0)
        used_mib = int(round(used_kib / 1024.0))
        total_mib = int(round(total_kib / 1024.0))
        return f"{used_mib} / {total_mib} MiB"
    except Exception:
        return_code, stdout, _ = await run_exec(
            ["free", "-m"],
            timeout=SUBPROC_SHORT_TIMEOUT,
        )
        if return_code != 0:
            return "н/д"
        lines = stdout.splitlines()
        if len(lines) < 2:
            return "н/д"
        memory = re.split(r"\s+", lines[1].strip())
        try:
            return f"{int(memory[2])} / {int(memory[1])} MiB"
        except (IndexError, ValueError):
            return "н/д"


async def disk_root() -> str:
    try:
        usage = await asyncio.to_thread(shutil.disk_usage, "/")
        percentage = int(round((usage.used / usage.total) * 100)) if usage.total else 0
        return (
            f"{_fmt_bytes_binary(usage.used)} / {_fmt_bytes_binary(usage.total)} "
            f"(avail {_fmt_bytes_binary(usage.free)}, {percentage}%) mount /"
        )
    except Exception:
        return_code, stdout, _ = await run_exec(
            ["df", "-h", "/"],
            timeout=SUBPROC_SHORT_TIMEOUT,
        )
        if return_code != 0:
            return "н/д"
        lines = stdout.splitlines()
        if len(lines) < 2:
            return "н/д"
        fields = re.split(r"\s+", lines[1].strip())
        if len(fields) < 6:
            return "н/д"
        size, used, available, df_percentage, mount = fields[1:6]
        return f"{used} / {size} (avail {available}, {df_percentage}) mount {mount}"


__all__ = ["check_uptime", "disk_root", "meminfo"]
