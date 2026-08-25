from __future__ import annotations


def format_uptime_seconds(seconds: float | None) -> str:
    if seconds is None:
        return "н/д"
    total_seconds = int(seconds)
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes = remainder // 60
    parts: list[str] = []
    if days:
        parts.append(f"{days} д")
    if hours:
        parts.append(f"{hours} ч")
    if minutes or not parts:
        parts.append(f"{minutes} м")
    return " ".join(parts)


def format_memory_bytes(used: int | None, total: int | None) -> str:
    if used is None or total is None or total <= 0:
        return "н/д"
    used_mib = int(round(used / (1024 * 1024)))
    total_mib = int(round(total / (1024 * 1024)))
    return f"{used_mib} / {total_mib} MiB"


__all__ = ["format_memory_bytes", "format_uptime_seconds"]
