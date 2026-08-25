"""Pure parsing of Fail2ban log events."""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import datetime
from zoneinfo import ZoneInfo

from ...config import TZ
from .models import Fail2banEvent

F2B_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d{3})?)\s+"
    r"(?P<logger>\S+)\s+\[\d+\]:\s+"
    r"(?P<level>[A-Z]+)\s+\[(?P<jail>[^\]]+)\]\s+"
    r"(?P<msg>.+?)\s*$",
)
F2B_IP_RE = re.compile(
    r"(?P<ip>"
    r"\b(?:\d{1,3}\.){3}\d{1,3}\b"
    r"|"
    r"(?:[0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}"
    r")",
)


def _f2b_parse_time(timestamp: str, tz: ZoneInfo = TZ) -> datetime | None:
    normalized = (timestamp or "").strip()
    if not normalized:
        return None
    for date_format in ("%Y-%m-%d %H:%M:%S,%f", "%Y-%m-%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(normalized, date_format)
            return parsed.replace(tzinfo=tz)
        except ValueError:
            continue
    return None


def _event_action(message: str) -> str:
    if "Restore Ban" in message:
        return "Restore Ban"
    if "Unban" in message:
        return "Unban"
    if "Ban" in message:
        return "Ban"
    if "Jail started" in message:
        return "Jail started"
    if "Jail stopped" in message:
        return "Jail stopped"
    return "-"


def parse_fail2ban_events(
    lines: Iterable[str],
    *,
    timezone: ZoneInfo = TZ,
) -> list[Fail2banEvent]:
    events: list[Fail2banEvent] = []
    for raw_line in lines:
        match = F2B_LINE_RE.match(raw_line or "")
        if not match:
            continue
        timestamp = _f2b_parse_time(match.group("ts"), timezone)
        if timestamp is None:
            continue
        message = (match.group("msg") or "").strip()
        ip_match = F2B_IP_RE.search(message)
        events.append(
            Fail2banEvent(
                ts=timestamp,
                jail=match.group("jail"),
                action=_event_action(message),
                ip=ip_match.group("ip") if ip_match else None,
                raw=raw_line,
            ),
        )
    return events


__all__ = ["F2B_IP_RE", "F2B_LINE_RE", "parse_fail2ban_events"]
