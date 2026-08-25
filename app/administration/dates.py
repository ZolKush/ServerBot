from __future__ import annotations

from datetime import datetime

from ..config import TZ


def parse_input_datetime(value: str) -> datetime | None:
    try:
        parsed = datetime.strptime(value.strip(), "%d.%m.%Y %H:%M")
    except ValueError:
        return None
    return parsed.replace(tzinfo=TZ)


def parse_datetime(value: object) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=TZ)
    return parsed.astimezone(TZ)


def datetime_text(value: object) -> str:
    parsed = parse_datetime(value)
    return parsed.strftime("%d.%m.%Y %H:%M") if parsed else "-"


__all__ = [
    "datetime_text",
    "parse_datetime",
    "parse_input_datetime",
]
