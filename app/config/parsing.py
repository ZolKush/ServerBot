from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

_COUNTRY_NAMES = {
    "DE": "Germany",
    "NL": "Netherlands",
    "RU": "Russia",
    "FI": "Finland",
    "FR": "France",
    "PL": "Poland",
    "US": "United States",
    "GB": "United Kingdom",
}


def split_env_list(raw: Any, *, dedupe: bool = True) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        value = raw.strip()
        if not value:
            return []
        if value.startswith("[") and value.endswith("]"):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    raw = parsed
            except json.JSONDecodeError:
                pass
    if isinstance(raw, list):
        items = [str(item).strip() for item in raw]
    else:
        items = [part.strip() for part in str(raw).split(",")]
    result: list[str] = []
    for item in items:
        if item and (not dedupe or item not in result):
            result.append(item)
    return result


def split_env_groups(raw: Any) -> list[list[str]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        groups: list[list[str]] = []
        for item in raw:
            group = split_env_list(item)
            if group:
                groups.append(group)
        return groups

    value = str(raw or "").strip()
    if not value:
        return []
    if ";" in value:
        return [split_env_list(part, dedupe=False) for part in value.split(";")]
    items = split_env_list(value)
    return [items] if items else []


def resolve_path(value: str, base: Path) -> str:
    raw = (value or "").strip()
    if not raw:
        return str(base)
    path = Path(raw)
    return str(path if path.is_absolute() else (base / path))


def resolve_bin(*candidates: str) -> str:
    for candidate in candidates:
        if not candidate:
            continue
        path = shutil.which(candidate)
        if path:
            return path
    return candidates[-1] if candidates else ""


def country_flag(value: str) -> str:
    code = (value or "").strip().upper()
    if len(code) != 2 or not code.isalpha():
        return "🖥"
    base = 0x1F1E6
    return "".join(chr(base + ord(char) - ord("A")) for char in code)


def country_label(value: str, fallback: str) -> str:
    code = (value or "").strip().upper()
    if len(code) == 2 and code.isalpha():
        return _COUNTRY_NAMES.get(code, code)
    return fallback.strip() or "Server"


def nth_or_default(items: list[str], index: int, default: str = "") -> str:
    if index < len(items):
        return items[index]
    return default


def domains_for_index(
    groups: list[list[str]],
    index: int,
    total: int,
    fallback: list[str] | None = None,
) -> list[str]:
    if not groups:
        return list(fallback or [])
    if len(groups) == total:
        return list(groups[index])
    if len(groups) == 1:
        one = list(groups[0])
        if total > 1 and len(one) == total:
            return [one[index]]
        return one if index == 0 else list(fallback or [])
    return list(groups[index]) if index < len(groups) else list(fallback or [])


def group_for_index(
    groups: list[list[str]],
    index: int,
    fallback: list[str] | None = None,
) -> list[str]:
    if not groups:
        return list(fallback or [])
    return list(groups[index]) if index < len(groups) else list(fallback or [])


__all__ = [
    "country_flag",
    "country_label",
    "domains_for_index",
    "group_for_index",
    "nth_or_default",
    "resolve_bin",
    "resolve_path",
    "split_env_groups",
    "split_env_list",
]
