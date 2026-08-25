from __future__ import annotations

import shutil
from pathlib import Path


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


__all__ = [
    "country_flag",
    "resolve_bin",
    "resolve_path",
]
