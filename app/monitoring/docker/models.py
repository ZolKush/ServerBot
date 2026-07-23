"""Docker monitoring values and validation rules."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TypeAlias

ContainerRecord: TypeAlias = tuple[str, bool, str, str]

_CONTAINER_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.\-]{0,62}$")


def is_valid_container_name(name: str) -> bool:
    normalized = (name or "").strip()
    return bool(normalized and _CONTAINER_NAME_RE.fullmatch(normalized))


def docker_status_is_running(status: object) -> bool:
    normalized = str(status or "").strip().lower()
    return normalized.startswith("up") and "paused" not in normalized


@dataclass(frozen=True, slots=True)
class ContainerSnapshot:
    name: str
    running: bool
    status: str
    restarts: str = "-"

    def as_record(self) -> ContainerRecord:
        return self.name, self.running, self.status, self.restarts


__all__ = [
    "ContainerRecord",
    "ContainerSnapshot",
    "docker_status_is_running",
    "is_valid_container_name",
]
