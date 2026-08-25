"""Fail2ban monitoring values."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class FileIdentity:
    size: int
    mtime: datetime
    device: int
    inode: int

    def same_file_as(self, other: FileIdentity) -> bool:
        return self.device == other.device and self.inode == other.inode


@dataclass(frozen=True, slots=True)
class FileRangeRead:
    text: str
    consumed: int
    identity: FileIdentity


class FileIdentityChangedError(RuntimeError):
    """The path stopped referring to the selected log while it was read."""


@dataclass(frozen=True, slots=True)
class Fail2banEvent:
    ts: datetime
    jail: str
    action: str
    ip: str | None
    raw: str


__all__ = [
    "Fail2banEvent",
    "FileIdentity",
    "FileIdentityChangedError",
    "FileRangeRead",
]
