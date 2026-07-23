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


@dataclass(frozen=True, slots=True)
class Fail2banEvent:
    ts: datetime
    jail: str
    action: str
    ip: str | None
    raw: str


__all__ = ["Fail2banEvent", "FileIdentity"]
