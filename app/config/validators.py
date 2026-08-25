from __future__ import annotations

import re

SERVER_KEY_PATTERN = r"[a-z0-9_-]{1,12}"

_SSH_TARGET_RE = re.compile(r"^[A-Za-z0-9_.:@\-\[\]]{1,255}$")
_CONTAINER_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.\-]{0,62}$")
_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def validate_ssh_target(value: str) -> str:
    target = str(value or "").strip()
    if not target:
        return ""
    if target.startswith("-") or not _SSH_TARGET_RE.fullmatch(target):
        raise ValueError("SSH target has invalid format")
    host_part = target.rsplit("@", 1)[-1]
    port_text = ""
    if host_part.startswith("["):
        closing = host_part.find("]")
        if closing < 0:
            raise ValueError("SSH target has an unclosed IPv6 bracket")
        suffix = host_part[closing + 1 :]
        if suffix:
            if not suffix.startswith(":") or not suffix[1:].isdigit():
                raise ValueError("SSH target port has invalid format")
            port_text = suffix[1:]
    elif host_part.count(":") == 1:
        _host, maybe_port = host_part.rsplit(":", 1)
        if maybe_port.isdigit():
            port_text = maybe_port
    if port_text and not 1 <= int(port_text) <= 65535:
        raise ValueError("SSH target port out of range")
    return target


def is_container_name(value: str) -> bool:
    return _CONTAINER_NAME_RE.fullmatch(value) is not None


def is_uuid(value: str) -> bool:
    return _UUID_RE.fullmatch(value) is not None


__all__ = [
    "SERVER_KEY_PATTERN",
    "is_container_name",
    "is_uuid",
    "validate_ssh_target",
]
