"""UFW output parsing and local command execution."""

from __future__ import annotations

import re

from ...config import PRIVILEGED_HELPER_BIN, SUBPROC_SHORT_TIMEOUT, SUDO_BIN, UFW_BIN
from ...runtime.process import run_exec


def _parse_ufw_status(output: str) -> str:
    first_line = (output.strip().splitlines()[:1] or [""])[0].strip().lower()
    # ``inactive`` contains ``active``; negative states must be checked first.
    if re.search(r"\b(inactive|disabled)\b", first_line) or any(
        word in first_line for word in ("неактив", "отключ", "выключ")
    ):
        return "inactive"
    if re.search(r"\b(active|enabled)\b", first_line) or any(word in first_line for word in ("актив", "включ")):
        return "active"
    return "н/д"


def _ufw_candidates() -> list[list[str]]:
    binaries: list[str] = []
    for binary in (UFW_BIN, "ufw"):
        if binary and binary not in binaries:
            binaries.append(binary)

    commands = [[binary, "status"] for binary in binaries]
    if SUDO_BIN and PRIVILEGED_HELPER_BIN:
        commands.append([SUDO_BIN, "-n", PRIVILEGED_HELPER_BIN, "ufw-status"])
    return commands


def _unique(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _parse_ufw_rules(output: str) -> tuple[list[str], list[str], list[str]]:
    allow: list[str] = []
    deny: list[str] = []
    reject: list[str] = []

    lines = [line.rstrip() for line in (output or "").splitlines()]
    if not lines:
        return allow, deny, reject

    start_index = 0
    for index, line in enumerate(lines[:10]):
        if re.search(r"\bTo\b", line) and re.search(r"\bAction\b", line):
            start_index = index + 1
            break

    for line in lines[start_index:]:
        if not line.strip():
            continue

        parts = [part.strip() for part in re.split(r"\s{2,}", line.strip()) if part.strip()]
        if len(parts) < 2:
            continue
        destination, action = parts[0], parts[1].upper()
        source = parts[2] if len(parts) > 2 else ""
        item = destination.strip()
        if not item:
            continue
        if source and source.lower() not in {"anywhere", "anywhere (v6)"}:
            item = f"{item} <- {source}"
        if action.startswith("ALLOW"):
            allow.append(item)
        elif action.startswith("DENY"):
            deny.append(item)
        elif action.startswith("REJECT"):
            reject.append(item)

    return _unique(allow), _unique(deny), _unique(reject)


async def _read_ufw_status() -> str:
    for arguments in _ufw_candidates():
        return_code, stdout, _ = await run_exec(
            arguments,
            timeout=SUBPROC_SHORT_TIMEOUT,
        )
        if return_code == 0 and (stdout or "").strip():
            return stdout
    return ""


async def ufw_status_basic() -> str:
    output = await _read_ufw_status()
    return _parse_ufw_status(output) if output else "н/д"


async def ufw_summary_for_admin() -> tuple[str, list[str], list[str], list[str]]:
    output = await _read_ufw_status()
    if not output:
        return "н/д", [], [], []

    status = _parse_ufw_status(output)
    allow, deny, reject = _parse_ufw_rules(output)
    return status, allow, deny, reject


__all__ = ["ufw_status_basic", "ufw_summary_for_admin"]
