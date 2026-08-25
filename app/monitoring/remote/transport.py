"""Bounded, non-interactive SSH transport for monitoring adapters."""

from __future__ import annotations

import shlex
from collections.abc import Sequence

from ...config import (
    SSH_BIN,
    SSH_IDENTITY_FILE,
    SSH_KNOWN_HOSTS_FILE,
    SSH_STRICT_HOST_KEY_CHECKING,
)
from ...runtime.process import run_exec

_OUT_BEGIN = "__MBOT_OUT_BEGIN_43e1f3c4__"
_OUT_END = "__MBOT_OUT_END_43e1f3c4__"


def _extract_wrapped_stdout(text: str) -> str:
    raw = text or ""
    start = raw.find(_OUT_BEGIN)
    if start < 0:
        return raw
    start = raw.find("\n", start)
    if start < 0:
        return ""
    start += 1
    end = raw.find(_OUT_END, start)
    if end < 0:
        return raw[start:]
    payload = raw[start:end]
    return payload[:-1] if payload.endswith("\n") else payload


def _split_ssh_target(target: str) -> tuple[str, int | None]:
    target = (target or "").strip()
    if not target:
        return target, None

    user_prefix, host_part = target.rsplit("@", 1) if "@" in target else ("", target)
    prefix = f"{user_prefix}@" if user_prefix else ""
    if host_part.startswith("["):
        closing = host_part.find("]")
        if closing < 0:
            raise ValueError("invalid bracketed IPv6 SSH target")
        host = prefix + host_part[1:closing]
        suffix = host_part[closing + 1 :]
        if not suffix:
            return host, None
        if not suffix.startswith(":") or not suffix[1:].isdigit():
            raise ValueError("invalid SSH target port")
        port = int(suffix[1:])
    else:
        # An unbracketed IPv6 literal never has an implicit SSH port.
        if host_part.count(":") != 1:
            return target, None
        host_name, port_text = host_part.rsplit(":", 1)
        if not port_text.isdigit():
            return target, None
        host = prefix + host_name
        port = int(port_text)
    if not 1 <= port <= 65535:
        raise ValueError("ssh port out of range")
    return host, port


async def ssh_run_shell(
    target: str,
    command: str,
    timeout: int,
    *,
    max_output_bytes: int | None = None,
) -> tuple[int, str, str]:
    target = (target or "").strip()
    if not target:
        return 127, "", "ssh target is not configured"
    try:
        ssh_host, ssh_port = _split_ssh_target(target)
    except ValueError as exc:
        return 127, "", str(exc)

    wrapped = (
        "PATH=/usr/sbin:/usr/bin:/sbin:/bin:$PATH; LANG=C; LC_ALL=C; export PATH LANG LC_ALL; "
        f"printf '%s\\n' {shlex.quote(_OUT_BEGIN)}; "
        f"{command}; "
        "rc=$?; "
        f"printf '\\n%s\\n' {shlex.quote(_OUT_END)}; "
        "exit $rc"
    )
    arguments = [
        SSH_BIN,
        "-o",
        "BatchMode=yes",
        "-o",
        f"ConnectTimeout={max(2, min(timeout, 10))}",
        "-o",
        "LogLevel=ERROR",
        "-o",
        f"StrictHostKeyChecking={SSH_STRICT_HOST_KEY_CHECKING}",
    ]
    if SSH_KNOWN_HOSTS_FILE:
        arguments.extend(["-o", f"UserKnownHostsFile={SSH_KNOWN_HOSTS_FILE}"])
    if SSH_IDENTITY_FILE:
        arguments.extend(["-o", "IdentitiesOnly=yes", "-i", SSH_IDENTITY_FILE])
    if ssh_port is not None:
        arguments.extend(["-p", str(ssh_port)])
    arguments.append(ssh_host)
    arguments.append("sh -c " + shlex.quote(wrapped))
    run_kwargs = {"max_output_bytes": max_output_bytes} if max_output_bytes is not None else {}
    return_code, stdout, stderr = await run_exec(
        arguments,
        timeout=max(timeout + 2, 5),
        **run_kwargs,
    )
    return return_code, _extract_wrapped_stdout(stdout), stderr


async def ssh_run_exec(
    target: str,
    argv: Sequence[str],
    timeout: int,
) -> tuple[int, str, str]:
    command = " ".join(shlex.quote(str(argument)) for argument in argv if str(argument))
    return await ssh_run_shell(target, command, timeout)


__all__ = ["ssh_run_exec", "ssh_run_shell"]
