"""Remote Fail2Ban log access with byte-exact cursors."""

from __future__ import annotations

import base64
import binascii
import shlex
from datetime import datetime
from zoneinfo import ZoneInfo

from ...config import (
    PRIVILEGED_HELPER_BIN,
    SUBPROC_MEDIUM_TIMEOUT,
    SUBPROC_SHORT_TIMEOUT,
    SUDO_BIN,
    TZ,
    logger,
)
from ..fail2ban.models import Fail2banEvent, FileIdentity, FileIdentityChangedError, FileRangeRead
from ..fail2ban.parser import parse_fail2ban_events
from .transport import ssh_run_shell


def _file_access_commands(path: str) -> tuple[str, str, str]:
    return (
        shlex.quote(path),
        shlex.quote(SUDO_BIN or "sudo"),
        shlex.quote(PRIVILEGED_HELPER_BIN),
    )


async def remote_tail_text_file(
    ssh_target: str,
    path: str,
    n_lines: int,
    max_bytes: int = 2_000_000,
) -> str:
    line_count = max(1, min(int(n_lines), 50_000))
    byte_limit = max(1, min(int(max_bytes), 3_000_000))
    quoted, sudo_bin, helper_bin = _file_access_commands(path)
    command = (
        f"if [ ! -e {quoted} ]; then printf '__FNF__'; "
        f"elif [ -r {quoted} ]; then tail -n {line_count} {quoted} | tail -c {byte_limit}; "
        f"else {sudo_bin} -n {helper_bin} file-tail {quoted} {line_count} {byte_limit} "
        "2>/dev/null || printf '__PERM__'; fi"
    )
    return_code, stdout, stderr = await ssh_run_shell(
        ssh_target,
        command,
        timeout=SUBPROC_MEDIUM_TIMEOUT,
        max_output_bytes=byte_limit + 8192,
    )
    if stdout.startswith("__FNF__"):
        raise FileNotFoundError(path)
    if stdout.startswith("__PERM__"):
        raise PermissionError(path)
    if return_code != 0 and (stderr or "").strip():
        raise RuntimeError(stderr.strip())
    return stdout


async def remote_fail2ban_stat(
    ssh_target: str,
    path: str,
) -> tuple[int, datetime] | None:
    identity = await remote_fail2ban_identity(ssh_target, path)
    return (identity.size, identity.mtime) if identity else None


async def remote_fail2ban_identity(
    ssh_target: str,
    path: str,
) -> FileIdentity | None:
    quoted, sudo_bin, helper_bin = _file_access_commands(path)
    commands = (
        f"stat -c '%s|%Y|%d|%i' {quoted} 2>/dev/null || true",
        f"{sudo_bin} -n {helper_bin} file-stat {quoted} 2>/dev/null || true",
    )
    for command in commands:
        return_code, stdout, _ = await ssh_run_shell(
            ssh_target,
            command,
            timeout=SUBPROC_SHORT_TIMEOUT,
        )
        if return_code != 0 or not stdout.strip():
            continue
        try:
            parts = stdout.strip().split("|")
            return FileIdentity(
                size=int(parts[0]),
                mtime=datetime.fromtimestamp(int(parts[1]), tz=TZ),
                device=int(parts[2]) if len(parts) > 2 else 0,
                inode=int(parts[3]) if len(parts) > 3 else 0,
            )
        except Exception:
            logger.debug("remote_fail2ban_stat parse failed for %s", ssh_target)
            return None
    return None


async def remote_read_text_range(
    ssh_target: str,
    path: str,
    offset: int,
    max_bytes: int,
    *,
    expected_identity: FileIdentity | None = None,
) -> FileRangeRead:
    offset = max(0, int(offset))
    limit = max(1, min(int(max_bytes), 3_000_000))
    before = await remote_fail2ban_identity(ssh_target, path)
    if before is None or (expected_identity is not None and not before.same_file_as(expected_identity)):
        raise FileIdentityChangedError(f"remote fail2ban log changed before read: {path}")
    quoted, sudo_bin, helper_bin = _file_access_commands(path)
    direct = f"tail -c +{offset + 1} -- {quoted} | head -c {limit}"
    privileged = f"{sudo_bin} -n {helper_bin} file-read {quoted} {offset} {limit}"
    command = (
        f"if [ ! -e {quoted} ]; then printf '__FNF__'; "
        f"elif [ -r {quoted} ]; then ({direct}) | base64 | tr -d '\\n'; "
        f"elif _mbot_data=$({privileged} 2>/dev/null); then printf '%s' \"$_mbot_data\"; "
        "else exit 77; fi"
    )
    encoded_limit = ((limit + 2) // 3) * 4 + 8192
    return_code, stdout, stderr = await ssh_run_shell(
        ssh_target,
        command,
        timeout=SUBPROC_MEDIUM_TIMEOUT + 4,
        max_output_bytes=encoded_limit,
    )
    if stdout.startswith("__FNF__"):
        raise FileIdentityChangedError(f"remote fail2ban log disappeared during read: {path}")
    if return_code != 0:
        raise RuntimeError((stderr or "remote file read failed").strip())
    try:
        data = base64.b64decode(stdout.strip(), validate=True) if stdout.strip() else b""
    except (binascii.Error, ValueError) as exc:
        raise RuntimeError("invalid base64 from remote file reader") from exc
    if len(data) > limit:
        raise RuntimeError("remote file reader exceeded requested byte limit")
    after = await remote_fail2ban_identity(ssh_target, path)
    if after is None or not before.same_file_as(after) or after.size < before.size:
        raise FileIdentityChangedError(f"remote fail2ban log changed during read: {path}")
    return FileRangeRead(
        text=data.decode("utf-8", errors="replace"),
        consumed=len(data),
        identity=after,
    )


async def remote_fail2ban_events(
    ssh_target: str,
    path: str,
    n_lines: int,
    *,
    timezone: ZoneInfo = TZ,
    max_bytes: int = 2_000_000,
) -> list[Fail2banEvent]:
    raw = await remote_tail_text_file(
        ssh_target,
        path=path,
        n_lines=n_lines,
        max_bytes=max_bytes,
    )
    return parse_fail2ban_events(raw.splitlines(), timezone=timezone)


__all__ = [
    "remote_fail2ban_events",
    "remote_fail2ban_identity",
    "remote_fail2ban_stat",
    "remote_read_text_range",
    "remote_tail_text_file",
]
