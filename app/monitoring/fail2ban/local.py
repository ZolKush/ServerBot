"""Local Fail2ban log file access with privileged-helper fallback."""

from __future__ import annotations

import asyncio
import base64
import binascii
import os
from datetime import datetime
from pathlib import Path

from ...config import (
    PRIVILEGED_HELPER_BIN,
    SUBPROC_MEDIUM_TIMEOUT,
    SUBPROC_SHORT_TIMEOUT,
    SUDO_BIN,
    TZ,
    logger,
)
from ...runtime.process import run_exec
from .models import FileIdentity


def tail_text_file(path: str, n_lines: int, max_bytes: int = 2_000_000) -> str:
    line_limit = max(1, min(int(n_lines), 50_000))
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(path)

    with file_path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        read_size = min(size, max_bytes)
        handle.seek(-read_size, os.SEEK_END)
        contents = handle.read(read_size)

    lines = contents.decode("utf-8", errors="replace").splitlines()
    tail = lines[-line_limit:] if len(lines) > line_limit else lines
    return "\n".join(tail)


async def tail_text_file_async(
    path: str,
    n_lines: int,
    max_bytes: int = 2_000_000,
) -> str:
    return await asyncio.to_thread(tail_text_file, path, n_lines, max_bytes)


async def tail_text_file_with_sudo_async(
    path: str,
    n_lines: int,
    max_bytes: int = 2_000_000,
) -> str:
    try:
        return await tail_text_file_async(path, n_lines, max_bytes)
    except PermissionError:
        if not SUDO_BIN or not PRIVILEGED_HELPER_BIN:
            raise

    line_limit = max(1, min(int(n_lines), 50_000))
    byte_limit = max(1, min(int(max_bytes), 3_000_000))
    return_code, stdout, stderr = await run_exec(
        [
            SUDO_BIN,
            "-n",
            PRIVILEGED_HELPER_BIN,
            "file-tail",
            path,
            str(line_limit),
            str(byte_limit),
        ],
        timeout=SUBPROC_MEDIUM_TIMEOUT,
        max_output_bytes=byte_limit + 4096,
    )
    if return_code == 0:
        return stdout
    error = (stderr or stdout or "").strip().lower()
    if "no such file" in error or "cannot open" in error or "cannot access" in error:
        raise FileNotFoundError(path) from None
    raise PermissionError(path) from None


async def fail2ban_stat_with_sudo_async(path: str) -> tuple[int, datetime] | None:
    identity = await fail2ban_identity_with_sudo_async(path)
    return (identity.size, identity.mtime) if identity else None


async def fail2ban_identity_with_sudo_async(path: str) -> FileIdentity | None:
    file_path = Path(path)
    try:
        stat = await asyncio.to_thread(file_path.stat)
        return FileIdentity(
            size=stat.st_size,
            mtime=datetime.fromtimestamp(stat.st_mtime, tz=TZ),
            device=stat.st_dev,
            inode=stat.st_ino,
        )
    except FileNotFoundError:
        raise
    except PermissionError:
        pass
    except Exception:
        return None

    if not SUDO_BIN or not PRIVILEGED_HELPER_BIN:
        raise PermissionError(path)

    return_code, stdout, stderr = await run_exec(
        [SUDO_BIN, "-n", PRIVILEGED_HELPER_BIN, "file-stat", path],
        timeout=SUBPROC_SHORT_TIMEOUT,
    )
    if return_code != 0:
        error = (stderr or stdout or "").strip().lower()
        if "no such file" in error or "cannot stat" in error:
            raise FileNotFoundError(path)
        raise PermissionError(path)
    try:
        parts = stdout.strip().split("|")
        size, modified_at = parts[0], parts[1]
        device = parts[2] if len(parts) > 2 else "0"
        inode = parts[3] if len(parts) > 3 else "0"
        return FileIdentity(
            size=int(size),
            mtime=datetime.fromtimestamp(int(modified_at), tz=TZ),
            device=int(device),
            inode=int(inode),
        )
    except Exception:
        logger.debug(
            "fail2ban_stat_with_sudo_async parse failed for %s",
            path,
        )
        return None


async def read_text_range_with_sudo_async(
    path: str,
    offset: int,
    max_bytes: int,
) -> tuple[str, int]:
    normalized_offset = max(0, int(offset))
    byte_limit = max(1, min(int(max_bytes), 3_000_000))

    def read_range() -> tuple[str, int]:
        with Path(path).open("rb") as handle:
            handle.seek(normalized_offset)
            data = handle.read(byte_limit)
        return data.decode("utf-8", errors="replace"), len(data)

    try:
        return await asyncio.to_thread(read_range)
    except PermissionError:
        if not SUDO_BIN or not PRIVILEGED_HELPER_BIN:
            raise

    return_code, stdout, stderr = await run_exec(
        [
            SUDO_BIN,
            "-n",
            PRIVILEGED_HELPER_BIN,
            "file-read",
            path,
            str(normalized_offset),
            str(byte_limit),
        ],
        timeout=SUBPROC_MEDIUM_TIMEOUT,
        max_output_bytes=((byte_limit + 2) // 3) * 4 + 4096,
    )
    if return_code != 0:
        error = (stderr or stdout or "").lower()
        if "no such file" in error:
            raise FileNotFoundError(path)
        raise PermissionError(path)
    try:
        data = base64.b64decode(stdout.strip(), validate=True) if stdout.strip() else b""
    except (binascii.Error, ValueError) as exc:
        raise RuntimeError("invalid base64 from privileged file reader") from exc
    if len(data) > byte_limit:
        raise RuntimeError("privileged file reader exceeded requested byte limit")
    return data.decode("utf-8", errors="replace"), len(data)


__all__ = [
    "fail2ban_identity_with_sudo_async",
    "fail2ban_stat_with_sudo_async",
    "read_text_range_with_sudo_async",
    "tail_text_file",
    "tail_text_file_async",
    "tail_text_file_with_sudo_async",
]
