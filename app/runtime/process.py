from __future__ import annotations

import asyncio
import contextlib
import os
import signal
from collections.abc import Sequence

from ..config import SUBPROC_MAX_OUTPUT_BYTES

_READ_CHUNK = 64 * 1024


async def _read_limited(stream: asyncio.StreamReader | None, limit: int) -> tuple[bytes, bool]:
    if stream is None:
        return b"", False
    chunks: list[bytes] = []
    kept = 0
    truncated = False
    while True:
        chunk = await stream.read(_READ_CHUNK)
        if not chunk:
            break
        remaining = limit - kept
        if remaining > 0:
            part = chunk[:remaining]
            chunks.append(part)
            kept += len(part)
        if len(chunk) > max(remaining, 0):
            truncated = True
    return b"".join(chunks), truncated


async def _kill_process_group(proc: asyncio.subprocess.Process) -> None:
    if os.name != "nt":
        killpg = getattr(os, "killpg", None)
        sigkill = getattr(signal, "SIGKILL", 9)
        with contextlib.suppress(ProcessLookupError, PermissionError):
            if callable(killpg):
                killpg(proc.pid, sigkill)
    elif proc.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
    if proc.returncode is None:
        with contextlib.suppress(Exception):
            await proc.wait()


def _decode(data: bytes, truncated: bool) -> str:
    text = data.decode("utf-8", errors="replace")
    if truncated:
        text += "\n…(output truncated by MaintBot)…"
    return text


async def run_exec(
    args: Sequence[str],
    timeout: int,
    *,
    max_output_bytes: int = SUBPROC_MAX_OUTPUT_BYTES,
) -> tuple[int, str, str]:
    if not args or not str(args[0]):
        return 127, "", "empty command"
    limit = max(1024, min(int(max_output_bytes), 10_000_000))
    try:
        if os.name == "nt":
            # Only the process-group creation flag is used; commands still go
            # through asyncio.create_subprocess_exec without a shell.
            import subprocess  # nosec B404

            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            )
        else:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.DEVNULL,
                start_new_session=True,
            )
    except FileNotFoundError:
        return 127, "", f"command not found: {args[0]}"
    except Exception as exc:
        return 127, "", f"spawn error: {exc}"

    stdout_task = asyncio.create_task(_read_limited(proc.stdout, limit))
    stderr_task = asyncio.create_task(_read_limited(proc.stderr, limit))
    wait_task = asyncio.create_task(proc.wait())
    completion = asyncio.gather(wait_task, stdout_task, stderr_task)
    try:
        # Include pipe EOF in the deadline. A short-lived parent may otherwise
        # leave a child holding stdout open and bypass the process timeout.
        await asyncio.wait_for(asyncio.shield(completion), timeout=timeout)
        out_result = stdout_task.result()
        err_result = stderr_task.result()
    except asyncio.TimeoutError:
        await _kill_process_group(proc)
        try:
            await asyncio.wait_for(asyncio.shield(completion), timeout=5)
            out_result = stdout_task.result()
            err_result = stderr_task.result()
        except asyncio.CancelledError:
            completion.cancel()
            with contextlib.suppress(BaseException):
                await completion
            raise
        except asyncio.TimeoutError:
            completion.cancel()
            with contextlib.suppress(BaseException):
                await completion
            out_result = (b"", False)
            err_result = (b"", False)
        return 124, _decode(*out_result), _decode(*err_result) or "timeout"
    except asyncio.CancelledError:
        await _kill_process_group(proc)
        completion.cancel()
        with contextlib.suppress(BaseException):
            await completion
        raise
    except Exception:
        await _kill_process_group(proc)
        completion.cancel()
        with contextlib.suppress(BaseException):
            await completion
        raise

    return int(proc.returncode or 0), _decode(*out_result), _decode(*err_result)


__all__ = ["run_exec"]
