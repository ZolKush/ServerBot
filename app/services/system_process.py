import asyncio
import contextlib
from collections.abc import Sequence


async def run_exec(args: Sequence[str], timeout: int) -> tuple[int, str, str]:
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.DEVNULL,
        )
    except FileNotFoundError:
        return 127, "", f"command not found: {args[0]}"
    except Exception as e:
        return 127, "", f"spawn error: {e}"

    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        with contextlib.suppress(Exception):
            proc.kill()
        with contextlib.suppress(Exception):
            await proc.wait()
        return 124, "", "timeout"

    return (
        int(proc.returncode or 0),
        (out or b"").decode(errors="ignore"),
        (err or b"").decode(errors="ignore"),
    )
