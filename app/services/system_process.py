import asyncio
from typing import Sequence, Tuple


async def run_exec(args: Sequence[str], timeout: int) -> Tuple[int, str, str]:
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
        try:
            proc.kill()
        except Exception:
            pass
        try:
            await proc.wait()
        except Exception:
            pass
        return 124, "", "timeout"

    return (
        int(proc.returncode or 0),
        (out or b"").decode(errors="ignore"),
        (err or b"").decode(errors="ignore"),
    )
