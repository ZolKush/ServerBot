from __future__ import annotations

import contextlib
import errno
import os
import sys
from pathlib import Path
from typing import IO, Any

ALREADY_RUNNING_EXIT_CODE = 75


class InstanceAlreadyRunning(RuntimeError):
    def __init__(self, lock_path: Path, owner: str = "") -> None:
        detail = f" (PID {owner})" if owner else ""
        super().__init__(f"другой экземпляр уже удерживает {lock_path}{detail}")
        self.lock_path = lock_path
        self.owner = owner


class SingleInstanceLock:
    """Cross-platform advisory lock held for the lifetime of the process."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        self._file: IO[str] | None = None

    def acquire(self) -> None:
        if self._file is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        with contextlib.suppress(OSError):
            os.chmod(self.path, 0o600)
        lock_file = os.fdopen(fd, "r+", encoding="ascii", newline="")
        try:
            self._lock_file(lock_file)
        except OSError as exc:
            owner = ""
            with contextlib.suppress(OSError):
                # Byte zero is the Windows lock range; metadata starts at byte one.
                lock_file.seek(1 if sys.platform == "win32" else 0)
                owner = lock_file.read(32).strip()
            lock_file.close()
            if exc.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                raise InstanceAlreadyRunning(self.path, owner) from exc
            raise

        lock_file.seek(1 if sys.platform == "win32" else 0)
        lock_file.truncate()
        lock_file.write(str(os.getpid()))
        lock_file.flush()
        os.fsync(lock_file.fileno())
        self._file = lock_file

    @staticmethod
    def _lock_file(lock_file: IO[str]) -> None:
        if sys.platform == "win32":
            import msvcrt

            if os.fstat(lock_file.fileno()).st_size < 1:
                lock_file.seek(0)
                lock_file.write("0")
                lock_file.flush()
            lock_file.seek(0)
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            return

        fcntl: Any = __import__("fcntl")
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def release(self) -> None:
        lock_file = self._file
        if lock_file is None:
            return
        try:
            if sys.platform == "win32":
                import msvcrt

                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl: Any = __import__("fcntl")
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        finally:
            lock_file.close()
            self._file = None

    def __enter__(self) -> SingleInstanceLock:
        self.acquire()
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.release()


__all__ = [
    "ALREADY_RUNNING_EXIT_CODE",
    "InstanceAlreadyRunning",
    "SingleInstanceLock",
]
