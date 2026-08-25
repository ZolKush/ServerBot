"""A process- and thread-safe re-entrant lock for all JSON state."""

from __future__ import annotations

import os
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any

from .io import secure_directory, tighten_file_permissions
from .layout import STATE_LOCK_FILE


@dataclass
class _SharedLockState:
    mutex: threading.RLock = field(default_factory=threading.RLock)
    depth: int = 0
    handle: IO[bytes] | None = None


_REGISTRY_GUARD = threading.Lock()
_LOCK_REGISTRY: dict[str, _SharedLockState] = {}


def _shared_state(path: Path) -> _SharedLockState:
    key = os.path.normcase(str(path.resolve()))
    with _REGISTRY_GUARD:
        return _LOCK_REGISTRY.setdefault(key, _SharedLockState())


class StateLock:
    """Exclusive global state lock, re-entrant in the owning thread."""

    def __init__(self, data_root: Path) -> None:
        self.path = data_root / STATE_LOCK_FILE
        self._state = _shared_state(self.path)
        self._local_depth = 0

    def __enter__(self) -> StateLock:
        self._state.mutex.acquire()
        try:
            if self._state.depth == 0:
                secure_directory(self.path.parent)
                descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
                handle = os.fdopen(descriptor, "r+b", buffering=0)
                try:
                    if sys.platform == "win32":
                        import msvcrt

                        if os.fstat(handle.fileno()).st_size < 1:
                            handle.write(b"0")
                            handle.flush()
                            os.fsync(handle.fileno())
                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                    else:
                        fcntl: Any = __import__("fcntl")

                        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                except BaseException:
                    handle.close()
                    raise
                tighten_file_permissions(self.path)
                self._state.handle = handle
            self._state.depth += 1
            self._local_depth += 1
            return self
        except BaseException:
            self._state.mutex.release()
            raise

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        if self._local_depth == 0:
            return
        try:
            self._state.depth -= 1
            if self._state.depth == 0:
                handle = self._state.handle
                self._state.handle = None
                if handle is not None:
                    try:
                        if sys.platform == "win32":
                            import msvcrt

                            handle.seek(0)
                            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                        else:
                            fcntl: Any = __import__("fcntl")

                            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                    finally:
                        handle.close()
        finally:
            self._local_depth -= 1
            self._state.mutex.release()


def clear_lock_registry_for_tests() -> None:
    """Forget unused lock objects; only for isolated test processes."""

    with _REGISTRY_GUARD:
        for state in _LOCK_REGISTRY.values():
            if state.depth:
                raise RuntimeError("cannot clear the lock registry while a lock is held")
        _LOCK_REGISTRY.clear()


__all__ = ["StateLock", "clear_lock_registry_for_tests"]
