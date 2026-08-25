"""Crash-safe persistence for Telegram conversation and navigation state."""

from __future__ import annotations

import contextlib
import os
import shutil
import tempfile
from pathlib import Path

from telegram.ext import PicklePersistence

from ..config import logger

_BACKUP_SUFFIX = ".bak"


def _backup_path(target: Path) -> Path:
    return target.with_name(target.name + _BACKUP_SUFFIX)


def _replace_file(source: Path, destination: Path) -> None:
    """Small seam kept separate so interrupted commits can be fault-tested."""

    os.replace(source, destination)


def _sync_file(path: Path) -> None:
    # Windows requires a writable descriptor for ``fsync``.
    with path.open("rb+") as handle:
        os.fsync(handle.fileno())


def _sync_directory(path: Path) -> None:
    # Windows does not support opening a directory for fsync. ``os.replace``
    # still provides the atomic name swap there.
    if os.name == "nt":
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _temporary_path(target: Path) -> Path:
    descriptor, raw_path = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=target.parent,
    )
    os.close(descriptor)
    return Path(raw_path)


def _commit_file(temporary: Path, target: Path) -> None:
    """Commit a complete temporary file and retain the previous valid image."""

    backup = _backup_path(target)
    previous_moved = False
    try:
        _sync_file(temporary)
        if target.exists():
            _replace_file(target, backup)
            previous_moved = True
            _sync_directory(target.parent)
        _replace_file(temporary, target)
        with contextlib.suppress(OSError):
            target.chmod(0o600)
        _sync_directory(target.parent)
    except BaseException:
        # The only unsafe window is between the two replacements. Put the
        # previous image back before surfacing the write error to PTB.
        if previous_moved and not target.exists() and backup.is_file():
            with contextlib.suppress(OSError):
                _replace_file(backup, target)
                _sync_directory(target.parent)
        raise
    finally:
        with contextlib.suppress(OSError):
            temporary.unlink()


def _restore_backup(backup: Path, target: Path) -> None:
    temporary = _temporary_path(target)
    try:
        shutil.copyfile(backup, temporary)
        _sync_file(temporary)
        _replace_file(temporary, target)
        with contextlib.suppress(OSError):
            target.chmod(0o600)
        _sync_directory(target.parent)
    finally:
        with contextlib.suppress(OSError):
            temporary.unlink()


class AtomicPicklePersistence(PicklePersistence):
    """PTB pickle persistence with atomic commits and one known-good backup.

    PTB's serializer is intentionally reused so bot references retain its
    supported pickle representation. Only the file commit/load boundary is
    replaced. The project pins PTB, and tests guard this private override.
    """

    __slots__ = ()

    def __init__(self, filepath: str | Path, *, update_interval: float = 60) -> None:
        super().__init__(
            filepath=filepath,
            single_file=True,
            on_flush=False,
            update_interval=update_interval,
        )

    def _dump_singlefile(self) -> None:
        target = self.filepath
        temporary = _temporary_path(target)
        self.filepath = temporary
        try:
            super()._dump_singlefile()
        except BaseException:
            with contextlib.suppress(OSError):
                temporary.unlink()
            raise
        finally:
            self.filepath = target
        _commit_file(temporary, target)

    def _load_singlefile(self) -> None:
        target = self.filepath
        backup = _backup_path(target)
        primary_error: TypeError | None = None

        if target.exists():
            try:
                super()._load_singlefile()
                return
            except TypeError as exc:
                primary_error = exc
                if not backup.is_file():
                    raise
        elif not backup.is_file():
            super()._load_singlefile()
            return

        self.filepath = backup
        try:
            super()._load_singlefile()
        except TypeError as backup_error:
            if primary_error is not None:
                raise primary_error from backup_error
            raise
        finally:
            self.filepath = target

        try:
            _restore_backup(backup, target)
        except OSError as exc:
            raise RuntimeError(f"could not restore Telegram persistence backup: {target.name}") from exc
        logger.warning(
            "Recovered Telegram persistence from backup file=%s",
            target.name,
            extra={"action": "telegram_persistence_recovered"},
        )


def build_atomic_persistence(path: str | Path) -> AtomicPicklePersistence:
    persistence_path = Path(path)
    persistence_dir = persistence_path.parent
    persistence_dir.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):
        persistence_dir.chmod(0o700)
    if persistence_path.exists():
        if not persistence_path.is_file():
            raise RuntimeError(f"PTB_PERSISTENCE_PATH не является файлом: {persistence_path}")
        with contextlib.suppress(OSError):
            persistence_path.chmod(0o600)
    backup = _backup_path(persistence_path)
    if backup.exists():
        if not backup.is_file():
            raise RuntimeError(f"Резервная копия PTB persistence не является файлом: {backup}")
        with contextlib.suppress(OSError):
            backup.chmod(0o600)
    return AtomicPicklePersistence(filepath=persistence_path)


__all__ = ["AtomicPicklePersistence", "build_atomic_persistence"]
