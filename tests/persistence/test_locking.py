from __future__ import annotations

from pathlib import Path

from app.persistence.locking import StateLock, clear_lock_registry_for_tests


def test_same_state_lock_instance_can_be_nested_without_leaking_lock(tmp_path: Path) -> None:
    lock = StateLock(tmp_path)

    with lock:  # noqa: SIM117 - deliberately re-enter the same context-manager instance
        with lock:
            pass

    # This raises if shared depth or the underlying handle was leaked.
    clear_lock_registry_for_tests()

    with StateLock(tmp_path):
        pass
