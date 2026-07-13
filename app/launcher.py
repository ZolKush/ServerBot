from __future__ import annotations

from .config import INSTANCE_LOCK_PATH, logger
from .single_instance import ALREADY_RUNNING_EXIT_CODE, InstanceAlreadyRunning, SingleInstanceLock


def main() -> None:
    lock = SingleInstanceLock(INSTANCE_LOCK_PATH)
    try:
        lock.acquire()
    except InstanceAlreadyRunning as exc:
        logger.warning("MaintBot не запущен: %s", exc)
        raise SystemExit(ALREADY_RUNNING_EXIT_CODE) from exc
    try:
        # Import handlers/storage only after the process owns the instance lock.
        # This prevents a duplicate process from even running state migrations.
        from .main import run_application

        run_application(instance_lock=lock)
    finally:
        lock.release()


if __name__ == "__main__":
    main()
