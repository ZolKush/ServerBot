"""Explicit container storage preparation with once-only legacy migration."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from ..runtime.lock import InstanceAlreadyRunning
from .bootstrap import initialize_empty_layout
from .errors import PersistenceError
from .layout import LAYOUT_FILE
from .migrate_sqlite import migrate_to_sqlite
from .sqlite_backend import SQLiteBackend


def prepare_storage(
    data_root: Path | str,
    *,
    ptb_path: Path | str | None = None,
    instance_lock: Path | None = None,
) -> dict[str, Any]:
    """Recover SQLite, migrate existing JSON once, or initialize empty storage.

    Existing SQLite always wins, including over an obsolete explicit pickle
    path. Legacy validation and publication remain owned by the migrator. A
    partial legacy layout is refused by bootstrap rather than hidden by a new
    empty database. Both write paths acquire the process lock themselves.
    """
    root = Path(data_root).resolve()
    backend = SQLiteBackend(root)
    if not backend.exists() and ptb_path is None and (root / "telegram-migration.pickle").exists():
        ptb_path = root / "telegram-migration.pickle"
    split_json = (root / LAYOUT_FILE).exists()
    if not backend.exists() and (split_json or ptb_path is not None):
        selected_ptb = Path(ptb_path) if ptb_path is not None else None
        if selected_ptb is not None and not selected_ptb.is_absolute():
            selected_ptb = root / selected_ptb
        report = migrate_to_sqlite(root, ptb_path=selected_ptb, instance_lock=instance_lock)
        return {"action": "migrated", **report.to_dict()}

    initialized = initialize_empty_layout(root, instance_lock=instance_lock)
    return {
        "action": "existing" if initialized.already_initialized else "initialized",
        "database": str(backend.database_path),
        **initialized.to_dict(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prepare SQLite and migrate existing legacy data once before bot startup."
    )
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--instance-lock", type=Path)
    parser.add_argument("--ptb-pickle", type=Path, default=os.getenv("MAINTBOT_PTB_PICKLE", "").strip() or None)
    args = parser.parse_args()
    try:
        report = prepare_storage(args.data_dir, ptb_path=args.ptb_pickle, instance_lock=args.instance_lock)
    except (PersistenceError, InstanceAlreadyRunning, OSError, sqlite3.Error, ValueError, TypeError) as exc:
        parser.exit(2, f"storage preparation refused: {exc}\n")
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main", "prepare_storage"]
