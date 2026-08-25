"""Command line entry point for the explicit split-storage migration."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..errors import PersistenceError
from .runner import migrate_v4_to_split


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Migrate exact monolithic JSON schema v4 to split layout v1.",
    )
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--backup-root", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    try:
        report = migrate_v4_to_split(
            args.data_dir,
            dry_run=args.dry_run,
            backup_root=args.backup_root,
        )
    except PersistenceError as exc:
        parser.exit(2, f"migration refused: {exc}\n")
    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
