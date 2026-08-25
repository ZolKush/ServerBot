"""Explicit initializer for a brand-new empty split-storage layout."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .backend import SplitJsonBackend
from .errors import PersistenceError, StorageConflictError
from .io import secure_directory


@dataclass(frozen=True, slots=True)
class BootstrapReport:
    already_initialized: bool
    revision: int
    store_count: int
    data_dir: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def initialize_empty_layout(data_root: Path | str) -> BootstrapReport:
    """Create split layout v1 only when no monolithic source is present."""
    root = Path(data_root).resolve()
    backend = SplitJsonBackend(root)
    if backend.exists():
        snapshot = backend.inspect()
        secure_directory(root / "telegram")
        return BootstrapReport(
            already_initialized=True,
            revision=snapshot.revision,
            store_count=len(snapshot.stores),
            data_dir=str(root),
        )
    if backend.has_pending_transactions():
        raise StorageConflictError("pending transaction found; empty initialization refused")
    monolithic = [name for name in ("user_data.json", "important_data.json") if (root / name).exists()]
    if monolithic:
        raise StorageConflictError(f"monolithic data found ({', '.join(monolithic)}); use app.persistence.migration")

    secure_directory(root / "telegram")
    snapshot = backend.bootstrap()
    return BootstrapReport(
        already_initialized=False,
        revision=snapshot.revision,
        store_count=len(snapshot.stores),
        data_dir=str(root),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize a brand-new empty split-storage layout v1.")
    parser.add_argument("--data-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        report = initialize_empty_layout(args.data_dir)
    except PersistenceError as exc:
        parser.exit(2, f"initialization refused: {exc}\n")
    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["BootstrapReport", "initialize_empty_layout", "main"]
