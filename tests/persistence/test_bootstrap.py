import json
from pathlib import Path

import pytest

from app.persistence import SplitJsonBackend, StorageConflictError
from app.persistence.bootstrap import initialize_empty_layout
from app.persistence.layout import STORE_SPECS


def test_empty_layout_bootstrap_is_explicit_and_idempotent(tmp_path: Path) -> None:
    root = tmp_path / "data"

    first = initialize_empty_layout(root)
    second = initialize_empty_layout(root)
    snapshot = SplitJsonBackend(root).inspect()

    assert first.already_initialized is False
    assert second.already_initialized is True
    assert first.revision == second.revision == snapshot.revision == 1
    assert first.store_count == second.store_count == len(STORE_SPECS)
    assert (root / "telegram").is_dir()
    assert set(snapshot.stores) == set(STORE_SPECS)
    assert all(store.revision == 1 for store in snapshot.stores.values())
    assert all(snapshot.data(name) == spec.default_data() for name, spec in STORE_SPECS.items())
    manifest = json.loads((root / "storage_layout.json").read_text(encoding="utf-8"))
    assert manifest["layout"] == "split-json"
    assert manifest["revision"] == 1
    assert set(manifest["stores"]) == set(STORE_SPECS)


@pytest.mark.parametrize("source_name", ["user_data.json", "important_data.json"])
def test_empty_layout_bootstrap_refuses_monolithic_source(tmp_path: Path, source_name: str) -> None:
    root = tmp_path / "data"
    root.mkdir()
    (root / source_name).write_text("{}\n", encoding="utf-8")

    with pytest.raises(StorageConflictError, match="use app.persistence.migration"):
        initialize_empty_layout(root)

    assert not (root / "storage_layout.json").exists()
