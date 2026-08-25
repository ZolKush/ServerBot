"""Immutable snapshots shared by persistence backends and units of work."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class StoreSnapshot:
    name: str
    revision: int
    data: Any


@dataclass(frozen=True, slots=True)
class BackendSnapshot:
    revision: int
    transaction_id: str
    stores: dict[str, StoreSnapshot]
    migration: dict[str, Any] | None

    def data(self, store_name: str) -> Any:
        try:
            return copy.deepcopy(self.stores[store_name].data)
        except KeyError as exc:
            raise KeyError(f"unknown store: {store_name}") from exc


__all__ = ["BackendSnapshot", "StoreSnapshot"]
