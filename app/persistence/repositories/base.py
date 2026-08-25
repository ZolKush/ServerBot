"""Small mutable repository implementations used inside one Unit of Work."""

from __future__ import annotations

import copy
from collections.abc import Iterable, Mapping
from typing import Any


class MappingRepository:
    def __init__(self, data: Mapping[str, Any]) -> None:
        self._items: dict[str, Any] = copy.deepcopy(dict(data))

    def get(self, key: str | int) -> Any | None:
        value = self._items.get(str(key))
        return copy.deepcopy(value)

    def require(self, key: str | int) -> Any:
        normalized = str(key)
        if normalized not in self._items:
            raise KeyError(normalized)
        return copy.deepcopy(self._items[normalized])

    def put(self, key: str | int, value: Any) -> None:
        self._items[str(key)] = copy.deepcopy(value)

    def remove(self, key: str | int) -> Any | None:
        value = self._items.pop(str(key), None)
        return copy.deepcopy(value)

    def contains(self, key: str | int) -> bool:
        return str(key) in self._items

    def replace(self, values: Mapping[str, Any]) -> None:
        self._items = copy.deepcopy(dict(values))

    def keys(self) -> tuple[str, ...]:
        return tuple(self._items)

    def values(self) -> list[Any]:
        return copy.deepcopy(list(self._items.values()))

    def items(self) -> list[tuple[str, Any]]:
        return [(key, copy.deepcopy(value)) for key, value in self._items.items()]

    def export(self) -> dict[str, Any]:
        return copy.deepcopy(self._items)


class IndexedRepository(MappingRepository):
    def __init__(self, data: Mapping[str, Any]) -> None:
        next_id = data.get("next_id")
        items = data.get("items")
        if isinstance(next_id, bool) or not isinstance(next_id, int) or next_id < 0:
            raise ValueError("indexed repository requires a non-negative next_id")
        if not isinstance(items, dict):
            raise ValueError("indexed repository requires an items object")
        super().__init__(items)
        self._next_id = next_id

    @property
    def next_id(self) -> int:
        return self._next_id

    def allocate_id(self) -> int:
        self._next_id += 1
        return self._next_id

    def put(self, key: str | int, value: Any) -> None:
        normalized = str(key)
        super().put(normalized, value)
        try:
            numeric = int(normalized)
        except (TypeError, ValueError):
            return
        self._next_id = max(self._next_id, numeric)

    def replace_all(self, *, next_id: int, items: Mapping[str, Any]) -> None:
        if isinstance(next_id, bool) or not isinstance(next_id, int) or next_id < 0:
            raise ValueError("indexed repository requires a non-negative next_id")
        self._next_id = next_id
        self.replace(items)

    def export(self) -> dict[str, Any]:
        return {"next_id": self._next_id, "items": super().export()}


class ListRepository:
    def __init__(self, data: Iterable[Any]) -> None:
        self._items = copy.deepcopy(list(data))

    def append(self, value: Any) -> None:
        self._items.append(copy.deepcopy(value))

    def extend(self, values: Iterable[Any]) -> None:
        self._items.extend(copy.deepcopy(list(values)))

    def values(self) -> list[Any]:
        return copy.deepcopy(self._items)

    def replace(self, values: Iterable[Any]) -> None:
        self._items = copy.deepcopy(list(values))

    def trim_to_last(self, count: int) -> None:
        if count < 0:
            raise ValueError("trim count must be non-negative")
        if count == 0:
            self._items.clear()
        elif len(self._items) > count:
            self._items = self._items[-count:]

    def export(self) -> list[Any]:
        return copy.deepcopy(self._items)


class DocumentRepository:
    def __init__(self, data: Mapping[str, Any]) -> None:
        self._data = copy.deepcopy(dict(data))

    def get(self, key: str, default: Any = None) -> Any:
        return copy.deepcopy(self._data.get(key, default))

    def set(self, key: str, value: Any) -> None:
        self._data[key] = copy.deepcopy(value)

    def replace(self, value: Mapping[str, Any]) -> None:
        self._data = copy.deepcopy(dict(value))

    def export(self) -> dict[str, Any]:
        return copy.deepcopy(self._data)


__all__ = [
    "DocumentRepository",
    "IndexedRepository",
    "ListRepository",
    "MappingRepository",
]
