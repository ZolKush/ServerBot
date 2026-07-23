from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class NodeMetrics:
    uuid: str
    status: int | None
    online_users: int | None
    uptime_s: float | None
    mem_total: int | None
    mem_free: int | None
    cpu_count: int | None
    network_rx_per_sec: float | None
    network_tx_per_sec: float | None
    node_name: str = ""
    country_emoji: str = ""

    @property
    def is_online(self) -> bool:
        return self.status == 1

    @property
    def mem_used(self) -> int | None:
        if self.mem_total is None or self.mem_free is None:
            return None
        return max(self.mem_total - self.mem_free, 0)


@dataclass
class MetricsSnapshot:
    nodes: dict[str, NodeMetrics] = field(default_factory=dict)
    fetched_at: datetime | None = None
    error: str = ""

    @property
    def ok(self) -> bool:
        return not self.error

    def get(self, uuid: str) -> NodeMetrics | None:
        if not uuid:
            return None
        return self.nodes.get(uuid)


__all__ = ["MetricsSnapshot", "NodeMetrics"]
