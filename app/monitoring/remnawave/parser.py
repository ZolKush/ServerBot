from __future__ import annotations

import math
import re
from typing import TypeAlias

from .models import NodeMetrics

MetricRecord: TypeAlias = tuple[dict[str, str], float]
MetricGroups: TypeAlias = dict[str, dict[str, MetricRecord]]

_METRIC_LINE_RE = re.compile(
    r'^([A-Za-z_:][A-Za-z0-9_:]*)(?:\{((?:[^{}"\\]|"(?:[^"\\]|\\.)*")*)\})?\s+(\S+)(?:\s+[+-]?\d+)?\s*$'
)
_LABEL_RE = re.compile(r'([A-Za-z_][A-Za-z0-9_]*)="((?:[^"\\]|\\.)*)"')

_HOT_METRIC_NAMES = {
    "remnawave_node_status",
    "remnawave_node_online_users",
    "remnawave_node_uptime_seconds",
    "remnawave_node_memory_total_bytes",
    "remnawave_node_memory_free_bytes",
    "remnawave_node_cpu_count",
    "remnawave_node_network_rx_bytes_per_sec",
    "remnawave_node_network_tx_bytes_per_sec",
    "remnawave_node_basic_info",
}


def _unescape_label_value(raw: str) -> str:
    # Prometheus escapes only backslashes, quotes and newlines in label values.
    # ``unicode_escape`` would corrupt multi-byte UTF-8 characters such as emoji.
    result: list[str] = []
    index = 0
    while index < len(raw):
        character = raw[index]
        if character == "\\" and index + 1 < len(raw):
            following = raw[index + 1]
            if following == "n":
                result.append("\n")
                index += 2
                continue
            if following in ('"', "\\"):
                result.append(following)
                index += 2
                continue
        result.append(character)
        index += 1
    return "".join(result)


def _parse_labels(raw: str) -> dict[str, str]:
    if not raw:
        return {}
    return {match.group(1): _unescape_label_value(match.group(2)) for match in _LABEL_RE.finditer(raw)}


def _parse_value(raw: str) -> float | None:
    value = (raw or "").strip()
    if not value:
        return None
    try:
        parsed = float(value)
        # Non-finite samples are legal Prometheus, but unusable node gauges.
        return parsed if math.isfinite(parsed) and parsed >= 0 else None
    except ValueError:
        return None


def parse_prometheus_text(text: str) -> MetricGroups:
    """Parse the RemnaWave subset of Prometheus text metrics, grouped by node UUID."""
    grouped: MetricGroups = {}
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = _METRIC_LINE_RE.match(line)
        if not match:
            continue
        name = match.group(1)
        if name not in _HOT_METRIC_NAMES:
            continue
        labels = _parse_labels(match.group(2) or "")
        value = _parse_value(match.group(3) or "")
        node_uuid = labels.get("node_uuid", "")
        if value is None or not node_uuid:
            continue
        grouped.setdefault(name, {})[node_uuid] = (labels, value)
    return grouped


def build_nodes(grouped: MetricGroups) -> dict[str, NodeMetrics]:
    uuids = {uuid for per_metric in grouped.values() for uuid in per_metric}

    def integer(metric_name: str, uuid: str) -> int | None:
        record = grouped.get(metric_name, {}).get(uuid)
        if not record or not math.isfinite(record[1]) or not 0 <= record[1] < 2**63:
            return None
        return int(record[1]) if float(record[1]).is_integer() else None

    def floating(metric_name: str, uuid: str) -> float | None:
        record = grouped.get(metric_name, {}).get(uuid)
        return float(record[1]) if record and math.isfinite(record[1]) and record[1] >= 0 else None

    nodes: dict[str, NodeMetrics] = {}
    for uuid in uuids:
        info = grouped.get("remnawave_node_basic_info", {}).get(uuid)
        labels = info[0] if info else {}
        status = integer("remnawave_node_status", uuid)
        nodes[uuid] = NodeMetrics(
            uuid=uuid,
            status=status if status in (0, 1) else None,
            online_users=integer("remnawave_node_online_users", uuid),
            uptime_s=floating("remnawave_node_uptime_seconds", uuid),
            mem_total=integer("remnawave_node_memory_total_bytes", uuid),
            mem_free=integer("remnawave_node_memory_free_bytes", uuid),
            cpu_count=integer("remnawave_node_cpu_count", uuid),
            network_rx_per_sec=floating("remnawave_node_network_rx_bytes_per_sec", uuid),
            network_tx_per_sec=floating("remnawave_node_network_tx_bytes_per_sec", uuid),
            node_name=(labels.get("node_name") or "").strip(),
            country_emoji=(labels.get("node_country_emoji") or "").strip(),
        )
    return nodes


__all__ = ["MetricGroups", "build_nodes", "parse_prometheus_text"]
