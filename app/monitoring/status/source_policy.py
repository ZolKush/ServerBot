"""Rules for selecting and validating a server's primary metric source."""

from __future__ import annotations

from ...config import ServerTarget
from ..remnawave import NodeMetrics


def server_uses_metrics(server: ServerTarget) -> bool:
    return server.monitoring_source == "remnawave" and bool((server.remnawave_uuid or "").strip())


def node_metrics_problem(node: NodeMetrics | None) -> str:
    if node is None:
        return "Нода не найдена в ответе панели метрик"
    if node.status is None:
        return "Панель метрик не вернула статус ноды"
    if node.is_online and any(value is None for value in (node.uptime_s, node.mem_total, node.mem_free)):
        return "Панель метрик вернула неполные данные ресурсов ноды"
    return ""


__all__ = ["node_metrics_problem", "server_uses_metrics"]
