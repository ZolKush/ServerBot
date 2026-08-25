from app.monitoring.remnawave import (
    NodeMetrics,
    format_memory_bytes,
    format_uptime_seconds,
    parse_prometheus_text,
)
from app.monitoring.remnawave.parser import build_nodes


def test_prometheus_parser_builds_node_metrics_without_corrupting_utf8_labels() -> None:
    grouped = parse_prometheus_text(
        "\n".join(
            [
                'remnawave_node_basic_info{node_uuid="node-1",node_name="Edge \\"A\\"",node_country_emoji="🇫🇮"} 1',
                'remnawave_node_status{node_uuid="node-1"} 1',
                'remnawave_node_online_users{node_uuid="node-1"} 7',
                'remnawave_node_memory_total_bytes{node_uuid="node-1"} 2097152',
                'remnawave_node_memory_free_bytes{node_uuid="node-1"} 524288',
                'unrelated_metric{node_uuid="node-1"} 999',
            ]
        )
    )

    node = build_nodes(grouped)["node-1"]

    assert node.node_name == 'Edge "A"'
    assert node.country_emoji == "🇫🇮"
    assert node.is_online is True
    assert node.online_users == 7
    assert node.mem_used == 1_572_864


def test_prometheus_parser_ignores_invalid_and_uuid_less_samples() -> None:
    grouped = parse_prometheus_text(
        "\n".join(
            [
                "# HELP remnawave_node_status status",
                'remnawave_node_status{node_uuid=""} 1',
                'remnawave_node_status{node_uuid="node-1"} invalid',
                "not prometheus",
            ]
        )
    )

    assert grouped == {}


def test_remnawave_formatters_keep_operator_facing_contract() -> None:
    node = NodeMetrics(
        uuid="node-1",
        status=0,
        online_users=None,
        uptime_s=None,
        mem_total=None,
        mem_free=None,
        cpu_count=None,
        network_rx_per_sec=None,
        network_tx_per_sec=None,
    )

    assert node.is_online is False
    assert node.mem_used is None
    assert format_uptime_seconds(90_060) == "1 д 1 ч 1 м"
    assert format_uptime_seconds(None) == "н/д"
    assert format_memory_bytes(1_048_576, 2_097_152) == "1 / 2 MiB"
    assert format_memory_bytes(None, None) == "н/д"
