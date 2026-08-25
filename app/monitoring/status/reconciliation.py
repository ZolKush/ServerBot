"""Reconcile persisted monitoring projections with the startup inventory."""

from __future__ import annotations

from collections.abc import Mapping

from ...config import ServerTarget, server_monitoring_fingerprint
from ...persistence.aggregates import ImportantData
from ...storage import update_important_data

_SERVER_KEYED_FIELDS = (
    "dns_status",
    "daily_node_status",
    "docker_status",
    "fail2ban_cursors",
)


def _expected_tls(servers: Mapping[str, ServerTarget]) -> dict[str, tuple[list[str], list[int]]]:
    grouped: dict[str, tuple[list[str], list[int]]] = {}
    for server_key, server in servers.items():
        for endpoint in server.tls_endpoints:
            storage_key = f"{endpoint.host}:{endpoint.primary_port}"
            server_keys, fallback_ports = grouped.setdefault(storage_key, ([], []))
            if server_key not in server_keys:
                server_keys.append(server_key)
            for port in endpoint.fallback_ports:
                if port not in fallback_ports:
                    fallback_ports.append(port)
    return grouped


async def reconcile_configured_servers(servers: Mapping[str, ServerTarget]) -> dict[str, int]:
    """Drop projections for deleted servers and for changed, reused server keys."""

    fingerprints = {str(key): server_monitoring_fingerprint(server) for key, server in servers.items()}
    expected_tls = _expected_tls(servers)

    def apply(aggregate: ImportantData) -> dict[str, int]:
        removed: dict[str, int] = {}
        for field_name in _SERVER_KEYED_FIELDS:
            mapping = getattr(aggregate, field_name)
            stale = [
                key
                for key, payload in mapping.items()
                if key not in fingerprints
                or not isinstance(payload, dict)
                or payload.get("_config_fingerprint") != fingerprints[key]
            ]
            for key in stale:
                mapping.pop(key, None)
            removed[field_name] = len(stale)

        tls_removed = 0
        for storage_key, raw_item in list(aggregate.tls_certificates.items()):
            item = dict(raw_item) if isinstance(raw_item, dict) else {}
            expected = expected_tls.get(storage_key)
            actual_servers = [str(key) for key in item.get("servers", [])]
            actual_fallbacks = [int(port) for port in item.get("fallback_ports", []) if str(port).isdigit()]
            if expected is None or actual_servers != expected[0] or actual_fallbacks != expected[1]:
                aggregate.tls_certificates.pop(storage_key, None)
                tls_removed += 1
        removed["tls_certificates"] = tls_removed
        return removed

    return await update_important_data(apply)


__all__ = ["reconcile_configured_servers"]
