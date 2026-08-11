"""Runtime server targets built from the canonical TOML inventory."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .inventory import ServerInventoryDocument, load_inventory_document
from .parsing import country_flag


@dataclass(frozen=True)
class TLSEndpoint:
    host: str
    primary_port: int = 443
    fallback_ports: tuple[int, ...] = ()


@dataclass(frozen=True)
class ServerTarget:
    key: str
    label: str
    flag: str
    mode: Literal["local", "ssh"]
    expected_a_ip: str
    check_a_domains: list[str]
    monitor_containers: list[str]
    fail2ban_log_path: str
    fail2ban_enabled: bool = True
    fail2ban_timezone: str = ""
    ssh_target: str = ""
    remnawave_uuid: str = ""
    monitoring_source: Literal["system", "remnawave"] = "system"
    tls_endpoints: tuple[TLSEndpoint, ...] = ()


def build_servers(
    inventory: ServerInventoryDocument,
    *,
    timezone_name: str,
) -> dict[str, ServerTarget]:
    servers: dict[str, ServerTarget] = {}
    for key, item in inventory.servers.items():
        dns_domains = [domain.host for domain in item.domains if "dns" in domain.checks]
        tls_endpoints = tuple(
            TLSEndpoint(
                host=domain.host,
                primary_port=domain.tls_primary_port,
                fallback_ports=tuple(domain.tls_fallback_ports),
            )
            for domain in item.domains
            if "tls" in domain.checks
        )
        servers[key] = ServerTarget(
            key=key,
            label=item.label,
            flag=country_flag(item.flag),
            mode=item.connection.transport,
            expected_a_ip=item.dns.expected_a_ip,
            check_a_domains=dns_domains,
            monitor_containers=list(item.docker.containers),
            fail2ban_log_path=item.fail2ban.log_path,
            fail2ban_enabled=item.fail2ban.enabled,
            fail2ban_timezone=item.fail2ban.timezone or timezone_name,
            ssh_target=item.connection.target,
            remnawave_uuid=item.monitoring.node_uuid,
            monitoring_source=item.monitoring.source,
            tls_endpoints=tls_endpoints,
        )
    return servers


def load_servers(path: str | Path, *, timezone_name: str) -> dict[str, ServerTarget]:
    return build_servers(load_inventory_document(path), timezone_name=timezone_name)


__all__ = [
    "ServerTarget",
    "TLSEndpoint",
    "build_servers",
    "load_servers",
]
