"""Configured TLS endpoint identity and fallback execution policy."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ...config import SERVERS
from .checks import check_tls_endpoint


@dataclass(frozen=True)
class ConfiguredTLSEndpoint:
    domain: str
    primary_port: int
    fallback_ports: tuple[int, ...]
    server_keys: tuple[str, ...]

    @property
    def storage_key(self) -> str:
        return f"{self.domain}:{self.primary_port}"


def configured_tls_endpoints() -> list[ConfiguredTLSEndpoint]:
    grouped: dict[tuple[str, int], dict[str, list[Any]]] = {}
    for server_key, server in SERVERS.items():
        for endpoint in server.tls_endpoints:
            key = (endpoint.host, endpoint.primary_port)
            value = grouped.setdefault(key, {"fallback_ports": [], "server_keys": []})
            value["fallback_ports"].extend(endpoint.fallback_ports)
            value["server_keys"].append(server_key)
    return [
        ConfiguredTLSEndpoint(
            domain=domain,
            primary_port=primary_port,
            fallback_ports=tuple(dict.fromkeys(int(port) for port in value["fallback_ports"])),
            server_keys=tuple(dict.fromkeys(str(key) for key in value["server_keys"])),
        )
        for (domain, primary_port), value in sorted(grouped.items())
    ]


async def check_tls_with_fallback(target: ConfiguredTLSEndpoint) -> dict[str, Any]:
    attempted_ports: list[int] = []
    attempt_errors: list[str] = []
    selected: dict[str, Any] | None = None
    ports = (target.primary_port, *target.fallback_ports)
    for index, port in enumerate(ports):
        attempted_ports.append(port)
        result = await check_tls_endpoint(target.domain, port, list(target.server_keys))
        error = str(result.get("error") or "").strip()
        if error:
            attempt_errors.append(f"{target.domain}:{port} — {error}"[:1000])
        selected = result
        transport_failure = result.get("status") == "error" and result.get("failure_kind") == "transport"
        if not transport_failure:
            break
        if index == len(ports) - 1:
            selected = result

    if selected is None:
        selected = {
            "domain": target.domain,
            "port": target.primary_port,
            "status": "error",
            "failure_kind": "internal",
            "error": "TLS endpoint check did not produce a result",
        }
    effective_port = int(selected.get("port", target.primary_port) or target.primary_port)
    selected.update(
        {
            "port": effective_port,
            "primary_port": target.primary_port,
            "fallback_ports": list(target.fallback_ports),
            "effective_port": effective_port,
            "used_fallback": effective_port != target.primary_port,
            "attempt_errors": attempt_errors,
            "attempted_ports": attempted_ports,
            "servers": list(target.server_keys),
        }
    )
    if selected.get("status") == "error" and len(attempt_errors) > 1:
        selected["error"] = "; ".join(attempt_errors)[:1000]
    return selected


__all__ = [
    "ConfiguredTLSEndpoint",
    "check_tls_with_fallback",
    "configured_tls_endpoints",
]
