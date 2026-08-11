"""Strict TOML schema for the server inventory."""

from __future__ import annotations

import ipaddress
import re
import sys
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .validators import SERVER_KEY_PATTERN, is_container_name, is_uuid, validate_ssh_target

if sys.version_info >= (3, 11):  # pragma: no branch - version-specific import
    import tomllib
else:  # pragma: no cover - exercised on Python 3.10
    import tomli as tomllib


class InventoryError(RuntimeError):
    """Raised when the inventory cannot be loaded safely."""


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, hide_input_in_errors=True)


class ConnectionInventory(_StrictModel):
    transport: Literal["local", "ssh"]
    target: str = ""

    @model_validator(mode="after")
    def _validate_target(self) -> ConnectionInventory:
        self.target = validate_ssh_target(self.target)
        if self.transport == "ssh" and not self.target:
            raise ValueError("connection.target is required when transport='ssh'")
        if self.transport == "local" and self.target:
            raise ValueError("connection.target must be empty when transport='local'")
        return self


class MonitoringInventory(_StrictModel):
    source: Literal["system", "remnawave"] = "system"
    node_uuid: str = ""

    @model_validator(mode="after")
    def _validate_node_uuid(self) -> MonitoringInventory:
        if self.node_uuid and not is_uuid(self.node_uuid):
            raise ValueError("monitoring.node_uuid has invalid UUID format")
        if self.source == "remnawave" and not self.node_uuid:
            raise ValueError("monitoring.node_uuid is required when source='remnawave'")
        if self.source == "system" and self.node_uuid:
            raise ValueError("monitoring.node_uuid is only valid when source='remnawave'")
        return self


class DnsInventory(_StrictModel):
    expected_a_ip: str = ""

    @field_validator("expected_a_ip")
    @classmethod
    def _validate_expected_ip(cls, value: str) -> str:
        if not value:
            return ""
        try:
            return str(ipaddress.ip_address(value))
        except ValueError as exc:
            raise ValueError("dns.expected_a_ip must be an IPv4 or IPv6 address") from exc


def _default_domain_checks() -> list[Literal["dns", "tls"]]:
    return ["dns", "tls"]


class DomainInventory(_StrictModel):
    host: str
    checks: list[Literal["dns", "tls"]] = Field(default_factory=_default_domain_checks)
    tls_primary_port: int = 443
    tls_fallback_ports: list[int] = Field(default_factory=list)

    @field_validator("host")
    @classmethod
    def _normalize_host(cls, value: str) -> str:
        host = value.strip().lower().rstrip(".")
        if not host or len(host) > 253:
            raise ValueError("domain host is empty or too long")
        try:
            ascii_host = host.encode("idna").decode("ascii")
        except UnicodeError as exc:
            raise ValueError("domain host is not valid IDNA") from exc
        labels = ascii_host.split(".")
        if any(
            not label
            or len(label) > 63
            or label.startswith("-")
            or label.endswith("-")
            or re.fullmatch(r"[a-z0-9-]+", label) is None
            for label in labels
        ):
            raise ValueError("domain host has invalid labels")
        return ascii_host

    @field_validator("checks")
    @classmethod
    def _validate_checks(cls, value: list[str]) -> list[str]:
        unique = list(dict.fromkeys(value))
        if not unique:
            raise ValueError("domains[].checks must contain dns and/or tls")
        return unique

    @field_validator("tls_primary_port")
    @classmethod
    def _validate_primary_port(cls, value: int) -> int:
        if not 1 <= int(value) <= 65535:
            raise ValueError("tls_primary_port must be in range 1..65535")
        return int(value)

    @field_validator("tls_fallback_ports")
    @classmethod
    def _validate_fallback_ports(cls, value: list[int]) -> list[int]:
        ports = list(dict.fromkeys(int(port) for port in value))
        if any(not 1 <= port <= 65535 for port in ports):
            raise ValueError("tls_fallback_ports must contain ports in range 1..65535")
        return ports

    @model_validator(mode="after")
    def _validate_tls_options(self) -> DomainInventory:
        if "tls" not in self.checks and (self.tls_primary_port != 443 or self.tls_fallback_ports):
            raise ValueError("TLS ports require 'tls' in domains[].checks")
        if self.tls_primary_port in self.tls_fallback_ports:
            raise ValueError("tls_fallback_ports must not repeat tls_primary_port")
        return self


class DockerInventory(_StrictModel):
    containers: list[str] = Field(default_factory=list)

    @field_validator("containers")
    @classmethod
    def _validate_containers(cls, value: list[str]) -> list[str]:
        containers = list(dict.fromkeys(item.strip() for item in value if item.strip()))
        invalid = [name for name in containers if not is_container_name(name)]
        if invalid:
            raise ValueError(f"invalid Docker container names: {', '.join(invalid)}")
        return containers


class Fail2BanInventory(_StrictModel):
    enabled: bool = True
    log_path: str = "/var/log/fail2ban.log"
    timezone: str = ""

    @field_validator("log_path")
    @classmethod
    def _validate_log_path(cls, value: str) -> str:
        if not value or "\x00" in value or len(value) > 1000:
            raise ValueError("fail2ban.log_path is invalid")
        return value

    @field_validator("timezone")
    @classmethod
    def _validate_timezone(cls, value: str) -> str:
        if not value:
            return ""
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown fail2ban timezone: {value}") from exc
        return value


class ServerInventory(_StrictModel):
    label: str
    flag: str = ""
    connection: ConnectionInventory
    monitoring: MonitoringInventory = Field(default_factory=MonitoringInventory)
    dns: DnsInventory = Field(default_factory=DnsInventory)
    domains: list[DomainInventory] = Field(default_factory=list)
    docker: DockerInventory = Field(default_factory=DockerInventory)
    fail2ban: Fail2BanInventory = Field(default_factory=Fail2BanInventory)

    @field_validator("label")
    @classmethod
    def _validate_label(cls, value: str) -> str:
        if not value or len(value) > 80:
            raise ValueError("server label is empty or too long")
        return value

    @field_validator("flag")
    @classmethod
    def _validate_flag(cls, value: str) -> str:
        flag = value.upper()
        if flag and (len(flag) != 2 or not flag.isalpha()):
            raise ValueError("server flag must be an ISO 3166-1 alpha-2 code")
        return flag

    @model_validator(mode="after")
    def _validate_domains(self) -> ServerInventory:
        hosts = [item.host for item in self.domains]
        if len(set(hosts)) != len(hosts):
            raise ValueError("server domains must contain unique hosts")
        if self.dns.expected_a_ip and not any("dns" in item.checks for item in self.domains):
            raise ValueError("dns.expected_a_ip requires at least one domain with the dns check")
        return self


class ServerInventoryDocument(_StrictModel):
    version: Literal[1]
    servers: dict[str, ServerInventory]

    @field_validator("servers")
    @classmethod
    def _validate_server_keys(cls, value: dict[str, ServerInventory]) -> dict[str, ServerInventory]:
        if not value:
            raise ValueError("at least one server must be configured")
        pattern = re.compile(rf"^{SERVER_KEY_PATTERN}$")
        invalid = [key for key in value if not pattern.fullmatch(key)]
        if invalid:
            raise ValueError(f"invalid server keys: {', '.join(invalid)}")
        local_count = sum(item.connection.transport == "local" for item in value.values())
        if local_count > 1:
            raise ValueError("only one server may use connection.transport='local'")
        return value


def load_inventory_document(path: str | Path) -> ServerInventoryDocument:
    inventory_path = Path(path)
    if not inventory_path.is_file():
        raise InventoryError(f"SERVER_INVENTORY_FILE not found: {inventory_path}")
    try:
        with inventory_path.open("rb") as stream:
            raw = tomllib.load(stream)
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise InventoryError(f"cannot read SERVER_INVENTORY_FILE {inventory_path}: {exc}") from exc
    try:
        return ServerInventoryDocument.model_validate(raw)
    except Exception as exc:
        raise InventoryError(f"invalid SERVER_INVENTORY_FILE {inventory_path}: {exc}") from None


__all__ = [
    "DomainInventory",
    "InventoryError",
    "ServerInventoryDocument",
    "load_inventory_document",
]
