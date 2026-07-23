from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from .parsing import (
    country_flag,
    country_label,
    domains_for_index,
    group_for_index,
    nth_or_default,
)
from .validators import normalize_server_key

if TYPE_CHECKING:
    from .schema import AppSettings


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


def build_servers(settings: AppSettings, *, timezone_name: str) -> dict[str, ServerTarget]:
    local_code = settings.LOCAL_SERVER_CODE
    local_flag = settings.LOCAL_SERVER_FLAG.strip().upper() or local_code.upper()
    monitor_containers = list(settings.MONITOR_CONTAINERS)
    servers: dict[str, ServerTarget] = {
        local_code: ServerTarget(
            key=local_code,
            label=settings.LOCAL_SERVER_LABEL or country_label(local_flag, "Main"),
            flag=country_flag(local_flag),
            mode="local",
            expected_a_ip=settings.EXPECTED_A_IP.strip(),
            check_a_domains=list(settings.CHECK_A_DOMAINS),
            monitor_containers=monitor_containers,
            fail2ban_log_path=settings.FAIL2BAN_LOG_PATH.strip(),
            fail2ban_enabled=bool(settings.FAIL2BAN_ENABLED),
            fail2ban_timezone=settings.FAIL2BAN_TIMEZONE.strip() or timezone_name,
            remnawave_uuid=settings.LOCAL_SERVER_REMNAWAVE_UUID,
        )
    }

    if not settings.REMOTE_SERVER_ENABLED:
        return servers

    targets = list(settings.REMOTE_SERVER_SSH_TARGETS)
    if not targets and settings.REMOTE_SERVER_SSH_TARGET.strip():
        targets = [settings.REMOTE_SERVER_SSH_TARGET.strip()]
    if not targets:
        return servers

    total = len(targets)
    remote_flag = settings.REMOTE_SERVER_FLAG.strip().upper()
    flags = list(settings.REMOTE_SERVER_FLAGS) or ([remote_flag] if remote_flag else [])
    labels = list(settings.REMOTE_SERVER_LABELS)
    codes = list(settings.REMOTE_SERVER_CODES)
    remote_expected_ip = settings.REMOTE_SERVER_EXPECTED_A_IP.strip()
    ips = list(settings.REMOTE_SERVER_EXPECTED_A_IPS) or ([remote_expected_ip] if remote_expected_ip else [])
    old_domains = [list(settings.REMOTE_SERVER_CHECK_A_DOMAINS)] if settings.REMOTE_SERVER_CHECK_A_DOMAINS else []
    domain_groups = [list(group) for group in settings.REMOTE_SERVER_DOMAINS] or old_domains
    container_groups = [list(group) for group in settings.REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER]
    remote_monitor_containers = list(settings.REMOTE_SERVER_MONITOR_CONTAINERS) or monitor_containers
    remote_fail2ban_log_path = settings.REMOTE_SERVER_FAIL2BAN_LOG_PATH.strip() or "/var/log/fail2ban.log"

    flag_counts: dict[str, int] = {}
    key_counts: dict[str, int] = {}
    for index, target in enumerate(targets):
        flag_code = nth_or_default(flags, index, "").strip().upper()
        fallback_code = flag_code.lower() if flag_code else f"srv{index + 1}"
        explicit_code = nth_or_default(codes, index, "")
        key = normalize_server_key(explicit_code, fallback_code)
        if key in servers:
            if explicit_code:
                raise RuntimeError(f"Duplicate server code: {key}")
            base_key = key
            key_counts[base_key] = max(key_counts.get(base_key, 0), 1)
            while key in servers:
                key_counts[base_key] += 1
                key = normalize_server_key(f"{base_key}{key_counts[base_key]}", f"srv{index + 1}")
        else:
            key_counts[key] = max(key_counts.get(key, 0), 1)

        label = nth_or_default(labels, index, "")
        if not label:
            country = country_label(flag_code, f"Server {index + 1}")
            flag_counts[flag_code or key] = flag_counts.get(flag_code or key, 0) + 1
            label = f"{country}(S{flag_counts[flag_code or key]})"

        servers[key] = ServerTarget(
            key=key,
            label=label,
            flag=country_flag(flag_code),
            mode="ssh",
            expected_a_ip=nth_or_default(ips, index, ""),
            check_a_domains=domains_for_index(domain_groups, index, total),
            monitor_containers=group_for_index(container_groups, index, remote_monitor_containers),
            fail2ban_log_path=nth_or_default(
                settings.REMOTE_SERVER_FAIL2BAN_LOG_PATHS,
                index,
                remote_fail2ban_log_path,
            ),
            fail2ban_enabled=nth_or_default(settings.REMOTE_SERVER_FAIL2BAN_ENABLED, index, "true").strip().lower()
            in {"1", "true", "yes", "on"},
            fail2ban_timezone=nth_or_default(
                settings.REMOTE_SERVER_FAIL2BAN_TIMEZONES,
                index,
                timezone_name,
            )
            or timezone_name,
            ssh_target=target,
            remnawave_uuid=nth_or_default(settings.REMOTE_SERVER_REMNAWAVE_UUIDS, index, ""),
        )
    return servers


__all__ = [
    "ServerTarget",
    "build_servers",
]
