from __future__ import annotations

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .schema_fields import SettingsFields
from .validators import normalize_server_key


def validate_settings_consistency(settings: SettingsFields) -> None:
    """Validate relationships that involve more than one configuration field."""
    _validate_fail2ban_timezones(settings)
    _validate_remote_servers(settings)
    if settings.SUBPROC_SHORT_TIMEOUT > settings.SUBPROC_MEDIUM_TIMEOUT:
        raise ValueError("SUBPROC_SHORT_TIMEOUT must be <= SUBPROC_MEDIUM_TIMEOUT")
    if settings.BOT_MODE == "mixed" and not settings.REMNAWAVE_METRICS_URL:
        raise ValueError("BOT_MODE=mixed requires REMNAWAVE_METRICS_URL to be set")


def _validate_fail2ban_timezones(settings: SettingsFields) -> None:
    for timezone_name in [settings.FAIL2BAN_TIMEZONE, *settings.REMOTE_SERVER_FAIL2BAN_TIMEZONES]:
        if not timezone_name:
            continue
        try:
            ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown fail2ban timezone: {timezone_name}") from exc


def _validate_remote_servers(settings: SettingsFields) -> None:
    targets = list(settings.REMOTE_SERVER_SSH_TARGETS)
    if not targets and settings.REMOTE_SERVER_SSH_TARGET.strip():
        targets = [settings.REMOTE_SERVER_SSH_TARGET.strip()]
    if not settings.REMOTE_SERVER_ENABLED or not targets:
        return

    if not settings.REMOTE_SERVER_CODES and settings.REMOTE_SERVER_CODE == settings.LOCAL_SERVER_CODE:
        raise ValueError("LOCAL_SERVER_CODE and REMOTE_SERVER_CODE must differ")
    total = len(targets)
    exact_order_fields = {
        "REMOTE_SERVER_FLAGS": settings.REMOTE_SERVER_FLAGS,
        "REMOTE_SERVER_EXPECTED_A_IPS": settings.REMOTE_SERVER_EXPECTED_A_IPS,
        "REMOTE_SERVER_CODES": settings.REMOTE_SERVER_CODES,
        "REMOTE_SERVER_LABELS": settings.REMOTE_SERVER_LABELS,
        "REMOTE_SERVER_FAIL2BAN_LOG_PATHS": settings.REMOTE_SERVER_FAIL2BAN_LOG_PATHS,
        "REMOTE_SERVER_FAIL2BAN_ENABLED": settings.REMOTE_SERVER_FAIL2BAN_ENABLED,
        "REMOTE_SERVER_FAIL2BAN_TIMEZONES": settings.REMOTE_SERVER_FAIL2BAN_TIMEZONES,
        "REMOTE_SERVER_REMNAWAVE_UUIDS": settings.REMOTE_SERVER_REMNAWAVE_UUIDS,
    }
    for field_name, values in exact_order_fields.items():
        if values and len(values) != total:
            raise ValueError(f"{field_name} must contain exactly {total} comma-separated values")

    _validate_remote_fail2ban_flags(settings)
    _validate_remote_server_codes(settings)
    _validate_remote_groups(settings, total)
    if settings.SSH_STRICT_HOST_KEY_CHECKING != "yes":
        raise ValueError("SSH_STRICT_HOST_KEY_CHECKING must be yes when remote servers are enabled")
    if not settings.SSH_KNOWN_HOSTS_FILE.strip():
        raise ValueError("SSH_KNOWN_HOSTS_FILE is required when remote servers are enabled")
    if not settings.SSH_IDENTITY_FILE.strip():
        raise ValueError("SSH_IDENTITY_FILE is required when remote servers are enabled")


def _validate_remote_fail2ban_flags(settings: SettingsFields) -> None:
    allowed = {"1", "0", "true", "false", "yes", "no", "on", "off"}
    if any(value.strip().lower() not in allowed for value in settings.REMOTE_SERVER_FAIL2BAN_ENABLED):
        raise ValueError("REMOTE_SERVER_FAIL2BAN_ENABLED values must be true/false")


def _validate_remote_server_codes(settings: SettingsFields) -> None:
    if not settings.REMOTE_SERVER_CODES:
        return
    normalized_codes = [normalize_server_key(code, "srv") for code in settings.REMOTE_SERVER_CODES]
    if len(set(normalized_codes)) != len(normalized_codes):
        raise ValueError("REMOTE_SERVER_CODES must contain unique values")
    if settings.LOCAL_SERVER_CODE in normalized_codes:
        raise ValueError("REMOTE_SERVER_CODES must not contain LOCAL_SERVER_CODE")


def _validate_remote_groups(settings: SettingsFields, total: int) -> None:
    if settings.REMOTE_SERVER_DOMAINS:
        group_count = len(settings.REMOTE_SERVER_DOMAINS)
        if group_count > 1 and group_count != total:
            raise ValueError(f"REMOTE_SERVER_DOMAINS must contain exactly {total} semicolon-separated groups")
        if group_count == 1 and total > 1 and len(settings.REMOTE_SERVER_DOMAINS[0]) != total:
            raise ValueError(
                "REMOTE_SERVER_DOMAINS must contain one comma-separated domain per server, "
                "or semicolon-separated domain groups per server"
            )
    if settings.REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER:
        group_count = len(settings.REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER)
        if group_count != total:
            raise ValueError(
                f"REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER must contain exactly {total} semicolon-separated groups"
            )


__all__ = ["validate_settings_consistency"]
