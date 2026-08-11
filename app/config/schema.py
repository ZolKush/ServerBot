from __future__ import annotations

from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import ValidationInfo, field_validator, model_validator
from pydantic_settings import SettingsConfigDict
from pydantic_settings.sources import DotEnvSettingsSource

from .locations import ENV_FILE
from .parsing import split_env_list
from .schema_fields import SettingsFields
from .schema_rules import validate_settings_consistency
from .validators import is_uuid


class _PermissiveDotEnvSource(DotEnvSettingsSource):
    """Allow comma-separated list values in .env without requiring JSON."""

    def decode_complex_value(self, field_name: str, field: Any, value: Any) -> Any:
        try:
            return super().decode_complex_value(field_name, field, value)
        except Exception:
            return value


class AppSettings(SettingsFields):
    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE),
        env_file_encoding="utf-8",
        extra="forbid",
        env_ignore_empty=True,
        hide_input_in_errors=True,
    )

    @classmethod
    def settings_customise_sources(cls, settings_cls, init_settings, env_settings, dotenv_settings, **kwargs):
        _ = dotenv_settings
        secrets = kwargs.get("secrets_settings") or kwargs.get("file_secret_settings")
        sources = [init_settings, env_settings, _PermissiveDotEnvSource(settings_cls)]
        if secrets is not None:
            sources.append(secrets)
        return tuple(sources)

    @field_validator("DNS_RESOLVERS", "REMNAWAVE_HIDDEN_UUIDS", mode="before")
    @classmethod
    def _parse_list(cls, value: Any) -> list[str]:
        return split_env_list(value)

    @field_validator("REMNAWAVE_HIDDEN_UUIDS", mode="after")
    @classmethod
    def _validate_uuids(cls, value: list[str]) -> list[str]:
        for item in value:
            if not is_uuid(item):
                raise ValueError(f"REMNAWAVE_HIDDEN_UUIDS: invalid UUID '{item}'")
        return value

    @field_validator("TZ")
    @classmethod
    def _validate_tz(cls, value: str) -> str:
        timezone_name = (value or "").strip() or "UTC"
        try:
            ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown timezone: {timezone_name}") from exc
        return timezone_name

    @field_validator("LOG_LEVEL", mode="before")
    @classmethod
    def _normalize_log_level(cls, value: Any) -> str:
        normalized = str(value or "").strip().upper() or "INFO"
        normalized = {"WARN": "WARNING", "FATAL": "CRITICAL"}.get(normalized, normalized)
        allowed = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"}
        if normalized not in allowed:
            raise ValueError("LOG_LEVEL must be one of CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET")
        return normalized

    @field_validator("FAIL2BAN_DAILY_AT", "DNS_DAILY_REFRESH_AT", "DAILY_NODE_STATUS_REFRESH_AT")
    @classmethod
    def _validate_hhmm(cls, value: str, info: ValidationInfo) -> str:
        normalized = (value or "").strip()
        try:
            hours, minutes = normalized.split(":", 1)
            hour, minute = int(hours), int(minutes)
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{info.field_name} must be HH:MM") from exc
        return f"{hour:02d}:{minute:02d}"

    @field_validator(
        "SUBPROC_SHORT_TIMEOUT",
        "SUBPROC_MEDIUM_TIMEOUT",
        "DNS_STARTUP_REFRESH_DELAY_SEC",
        "MAINT_RESTART_NOTIFY_DELAY_SEC",
    )
    @classmethod
    def _positive_small_int(cls, value: int, info: ValidationInfo) -> int:
        normalized = int(value)
        if not 1 <= normalized <= 3600:
            raise ValueError(f"{info.field_name} must be in range 1..3600")
        return normalized

    @field_validator(
        "AUTH_FAIL_WINDOW_SEC",
        "AUTH_MAX_FAILS_IN_WINDOW",
        "AUTH_GLOBAL_MAX_FAILS_IN_WINDOW",
        "AUTH_LOCKOUT_SEC",
        "AUTH_PRUNE_INTERVAL_SEC",
        "ACCESS_REQUEST_COOLDOWN_SEC",
        "ERROR_NOTIFY_INTERVAL_SEC",
        "OUTBOX_PROCESS_INTERVAL_SEC",
        "MAINT_RESTART_REMINDER_INTERVAL_SEC",
        "STATUS_CACHE_TTL_SEC",
    )
    @classmethod
    def _positive_int(cls, value: int, info: ValidationInfo) -> int:
        normalized = int(value)
        if normalized < 1:
            raise ValueError(f"{info.field_name} must be >= 1")
        return normalized

    @field_validator("NAVIGATION_RETENTION_HOURS")
    @classmethod
    def _message_retention_hours(cls, value: int) -> int:
        normalized = int(value)
        if not 1 <= normalized <= 36:
            raise ValueError("NAVIGATION_RETENTION_HOURS must be in range 1..36")
        return normalized

    @field_validator("NAVIGATION_CLEANUP_INTERVAL_SEC")
    @classmethod
    def _message_cleanup_interval(cls, value: int) -> int:
        normalized = int(value)
        if not 60 <= normalized <= 3600:
            raise ValueError("NAVIGATION_CLEANUP_INTERVAL_SEC must be in range 60..3600")
        return normalized

    @field_validator("REMNAWAVE_METRICS_URL", "SERVER_INVENTORY_FILE", mode="before")
    @classmethod
    def _strip_string(cls, value: Any) -> str:
        return str(value or "").strip()

    @field_validator("REMNAWAVE_METRICS_TIMEOUT_SEC", "REMNAWAVE_METRICS_CACHE_TTL_SEC")
    @classmethod
    def _positive_metrics_int(cls, value: int) -> int:
        normalized = int(value)
        if not 1 <= normalized <= 600:
            raise ValueError("metrics timeout/ttl out of range (1..600)")
        return normalized

    @field_validator("REMNAWAVE_METRICS_MAX_BYTES", "SUBPROC_MAX_OUTPUT_BYTES")
    @classmethod
    def _max_bytes(cls, value: int, info: ValidationInfo) -> int:
        normalized = int(value)
        if not 1024 <= normalized <= 20_000_000:
            raise ValueError(f"{info.field_name} must be in range 1024..20000000")
        return normalized

    @field_validator("FAIL2BAN_DIGEST_TAIL_LINES")
    @classmethod
    def _fail2ban_tail_lines(cls, value: int) -> int:
        normalized = int(value)
        if not 1 <= normalized <= 50_000:
            raise ValueError("FAIL2BAN_DIGEST_TAIL_LINES must be in range 1..50000")
        return normalized

    @field_validator("FAIL2BAN_DIGEST_MAX_BYTES")
    @classmethod
    def _fail2ban_max_bytes(cls, value: int) -> int:
        normalized = int(value)
        if not 1024 <= normalized <= 3_000_000:
            raise ValueError("FAIL2BAN_DIGEST_MAX_BYTES must be in range 1024..3000000")
        return normalized

    @field_validator("SSH_STRICT_HOST_KEY_CHECKING", mode="before")
    @classmethod
    def _validate_ssh_host_key_mode(cls, value: Any) -> str:
        normalized = str(value or "").strip().lower() or "yes"
        if normalized not in {"yes", "no", "ask", "accept-new"}:
            raise ValueError("SSH_STRICT_HOST_KEY_CHECKING must be yes, no, ask, or accept-new")
        return normalized

    @model_validator(mode="after")
    def _validate_consistency(self) -> AppSettings:
        validate_settings_consistency(self)
        return self


__all__ = ["AppSettings"]
