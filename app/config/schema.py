from __future__ import annotations

from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import ValidationInfo, field_validator, model_validator
from pydantic_settings import SettingsConfigDict
from pydantic_settings.sources import DotEnvSettingsSource

from ..runtime.logging import logger
from .locations import ENV_FILE
from .parsing import split_env_groups, split_env_list
from .schema_fields import SettingsFields
from .schema_rules import validate_settings_consistency
from .validators import is_container_name, is_uuid, normalize_server_key, validate_ssh_target


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
        extra="ignore",
        env_ignore_empty=True,
        hide_input_in_errors=True,
    )

    @classmethod
    def settings_customise_sources(cls, settings_cls, init_settings, env_settings, dotenv_settings, **kwargs):
        # Pydantic supplies its default dotenv source, but this project
        # replaces it with the comma-list-aware source below.
        _ = dotenv_settings
        secrets = kwargs.get("secrets_settings") or kwargs.get("file_secret_settings")
        sources = [init_settings, env_settings, _PermissiveDotEnvSource(settings_cls)]
        if secrets is not None:
            sources.append(secrets)
        return tuple(sources)

    @field_validator(
        "MONITOR_CONTAINERS",
        "CHECK_A_DOMAINS",
        "DNS_RESOLVERS",
        "REMOTE_SERVER_CHECK_A_DOMAINS",
        "REMOTE_SERVER_MONITOR_CONTAINERS",
        "REMOTE_SERVER_FLAGS",
        "REMOTE_SERVER_SSH_TARGETS",
        "REMOTE_SERVER_EXPECTED_A_IPS",
        "REMOTE_SERVER_CODES",
        "REMOTE_SERVER_LABELS",
        "REMOTE_SERVER_FAIL2BAN_LOG_PATHS",
        "REMOTE_SERVER_FAIL2BAN_ENABLED",
        "REMOTE_SERVER_FAIL2BAN_TIMEZONES",
        "REMNAWAVE_HIDDEN_UUIDS",
        mode="before",
    )
    @classmethod
    def _parse_list(cls, value: Any, info: ValidationInfo) -> list[str]:
        ordered_fields = {
            "REMOTE_SERVER_FLAGS",
            "REMOTE_SERVER_SSH_TARGETS",
            "REMOTE_SERVER_EXPECTED_A_IPS",
            "REMOTE_SERVER_CODES",
            "REMOTE_SERVER_LABELS",
            "REMOTE_SERVER_FAIL2BAN_LOG_PATHS",
            "REMOTE_SERVER_FAIL2BAN_ENABLED",
            "REMOTE_SERVER_FAIL2BAN_TIMEZONES",
        }
        return split_env_list(value, dedupe=info.field_name not in ordered_fields)

    @field_validator("REMOTE_SERVER_REMNAWAVE_UUIDS", mode="before")
    @classmethod
    def _parse_uuid_list_keep_empty(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item or "").strip() for item in value]
        raw = str(value or "").strip()
        if not raw:
            return []
        # Empty positions are meaningful: a remote node may not have a
        # Remnawave UUID while later nodes do.
        return [part.strip() for part in raw.split(",")]

    @field_validator("BOT_MODE", mode="before")
    @classmethod
    def _normalize_bot_mode(cls, value: Any) -> str:
        mode = str(value or "").strip().lower() or "ssh"
        if mode not in ("ssh", "mixed"):
            raise ValueError("BOT_MODE must be 'ssh' or 'mixed'")
        return mode

    @field_validator("REMNAWAVE_METRICS_URL", mode="before")
    @classmethod
    def _normalize_metrics_url(cls, value: Any) -> str:
        return str(value or "").strip()

    @field_validator(
        "REMNAWAVE_METRICS_USER",
        "REMNAWAVE_METRICS_PASS",
        "LOCAL_SERVER_REMNAWAVE_UUID",
        mode="before",
    )
    @classmethod
    def _strip_str(cls, value: Any) -> str:
        return str(value or "").strip()

    @field_validator(
        "LOCAL_SERVER_REMNAWAVE_UUID",
        "REMOTE_SERVER_REMNAWAVE_UUIDS",
        "REMNAWAVE_HIDDEN_UUIDS",
        mode="after",
    )
    @classmethod
    def _validate_uuids(cls, value: Any, info: ValidationInfo) -> Any:
        if isinstance(value, str):
            normalized = value.strip()
            if normalized and not is_uuid(normalized):
                raise ValueError(f"{info.field_name}: invalid UUID format")
            return normalized
        if isinstance(value, list):
            for item in value:
                normalized = str(item or "").strip()
                if normalized and not is_uuid(normalized):
                    raise ValueError(f"{info.field_name}: invalid UUID '{item}'")
            return [str(item or "").strip() for item in value]
        return value

    @field_validator("REMNAWAVE_METRICS_TIMEOUT_SEC", "REMNAWAVE_METRICS_CACHE_TTL_SEC")
    @classmethod
    def _positive_metrics_int(cls, value: int) -> int:
        normalized = int(value)
        if normalized < 1 or normalized > 600:
            raise ValueError("metrics timeout/ttl out of range (1..600)")
        return normalized

    @field_validator("REMNAWAVE_METRICS_MAX_BYTES")
    @classmethod
    def _metrics_max_bytes(cls, value: int) -> int:
        normalized = int(value)
        if not 1024 <= normalized <= 20_000_000:
            raise ValueError("REMNAWAVE_METRICS_MAX_BYTES out of range")
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

    @field_validator("SUBPROC_MAX_OUTPUT_BYTES")
    @classmethod
    def _subprocess_max_output_bytes(cls, value: int) -> int:
        normalized = int(value)
        if not 1024 <= normalized <= 20_000_000:
            raise ValueError("SUBPROC_MAX_OUTPUT_BYTES must be in range 1024..20000000")
        return normalized

    @field_validator("DAILY_NODE_STATUS_REFRESH_AT")
    @classmethod
    def _validate_daily_node_status_at(cls, value: str) -> str:
        normalized = (value or "").strip() or "12:00"
        try:
            hours, minutes = normalized.split(":", 1)
            hour, minute = int(hours), int(minutes)
            if hour < 0 or hour > 23 or minute < 0 or minute > 59:
                raise ValueError
        except Exception as exc:
            raise ValueError("DAILY_NODE_STATUS_REFRESH_AT must be HH:MM") from exc
        return f"{hour:02d}:{minute:02d}"

    @field_validator(
        "REMOTE_SERVER_DOMAINS",
        "REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER",
        mode="before",
    )
    @classmethod
    def _parse_domain_groups(cls, value: Any) -> list[list[str]]:
        return split_env_groups(value)

    @field_validator("MONITOR_CONTAINERS", "REMOTE_SERVER_MONITOR_CONTAINERS", mode="after")
    @classmethod
    def _filter_container_names(cls, value: list[str]) -> list[str]:
        valid: list[str] = []
        for name in value:
            normalized = (name or "").strip()
            if not normalized:
                continue
            if is_container_name(normalized):
                valid.append(normalized)
            else:
                logger.warning("Invalid container name in config skipped: %s", normalized)
        return valid

    @field_validator("REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER", mode="after")
    @classmethod
    def _filter_container_groups(cls, value: list[list[str]]) -> list[list[str]]:
        result: list[list[str]] = []
        for group in value:
            valid: list[str] = []
            for name in group:
                normalized = (name or "").strip()
                if not normalized:
                    continue
                if is_container_name(normalized):
                    valid.append(normalized)
                else:
                    logger.warning("Invalid container name in config skipped: %s", normalized)
            result.append(valid)
        return result

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
        aliases = {"WARN": "WARNING", "FATAL": "CRITICAL"}
        normalized = aliases.get(normalized, normalized)
        allowed = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"}
        if normalized not in allowed:
            raise ValueError("LOG_LEVEL must be one of CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET")
        return normalized

    @field_validator("FAIL2BAN_DAILY_AT", "DNS_DAILY_REFRESH_AT")
    @classmethod
    def _validate_hhmm(cls, value: str) -> str:
        normalized = (value or "").strip()
        if not normalized:
            raise ValueError("HH:MM value is empty")
        hours, minutes = normalized.split(":", 1)
        hour, minute = int(hours), int(minutes)
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            raise ValueError("HH:MM value must be HH:MM")
        return f"{hour:02d}:{minute:02d}"

    @field_validator(
        "SUBPROC_SHORT_TIMEOUT",
        "SUBPROC_MEDIUM_TIMEOUT",
        "DNS_STARTUP_REFRESH_DELAY_SEC",
        "MAINT_RESTART_NOTIFY_DELAY_SEC",
    )
    @classmethod
    def _positive_small_int(cls, value: int) -> int:
        normalized = int(value)
        if normalized < 1 or normalized > 3600:
            raise ValueError("timeout/count out of range")
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

    @field_validator("MESSAGE_RETENTION_HOURS")
    @classmethod
    def _message_retention_hours(cls, value: int) -> int:
        normalized = int(value)
        if not 1 <= normalized <= 36:
            raise ValueError("MESSAGE_RETENTION_HOURS must be in range 1..36")
        return normalized

    @field_validator("MESSAGE_CLEANUP_INTERVAL_SEC")
    @classmethod
    def _message_cleanup_interval(cls, value: int) -> int:
        normalized = int(value)
        if not 60 <= normalized <= 3600:
            raise ValueError("MESSAGE_CLEANUP_INTERVAL_SEC must be in range 60..3600")
        return normalized

    @field_validator("LOCAL_SERVER_CODE", "REMOTE_SERVER_CODE", mode="before")
    @classmethod
    def _normalize_server_code(cls, value: Any) -> str:
        return normalize_server_key(str(value or ""), "srv")

    @field_validator("LOCAL_SERVER_LABEL", "REMOTE_SERVER_LABEL", mode="before")
    @classmethod
    def _normalize_label(cls, value: Any) -> str:
        return str(value or "").strip()

    @field_validator("REMOTE_SERVER_SSH_TARGET", mode="before")
    @classmethod
    def _normalize_ssh_target(cls, value: Any) -> str:
        return validate_ssh_target(str(value or ""))

    @field_validator("REMOTE_SERVER_SSH_TARGETS", mode="after")
    @classmethod
    def _normalize_ssh_targets(cls, value: list[str]) -> list[str]:
        return [validate_ssh_target(item) for item in value if str(item or "").strip()]

    @field_validator("SSH_STRICT_HOST_KEY_CHECKING", mode="before")
    @classmethod
    def _validate_ssh_host_key_mode(cls, value: Any) -> str:
        normalized = str(value or "").strip().lower() or "yes"
        if normalized not in {"yes", "no", "ask", "accept-new"}:
            raise ValueError("SSH_STRICT_HOST_KEY_CHECKING must be yes, no, ask, or accept-new")
        return normalized

    @model_validator(mode="after")
    def _validate_server_codes(self) -> AppSettings:
        validate_settings_consistency(self)
        return self


__all__ = ["AppSettings"]
