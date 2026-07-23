from __future__ import annotations

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings

from .locations import ROOT_DIR


class SettingsFields(BaseSettings):
    """Environment-backed fields, separated from cross-field validation."""

    TZ: str = "Europe/Moscow"
    LOG_LEVEL: str = "INFO"
    LOG_JSON: bool = False

    DATA_DIR: str = str(ROOT_DIR / "data")

    MONITOR_CONTAINERS: list[str] = Field(default_factory=list)
    EXPECTED_A_IP: str = ""
    CHECK_A_DOMAINS: list[str] = Field(default_factory=list)
    DNS_RESOLVERS: list[str] = Field(default_factory=lambda: ["1.1.1.1", "8.8.8.8", "77.88.8.8"])

    FAIL2BAN_LOG_PATH: str = "/var/log/fail2ban.log"
    FAIL2BAN_ENABLED: bool = True
    FAIL2BAN_TIMEZONE: str = ""
    FAIL2BAN_DAILY_AT: str = "12:00"
    FAIL2BAN_DIGEST_TAIL_LINES: int = 20000
    FAIL2BAN_DIGEST_MAX_BYTES: int = 3_000_000
    DNS_DAILY_REFRESH_AT: str = "03:05"
    DNS_STARTUP_REFRESH_DELAY_SEC: int = 5
    MAINT_RESTART_NOTIFY_DELAY_SEC: int = 2
    MAINT_RESTART_REMINDER_INTERVAL_SEC: int = 1800

    SUBPROC_SHORT_TIMEOUT: int = 3
    SUBPROC_MEDIUM_TIMEOUT: int = 8

    SSH_STRICT_HOST_KEY_CHECKING: str = "yes"
    SSH_KNOWN_HOSTS_FILE: str = ""
    SSH_IDENTITY_FILE: str = ""
    PRIVILEGED_HELPER_BIN: str = "/usr/local/libexec/maintbot-helper"

    AUTH_FAIL_WINDOW_SEC: int = 300
    AUTH_MAX_FAILS_IN_WINDOW: int = 5
    AUTH_GLOBAL_MAX_FAILS_IN_WINDOW: int = 40
    AUTH_LOCKOUT_SEC: int = 600
    AUTH_PRUNE_INTERVAL_SEC: int = 300
    ACCESS_REQUEST_COOLDOWN_SEC: int = 300
    ERROR_NOTIFY_INTERVAL_SEC: int = 300
    OUTBOX_PROCESS_INTERVAL_SEC: int = 10
    MESSAGE_CLEANUP_ENABLED: bool = True
    MESSAGE_RETENTION_HOURS: int = 24
    MESSAGE_CLEANUP_INTERVAL_SEC: int = 1800

    INSTANCE_LOCK_PATH: str = ""
    PTB_PERSISTENCE_PATH: str = ""
    SUBPROC_MAX_OUTPUT_BYTES: int = 1_000_000
    STATUS_CACHE_TTL_SEC: int = 5

    LOCAL_SERVER_CODE: str = "local"
    LOCAL_SERVER_LABEL: str = "Local server"
    LOCAL_SERVER_FLAG: str = ""

    REMOTE_SERVER_ENABLED: bool = True
    REMOTE_SERVER_CODE: str = "remote"
    REMOTE_SERVER_LABEL: str = "Remote server"
    REMOTE_SERVER_FLAG: str = ""
    REMOTE_SERVER_SSH_TARGET: str = ""
    REMOTE_SERVER_EXPECTED_A_IP: str = ""
    REMOTE_SERVER_CHECK_A_DOMAINS: list[str] = Field(default_factory=list)
    REMOTE_SERVER_FAIL2BAN_LOG_PATH: str = "/var/log/fail2ban.log"
    REMOTE_SERVER_FAIL2BAN_LOG_PATHS: list[str] = Field(default_factory=list)
    REMOTE_SERVER_FAIL2BAN_ENABLED: list[str] = Field(default_factory=list)
    REMOTE_SERVER_FAIL2BAN_TIMEZONES: list[str] = Field(default_factory=list)
    REMOTE_SERVER_MONITOR_CONTAINERS: list[str] = Field(default_factory=list)

    REMOTE_SERVER_FLAGS: list[str] = Field(default_factory=list)
    REMOTE_SERVER_SSH_TARGETS: list[str] = Field(default_factory=list)
    REMOTE_SERVER_EXPECTED_A_IPS: list[str] = Field(default_factory=list)
    REMOTE_SERVER_DOMAINS: list[list[str]] = Field(default_factory=list)
    REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER: list[list[str]] = Field(default_factory=list)
    REMOTE_SERVER_CODES: list[str] = Field(default_factory=list)
    REMOTE_SERVER_LABELS: list[str] = Field(default_factory=list)

    BOT_MODE: Literal["ssh", "mixed"] = "ssh"
    REMNAWAVE_METRICS_URL: str = ""
    REMNAWAVE_METRICS_USER: str = ""
    REMNAWAVE_METRICS_PASS: str = ""
    REMNAWAVE_METRICS_TIMEOUT_SEC: int = 3
    REMNAWAVE_METRICS_CACHE_TTL_SEC: int = 8
    REMNAWAVE_METRICS_MAX_BYTES: int = 2_000_000
    REMNAWAVE_HIDDEN_UUIDS: list[str] = Field(default_factory=list)
    LOCAL_SERVER_REMNAWAVE_UUID: str = ""
    REMOTE_SERVER_REMNAWAVE_UUIDS: list[str] = Field(default_factory=list)
    DAILY_NODE_STATUS_REFRESH_AT: str = "12:00"


__all__ = ["SettingsFields"]
