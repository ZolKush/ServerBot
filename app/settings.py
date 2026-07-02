import json
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

from dotenv import dotenv_values
from pydantic import BaseModel, Field, ValidationError, ValidationInfo, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic_settings.sources import DotEnvSettingsSource

from .logging_setup import logger

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent

_ENV_PATH = os.getenv("ENV_PATH", "").strip()
ENV_FILE = Path(_ENV_PATH) if _ENV_PATH else (BASE_DIR / ".env")
_SECRETS_ENV_PATH = os.getenv("SECRETS_ENV_PATH", "").strip()
SECRETS_ENV_FILE = Path(_SECRETS_ENV_PATH) if _SECRETS_ENV_PATH else (BASE_DIR / "env.secrets")
_SECRET_KEYS = ("BOT_TOKEN", "AUTH_PASSWORD", "ADMIN_PASSWORD")
_SSH_TARGET_RE = re.compile(r"^[A-Za-z0-9_.:@\-\[\]]{1,255}$")
_SSH_TARGET_WITH_PORT_RE = re.compile(r"^[A-Za-z0-9_.@\-\[\]:]+:(\d{1,5})$")
_SERVER_CODE_RE = re.compile(r"[^a-z0-9_-]+")
_CONTAINER_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.\-]{0,62}$")
_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
SERVER_KEY_PATTERN = r"[a-z0-9_-]{1,12}"
_COUNTRY_NAMES = {
    "DE": "Germany",
    "NL": "Netherlands",
    "RU": "Russia",
    "FI": "Finland",
    "FR": "France",
    "PL": "Poland",
    "US": "United States",
    "GB": "United Kingdom",
}


def split_env_list(raw: Any, *, dedupe: bool = True) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        if s.startswith("[") and s.endswith("]"):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    raw = parsed
            except Exception:
                pass
    if isinstance(raw, list):
        items = [str(x).strip() for x in raw]
    else:
        items = [p.strip() for p in str(raw).split(",")]
    out: list[str] = []
    for item in items:
        if item and (not dedupe or item not in out):
            out.append(item)
    return out


def split_env_groups(raw: Any) -> list[list[str]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        groups: list[list[str]] = []
        for item in raw:
            group = split_env_list(item)
            if group:
                groups.append(group)
        return groups

    s = str(raw or "").strip()
    if not s:
        return []
    if ";" in s:
        return [split_env_list(part, dedupe=False) for part in s.split(";")]
    items = split_env_list(s)
    return [items] if items else []


def resolve_path(value: str, base: Path) -> str:
    v = (value or "").strip()
    if not v:
        return str(base)
    p = Path(v)
    return str(p if p.is_absolute() else (base / p))


def resolve_bin(*candidates: str) -> str:
    for cand in candidates:
        if not cand:
            continue
        path = shutil.which(cand)
        if path:
            return path
    return candidates[-1] if candidates else ""


def country_flag(value: str) -> str:
    code = (value or "").strip().upper()
    if len(code) != 2 or not code.isalpha():
        return "🖥"
    base = 0x1F1E6
    return "".join(chr(base + ord(ch) - ord("A")) for ch in code)


def country_label(value: str, fallback: str) -> str:
    code = (value or "").strip().upper()
    if len(code) == 2 and code.isalpha():
        return _COUNTRY_NAMES.get(code, code)
    return fallback.strip() or "Server"


def normalize_server_key(value: str, fallback: str) -> str:
    raw = (value or fallback or "srv").strip().lower()
    raw = _SERVER_CODE_RE.sub("-", raw).strip("-_")
    return (raw or "srv")[:12]


def _nth_or_default(items: list[str], index: int, default: str = "") -> str:
    if index < len(items):
        return items[index]
    return default


def _domains_for_index(groups: list[list[str]], index: int, total: int, fallback: list[str] | None = None) -> list[str]:
    if not groups:
        return list(fallback or [])
    if len(groups) == total:
        return list(groups[index])
    if len(groups) == 1:
        one = list(groups[0])
        if total > 1 and len(one) == total:
            return [one[index]]
        return one if index == 0 else list(fallback or [])
    return list(groups[index]) if index < len(groups) else list(fallback or [])


def _group_for_index(groups: list[list[str]], index: int, fallback: list[str] | None = None) -> list[str]:
    if not groups:
        return list(fallback or [])
    return list(groups[index]) if index < len(groups) else list(fallback or [])


def _validate_ssh_target(value: str) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    if s.startswith("-") or not _SSH_TARGET_RE.fullmatch(s):
        raise ValueError("SSH target has invalid format")
    m = _SSH_TARGET_WITH_PORT_RE.fullmatch(s)
    if m and not 1 <= int(m.group(1)) <= 65535:
        raise ValueError("SSH target port out of range")
    return s


class SecretSettings(BaseModel):
    BOT_TOKEN: str
    AUTH_PASSWORD: str = ""
    ADMIN_PASSWORD: str = ""

    @field_validator("BOT_TOKEN", mode="before")
    @classmethod
    def _strip_bot_token(cls, v: Any) -> str:
        s = str(v or "").strip()
        if not s:
            raise ValueError("empty secret")
        return s

    @field_validator("AUTH_PASSWORD", "ADMIN_PASSWORD", mode="before")
    @classmethod
    def _strip_password(cls, v: Any) -> str:
        return str(v or "").strip()

    @model_validator(mode="after")
    def _require_at_least_one_password(self) -> "SecretSettings":
        if not self.AUTH_PASSWORD and not self.ADMIN_PASSWORD:
            raise ValueError("хотя бы один из AUTH_PASSWORD или ADMIN_PASSWORD должен быть задан")
        return self


def _load_env_file_values(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    if not path.is_file():
        raise RuntimeError(f"Путь к env-файлу не является файлом: {path}")
    out: dict[str, str] = {}
    try:
        raw = dotenv_values(path)
    except Exception as e:
        raise RuntimeError(f"Не удалось прочитать env-файл {path}: {e}") from e
    for key, value in raw.items():
        if value is None:
            continue
        sval = str(value).strip()
        if sval:
            out[str(key)] = sval
    return out


def _extract_missing_fields(exc: ValidationError) -> str:
    missing: list[str] = []
    for err in exc.errors():
        loc = err.get("loc") or ()
        if loc:
            missing.append(str(loc[0]))
    return ", ".join(sorted(set(missing))) if missing else str(exc)


def load_required_secrets(path: Path, *, fallback_path: Path | None = None) -> SecretSettings:
    merged: dict[str, str] = {}
    checked_sources: list[str] = []

    if fallback_path:
        checked_sources.append(str(fallback_path))
        merged.update({k: v for k, v in _load_env_file_values(fallback_path).items() if k in _SECRET_KEYS})

    checked_sources.append(str(path))
    merged.update({k: v for k, v in _load_env_file_values(path).items() if k in _SECRET_KEYS})

    for key in _SECRET_KEYS:
        env_value = os.getenv(key, "").strip()
        if env_value:
            merged[key] = env_value
    checked_sources.append("переменные окружения процесса")

    try:
        return SecretSettings.model_validate(merged)
    except ValidationError as e:
        missing_s = _extract_missing_fields(e)
        raise RuntimeError(
            "Не заданы обязательные секреты: "
            f"{missing_s}. Проверены источники: {', '.join(checked_sources)}. "
            "Рекомендуемый вариант: хранить секреты в app/env.secrets; "
            "также поддерживаются app/.env и переменные окружения процесса."
        ) from e


class _PermissiveDotEnvSource(DotEnvSettingsSource):
    """Allows comma-separated list values in .env without requiring JSON format."""

    def decode_complex_value(self, field_name: str, field: Any, value: Any) -> Any:
        try:
            return super().decode_complex_value(field_name, field, value)
        except Exception:
            return value


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE),
        env_file_encoding="utf-8",
        extra="ignore",
        env_ignore_empty=True,
    )

    @classmethod
    def settings_customise_sources(cls, settings_cls, init_settings, env_settings, dotenv_settings, **kwargs):
        secrets = kwargs.get("secrets_settings") or kwargs.get("file_secret_settings")
        sources = [init_settings, env_settings, _PermissiveDotEnvSource(settings_cls)]
        if secrets is not None:
            sources.append(secrets)
        return tuple(sources)

    TZ: str = "Europe/Moscow"
    LOG_LEVEL: str = "INFO"
    LOG_JSON: bool = False

    USER_DATA_PATH: str = str(ROOT_DIR / "data" / "user_data.json")
    IMPORTANT_DATA_PATH: str = str(ROOT_DIR / "data" / "important_data.json")
    CONFIG_PATH: str = str(ROOT_DIR / "data" / "config.json")

    MONITOR_CONTAINERS: list[str] = Field(default_factory=list)
    EXPECTED_A_IP: str = ""
    CHECK_A_DOMAINS: list[str] = Field(default_factory=list)
    DNS_RESOLVERS: list[str] = Field(default_factory=lambda: ["1.1.1.1", "8.8.8.8", "77.88.8.8"])

    FAIL2BAN_LOG_PATH: str = "/var/log/fail2ban.log"
    FAIL2BAN_STATE_PATH: str = ""
    FAIL2BAN_DAILY_AT: str = "12:00"
    FAIL2BAN_DIGEST_TAIL_LINES: int = 20000
    FAIL2BAN_DIGEST_MAX_BYTES: int = 3_000_000
    DNS_DAILY_REFRESH_AT: str = "03:05"
    DNS_STARTUP_REFRESH_DELAY_SEC: int = 5
    MAINT_RESTART_NOTIFY_DELAY_SEC: int = 2
    MAINT_RESTART_REMINDER_INTERVAL_SEC: int = 1800

    SUBPROC_SHORT_TIMEOUT: int = 3
    SUBPROC_MEDIUM_TIMEOUT: int = 8

    SSH_STRICT_HOST_KEY_CHECKING: str = "accept-new"
    SSH_KNOWN_HOSTS_FILE: str = ""

    BROADCAST_MAX_CONCURRENCY: int = 8
    BROADCAST_MAX_ATTEMPTS: int = 3

    AUTH_FAIL_WINDOW_SEC: int = 300
    AUTH_MAX_FAILS_IN_WINDOW: int = 5
    AUTH_LOCKOUT_SEC: int = 600
    AUTH_PRUNE_INTERVAL_SEC: int = 300
    ERROR_NOTIFY_INTERVAL_SEC: int = 300

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
    REMNAWAVE_HIDDEN_UUIDS: list[str] = Field(default_factory=list)
    LOCAL_SERVER_REMNAWAVE_UUID: str = ""
    REMOTE_SERVER_REMNAWAVE_UUIDS: list[str] = Field(default_factory=list)
    DAILY_NODE_STATUS_REFRESH_AT: str = "12:00"

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
        "REMNAWAVE_HIDDEN_UUIDS",
        mode="before",
    )
    @classmethod
    def _parse_list(cls, v: Any, info: ValidationInfo) -> list[str]:
        ordered_fields = {
            "REMOTE_SERVER_FLAGS",
            "REMOTE_SERVER_SSH_TARGETS",
            "REMOTE_SERVER_EXPECTED_A_IPS",
            "REMOTE_SERVER_CODES",
            "REMOTE_SERVER_LABELS",
        }
        return split_env_list(v, dedupe=info.field_name not in ordered_fields)

    @field_validator("REMOTE_SERVER_REMNAWAVE_UUIDS", mode="before")
    @classmethod
    def _parse_uuid_list_keep_empty(cls, v: Any) -> list[str]:
        if v is None:
            return []
        if isinstance(v, list):
            return [str(x or "").strip() for x in v]
        s = str(v or "").strip()
        if not s:
            return []
        # Сохраняем пустые позиции — пользователь может пропустить сервер,
        # для которого UUID нет (например, нода не зарегистрирована в RW).
        return [p.strip() for p in s.split(",")]

    @field_validator("BOT_MODE", mode="before")
    @classmethod
    def _normalize_bot_mode(cls, v: Any) -> str:
        s = str(v or "").strip().lower() or "ssh"
        if s not in ("ssh", "mixed"):
            raise ValueError("BOT_MODE must be 'ssh' or 'mixed'")
        return s

    @field_validator("REMNAWAVE_METRICS_URL", mode="before")
    @classmethod
    def _normalize_metrics_url(cls, v: Any) -> str:
        return str(v or "").strip()

    @field_validator("REMNAWAVE_METRICS_USER", "REMNAWAVE_METRICS_PASS", "LOCAL_SERVER_REMNAWAVE_UUID", mode="before")
    @classmethod
    def _strip_str(cls, v: Any) -> str:
        return str(v or "").strip()

    @field_validator("LOCAL_SERVER_REMNAWAVE_UUID", "REMOTE_SERVER_REMNAWAVE_UUIDS", "REMNAWAVE_HIDDEN_UUIDS", mode="after")
    @classmethod
    def _validate_uuids(cls, v: Any, info: ValidationInfo) -> Any:
        if isinstance(v, str):
            s = v.strip()
            if s and not _UUID_RE.fullmatch(s):
                raise ValueError(f"{info.field_name}: invalid UUID format")
            return s
        if isinstance(v, list):
            for item in v:
                s = str(item or "").strip()
                if s and not _UUID_RE.fullmatch(s):
                    raise ValueError(f"{info.field_name}: invalid UUID '{item}'")
            return [str(x or "").strip() for x in v]
        return v

    @field_validator("REMNAWAVE_METRICS_TIMEOUT_SEC", "REMNAWAVE_METRICS_CACHE_TTL_SEC")
    @classmethod
    def _positive_metrics_int(cls, v: int) -> int:
        iv = int(v)
        if iv < 1 or iv > 600:
            raise ValueError("metrics timeout/ttl out of range (1..600)")
        return iv

    @field_validator("DAILY_NODE_STATUS_REFRESH_AT")
    @classmethod
    def _validate_daily_node_status_at(cls, v: str) -> str:
        s = (v or "").strip() or "12:00"
        try:
            hh, mm = s.split(":", 1)
            ih, im = int(hh), int(mm)
            if ih < 0 or ih > 23 or im < 0 or im > 59:
                raise ValueError
        except Exception as e:
            raise ValueError("DAILY_NODE_STATUS_REFRESH_AT must be HH:MM") from e
        return f"{ih:02d}:{im:02d}"

    @field_validator("REMOTE_SERVER_DOMAINS", "REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER", mode="before")
    @classmethod
    def _parse_domain_groups(cls, v: Any) -> list[list[str]]:
        return split_env_groups(v)

    @field_validator("MONITOR_CONTAINERS", "REMOTE_SERVER_MONITOR_CONTAINERS", mode="after")
    @classmethod
    def _filter_container_names(cls, v: list[str]) -> list[str]:
        valid: list[str] = []
        for name in v:
            nm = (name or "").strip()
            if not nm:
                continue
            if _CONTAINER_NAME_RE.fullmatch(nm):
                valid.append(nm)
            else:
                logger.warning("Invalid container name in config skipped: %s", nm)
        return valid

    @field_validator("REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER", mode="after")
    @classmethod
    def _filter_container_groups(cls, v: list[list[str]]) -> list[list[str]]:
        out: list[list[str]] = []
        for group in v:
            valid: list[str] = []
            for name in group:
                nm = (name or "").strip()
                if not nm:
                    continue
                if _CONTAINER_NAME_RE.fullmatch(nm):
                    valid.append(nm)
                else:
                    logger.warning("Invalid container name in config skipped: %s", nm)
            out.append(valid)
        return out

    @field_validator("TZ")
    @classmethod
    def _validate_tz(cls, v: str) -> str:
        ZoneInfo((v or "").strip() or "UTC")
        return (v or "").strip()

    @field_validator("LOG_LEVEL", mode="before")
    @classmethod
    def _normalize_log_level(cls, v: Any) -> str:
        s = str(v or "").strip().upper() or "INFO"
        aliases = {"WARN": "WARNING", "FATAL": "CRITICAL"}
        s = aliases.get(s, s)
        allowed = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"}
        if s not in allowed:
            raise ValueError("LOG_LEVEL must be one of CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET")
        return s

    @field_validator("FAIL2BAN_DAILY_AT", "DNS_DAILY_REFRESH_AT")
    @classmethod
    def _validate_hhmm(cls, v: str) -> str:
        s = (v or "").strip()
        if not s:
            raise ValueError("HH:MM value is empty")
        hh, mm = s.split(":", 1)
        ih, im = int(hh), int(mm)
        if ih < 0 or ih > 23 or im < 0 or im > 59:
            raise ValueError("HH:MM value must be HH:MM")
        return f"{ih:02d}:{im:02d}"

    @field_validator(
        "SUBPROC_SHORT_TIMEOUT",
        "SUBPROC_MEDIUM_TIMEOUT",
        "DNS_STARTUP_REFRESH_DELAY_SEC",
        "MAINT_RESTART_NOTIFY_DELAY_SEC",
    )
    @classmethod
    def _positive_small_int(cls, v: int) -> int:
        iv = int(v)
        if iv < 1 or iv > 3600:
            raise ValueError("timeout/count out of range")
        return iv

    @field_validator(
        "AUTH_FAIL_WINDOW_SEC",
        "AUTH_MAX_FAILS_IN_WINDOW",
        "AUTH_LOCKOUT_SEC",
        "AUTH_PRUNE_INTERVAL_SEC",
        "ERROR_NOTIFY_INTERVAL_SEC",
        "MAINT_RESTART_REMINDER_INTERVAL_SEC",
        "BROADCAST_MAX_CONCURRENCY",
        "BROADCAST_MAX_ATTEMPTS",
        "FAIL2BAN_DIGEST_TAIL_LINES",
        "FAIL2BAN_DIGEST_MAX_BYTES",
    )
    @classmethod
    def _positive_int(cls, v: int, info: ValidationInfo) -> int:
        iv = int(v)
        if iv < 1:
            raise ValueError(f"{info.field_name} must be >= 1")
        return iv

    @field_validator("LOCAL_SERVER_CODE", "REMOTE_SERVER_CODE", mode="before")
    @classmethod
    def _normalize_server_code(cls, v: Any) -> str:
        return normalize_server_key(str(v or ""), "srv")

    @field_validator("LOCAL_SERVER_LABEL", "REMOTE_SERVER_LABEL", mode="before")
    @classmethod
    def _normalize_label(cls, v: Any) -> str:
        return str(v or "").strip()

    @field_validator("REMOTE_SERVER_SSH_TARGET", mode="before")
    @classmethod
    def _normalize_ssh_target(cls, v: Any) -> str:
        return _validate_ssh_target(str(v or ""))

    @field_validator("REMOTE_SERVER_SSH_TARGETS", mode="after")
    @classmethod
    def _normalize_ssh_targets(cls, v: list[str]) -> list[str]:
        return [_validate_ssh_target(x) for x in v if str(x or "").strip()]

    @model_validator(mode="after")
    def _validate_server_codes(self) -> "AppSettings":
        targets = list(self.REMOTE_SERVER_SSH_TARGETS)
        if not targets and self.REMOTE_SERVER_SSH_TARGET.strip():
            targets = [self.REMOTE_SERVER_SSH_TARGET.strip()]
        if self.REMOTE_SERVER_ENABLED and targets:
            if not self.REMOTE_SERVER_CODES and self.REMOTE_SERVER_CODE == self.LOCAL_SERVER_CODE:
                raise ValueError("LOCAL_SERVER_CODE and REMOTE_SERVER_CODE must differ")
            total = len(targets)
            exact_order_fields = {
                "REMOTE_SERVER_FLAGS": self.REMOTE_SERVER_FLAGS,
                "REMOTE_SERVER_EXPECTED_A_IPS": self.REMOTE_SERVER_EXPECTED_A_IPS,
                "REMOTE_SERVER_CODES": self.REMOTE_SERVER_CODES,
                "REMOTE_SERVER_LABELS": self.REMOTE_SERVER_LABELS,
            }
            for field_name, values in exact_order_fields.items():
                if values and len(values) != total:
                    raise ValueError(f"{field_name} must contain exactly {total} comma-separated values")
            if self.REMOTE_SERVER_CODES:
                normalized_codes = [normalize_server_key(code, "srv") for code in self.REMOTE_SERVER_CODES]
                if len(set(normalized_codes)) != len(normalized_codes):
                    raise ValueError("REMOTE_SERVER_CODES must contain unique values")
                if self.LOCAL_SERVER_CODE in normalized_codes:
                    raise ValueError("REMOTE_SERVER_CODES must not contain LOCAL_SERVER_CODE")
            if self.REMOTE_SERVER_DOMAINS:
                group_count = len(self.REMOTE_SERVER_DOMAINS)
                if group_count > 1 and group_count != total:
                    raise ValueError(
                        f"REMOTE_SERVER_DOMAINS must contain exactly {total} semicolon-separated groups"
                    )
                if group_count == 1 and total > 1 and len(self.REMOTE_SERVER_DOMAINS[0]) != total:
                    raise ValueError(
                        "REMOTE_SERVER_DOMAINS must contain one comma-separated domain per server, "
                        "or semicolon-separated domain groups per server"
                    )
            if self.REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER:
                group_count = len(self.REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER)
                if group_count != total:
                    raise ValueError(
                        "REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER must contain exactly "
                        f"{total} semicolon-separated groups"
                    )
            if self.REMOTE_SERVER_REMNAWAVE_UUIDS and len(self.REMOTE_SERVER_REMNAWAVE_UUIDS) != total:
                raise ValueError(
                    f"REMOTE_SERVER_REMNAWAVE_UUIDS must contain exactly {total} comma-separated UUIDs "
                    "(use empty value between commas to skip a server)"
                )
        if self.SUBPROC_SHORT_TIMEOUT > self.SUBPROC_MEDIUM_TIMEOUT:
            raise ValueError("SUBPROC_SHORT_TIMEOUT must be <= SUBPROC_MEDIUM_TIMEOUT")
        if self.BOT_MODE == "mixed" and not self.REMNAWAVE_METRICS_URL:
            raise ValueError("BOT_MODE=mixed requires REMNAWAVE_METRICS_URL to be set")
        return self


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
    ssh_target: str = ""
    remnawave_uuid: str = ""


SETTINGS = AppSettings()
SECRETS = load_required_secrets(SECRETS_ENV_FILE, fallback_path=ENV_FILE)

BOT_TOKEN = SECRETS.BOT_TOKEN
AUTH_PASSWORD = SECRETS.AUTH_PASSWORD
ADMIN_PASSWORD = SECRETS.ADMIN_PASSWORD

TZ_NAME = SETTINGS.TZ.strip() or "Europe/Moscow"
try:
    TZ = ZoneInfo(TZ_NAME)
except Exception:
    logger.warning("Invalid TZ=%s, fallback to UTC", TZ_NAME)
    TZ_NAME = "UTC"
    TZ = ZoneInfo("UTC")

USER_DATA_PATH = resolve_path(SETTINGS.USER_DATA_PATH, ROOT_DIR)
IMPORTANT_DATA_PATH = resolve_path(SETTINGS.IMPORTANT_DATA_PATH, ROOT_DIR)
LEGACY_CONFIG_PATH = resolve_path(SETTINGS.CONFIG_PATH, ROOT_DIR)

FAIL2BAN_STATE_PATH = resolve_path(
    SETTINGS.FAIL2BAN_STATE_PATH or str(Path(IMPORTANT_DATA_PATH).with_suffix(".fail2ban_state.json")),
    ROOT_DIR,
)

MONITOR_CONTAINERS = list(SETTINGS.MONITOR_CONTAINERS)
EXPECTED_A_IP = SETTINGS.EXPECTED_A_IP.strip()
CHECK_A_DOMAINS = list(SETTINGS.CHECK_A_DOMAINS)
DNS_RESOLVERS = list(SETTINGS.DNS_RESOLVERS)

DOCKER_BIN = resolve_bin("/usr/bin/docker", "docker")
UFW_BIN = resolve_bin("/usr/sbin/ufw", "ufw")
SUDO_BIN = shutil.which("sudo") or ""
SSH_BIN = resolve_bin("/usr/bin/ssh", "ssh")

FAIL2BAN_LOG_PATH = SETTINGS.FAIL2BAN_LOG_PATH.strip()
FAIL2BAN_DAILY_AT = SETTINGS.FAIL2BAN_DAILY_AT.strip()
FAIL2BAN_DIGEST_TAIL_LINES = int(SETTINGS.FAIL2BAN_DIGEST_TAIL_LINES)
FAIL2BAN_DIGEST_MAX_BYTES = int(SETTINGS.FAIL2BAN_DIGEST_MAX_BYTES)
DNS_DAILY_REFRESH_AT = SETTINGS.DNS_DAILY_REFRESH_AT.strip()
DNS_STARTUP_REFRESH_DELAY_SEC = SETTINGS.DNS_STARTUP_REFRESH_DELAY_SEC
MAINT_RESTART_NOTIFY_DELAY_SEC = SETTINGS.MAINT_RESTART_NOTIFY_DELAY_SEC
MAINT_RESTART_REMINDER_INTERVAL_SEC = int(SETTINGS.MAINT_RESTART_REMINDER_INTERVAL_SEC)
BROADCAST_MAX_CONCURRENCY = int(SETTINGS.BROADCAST_MAX_CONCURRENCY)
BROADCAST_MAX_ATTEMPTS = int(SETTINGS.BROADCAST_MAX_ATTEMPTS)
AUTH_FAIL_WINDOW_SEC = int(SETTINGS.AUTH_FAIL_WINDOW_SEC)
AUTH_MAX_FAILS_IN_WINDOW = int(SETTINGS.AUTH_MAX_FAILS_IN_WINDOW)
AUTH_LOCKOUT_SEC = int(SETTINGS.AUTH_LOCKOUT_SEC)
AUTH_PRUNE_INTERVAL_SEC = int(SETTINGS.AUTH_PRUNE_INTERVAL_SEC)
ERROR_NOTIFY_INTERVAL_SEC = int(SETTINGS.ERROR_NOTIFY_INTERVAL_SEC)
LOG_LEVEL = SETTINGS.LOG_LEVEL
LOG_JSON = bool(SETTINGS.LOG_JSON)
LOCAL_SERVER_CODE = SETTINGS.LOCAL_SERVER_CODE
LOCAL_SERVER_LABEL = SETTINGS.LOCAL_SERVER_LABEL
LOCAL_SERVER_FLAG = SETTINGS.LOCAL_SERVER_FLAG.strip().upper()

REMOTE_SERVER_ENABLED = bool(SETTINGS.REMOTE_SERVER_ENABLED)
REMOTE_SERVER_CODE = SETTINGS.REMOTE_SERVER_CODE
REMOTE_SERVER_LABEL = SETTINGS.REMOTE_SERVER_LABEL
REMOTE_SERVER_FLAG = SETTINGS.REMOTE_SERVER_FLAG.strip().upper()
REMOTE_SERVER_SSH_TARGET = SETTINGS.REMOTE_SERVER_SSH_TARGET.strip()
REMOTE_SERVER_EXPECTED_A_IP = SETTINGS.REMOTE_SERVER_EXPECTED_A_IP.strip()
REMOTE_SERVER_CHECK_A_DOMAINS = list(SETTINGS.REMOTE_SERVER_CHECK_A_DOMAINS)
REMOTE_SERVER_FAIL2BAN_LOG_PATH = SETTINGS.REMOTE_SERVER_FAIL2BAN_LOG_PATH.strip() or "/var/log/fail2ban.log"
REMOTE_SERVER_MONITOR_CONTAINERS = list(SETTINGS.REMOTE_SERVER_MONITOR_CONTAINERS) or list(MONITOR_CONTAINERS)
REMOTE_SERVER_FLAGS = list(SETTINGS.REMOTE_SERVER_FLAGS)
REMOTE_SERVER_SSH_TARGETS = list(SETTINGS.REMOTE_SERVER_SSH_TARGETS)
REMOTE_SERVER_EXPECTED_A_IPS = list(SETTINGS.REMOTE_SERVER_EXPECTED_A_IPS)
REMOTE_SERVER_DOMAINS = [list(x) for x in SETTINGS.REMOTE_SERVER_DOMAINS]
REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER = [list(x) for x in SETTINGS.REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER]
REMOTE_SERVER_CODES = list(SETTINGS.REMOTE_SERVER_CODES)
REMOTE_SERVER_LABELS = list(SETTINGS.REMOTE_SERVER_LABELS)

BOT_MODE = SETTINGS.BOT_MODE
REMNAWAVE_METRICS_URL = SETTINGS.REMNAWAVE_METRICS_URL
REMNAWAVE_METRICS_USER = SETTINGS.REMNAWAVE_METRICS_USER
REMNAWAVE_METRICS_PASS = SETTINGS.REMNAWAVE_METRICS_PASS
REMNAWAVE_METRICS_TIMEOUT_SEC = int(SETTINGS.REMNAWAVE_METRICS_TIMEOUT_SEC)
REMNAWAVE_METRICS_CACHE_TTL_SEC = int(SETTINGS.REMNAWAVE_METRICS_CACHE_TTL_SEC)
REMNAWAVE_HIDDEN_UUIDS = list(SETTINGS.REMNAWAVE_HIDDEN_UUIDS)
LOCAL_SERVER_REMNAWAVE_UUID = SETTINGS.LOCAL_SERVER_REMNAWAVE_UUID
REMOTE_SERVER_REMNAWAVE_UUIDS = list(SETTINGS.REMOTE_SERVER_REMNAWAVE_UUIDS)
DAILY_NODE_STATUS_REFRESH_AT = SETTINGS.DAILY_NODE_STATUS_REFRESH_AT

SUBPROC_SHORT_TIMEOUT = SETTINGS.SUBPROC_SHORT_TIMEOUT
SUBPROC_MEDIUM_TIMEOUT = SETTINGS.SUBPROC_MEDIUM_TIMEOUT

SSH_STRICT_HOST_KEY_CHECKING = (SETTINGS.SSH_STRICT_HOST_KEY_CHECKING or "").strip() or "accept-new"
SSH_KNOWN_HOSTS_FILE = (SETTINGS.SSH_KNOWN_HOSTS_FILE or "").strip()

def _build_servers() -> dict[str, ServerTarget]:
    local_flag = LOCAL_SERVER_FLAG or LOCAL_SERVER_CODE.upper()
    servers: dict[str, ServerTarget] = {}
    servers[LOCAL_SERVER_CODE] = ServerTarget(
        key=LOCAL_SERVER_CODE,
        label=LOCAL_SERVER_LABEL or country_label(local_flag, "Main"),
        flag=country_flag(local_flag),
        mode="local",
        expected_a_ip=EXPECTED_A_IP,
        check_a_domains=list(CHECK_A_DOMAINS),
        monitor_containers=list(MONITOR_CONTAINERS),
        fail2ban_log_path=FAIL2BAN_LOG_PATH,
        remnawave_uuid=LOCAL_SERVER_REMNAWAVE_UUID,
    )

    if not REMOTE_SERVER_ENABLED:
        return servers

    targets = list(REMOTE_SERVER_SSH_TARGETS)
    if not targets and REMOTE_SERVER_SSH_TARGET:
        targets = [REMOTE_SERVER_SSH_TARGET]
    if not targets:
        return servers

    total = len(targets)
    flags = list(REMOTE_SERVER_FLAGS) or ([REMOTE_SERVER_FLAG] if REMOTE_SERVER_FLAG else [])
    labels = list(REMOTE_SERVER_LABELS)
    codes = list(REMOTE_SERVER_CODES)
    ips = list(REMOTE_SERVER_EXPECTED_A_IPS) or ([REMOTE_SERVER_EXPECTED_A_IP] if REMOTE_SERVER_EXPECTED_A_IP else [])
    old_domains = [list(REMOTE_SERVER_CHECK_A_DOMAINS)] if REMOTE_SERVER_CHECK_A_DOMAINS else []
    domain_groups = list(REMOTE_SERVER_DOMAINS) or old_domains
    container_groups = list(REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER)

    flag_counts: dict[str, int] = {}
    key_counts: dict[str, int] = {}
    for idx, target in enumerate(targets):
        flag_code = _nth_or_default(flags, idx, "").strip().upper()
        fallback_code = flag_code.lower() if flag_code else f"srv{idx + 1}"
        explicit_code = _nth_or_default(codes, idx, "")
        key = normalize_server_key(explicit_code, fallback_code)
        if key in servers:
            if explicit_code:
                raise RuntimeError(f"Duplicate server code: {key}")
            base_key = key
            key_counts[base_key] = max(key_counts.get(base_key, 0), 1)
            while key in servers:
                key_counts[base_key] += 1
                key = normalize_server_key(f"{base_key}{key_counts[base_key]}", f"srv{idx + 1}")
        else:
            key_counts[key] = max(key_counts.get(key, 0), 1)

        base_label = _nth_or_default(labels, idx, "")
        if not base_label:
            country = country_label(flag_code, f"Server {idx + 1}")
            flag_counts[flag_code or key] = flag_counts.get(flag_code or key, 0) + 1
            base_label = f"{country}(S{flag_counts[flag_code or key]})"

        servers[key] = ServerTarget(
            key=key,
            label=base_label,
            flag=country_flag(flag_code),
            mode="ssh",
            expected_a_ip=_nth_or_default(ips, idx, ""),
            check_a_domains=_domains_for_index(domain_groups, idx, total),
            monitor_containers=_group_for_index(container_groups, idx, REMOTE_SERVER_MONITOR_CONTAINERS),
            fail2ban_log_path=REMOTE_SERVER_FAIL2BAN_LOG_PATH,
            ssh_target=target,
            remnawave_uuid=_nth_or_default(REMOTE_SERVER_REMNAWAVE_UUIDS, idx, ""),
        )
    return servers


SERVERS: dict[str, ServerTarget] = _build_servers()
