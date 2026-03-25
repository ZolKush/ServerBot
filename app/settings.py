import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal
from zoneinfo import ZoneInfo

from dotenv import dotenv_values
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .logging_setup import logger

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent

_ENV_PATH = os.getenv("ENV_PATH", "").strip()
ENV_FILE = Path(_ENV_PATH) if _ENV_PATH else (BASE_DIR / ".env")
_SECRETS_ENV_PATH = os.getenv("SECRETS_ENV_PATH", "").strip()
SECRETS_ENV_FILE = Path(_SECRETS_ENV_PATH) if _SECRETS_ENV_PATH else (BASE_DIR / "env.secrets")
_SECRET_KEYS = ("BOT_TOKEN", "AUTH_PASSWORD", "ADMIN_PASSWORD")


def split_env_list(raw: Any) -> List[str]:
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
    out: List[str] = []
    for item in items:
        if item and item not in out:
            out.append(item)
    return out


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


class SecretSettings(BaseModel):
    BOT_TOKEN: str
    AUTH_PASSWORD: str
    ADMIN_PASSWORD: str

    @field_validator("BOT_TOKEN", "AUTH_PASSWORD", "ADMIN_PASSWORD", mode="before")
    @classmethod
    def _strip_non_empty(cls, v: Any) -> str:
        s = str(v or "").strip()
        if not s:
            raise ValueError("empty secret")
        return s


def _load_env_file_values(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    if not path.is_file():
        raise RuntimeError(f"Путь к env-файлу не является файлом: {path}")
    out: Dict[str, str] = {}
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
    missing = []
    for err in exc.errors():
        loc = err.get("loc") or []
        if loc:
            missing.append(str(loc[0]))
    return ", ".join(sorted(set(missing))) if missing else str(exc)


def load_required_secrets(path: Path, *, fallback_path: Path | None = None) -> SecretSettings:
    merged: Dict[str, str] = {}
    checked_sources: List[str] = []

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


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE),
        env_file_encoding="utf-8",
        extra="ignore",
        enable_decoding=False,
    )

    TZ: str = "Europe/Moscow"
    LOG_LEVEL: str = "INFO"
    LOG_JSON: bool = False

    USER_DATA_PATH: str = str(ROOT_DIR / "data" / "user_data.json")
    IMPORTANT_DATA_PATH: str = str(ROOT_DIR / "data" / "important_data.json")
    CONFIG_PATH: str = str(ROOT_DIR / "data" / "config.json")

    MONITOR_CONTAINERS: List[str] = Field(default_factory=lambda: ["remnawave", "remnawave-db", "remnawave-redis", "remnanode", "remnawave-nginx"])
    MONITOR_PANEL_HOST: str = "xvui.ittelecom.pl"
    PING_COUNT: int = 1
    PING_TIMEOUT_SEC: int = 1

    EXPECTED_A_IP: str = "95.164.47.185"
    CHECK_A_DOMAINS: List[str] = Field(default_factory=lambda: ["nxc.ittelecom.pl", "xvui.ittelecom.pl", "supsub.ittelecom.pl"])
    DNS_RESOLVERS: List[str] = Field(default_factory=lambda: ["1.1.1.1", "8.8.8.8", "77.88.8.8"])

    FAIL2BAN_LOG_PATH: str = "/var/log/fail2ban.log"
    FAIL2BAN_STATE_PATH: str = ""
    FAIL2BAN_DAILY_AT: str = "12:00"
    DNS_DAILY_REFRESH_AT: str = "03:05"
    DNS_STARTUP_REFRESH_DELAY_SEC: int = 5
    MAINT_RESTART_NOTIFY_DELAY_SEC: int = 2

    SUBPROC_SHORT_TIMEOUT: int = 3
    SUBPROC_MEDIUM_TIMEOUT: int = 8

    LOCAL_SERVER_CODE: str = "nl"
    LOCAL_SERVER_LABEL: str = "Netherlands"

    REMOTE_SERVER_ENABLED: bool = True
    REMOTE_SERVER_CODE: str = "de"
    REMOTE_SERVER_LABEL: str = "Germany"
    REMOTE_SERVER_SSH_TARGET: str = ""
    REMOTE_SERVER_EXPECTED_A_IP: str = "144.31.111.77"
    REMOTE_SERVER_CHECK_A_DOMAINS: List[str] = Field(default_factory=lambda: ["nextfiles.ittelecom.pl"])
    REMOTE_SERVER_FAIL2BAN_LOG_PATH: str = "/var/log/fail2ban.log"
    REMOTE_SERVER_MONITOR_CONTAINERS: List[str] = Field(default_factory=list)

    @field_validator(
        "MONITOR_CONTAINERS",
        "CHECK_A_DOMAINS",
        "DNS_RESOLVERS",
        "REMOTE_SERVER_CHECK_A_DOMAINS",
        "REMOTE_SERVER_MONITOR_CONTAINERS",
        mode="before",
    )
    @classmethod
    def _parse_list(cls, v: Any) -> List[str]:
        return split_env_list(v)

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
        "PING_COUNT",
        "PING_TIMEOUT_SEC",
        "DNS_STARTUP_REFRESH_DELAY_SEC",
        "MAINT_RESTART_NOTIFY_DELAY_SEC",
    )
    @classmethod
    def _positive_small_int(cls, v: int) -> int:
        iv = int(v)
        if iv < 1 or iv > 3600:
            raise ValueError("timeout/count out of range")
        return iv

    @field_validator("LOCAL_SERVER_CODE", "REMOTE_SERVER_CODE", mode="before")
    @classmethod
    def _normalize_server_code(cls, v: Any) -> str:
        s = (str(v or "").strip().lower() or "srv")[:12]
        if not s:
            raise ValueError("empty server code")
        return s

    @field_validator("LOCAL_SERVER_LABEL", "REMOTE_SERVER_LABEL", mode="before")
    @classmethod
    def _normalize_label(cls, v: Any) -> str:
        s = str(v or "").strip()
        if not s:
            raise ValueError("empty server label")
        return s

    @model_validator(mode="after")
    def _validate_server_codes(self) -> "AppSettings":
        if self.REMOTE_SERVER_ENABLED and self.REMOTE_SERVER_SSH_TARGET.strip() and self.REMOTE_SERVER_CODE == self.LOCAL_SERVER_CODE:
            raise ValueError("LOCAL_SERVER_CODE and REMOTE_SERVER_CODE must differ")
        if self.SUBPROC_SHORT_TIMEOUT > self.SUBPROC_MEDIUM_TIMEOUT:
            raise ValueError("SUBPROC_SHORT_TIMEOUT must be <= SUBPROC_MEDIUM_TIMEOUT")
        return self


@dataclass(frozen=True)
class ServerTarget:
    key: str
    label: str
    mode: Literal["local", "ssh"]
    expected_a_ip: str
    check_a_domains: List[str]
    monitor_containers: List[str]
    fail2ban_log_path: str
    ssh_target: str = ""


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
MONITOR_CONTAINER_SET = set(MONITOR_CONTAINERS)
MONITOR_PANEL_HOST = SETTINGS.MONITOR_PANEL_HOST
PING_COUNT = SETTINGS.PING_COUNT
PING_TIMEOUT_SEC = SETTINGS.PING_TIMEOUT_SEC
EXPECTED_A_IP = SETTINGS.EXPECTED_A_IP.strip()
CHECK_A_DOMAINS = list(SETTINGS.CHECK_A_DOMAINS)
DNS_RESOLVERS = list(SETTINGS.DNS_RESOLVERS)

DOCKER_BIN = resolve_bin("/usr/bin/docker", "docker")
UFW_BIN = resolve_bin("/usr/sbin/ufw", "ufw")
PING_BIN = resolve_bin("/bin/ping", "/usr/bin/ping", "ping")
SUDO_BIN = resolve_bin("/usr/bin/sudo", "sudo")
SSH_BIN = resolve_bin("/usr/bin/ssh", "ssh")

FAIL2BAN_LOG_PATH = SETTINGS.FAIL2BAN_LOG_PATH.strip()
FAIL2BAN_DAILY_AT = SETTINGS.FAIL2BAN_DAILY_AT.strip()
DNS_DAILY_REFRESH_AT = SETTINGS.DNS_DAILY_REFRESH_AT.strip()
DNS_STARTUP_REFRESH_DELAY_SEC = SETTINGS.DNS_STARTUP_REFRESH_DELAY_SEC
MAINT_RESTART_NOTIFY_DELAY_SEC = SETTINGS.MAINT_RESTART_NOTIFY_DELAY_SEC
LOG_LEVEL = SETTINGS.LOG_LEVEL
LOG_JSON = bool(SETTINGS.LOG_JSON)
LOCAL_SERVER_CODE = SETTINGS.LOCAL_SERVER_CODE
LOCAL_SERVER_LABEL = SETTINGS.LOCAL_SERVER_LABEL

REMOTE_SERVER_ENABLED = bool(SETTINGS.REMOTE_SERVER_ENABLED)
REMOTE_SERVER_CODE = SETTINGS.REMOTE_SERVER_CODE
REMOTE_SERVER_LABEL = SETTINGS.REMOTE_SERVER_LABEL
REMOTE_SERVER_SSH_TARGET = SETTINGS.REMOTE_SERVER_SSH_TARGET.strip()
REMOTE_SERVER_EXPECTED_A_IP = SETTINGS.REMOTE_SERVER_EXPECTED_A_IP.strip()
REMOTE_SERVER_CHECK_A_DOMAINS = list(SETTINGS.REMOTE_SERVER_CHECK_A_DOMAINS)
REMOTE_SERVER_FAIL2BAN_LOG_PATH = SETTINGS.REMOTE_SERVER_FAIL2BAN_LOG_PATH.strip() or "/var/log/fail2ban.log"
REMOTE_SERVER_MONITOR_CONTAINERS = list(SETTINGS.REMOTE_SERVER_MONITOR_CONTAINERS) or list(MONITOR_CONTAINERS)
ALL_MONITOR_CONTAINER_SET = set(MONITOR_CONTAINERS) | set(REMOTE_SERVER_MONITOR_CONTAINERS)

SUBPROC_SHORT_TIMEOUT = SETTINGS.SUBPROC_SHORT_TIMEOUT
SUBPROC_MEDIUM_TIMEOUT = SETTINGS.SUBPROC_MEDIUM_TIMEOUT

SERVERS: Dict[str, ServerTarget] = {
    LOCAL_SERVER_CODE: ServerTarget(
        key=LOCAL_SERVER_CODE,
        label=LOCAL_SERVER_LABEL,
        mode="local",
        expected_a_ip=EXPECTED_A_IP,
        check_a_domains=list(CHECK_A_DOMAINS),
        monitor_containers=list(MONITOR_CONTAINERS),
        fail2ban_log_path=FAIL2BAN_LOG_PATH,
    )
}
if REMOTE_SERVER_ENABLED and REMOTE_SERVER_SSH_TARGET:
    SERVERS[REMOTE_SERVER_CODE] = ServerTarget(
        key=REMOTE_SERVER_CODE,
        label=REMOTE_SERVER_LABEL,
        mode="ssh",
        expected_a_ip=REMOTE_SERVER_EXPECTED_A_IP,
        check_a_domains=list(REMOTE_SERVER_CHECK_A_DOMAINS),
        monitor_containers=list(REMOTE_SERVER_MONITOR_CONTAINERS),
        fail2ban_log_path=REMOTE_SERVER_FAIL2BAN_LOG_PATH,
        ssh_target=REMOTE_SERVER_SSH_TARGET,
    )
