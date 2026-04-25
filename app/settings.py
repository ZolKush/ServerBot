import json
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional
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


def split_env_list(raw: Any, *, dedupe: bool = True) -> List[str]:
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
        if item and (not dedupe or item not in out):
            out.append(item)
    return out


def split_env_groups(raw: Any) -> List[List[str]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        groups: List[List[str]] = []
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


def _nth_or_default(items: List[str], index: int, default: str = "") -> str:
    if index < len(items):
        return items[index]
    return default


def _domains_for_index(groups: List[List[str]], index: int, total: int, fallback: Optional[List[str]] = None) -> List[str]:
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


def _group_for_index(groups: List[List[str]], index: int, fallback: Optional[List[str]] = None) -> List[str]:
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

    MONITOR_CONTAINERS: List[str] = Field(default_factory=lambda: ["remnawave", "remnawave-db", "remnawave-redis", "remnanode", "remnawave-nginx"])
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
    LOCAL_SERVER_FLAG: str = ""

    REMOTE_SERVER_ENABLED: bool = True
    REMOTE_SERVER_CODE: str = "de"
    REMOTE_SERVER_LABEL: str = "Germany"
    REMOTE_SERVER_FLAG: str = ""
    REMOTE_SERVER_SSH_TARGET: str = ""
    REMOTE_SERVER_EXPECTED_A_IP: str = "144.31.111.77"
    REMOTE_SERVER_CHECK_A_DOMAINS: List[str] = Field(default_factory=lambda: ["nextfiles.ittelecom.pl"])
    REMOTE_SERVER_FAIL2BAN_LOG_PATH: str = "/var/log/fail2ban.log"
    REMOTE_SERVER_MONITOR_CONTAINERS: List[str] = Field(default_factory=list)

    REMOTE_SERVER_FLAGS: List[str] = Field(default_factory=list)
    REMOTE_SERVER_SSH_TARGETS: List[str] = Field(default_factory=list)
    REMOTE_SERVER_EXPECTED_A_IPS: List[str] = Field(default_factory=list)
    REMOTE_SERVER_DOMAINS: List[List[str]] = Field(default_factory=list)
    REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER: List[List[str]] = Field(default_factory=list)
    REMOTE_SERVER_CODES: List[str] = Field(default_factory=list)
    REMOTE_SERVER_LABELS: List[str] = Field(default_factory=list)

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
        mode="before",
    )
    @classmethod
    def _parse_list(cls, v: Any, info: ValidationInfo) -> List[str]:
        ordered_fields = {
            "REMOTE_SERVER_FLAGS",
            "REMOTE_SERVER_SSH_TARGETS",
            "REMOTE_SERVER_EXPECTED_A_IPS",
            "REMOTE_SERVER_CODES",
            "REMOTE_SERVER_LABELS",
        }
        return split_env_list(v, dedupe=info.field_name not in ordered_fields)

    @field_validator("REMOTE_SERVER_DOMAINS", "REMOTE_SERVER_MONITOR_CONTAINERS_BY_SERVER", mode="before")
    @classmethod
    def _parse_domain_groups(cls, v: Any) -> List[List[str]]:
        return split_env_groups(v)

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

    @field_validator("LOCAL_SERVER_CODE", "REMOTE_SERVER_CODE", mode="before")
    @classmethod
    def _normalize_server_code(cls, v: Any) -> str:
        return normalize_server_key(str(v or ""), "srv")

    @field_validator("LOCAL_SERVER_LABEL", "REMOTE_SERVER_LABEL", mode="before")
    @classmethod
    def _normalize_label(cls, v: Any) -> str:
        s = str(v or "").strip()
        if not s:
            raise ValueError("empty server label")
        return s

    @field_validator("REMOTE_SERVER_SSH_TARGET", mode="before")
    @classmethod
    def _normalize_ssh_target(cls, v: Any) -> str:
        return _validate_ssh_target(str(v or ""))

    @field_validator("REMOTE_SERVER_SSH_TARGETS", mode="after")
    @classmethod
    def _normalize_ssh_targets(cls, v: List[str]) -> List[str]:
        return [_validate_ssh_target(x) for x in v if str(x or "").strip()]

    @model_validator(mode="after")
    def _validate_server_codes(self) -> "AppSettings":
        if self.REMOTE_SERVER_ENABLED and self.REMOTE_SERVER_SSH_TARGET.strip() and self.REMOTE_SERVER_CODE == self.LOCAL_SERVER_CODE:
            raise ValueError("LOCAL_SERVER_CODE and REMOTE_SERVER_CODE must differ")
        targets = list(self.REMOTE_SERVER_SSH_TARGETS)
        if not targets and self.REMOTE_SERVER_SSH_TARGET.strip():
            targets = [self.REMOTE_SERVER_SSH_TARGET.strip()]
        if self.REMOTE_SERVER_ENABLED and targets:
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
        if self.SUBPROC_SHORT_TIMEOUT > self.SUBPROC_MEDIUM_TIMEOUT:
            raise ValueError("SUBPROC_SHORT_TIMEOUT must be <= SUBPROC_MEDIUM_TIMEOUT")
        return self


@dataclass(frozen=True)
class ServerTarget:
    key: str
    label: str
    flag: str
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
EXPECTED_A_IP = SETTINGS.EXPECTED_A_IP.strip()
CHECK_A_DOMAINS = list(SETTINGS.CHECK_A_DOMAINS)
DNS_RESOLVERS = list(SETTINGS.DNS_RESOLVERS)

DOCKER_BIN = resolve_bin("/usr/bin/docker", "docker")
UFW_BIN = resolve_bin("/usr/sbin/ufw", "ufw")
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

SUBPROC_SHORT_TIMEOUT = SETTINGS.SUBPROC_SHORT_TIMEOUT
SUBPROC_MEDIUM_TIMEOUT = SETTINGS.SUBPROC_MEDIUM_TIMEOUT

def _build_servers() -> Dict[str, ServerTarget]:
    local_flag = LOCAL_SERVER_FLAG or LOCAL_SERVER_CODE.upper()
    servers: Dict[str, ServerTarget] = {}
    servers[LOCAL_SERVER_CODE] = ServerTarget(
        key=LOCAL_SERVER_CODE,
        label=LOCAL_SERVER_LABEL or country_label(local_flag, "Main"),
        flag=country_flag(local_flag),
        mode="local",
        expected_a_ip=EXPECTED_A_IP,
        check_a_domains=list(CHECK_A_DOMAINS),
        monitor_containers=list(MONITOR_CONTAINERS),
        fail2ban_log_path=FAIL2BAN_LOG_PATH,
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

    flag_counts: Dict[str, int] = {}
    key_counts: Dict[str, int] = {}
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
        )
    return servers


SERVERS: Dict[str, ServerTarget] = _build_servers()
