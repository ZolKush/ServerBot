import logging
import os
import shutil
from pathlib import Path
from typing import Any, List
from zoneinfo import ZoneInfo

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("maint-bot")

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent

_ENV_PATH = os.getenv("ENV_PATH", "").strip()
_ENV_FILE = Path(_ENV_PATH) if _ENV_PATH else (BASE_DIR / ".env")


def _split_env_list(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        items = [str(x).strip() for x in raw]
    else:
        items = [p.strip() for p in str(raw).split(",")]
    out: List[str] = []
    for item in items:
        if item and item not in out:
            out.append(item)
    return out


def _resolve_path(value: str, base: Path) -> str:
    v = (value or "").strip()
    if not v:
        return str(base)
    p = Path(v)
    return str(p if p.is_absolute() else (base / p))


def _resolve_bin(*candidates: str) -> str:
    for cand in candidates:
        if not cand:
            continue
        path = shutil.which(cand)
        if path:
            return path
    return candidates[-1] if candidates else ""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    BOT_TOKEN: str = ""
    AUTH_PASSWORD: str = ""
    ADMIN_PASSWORD: str = ""
    TZ: str = "Europe/Moscow"

    USER_DATA_PATH: str = str(ROOT_DIR / "data" / "user_data.json")
    IMPORTANT_DATA_PATH: str = str(ROOT_DIR / "data" / "important_data.json")
    CONFIG_PATH: str = str(ROOT_DIR / "data" / "config.json")

    MONITOR_CONTAINERS: List[str] = Field(
        default_factory=lambda: [
            "remnawave",
            "remnawave-db",
            "remnawave-redis",
            "remnanode",
            "remnawave-nginx",
        ]
    )
    MONITOR_PANEL_HOST: str = "xvui.ittelecom.pl"
    PING_COUNT: int = 1
    PING_TIMEOUT_SEC: int = 1

    EXPECTED_A_IP: str = "95.164.47.185"
    CHECK_A_DOMAINS: List[str] = Field(
        default_factory=lambda: ["nxc.ittelecom.pl", "xvui.ittelecom.pl", "supsub.ittelecom.pl"]
    )

    DNS_RESOLVERS: List[str] = Field(default_factory=lambda: ["1.1.1.1", "8.8.8.8", "77.88.8.8"])

    FAIL2BAN_LOG_PATH: str = "/var/log/fail2ban.log"
    FAIL2BAN_STATE_PATH: str = ""
    FAIL2BAN_DAILY_AT: str = "12:00"

    SUBPROC_SHORT_TIMEOUT: int = 3
    SUBPROC_MEDIUM_TIMEOUT: int = 8

    @field_validator("MONITOR_CONTAINERS", "CHECK_A_DOMAINS", "DNS_RESOLVERS", mode="before")
    @classmethod
    def _parse_list(cls, v: Any) -> List[str]:
        return _split_env_list(v)


SETTINGS = Settings()

BOT_TOKEN = SETTINGS.BOT_TOKEN.strip()
AUTH_PASSWORD = SETTINGS.AUTH_PASSWORD.strip()
ADMIN_PASSWORD = SETTINGS.ADMIN_PASSWORD.strip()

TZ_NAME = SETTINGS.TZ.strip() or "Europe/Moscow"
try:
    TZ = ZoneInfo(TZ_NAME)
except Exception:
    logger.warning("Invalid TZ=%s, fallback to UTC", TZ_NAME)
    TZ_NAME = "UTC"
    TZ = ZoneInfo("UTC")

USER_DATA_PATH = _resolve_path(SETTINGS.USER_DATA_PATH, ROOT_DIR)
IMPORTANT_DATA_PATH = _resolve_path(SETTINGS.IMPORTANT_DATA_PATH, ROOT_DIR)
LEGACY_CONFIG_PATH = _resolve_path(SETTINGS.CONFIG_PATH, ROOT_DIR)

FAIL2BAN_STATE_PATH = _resolve_path(
    SETTINGS.FAIL2BAN_STATE_PATH
    or str(Path(IMPORTANT_DATA_PATH).with_suffix(".fail2ban_state.json")),
    ROOT_DIR,
)

MONITOR_CONTAINERS = SETTINGS.MONITOR_CONTAINERS
MONITOR_CONTAINER_SET = set(MONITOR_CONTAINERS)
MONITOR_PANEL_HOST = SETTINGS.MONITOR_PANEL_HOST
PING_COUNT = SETTINGS.PING_COUNT
PING_TIMEOUT_SEC = SETTINGS.PING_TIMEOUT_SEC
EXPECTED_A_IP = SETTINGS.EXPECTED_A_IP.strip()
CHECK_A_DOMAINS = SETTINGS.CHECK_A_DOMAINS
DNS_RESOLVERS = SETTINGS.DNS_RESOLVERS

DOCKER_BIN = _resolve_bin("/usr/bin/docker", "docker")
UFW_BIN = _resolve_bin("/usr/sbin/ufw", "ufw")
PING_BIN = _resolve_bin("/bin/ping", "/usr/bin/ping", "ping")
SUDO_BIN = _resolve_bin("/usr/bin/sudo", "sudo")

FAIL2BAN_LOG_PATH = SETTINGS.FAIL2BAN_LOG_PATH.strip()
FAIL2BAN_DAILY_AT = SETTINGS.FAIL2BAN_DAILY_AT.strip()

SUBPROC_SHORT_TIMEOUT = SETTINGS.SUBPROC_SHORT_TIMEOUT
SUBPROC_MEDIUM_TIMEOUT = SETTINGS.SUBPROC_MEDIUM_TIMEOUT

MENU_STATUS = "📊 Статус сервера"
MENU_TICKET = "🎫 Создать тикет"
MENU_USERS = "👥 Пользователи"
MENU_MAINT = "🛠 Техработы"
MENU_FAIL2BAN = "🛡 Fail2ban"
