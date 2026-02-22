import json
import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

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

    MONITOR_CONTAINERS: str = "remnawave,remnawave-db,remnawave-redis,remnanode,remnawave-nginx"
    MONITOR_PANEL_HOST: str = "xvui.ittelecom.pl"
    PING_COUNT: int = 1
    PING_TIMEOUT_SEC: int = 1

    EXPECTED_A_IP: str = "95.164.47.185"
    CHECK_A_DOMAINS: str = "nxc.ittelecom.pl,xvui.ittelecom.pl,supsub.ittelecom.pl"

    DNS_RESOLVERS: str = "1.1.1.1,8.8.8.8,77.88.8.8"

    FAIL2BAN_LOG_PATH: str = "/var/log/fail2ban.log"
    FAIL2BAN_STATE_PATH: str = ""
    FAIL2BAN_DAILY_AT: str = "12:00"

    SUBPROC_SHORT_TIMEOUT: int = 3
    SUBPROC_MEDIUM_TIMEOUT: int = 8

    LOCAL_SERVER_CODE: str = "nl"
    LOCAL_SERVER_LABEL: str = "Netherlands"

    REMOTE_SERVER_ENABLED: bool = True
    REMOTE_SERVER_CODE: str = "de"
    REMOTE_SERVER_LABEL: str = "Germany"
    REMOTE_SERVER_SSH_TARGET: str = ""
    REMOTE_SERVER_EXPECTED_A_IP: str = "144.31.111.77"
    REMOTE_SERVER_CHECK_A_DOMAINS: str = "nextfiles.ittelecom.pl"
    REMOTE_SERVER_FAIL2BAN_LOG_PATH: str = "/var/log/fail2ban.log"
    REMOTE_SERVER_MONITOR_CONTAINERS: str = ""

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

MONITOR_CONTAINERS = _split_env_list(SETTINGS.MONITOR_CONTAINERS)
MONITOR_CONTAINER_SET = set(MONITOR_CONTAINERS)
MONITOR_PANEL_HOST = SETTINGS.MONITOR_PANEL_HOST
PING_COUNT = SETTINGS.PING_COUNT
PING_TIMEOUT_SEC = SETTINGS.PING_TIMEOUT_SEC
EXPECTED_A_IP = SETTINGS.EXPECTED_A_IP.strip()
CHECK_A_DOMAINS = _split_env_list(SETTINGS.CHECK_A_DOMAINS)
DNS_RESOLVERS = _split_env_list(SETTINGS.DNS_RESOLVERS)

DOCKER_BIN = _resolve_bin("/usr/bin/docker", "docker")
UFW_BIN = _resolve_bin("/usr/sbin/ufw", "ufw")
PING_BIN = _resolve_bin("/bin/ping", "/usr/bin/ping", "ping")
SUDO_BIN = _resolve_bin("/usr/bin/sudo", "sudo")
SSH_BIN = _resolve_bin("/usr/bin/ssh", "ssh")

FAIL2BAN_LOG_PATH = SETTINGS.FAIL2BAN_LOG_PATH.strip()
FAIL2BAN_DAILY_AT = SETTINGS.FAIL2BAN_DAILY_AT.strip()
LOCAL_SERVER_CODE = (SETTINGS.LOCAL_SERVER_CODE.strip().lower() or "nl")[:12]
LOCAL_SERVER_LABEL = SETTINGS.LOCAL_SERVER_LABEL.strip() or "Netherlands"

REMOTE_SERVER_ENABLED = bool(SETTINGS.REMOTE_SERVER_ENABLED)
REMOTE_SERVER_CODE = (SETTINGS.REMOTE_SERVER_CODE.strip().lower() or "de")[:12]
REMOTE_SERVER_LABEL = SETTINGS.REMOTE_SERVER_LABEL.strip() or "Germany"
REMOTE_SERVER_SSH_TARGET = SETTINGS.REMOTE_SERVER_SSH_TARGET.strip()
REMOTE_SERVER_EXPECTED_A_IP = SETTINGS.REMOTE_SERVER_EXPECTED_A_IP.strip()
REMOTE_SERVER_CHECK_A_DOMAINS = _split_env_list(SETTINGS.REMOTE_SERVER_CHECK_A_DOMAINS)
REMOTE_SERVER_FAIL2BAN_LOG_PATH = SETTINGS.REMOTE_SERVER_FAIL2BAN_LOG_PATH.strip() or "/var/log/fail2ban.log"
REMOTE_SERVER_MONITOR_CONTAINERS = _split_env_list(SETTINGS.REMOTE_SERVER_MONITOR_CONTAINERS) or list(MONITOR_CONTAINERS)

SUBPROC_SHORT_TIMEOUT = SETTINGS.SUBPROC_SHORT_TIMEOUT
SUBPROC_MEDIUM_TIMEOUT = SETTINGS.SUBPROC_MEDIUM_TIMEOUT


@dataclass(frozen=True)
class ServerTarget:
    key: str
    label: str
    mode: str  # local | ssh
    expected_a_ip: str
    check_a_domains: List[str]
    monitor_containers: List[str]
    fail2ban_log_path: str
    ssh_target: str = ""


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

MENU_STATUS = "📊 Статус сервера"
MENU_TICKET = "🎫 Создать тикет"
MENU_USERS = "👥 Пользователи"
MENU_MAINT = "🛠 Техработы"
MENU_FAIL2BAN = "🛡 Fail2ban"
