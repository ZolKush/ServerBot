import asyncio
import json
import logging
import os
import re
import shutil
import socket
import struct
from dataclasses import dataclass, field
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, TypeVar
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.constants import ChatType, ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("maint-bot")

# ============================================================
#                       CONFIG / ENV
# ============================================================

T = TypeVar("T")

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent

_ENV_PATH = os.getenv("ENV_PATH", "").strip()
if _ENV_PATH:
    load_dotenv(_ENV_PATH)
else:
    load_dotenv(dotenv_path=BASE_DIR / ".env")

def _resolve_path(value: str, base: Path) -> str:
    v = (value or "").strip()
    if not v:
        return str(base)
    p = Path(v)
    return str(p if p.is_absolute() else (base / p))

def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except Exception:
        return int(default)

def _split_env_list(raw: str) -> List[str]:
    out: List[str] = []
    for part in (raw or "").split(","):
        item = part.strip()
        if item and item not in out:
            out.append(item)
    return out

def _resolve_bin(*candidates: str) -> str:
    for cand in candidates:
        if not cand:
            continue
        path = shutil.which(cand)
        if path:
            return path
    return candidates[-1] if candidates else ""

def _normalize_bool(value: Any, truthy: Set[str]) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in truthy
    return bool(value)

USER_DATA_PATH = _resolve_path(
    os.getenv("USER_DATA_PATH", str(ROOT_DIR / "data" / "user_data.json")),
    ROOT_DIR,
)
IMPORTANT_DATA_PATH = _resolve_path(
    os.getenv("IMPORTANT_DATA_PATH", str(ROOT_DIR / "data" / "important_data.json")),
    ROOT_DIR,
)
LEGACY_CONFIG_PATH = _resolve_path(
    os.getenv("CONFIG_PATH", str(ROOT_DIR / "data" / "config.json")),
    ROOT_DIR,
)
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

AUTH_PASSWORD = os.getenv("AUTH_PASSWORD", "").strip()
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "").strip()

TZ_NAME = os.getenv("TZ", "Europe/Moscow").strip() or "Europe/Moscow"
try:
    TZ = ZoneInfo(TZ_NAME)
except Exception:
    logger.warning("Invalid TZ=%s, fallback to UTC", TZ_NAME)
    TZ_NAME = "UTC"
    TZ = ZoneInfo("UTC")

MONITOR_CONTAINERS = _split_env_list(
    os.getenv(
        "MONITOR_CONTAINERS",
        "remnawave,remnawave-db,remnawave-redis,remnanode,remnawave-nginx",
    )
)
MONITOR_CONTAINER_SET = set(MONITOR_CONTAINERS)
MONITOR_PANEL_HOST = os.getenv("MONITOR_PANEL_HOST", "xvui.ittelecom.pl").strip()
PING_COUNT = _env_int("PING_COUNT", 1)
PING_TIMEOUT_SEC = _env_int("PING_TIMEOUT_SEC", 1)

EXPECTED_A_IP = os.getenv("EXPECTED_A_IP", "95.164.47.185").strip()
CHECK_A_DOMAINS = _split_env_list(
    os.getenv(
        "CHECK_A_DOMAINS",
        "nxc.ittelecom.pl,xvui.ittelecom.pl,supsub.ittelecom.pl",
    )
)

DNS_RESOLVERS = _split_env_list(os.getenv("DNS_RESOLVERS", "1.1.1.1,8.8.8.8,77.88.8.8"))

DOCKER_BIN = _resolve_bin("/usr/bin/docker", "docker")
UFW_BIN = _resolve_bin("/usr/sbin/ufw", "ufw")
PING_BIN = _resolve_bin("/bin/ping", "/usr/bin/ping", "ping")
SUDO_BIN = _resolve_bin("/usr/bin/sudo", "sudo")

FAIL2BAN_LOG_PATH = os.getenv("FAIL2BAN_LOG_PATH", "/var/log/fail2ban.log").strip()

FAIL2BAN_STATE_PATH = _resolve_path(
    os.getenv(
        "FAIL2BAN_STATE_PATH",
        str(Path(IMPORTANT_DATA_PATH).with_suffix(".fail2ban_state.json")),
    ),
    ROOT_DIR,
)

FAIL2BAN_DAILY_AT = os.getenv("FAIL2BAN_DAILY_AT", "12:00").strip()

SUBPROC_SHORT_TIMEOUT = _env_int("SUBPROC_SHORT_TIMEOUT", 3)
SUBPROC_MEDIUM_TIMEOUT = _env_int("SUBPROC_MEDIUM_TIMEOUT", 8)

# ============================================================
#                       UI STRINGS
# ============================================================

MENU_STATUS = "\U0001f4ca Статус сервера"
MENU_TICKET = "\U0001f3ab Создать тикет"
MENU_USERS = "\U0001f465 Пользователи"
MENU_MAINT = "\U0001f6e0 Техработы"
MENU_FAIL2BAN = "\U0001f6e1 Fail2ban"

MENU_BUTTONS = [MENU_STATUS, MENU_TICKET, MENU_USERS, MENU_MAINT]
MENU_PATTERN = r"^(?:" + "|".join(re.escape(x) for x in MENU_BUTTONS) + r")$"
PRIVATE_TEXT = filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND

@dataclass

# ============================================================
#                       STORAGE
# ============================================================

class UserData:
    authorized_users: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    @staticmethod
    def _normalize_user(meta: Dict[str, Any]) -> Dict[str, Any]:
        meta = dict(meta or {})

        uid_raw = meta.get("user_id")
        try:
            uid = int(uid_raw)
        except Exception:
            uid = None

        role = meta.get("role", "user")
        if role not in ("user", "admin"):
            role = "user"

        enabled = _normalize_bool(meta.get("enabled", True), {"1", "true", "yes", "y", "on", "enabled"})

        meta["user_id"] = uid
        meta["role"] = role
        meta["enabled"] = enabled
        meta.setdefault("nickname", meta.get("nick") or meta.get("nickname") or None)
        meta.setdefault("username", meta.get("username") or None)
        meta.setdefault("first_name", meta.get("first_name") or None)
        meta.setdefault("last_name", meta.get("last_name") or None)
        meta.setdefault("auth_at", meta.get("auth_at") or None)

        meta["is_paid"] = _normalize_bool(meta.get("is_paid", False), {"1", "true", "yes", "y", "on", "paid"})
        return meta

    @staticmethod
    def _migrate(raw: Dict[str, Any]) -> "UserData":
        authorized_users: Dict[str, Dict[str, Any]] = {}

        if isinstance(raw.get("authorized_users"), dict):
            for k, meta in raw["authorized_users"].items():
                try:
                    uid = int((meta or {}).get("user_id", k))
                except Exception:
                    continue
                authorized_users[str(uid)] = UserData._normalize_user({**(meta or {}), "user_id": uid})
        else:
            allowed = raw.get("allowed_user_ids", [])
            if isinstance(allowed, list):
                for uid in allowed:
                    try:
                        uid_i = int(uid)
                    except Exception:
                        continue
                    authorized_users[str(uid_i)] = UserData._normalize_user({"user_id": uid_i, "role": "user"})

        return UserData(authorized_users=authorized_users)

    @classmethod
    def load(cls, path: str, legacy_path: Optional[str] = None) -> "UserData":
        for pth in [path, legacy_path]:
            if not pth:
                continue
            p = Path(pth)
            if not p.exists():
                continue
            try:
                raw = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    data = cls._migrate(raw)
                    if pth != path:
                        try:
                            data.save(path)
                        except Exception:
                            pass
                    return data
            except Exception as e:
                logger.error("Не удалось прочитать %s: %s", pth, e)
        return cls()

    def save(self, path: str) -> None:
        p = Path(path)
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(
                {"authorized_users": self.authorized_users},
                ensure_ascii=False,
                indent=2,
            )
            tmp = p.with_suffix(p.suffix + ".tmp")
            tmp.write_text(payload, encoding="utf-8")
            tmp.replace(p)
        except Exception as e:
            logger.error("Не удалось сохранить %s: %s", path, e)

@dataclass
class ImportantData:
    tickets_seq: int = 0
    maintenance: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def _migrate(raw: Dict[str, Any]) -> "ImportantData":
        tickets_seq = int(raw.get("tickets_seq", 0) or 0)
        maintenance = raw.get("maintenance", {})
        if not isinstance(maintenance, dict):
            maintenance = {}
        return ImportantData(tickets_seq=tickets_seq, maintenance=maintenance)

    @classmethod
    def load(cls, path: str, legacy_path: Optional[str] = None) -> "ImportantData":
        for pth in [path, legacy_path]:
            if not pth:
                continue
            p = Path(pth)
            if not p.exists():
                continue
            try:
                raw = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    data = cls._migrate(raw)
                    if pth != path:
                        try:
                            data.save(path)
                        except Exception:
                            pass
                    return data
            except Exception as e:
                logger.error("Не удалось прочитать %s: %s", pth, e)
        return cls()

    def save(self, path: str) -> None:
        p = Path(path)
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(
                {"tickets_seq": self.tickets_seq, "maintenance": self.maintenance},
                ensure_ascii=False,
                indent=2,
            )
            tmp = p.with_suffix(p.suffix + ".tmp")
            tmp.write_text(payload, encoding="utf-8")
            tmp.replace(p)
        except Exception as e:
            logger.error("Не удалось сохранить %s: %s", path, e)

USER_DATA = UserData.load(USER_DATA_PATH, legacy_path=LEGACY_CONFIG_PATH)
IMPORTANT_DATA = ImportantData.load(IMPORTANT_DATA_PATH, legacy_path=LEGACY_CONFIG_PATH)
USER_DATA_LOCK = asyncio.Lock()
IMPORTANT_DATA_LOCK = asyncio.Lock()

async def update_user_data(update_fn: Callable[[UserData], T]) -> T:
    async with USER_DATA_LOCK:
        result = update_fn(USER_DATA)
        await asyncio.to_thread(USER_DATA.save, USER_DATA_PATH)
    return result

async def update_important_data(update_fn: Callable[[ImportantData], T]) -> T:
    async with IMPORTANT_DATA_LOCK:
        result = update_fn(IMPORTANT_DATA)
        await asyncio.to_thread(IMPORTANT_DATA.save, IMPORTANT_DATA_PATH)
    return result

def _set_user_meta(cfg: UserData, uid: int, meta: Dict[str, Any]) -> Dict[str, Any]:
    normalized = UserData._normalize_user(meta)
    cfg.authorized_users[str(uid)] = normalized
    return normalized

def _remove_user(cfg: UserData, uid: int) -> Optional[Dict[str, Any]]:
    return cfg.authorized_users.pop(str(uid), None)

def _set_maintenance(cfg: ImportantData, payload: Dict[str, Any]) -> Dict[str, Any]:
    cfg.maintenance = payload
    return payload

def _clear_maintenance(cfg: ImportantData) -> None:
    cfg.maintenance = {}

def _get_active_maintenance() -> Optional[Dict[str, Any]]:
    m = getattr(IMPORTANT_DATA, "maintenance", None)
    if isinstance(m, dict) and m.get("active"):
        return m
    return None

# ============================================================
#                       SECURITY / PRESENTATION HELPERS
# ============================================================

def html_escape(s: str) -> str:

    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def wrap_as_codeblock_html(text: str) -> str:
    return f"<pre><code>{html_escape(text or '')}</code></pre>"

def now_str() -> str:
    return datetime.now(TZ).strftime("%d.%m.%Y %H:%M:%S")

# ============================================================
#                       AUTH / GUARDS
# ============================================================

def is_private(update: Update) -> bool:
    return bool(update.effective_chat and update.effective_chat.type == ChatType.PRIVATE)

def get_user_id(update: Update) -> Optional[int]:
    u = update.effective_user
    return int(u.id) if u else None

def get_user_meta(uid: int) -> Optional[Dict[str, Any]]:
    return USER_DATA.authorized_users.get(str(uid))

def is_authorized(update: Update) -> bool:
    uid = get_user_id(update)
    return bool(uid is not None and str(uid) in USER_DATA.authorized_users)

def is_enabled(update: Update) -> bool:
    uid = get_user_id(update)
    if uid is None:
        return False
    meta = get_user_meta(uid)
    return bool(meta and meta.get("enabled", True))

def is_admin(update: Update) -> bool:
    uid = get_user_id(update)
    if uid is None:
        return False
    meta = get_user_meta(uid)
    return bool(meta and meta.get("role") == "admin")

async def reply_disabled(update: Update) -> None:
    msg = update.effective_message
    if msg:
        await msg.reply_text("Вы отключены от этого бота. Обратитесь к администратору напрямую.")

async def reply_need_auth(update: Update) -> None:
    msg = update.effective_message
    if msg:
        await msg.reply_text(
            "Доступ ограничен. Авторизуйтесь командой:\n<b>/auth пароль</b>",
            parse_mode=ParseMode.HTML,
        )

def require_private(func: Callable[..., Any]) -> Callable[..., Any]:
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args: Any, **kwargs: Any):
        if not is_private(update):
            return ConversationHandler.END
        return await func(update, context, *args, **kwargs)

    return wrapper

def require_auth(func: Callable[..., Any]) -> Callable[..., Any]:
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args: Any, **kwargs: Any):
        if not await _ensure_access(update, role=None):
            return ConversationHandler.END
        return await func(update, context, *args, **kwargs)

    return wrapper

def require_admin(func: Callable[..., Any]) -> Callable[..., Any]:
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args: Any, **kwargs: Any):
        if not await _ensure_access(update, role="admin"):
            return ConversationHandler.END
        return await func(update, context, *args, **kwargs)

    return wrapper

async def _ensure_access(update: Update, role: Optional[str]) -> bool:
    if not is_private(update):
        return False
    if not is_authorized(update):
        await reply_need_auth(update)
        return False
    if not is_enabled(update):
        await reply_disabled(update)
        return False
    if role == "admin" and not is_admin(update):
        msg = update.effective_message
        if msg:
            await msg.reply_text("Доступ только для администратора.")
        return False
    return True

def display_name_from_meta(meta: Optional[Dict[str, Any]]) -> str:
    if not meta:
        return "пользователь"
    nick = (meta.get("nickname") or "").strip()
    if nick:
        return nick
    uname = meta.get("username")
    if uname:
        return f"@{uname}"
    nm = " ".join([x for x in [meta.get("first_name"), meta.get("last_name")] if x])
    if nm.strip():
        return nm.strip()
    uid = meta.get("user_id")
    return str(uid) if uid is not None else "пользователь"

def display_name(update: Update) -> str:
    u = update.effective_user
    if not u:
        return "пользователь"
    meta = get_user_meta(u.id)
    if meta:
        return display_name_from_meta(meta)
    if u.username:
        return f"@{u.username}"
    nm = " ".join([x for x in [u.first_name, u.last_name] if x])
    return nm if nm else str(u.id)

# ============================================================
#                       MAIN MENU
# ============================================================

def main_menu_kb(update: Update) -> ReplyKeyboardMarkup:
    rows = [[KeyboardButton(MENU_STATUS), KeyboardButton(MENU_TICKET)]]
    if is_admin(update):
        rows.append([KeyboardButton(MENU_USERS), KeyboardButton(MENU_MAINT)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

# ============================================================
#                       ASYNC SUBPROCESS
# ============================================================

async def run_exec(args: Sequence[str], timeout: int) -> Tuple[int, str, str]:
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.DEVNULL,
        )
    except FileNotFoundError:
        return 127, "", f"command not found: {args[0]}"
    except Exception as e:
        return 127, "", f"spawn error: {e}"

    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except Exception:
            pass
        try:
            await proc.wait()
        except Exception:
            pass
        return 124, "", "timeout"

    return (
        int(proc.returncode or 0),
        (out or b"").decode(errors="ignore"),
        (err or b"").decode(errors="ignore"),
    )

# ============================================================
#                       METRICS
# ============================================================

def _fmt_bytes_binary(n: int) -> str:

    if n < 0:
        return "0 B"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    v = float(n)
    idx = 0
    while v >= 1024 and idx < len(units) - 1:
        v /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(v)} {units[idx]}"
    return f"{v:.1f} {units[idx]}"

async def check_uptime() -> str:

    try:
        raw = Path("/proc/uptime").read_text(encoding="utf-8").split()
        seconds = int(float(raw[0]))
    except Exception:

        rc, out, _ = await run_exec(["uptime", "-p"], timeout=SUBPROC_SHORT_TIMEOUT)
        return out.strip() if rc == 0 else "н/д"

    td = timedelta(seconds=seconds)
    days = td.days
    hours, rem = divmod(td.seconds, 3600)
    minutes, _ = divmod(rem, 60)

    parts: List[str] = []
    if days:
        parts.append(f"{days} д")
    if hours:
        parts.append(f"{hours} ч")
    if minutes or not parts:
        parts.append(f"{minutes} м")
    return " ".join(parts)

async def loadavg() -> str:
    try:
        parts = Path("/proc/loadavg").read_text(encoding="utf-8").strip().split()
        return f"{parts[0]} / {parts[1]} / {parts[2]}" if len(parts) >= 3 else "н/д"
    except Exception:
        return "н/д"

async def meminfo() -> str:
    try:
        kv: Dict[str, int] = {}
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            m = re.match(r"^(\w+):\s+(\d+)\s+kB$", line.strip())
            if m:
                kv[m.group(1)] = int(m.group(2))
        mem_total_kb = kv.get("MemTotal", 0)
        mem_avail_kb = kv.get("MemAvailable", kv.get("MemFree", 0))
        mem_used_kb = max(mem_total_kb - mem_avail_kb, 0)

        sw_total_kb = kv.get("SwapTotal", 0)
        sw_free_kb = kv.get("SwapFree", 0)
        sw_used_kb = max(sw_total_kb - sw_free_kb, 0)

        def kb_to_mib(x: int) -> int:
            return int(round(x / 1024.0))

        mem_s = f"{kb_to_mib(mem_used_kb)} / {kb_to_mib(mem_total_kb)} MiB (avail {kb_to_mib(mem_avail_kb)} MiB)"
        sw_s = f"{kb_to_mib(sw_used_kb)} / {kb_to_mib(sw_total_kb)} MiB" if sw_total_kb else "н/д"
        return f"RAM: {mem_s}; Swap: {sw_s}"
    except Exception:

        rc, out, _ = await run_exec(["free", "-m"], timeout=SUBPROC_SHORT_TIMEOUT)
        if rc != 0:
            return "н/д"
        lines = out.splitlines()
        if len(lines) < 2:
            return "н/д"
        mem = re.split(r"\s+", lines[1].strip())
        swp = re.split(r"\s+", lines[2].strip()) if len(lines) > 2 else []
        try:
            mem_total = int(mem[1])
            mem_used = int(mem[2])
            mem_free = int(mem[3])
            mem_s = f"{mem_used} / {mem_total} MiB (free {mem_free} MiB)"
        except Exception:
            mem_s = "н/д"
        try:
            if swp and swp[0].lower().startswith("swap"):
                sw_total = int(swp[1])
                sw_used = int(swp[2])
                sw_s = f"{sw_used} / {sw_total} MiB"
            else:
                sw_s = "н/д"
        except Exception:
            sw_s = "н/д"
        return f"RAM: {mem_s}; Swap: {sw_s}"

async def disk_root() -> str:
    try:
        usage = shutil.disk_usage("/")
        total = usage.total
        used = usage.used
        free = usage.free

        usep = int(round((used / total) * 100)) if total else 0
        return f"{_fmt_bytes_binary(used)} / {_fmt_bytes_binary(total)} (avail {_fmt_bytes_binary(free)}, {usep}%) mount /"
    except Exception:
        rc, out, _ = await run_exec(["df", "-h", "/"], timeout=SUBPROC_SHORT_TIMEOUT)
        if rc != 0:
            return "н/д"
        lines = out.splitlines()
        if len(lines) < 2:
            return "н/д"
        parts = re.split(r"\s+", lines[1].strip())
        if len(parts) >= 6:
            size, used, avail, usep, mnt = parts[1], parts[2], parts[3], parts[4], parts[5]
            return f"{used} / {size} (avail {avail}, {usep}) mount {mnt}"
        return "н/д"

# ============================================================
#                       FIREWALL (UFW)
# ============================================================

def _ufw_candidates() -> List[List[str]]:
    bases: List[str] = []
    for b in [UFW_BIN, "ufw"]:
        if b and b not in bases:
            bases.append(b)
    cmds: List[List[str]] = []
    for b in bases:
        cmds.append([b, "status"])
        if SUDO_BIN:
            cmds.append([SUDO_BIN, "-n", b, "status"])
    return cmds

async def ufw_status_basic() -> str:

    candidates = _ufw_candidates()

    out = ""
    for args in candidates:
        rc, o, _ = await run_exec(args, timeout=SUBPROC_SHORT_TIMEOUT)
        if rc == 0 and (o or "").strip():
            out = o
            break

    if not out:
        return "н/д"

    first = (out.strip().splitlines()[:1] or [""])[0].lower()
    if "active" in first:
        return "active"
    if "inactive" in first:
        return "inactive"
    return "н/д"

def _parse_ufw_rules(out: str) -> Tuple[List[str], List[str], List[str]]:
    allow: List[str] = []
    deny: List[str] = []
    reject: List[str] = []

    lines = [ln.rstrip() for ln in (out or "").splitlines()]
    if not lines:
        return allow, deny, reject

    start_idx = 0
    for i, ln in enumerate(lines[:10]):
        if re.search(r"\bTo\b", ln) and re.search(r"\bAction\b", ln):
            start_idx = i + 1
            break

    for ln in lines[start_idx:]:
        if not ln.strip():
            continue

        parts = [p.strip() for p in re.split(r"\s{2,}", ln.strip()) if p.strip()]
        if len(parts) < 2:
            continue
        to, action = parts[0], parts[1].upper()
        src = parts[2] if len(parts) > 2 else ""
        item = to.strip()
        if not item:
            continue
        if src and src.lower() not in {"anywhere", "anywhere (v6)"}:
            item = f"{item} ← {src}"
        if action.startswith("ALLOW"):
            allow.append(item)
        elif action.startswith("DENY"):
            deny.append(item)
        elif action.startswith("REJECT"):
            reject.append(item)

    def uniq(xs: List[str]) -> List[str]:
        seen: Set[str] = set()
        outl: List[str] = []
        for x in xs:
            if x not in seen:
                seen.add(x)
                outl.append(x)
        return outl

    return uniq(allow), uniq(deny), uniq(reject)

async def ufw_summary_for_admin() -> Tuple[str, List[str], List[str], List[str]]:
    candidates = _ufw_candidates()

    out = ""
    for args in candidates:
        rc, o, _ = await run_exec(args, timeout=SUBPROC_SHORT_TIMEOUT)
        if rc == 0 and (o or "").strip():
            out = o
            break

    if not out:
        return "н/д", [], [], []

    status = "н/д"
    first = (out.strip().splitlines()[:1] or [""])[0].lower()
    if "active" in first:
        status = "active"
    elif "inactive" in first:
        status = "inactive"

    allow, deny, reject = _parse_ufw_rules(out)
    return status, allow, deny, reject

# ============================================================
#                       DOCKER STATUS
# ============================================================

async def docker_containers(names: Sequence[str]) -> List[Tuple[str, bool, str, str]]:
    rc, _, _ = await run_exec([DOCKER_BIN, "info"], timeout=SUBPROC_SHORT_TIMEOUT)
    if rc != 0:
        return [(n, False, "docker недоступен", "-") for n in names]

    rc, out, _ = await run_exec([DOCKER_BIN, "ps", "-a", "--format", "{{.Names}}|{{.Status}}"], timeout=SUBPROC_MEDIUM_TIMEOUT)
    if rc != 0:
        return [(n, False, "ошибка docker ps", "-") for n in names]

    info: Dict[str, str] = {}
    for line in out.splitlines():
        parts = line.split("|", 1)
        if len(parts) == 2:
            info[parts[0].strip()] = parts[1].strip()

    rc2, out2, _ = await run_exec([DOCKER_BIN, "ps", "-a", "--format", "{{.Names}}|{{.RestartCount}}"], timeout=SUBPROC_MEDIUM_TIMEOUT)
    restarts: Dict[str, str] = {}
    if rc2 == 0:
        for ln in out2.splitlines():
            p = ln.split("|", 1)
            if len(p) == 2:
                restarts[p[0].strip()] = p[1].strip()

    result: List[Tuple[str, bool, str, str]] = []
    for n in names:
        st = info.get(n)
        if st is None:
            result.append((n, False, "не найден", restarts.get(n, "-")))
        else:
            up = st.lower().startswith("up")
            result.append((n, up, st, restarts.get(n, "-")))
    return result

# ============================================================
#                       NETWORK / DNS
# ============================================================

_HOST_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9.\-]{0,252}$")

async def ping_host(host: str, count: int, timeout_sec: int) -> Tuple[bool, Optional[float]]:

    if not host or not _HOST_RE.fullmatch(host):
        return False, None

    args = [PING_BIN, "-c", str(max(1, count)), "-W", str(max(1, timeout_sec)), host]
    rc, out, _ = await run_exec(args, timeout=max(2, timeout_sec * (count + 2)))
    if rc != 0:
        return False, None

    rtt = None
    for line in out.splitlines():
        if "rtt min/avg/max" in line or "round-trip min/avg/max" in line:
            try:
                part = line.split("=")[1].strip().split(" ")[0]
                rtt = float(part.split("/")[1])
            except Exception:
                rtt = None
            break
    return True, rtt

def _dns_build_qname(domain: str) -> bytes:
    labels = [p for p in domain.strip(".").split(".") if p]
    out = bytearray()
    for lab in labels:
        b = lab.encode("ascii", "ignore")
        if not b or len(b) > 63:
            return b""
        out.append(len(b))
        out.extend(b)
    out.append(0)
    return bytes(out)

def _dns_read_name(msg: bytes, offset: int) -> Tuple[str, int]:
    labels: List[str] = []
    jumped = False
    orig_offset = offset
    steps = 0
    while True:
        steps += 1
        if steps > 50 or offset >= len(msg):
            return "", (orig_offset + 1 if not jumped else orig_offset)
        length = msg[offset]

        if length & 0xC0 == 0xC0:
            if offset + 1 >= len(msg):
                return "", (orig_offset + 2 if not jumped else orig_offset)
            ptr = ((length & 0x3F) << 8) | msg[offset + 1]
            if not jumped:
                orig_offset = offset + 2
                jumped = True
            offset = ptr
            continue
        if length == 0:
            offset += 1
            break
        offset += 1
        if offset + length > len(msg):
            return "", (orig_offset if jumped else offset)
        try:
            labels.append(msg[offset : offset + length].decode("ascii", "ignore"))
        except Exception:
            labels.append("")
        offset += length
    name = ".".join([x for x in labels if x])
    return name, (orig_offset if jumped else offset)

def _dns_query_a_udp(domain: str, server: str, timeout: float = 2.0) -> List[str]:
    dom = (domain or "").strip()
    srv = (server or "").strip()
    if not dom or not srv:
        return []
    try:
        socket.inet_aton(srv)
    except Exception:
        return []
    qname = _dns_build_qname(dom)
    if not qname:
        return []

    tid = int.from_bytes(os.urandom(2), "big")
    hdr = struct.pack("!HHHHHH", tid, 0x0100, 1, 0, 0, 0)
    q = qname + struct.pack("!HH", 1, 1)
    pkt = hdr + q

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(max(0.5, float(timeout)))
    try:
        s.sendto(pkt, (srv, 53))
        data, _ = s.recvfrom(4096)
    except Exception:
        return []
    finally:
        try:
            s.close()
        except Exception:
            pass

    if len(data) < 12:
        return []

    rid, flags, qd, an, _, _ = struct.unpack("!HHHHHH", data[:12])
    if rid != tid:
        return []
    rcode = flags & 0x000F
    if rcode != 0 or qd < 1:
        return []

    off = 12

    for _ in range(qd):
        _, off = _dns_read_name(data, off)
        off += 4
        if off > len(data):
            return []

    ips: List[str] = []
    for _ in range(an):
        _, off = _dns_read_name(data, off)
        if off + 10 > len(data):
            break
        rtype, rclass, _, rdlen = struct.unpack("!HHIH", data[off : off + 10])
        off += 10
        if off + rdlen > len(data):
            break
        rdata = data[off : off + rdlen]
        off += rdlen
        if rtype == 1 and rclass == 1 and rdlen == 4:
            ip = ".".join(str(b) for b in rdata)
            if ip and ip not in ips:
                ips.append(ip)
    return ips

async def resolve_a_record(domain: str, resolver: Optional[str] = None, timeout: float = 2.0) -> List[str]:
    dom = (domain or "").strip()
    if not dom or not _HOST_RE.fullmatch(dom):
        return []

    if resolver:
        return await asyncio.to_thread(_dns_query_a_udp, dom, resolver, timeout)

    def _resolve() -> List[str]:
        try:
            _, _, ips = socket.gethostbyname_ex(dom)
            return [ip for ip in ips if ip]
        except Exception:
            return []

    return await asyncio.to_thread(_resolve)

# ============================================================
#                       STATUS / HEALTH
# ============================================================

@require_auth
async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text, markup = await build_status_message(update)
    msg = update.effective_message
    if msg:
        await msg.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)

async def build_status_message(update: Update) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    up = await check_uptime()
    la = await loadavg()
    mem = await meminfo()
    disk = await disk_root()

    if is_admin(update):
        ufw_s, allow, deny, reject = await ufw_summary_for_admin()
    else:
        ufw_s = await ufw_status_basic()
        allow, deny, reject = [], [], []

    cont = await docker_containers(MONITOR_CONTAINERS)

    expected_ip = EXPECTED_A_IP
    ok_ping_expected, rtt_expected = await ping_host(expected_ip, count=PING_COUNT, timeout_sec=PING_TIMEOUT_SEC)

    lines: List[str] = []
    lines.append("<b>Статус сервера</b>")
    lines.append(f"• Время: <code>{html_escape(now_str())}</code> ({html_escape(TZ_NAME)})")
    lines.append(f"• Uptime: <code>{html_escape(up)}</code>")
    lines.append(f"• Load average (1/5/15): <code>{html_escape(la)}</code>")
    lines.append(f"• Память: <code>{html_escape(mem)}</code>")
    lines.append(f"• Диск /: <code>{html_escape(disk)}</code>")
    lines.append(f"• UFW: <code>{html_escape(ufw_s)}</code>")

    if is_admin(update) and ufw_s == "active":
        def join_short(xs: List[str]) -> str:
            if not xs:
                return "—"
            s = ", ".join(xs)
            return s if len(s) <= 200 else (s[:200] + "…")

        lines.append(f"  ALLOW: <code>{html_escape(join_short(allow))}</code>")
        lines.append(f"  DENY: <code>{html_escape(join_short(deny))}</code>")
        lines.append(f"  REJECT: <code>{html_escape(join_short(reject))}</code>")

    lines.append("\n<b>Docker контейнеры</b>")
    for name, upb, st, rst in cont:
        emoji = "🟢" if upb else "🔴"
        lines.append(f"• {emoji} <code>{html_escape(name)}</code> — {html_escape(st)} (restarts: {html_escape(rst)})")

    lines.append("\n<b>Сеть (ICMP)</b>")
    if ok_ping_expected:
        rtt_s = f"{rtt_expected:.1f} ms" if rtt_expected is not None else "ok"
        lines.append(f"• ping <code>{html_escape(expected_ip)}</code> — ok (avg {html_escape(rtt_s)})")
    else:
        lines.append(f"• ping <code>{html_escape(expected_ip)}</code> — fail/timeout")

    rows: List[List[InlineKeyboardButton]] = []
    if is_admin(update):
        rows.append([InlineKeyboardButton("🐳 Docker: inspect/logs", callback_data="docker:list")])
        rows.append([InlineKeyboardButton("🛡️ Fail2ban: logs", callback_data="f2b:menu")])
    rows.append([InlineKeyboardButton("DNS проверка", callback_data="dns:check")])
    markup = InlineKeyboardMarkup(rows)
    return "\n".join(lines), markup


async def build_dns_status_message() -> str:
    domains = CHECK_A_DOMAINS
    expected_ip = EXPECTED_A_IP
    dns_resolvers = DNS_RESOLVERS
    dns_map: Dict[str, Dict[str, List[str]]] = {}

    for d in domains:
        ips_by = await asyncio.gather(*[resolve_a_record(d, resolver=r) for r in dns_resolvers])
        dns_map[d] = {r: ips for r, ips in zip(dns_resolvers, ips_by)}

    lines: List[str] = []
    lines.append("<b>DNS A-записи</b>")
    lines.append(f"• Ожидаемый IP: <code>{html_escape(expected_ip)}</code>")
    for dom in domains:
        lines.append(f"• <code>{html_escape(dom)}</code>")
        per = dns_map.get(dom, {})
        for r in dns_resolvers:
            ips = per.get(r, []) or []
            ips_s = ", ".join(ips) if ips else "н/д"
            ok = bool(ips) and (expected_ip in ips)
            flag = "✅" if ok else ("⚠️" if not ips else "❌")
            lines.append(f"  {flag} <code>{html_escape(r)}</code> → <code>{html_escape(ips_s)}</code>")

    return "\n".join(lines)

@require_auth
async def dns_check_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer("Проверяю...")
    text = await build_dns_status_message()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Назад к статусу", callback_data="dns:back")]])
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)

@require_auth
async def dns_back_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    text, markup = await build_status_message(update)
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)

_CONTAINER_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.\-]{0,62}$")

def _is_allowed_container(name: str) -> bool:
    nm = (name or "").strip()
    return bool(nm and _CONTAINER_NAME_RE.fullmatch(nm) and nm in MONITOR_CONTAINER_SET)

# ============================================================
#                       FAIL2BAN (READ-ONLY)
# ============================================================

F2B_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d{3})?)\s+"
    r"(?P<logger>\S+)\s+\[\d+\]:\s+"
    r"(?P<level>[A-Z]+)\s+\[(?P<jail>[^\]]+)\]\s+"
    r"(?P<msg>.+?)\s*$"
)

F2B_IP_RE = re.compile(
    r"(?P<ip>\b(?:\d{1,3}\.){3}\d{1,3}\b|\b[0-9a-fA-F:]{2,}\b)"
)

def _f2b_parse_time(ts: str) -> Optional[datetime]:
    ts = (ts or "").strip()
    if not ts:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S,%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(ts, fmt)
            return dt.replace(tzinfo=TZ)
        except Exception:
            continue
    return None

def _load_json_file(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}

def _save_json_file(path: str, data: Dict[str, Any]) -> None:
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning("Не удалось сохранить %s: %s", path, e)

def tail_text_file(path: str, n_lines: int, max_bytes: int = 2_000_000) -> str:
    n_lines = max(1, min(int(n_lines), 10000))
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)

    with p.open("rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()

        read_size = min(size, max_bytes)
        f.seek(-read_size, os.SEEK_END)
        buf = f.read(read_size)

    text = buf.decode("utf-8", errors="replace")
    lines = text.splitlines()
    tail = lines[-n_lines:] if len(lines) > n_lines else lines
    return "\n".join(tail)

def read_fail2ban_new_lines(log_path: str, state_path: str, max_read_bytes: int = 5_000_000) -> List[str]:
    st = _load_json_file(state_path)
    p = Path(log_path)
    if not p.exists():
        return []

    try:
        stat = p.stat()
        inode = int(getattr(stat, "st_ino", 0) or 0)
        size = int(stat.st_size)
    except Exception:
        return []

    offset = int(st.get("offset", 0) or 0)
    last_inode = int(st.get("inode", 0) or 0)

    if last_inode and inode and inode != last_inode:
        offset = 0

    if offset > size:
        offset = 0

    if size - offset > max_read_bytes:
        offset = max(0, size - max_read_bytes)

    lines_out: List[str] = []
    try:
        with p.open("rb") as f:
            f.seek(offset, os.SEEK_SET)
            chunk = f.read(max_read_bytes)
            new_offset = f.tell()
        text = chunk.decode("utf-8", errors="replace")
        lines_out = text.splitlines()
    except Exception as e:
        logger.warning("fail2ban log read error (%s): %s", log_path, e)
        return []

    _save_json_file(
        state_path,
        {
            "inode": inode,
            "offset": new_offset,
            "updated_at": datetime.now(tz=TZ).isoformat(),
        },
    )
    return lines_out

@dataclass(frozen=True)
class Fail2banEvent:
    ts: datetime
    jail: str
    action: str
    ip: Optional[str]
    raw: str

def parse_fail2ban_events(lines_in: Iterable[str]) -> List[Fail2banEvent]:
    out: List[Fail2banEvent] = []
    for raw in lines_in:
        m = F2B_LINE_RE.match(raw or "")
        if not m:
            continue

        ts = _f2b_parse_time(m.group("ts"))
        if not ts:
            continue

        jail = (m.group("jail") or "").strip() or "unknown"
        msg = (m.group("msg") or "").strip()

        action: Optional[str] = None
        ip: Optional[str] = None

        if msg.startswith("Ban "):
            action = "Ban"
            ip = msg.split(" ", 1)[1].strip()
        elif msg.startswith("Unban "):
            action = "Unban"
            ip = msg.split(" ", 1)[1].strip()
        elif msg.startswith("Restore Ban "):
            action = "Restore Ban"
            ip = msg.split(" ", 2)[2].strip()
        elif msg.startswith("Jail started"):
            action = "Jail started"
        elif msg.startswith("Jail stopped"):
            action = "Jail stopped"
        elif msg.startswith("Added jail "):
            action = "Added jail"
        elif msg.startswith("Removed jail "):
            action = "Removed jail"

        if not action:
            continue

        if ip:
            ipm = F2B_IP_RE.search(ip)
            ip = (ipm.group("ip") if ipm else ip).strip()

        out.append(Fail2banEvent(ts=ts, jail=jail, action=action, ip=ip, raw=raw))
    return out

def _f2b_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📄 Tail 200", callback_data="f2b:tail:200"),
                InlineKeyboardButton("📄 Tail 600", callback_data="f2b:tail:600"),
            ],
            [
                InlineKeyboardButton("📄 Tail 2000", callback_data="f2b:tail:2000"),
            ],
            [
                InlineKeyboardButton("🧾 Выжимка за 24ч", callback_data="f2b:digest"),
            ],
            [
                InlineKeyboardButton("🔙 Назад к статусу", callback_data="f2b:back"),
            ],
        ]
    )

def _f2b_tail_kb(current: int) -> InlineKeyboardMarkup:
    choices = [200, 600, 2000]
    row = [InlineKeyboardButton(f"Tail {n}", callback_data=f"f2b:tail:{n}") for n in choices if n != current]
    kb_rows = []
    if row:
        kb_rows.append(row[:2])
        if len(row) > 2:
            kb_rows.append(row[2:])
    kb_rows.append([InlineKeyboardButton("🔙 Назад", callback_data="f2b:menu")])
    return InlineKeyboardMarkup(kb_rows)

def _f2b_digest_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="f2b:menu")]])

def _fmt_dt(dt: datetime) -> str:

    return dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

def build_fail2ban_menu_text() -> str:
    p = Path(FAIL2BAN_LOG_PATH)
    if not p.exists():
        return (
            "🛡 <b>Fail2ban</b>\n\n"
            f"Лог-файл не найден: <code>{html_escape(FAIL2BAN_LOG_PATH)}</code>\n\n"
            "Проверьте настройку <code>logtarget</code> в <code>/etc/fail2ban/fail2ban.conf</code> "
            "или <code>/etc/fail2ban/fail2ban.local</code> (файл может логировать в journald/SYSLOG вместо отдельного файла)."
        )

    try:
        st = p.stat()
        mtime = datetime.fromtimestamp(st.st_mtime, tz=TZ)
        size_kb = st.st_size / 1024.0
        return (
            "🛡 <b>Fail2ban</b>\n\n"
            f"Файл: <code>{html_escape(str(p))}</code>\n"
            f"Размер: <code>{size_kb:.1f} KiB</code>\n"
            f"Изменён: <code>{html_escape(_fmt_dt(mtime))}</code>\n\n"
            "Действия:"
        )
    except Exception:
        return (
            "🛡 <b>Fail2ban</b>\n\n"
            f"Файл: <code>{html_escape(str(p))}</code>\n\n"
            "Действия:"
        )

@require_admin
async def fail2ban_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if msg:
        await msg.reply_text(build_fail2ban_menu_text(), parse_mode=ParseMode.HTML, reply_markup=_f2b_menu_kb())

@require_admin
async def f2b_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    await q.edit_message_text(build_fail2ban_menu_text(), parse_mode=ParseMode.HTML, reply_markup=_f2b_menu_kb())

@require_admin
async def f2b_tail_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"f2b:tail:(\d{1,5})", q.data or "")
    if not m:
        return
    n = int(m.group(1))
    n = 200 if n < 50 else (5000 if n > 5000 else n)

    try:
        tail_txt = tail_text_file(FAIL2BAN_LOG_PATH, n_lines=n)
        if not tail_txt.strip():
            payload = "🛡 <b>Fail2ban: tail</b>\n\nЛог пуст или отсутствуют строки."
        else:
            payload = "🛡 <b>Fail2ban: tail</b>\n\n" + wrap_as_codeblock_html(_clip_text(tail_txt))
    except FileNotFoundError:
        payload = (
            "🛡 <b>Fail2ban: tail</b>\n\n"
            f"Лог-файл не найден: <code>{html_escape(FAIL2BAN_LOG_PATH)}</code>"
        )
    except PermissionError:
        payload = (
            "🛡 <b>Fail2ban: tail</b>\n\n"
            f"Нет прав на чтение: <code>{html_escape(FAIL2BAN_LOG_PATH)}</code>\n"
            "Запустите бота с правами, позволяющими читать лог, либо настройте права на файл."
        )
    except Exception as e:
        payload = f"🛡 <b>Fail2ban: tail</b>\n\nОшибка: <code>{html_escape(str(e))}</code>"

    await q.edit_message_text(payload, parse_mode=ParseMode.HTML, reply_markup=_f2b_tail_kb(current=n))

def build_fail2ban_digest_text(events: List[Fail2banEvent], since: datetime, until: datetime) -> str:

    per_jail: Dict[str, Dict[str, Any]] = {}
    for ev in events:
        j = per_jail.setdefault(ev.jail, {"ban": [], "unban": 0, "restore": 0, "started": 0, "stopped": 0})
        if ev.action == "Ban":
            j["ban"].append(ev)
        elif ev.action == "Unban":
            j["unban"] += 1
        elif ev.action == "Restore Ban":
            j["restore"] += 1
            j["ban"].append(ev)
        elif ev.action == "Jail started":
            j["started"] += 1
        elif ev.action == "Jail stopped":
            j["stopped"] += 1

    total_bans = sum(len(v["ban"]) for v in per_jail.values())

    header = (
        "🧾 <b>Fail2ban: выжимка</b>\n"
        f"Период: <code>{html_escape(_fmt_dt(since))}</code> — <code>{html_escape(_fmt_dt(until))}</code>\n"
    )

    if total_bans == 0 and not any((v["unban"] or v["started"] or v["stopped"]) for v in per_jail.values()):
        return header + "\nСобытий не найдено."

    lines: List[str] = [header]
    for jail in sorted(per_jail.keys()):
        v = per_jail[jail]
        bans: List[Fail2banEvent] = v["ban"]
        if not bans and not (v["unban"] or v["started"] or v["stopped"]):
            continue

        lines.append(f"\n<b>[{html_escape(jail)}]</b>")
        if v["started"]:
            lines.append(f"• jail started: <code>{v['started']}</code>")
        if v["stopped"]:
            lines.append(f"• jail stopped: <code>{v['stopped']}</code>")
        if bans:
            lines.append(f"• bans: <code>{len(bans)}</code> (включая restore={v['restore']})")

            last = sorted(bans, key=lambda e: e.ts)[-20:]
            for ev in last:
                ip = ev.ip or "-"
                lines.append(f"  • <code>{html_escape(_fmt_dt(ev.ts))}</code> — <code>{html_escape(ip)}</code> ({html_escape(ev.action)})")
            if len(bans) > 20:
                lines.append(f"  … ещё <code>{len(bans) - 20}</code> событий")
        if v["unban"]:
            lines.append(f"• unbans: <code>{v['unban']}</code>")

    out = "\n".join(lines)

    if len(out) > 3800:
        out = out[:3700] + "\n… (обрезано из-за лимита Telegram)"
    return out

@require_admin
async def f2b_digest_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    until = datetime.now(tz=TZ)
    since = until - timedelta(days=1)

    try:

        raw_tail = tail_text_file(FAIL2BAN_LOG_PATH, n_lines=20000, max_bytes=3_000_000)
        events = parse_fail2ban_events(raw_tail.splitlines())
        events = [e for e in events if since <= e.ts <= until]
        payload = build_fail2ban_digest_text(events, since=since, until=until)
    except FileNotFoundError:
        payload = (
            "🧾 <b>Fail2ban: выжимка</b>\n\n"
            f"Лог-файл не найден: <code>{html_escape(FAIL2BAN_LOG_PATH)}</code>"
        )
    except PermissionError:
        payload = (
            "🧾 <b>Fail2ban: выжимка</b>\n\n"
            f"Нет прав на чтение: <code>{html_escape(FAIL2BAN_LOG_PATH)}</code>"
        )
    except Exception as e:
        payload = f"🧾 <b>Fail2ban: выжимка</b>\n\nОшибка: <code>{html_escape(str(e))}</code>"

    await q.edit_message_text(payload, parse_mode=ParseMode.HTML, reply_markup=_f2b_digest_kb())

@require_admin
async def f2b_back_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    text, markup = await build_status_message(update)
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)

async def fail2ban_daily_digest(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        st_before = _load_json_file(FAIL2BAN_STATE_PATH)
        raw_lines = read_fail2ban_new_lines(FAIL2BAN_LOG_PATH, FAIL2BAN_STATE_PATH)
        if not raw_lines:
            return
        events = parse_fail2ban_events(raw_lines)

        ban_events = [e for e in events if e.action in ("Ban", "Restore Ban")]
        if not ban_events:
            return

        until = datetime.now(tz=TZ)

        st = st_before
        since = None
        try:
            if st.get("updated_at"):
                since = datetime.fromisoformat(st["updated_at"]).astimezone(TZ) - timedelta(seconds=1)
        except Exception:
            since = None
        if since is None:
            since = until - timedelta(days=1)

        payload = build_fail2ban_digest_text(events, since=since, until=until)

        admin_ids = authorized_ids(role_filter="admin")
        if not admin_ids:
            return
        await send_to_many(context, admin_ids, payload)
    except Exception as e:
        logger.warning("fail2ban_daily_digest error: %s", e)

# ============================================================
#                       DOCKER UI (ADMIN, READ-ONLY)
# ============================================================

def docker_list_kb() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for nm in MONITOR_CONTAINERS:
        if not _is_allowed_container(nm):
            continue
        row.append(InlineKeyboardButton(nm[:32], callback_data=f"docker:show:{nm}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Назад к статусу", callback_data="docker:back")])
    return InlineKeyboardMarkup(rows)

def docker_item_kb(name: str, tail: int = 120) -> InlineKeyboardMarkup:
    tail = int(tail)
    tail = 120 if tail < 120 else (600 if tail > 600 else tail)
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔎 Inspect", callback_data=f"docker:inspect:{name}"),
                InlineKeyboardButton(f"📜 Logs tail {tail}", callback_data=f"docker:logs:{name}:{tail}"),
            ],
            [InlineKeyboardButton("⬅️ К списку", callback_data="docker:list")],
            [InlineKeyboardButton("⬅️ К статусу", callback_data="docker:back")],
        ]
    )

def _clip_text(s: str, limit: int = 3300) -> str:
    if s is None:
        return ""
    s = str(s)
    return s if len(s) <= limit else (s[:limit] + "\n…(truncated)…")

async def docker_inspect_summary(name: str) -> str:
    rc, out, err = await run_exec([DOCKER_BIN, "inspect", name], timeout=SUBPROC_MEDIUM_TIMEOUT)
    if rc != 0:
        return f"docker inspect error: {err.strip() or out.strip() or 'н/д'}"
    try:
        data = json.loads(out)
        if not isinstance(data, list) or not data:
            return "inspect: пустой ответ"
        c = data[0]
        image = ((c.get("Config") or {}).get("Image")) or "-"
        state = c.get("State") or {}
        status = state.get("Status") or "-"
        running = state.get("Running")
        started = state.get("StartedAt") or "-"
        finished = state.get("FinishedAt") or "-"
        exit_code = state.get("ExitCode")
        error = (state.get("Error") or "").strip() or "-"
        health = ((state.get("Health") or {}).get("Status")) or "-"
        restart_count = c.get("RestartCount")

        ports = (((c.get("NetworkSettings") or {}).get("Ports")) or {})
        port_items: List[str] = []
        if isinstance(ports, dict):
            for k, v in ports.items():
                if v is None:
                    port_items.append(f"{k}→-")
                elif isinstance(v, list) and v:
                    b = v[0]
                    host_ip = b.get("HostIp", "")
                    host_port = b.get("HostPort", "")
                    port_items.append(f"{k}→{host_ip}:{host_port}")
                else:
                    port_items.append(f"{k}")

        lines: List[str] = []
        lines.append(f"Container: {name}")
        lines.append(f"Image: {image}")
        lines.append(f"State: {status} (running={running})")
        lines.append(f"Health: {health}")
        if restart_count is not None:
            lines.append(f"RestartCount: {restart_count}")
        if exit_code is not None:
            lines.append(f"ExitCode: {exit_code}")
        lines.append(f"StartedAt: {started}")
        if status not in ("running", "up"):
            lines.append(f"FinishedAt: {finished}")
        if error and error != "-":
            lines.append(f"Error: {error}")
        if port_items:
            lines.append("Ports: " + ", ".join(port_items))
        return "\n".join(lines)
    except Exception as e:
        return f"inspect parse error: {e}"

@require_admin
async def docker_list_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    await q.edit_message_text("<b>Docker: контейнеры</b>\nВыберите контейнер:", parse_mode=ParseMode.HTML, reply_markup=docker_list_kb())

@require_admin
async def docker_back_to_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    text, markup = await build_status_message(update)
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)

@require_admin
async def docker_show(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"docker:show:([a-zA-Z0-9_.\-]{1,64})", q.data or "")
    if not m:
        await q.edit_message_text("Некорректный запрос.", reply_markup=docker_list_kb())
        return
    name = m.group(1)
    if not _is_allowed_container(name):
        await q.edit_message_text("Контейнер недоступен.", reply_markup=docker_list_kb())
        return
    await q.edit_message_text(f"<b>Docker:</b> <code>{html_escape(name)}</code>", parse_mode=ParseMode.HTML, reply_markup=docker_item_kb(name))

@require_admin
async def docker_inspect(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"docker:inspect:([a-zA-Z0-9_.\-]{1,64})", q.data or "")
    if not m:
        return
    name = m.group(1)
    if not _is_allowed_container(name):
        await q.edit_message_text("Контейнер недоступен.", reply_markup=docker_list_kb())
        return
    summary = await docker_inspect_summary(name)
    payload = "<b>Inspect</b>\n" + wrap_as_codeblock_html(_clip_text(summary))
    await q.edit_message_text(payload, parse_mode=ParseMode.HTML, reply_markup=docker_item_kb(name))

@require_admin
async def docker_logs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"docker:logs:([a-zA-Z0-9_.\-]{1,64}):(\d{1,3})", q.data or "")
    if not m:
        return
    name = m.group(1)
    tail = int(m.group(2))
    tail = 120 if tail < 120 else (600 if tail > 600 else tail)
    if not _is_allowed_container(name):
        await q.edit_message_text("Контейнер недоступен.", reply_markup=docker_list_kb())
        return

    rc, out, err = await run_exec([DOCKER_BIN, "logs", "--tail", str(tail), name], timeout=SUBPROC_MEDIUM_TIMEOUT)
    if rc != 0:
        log_text = f"docker logs error: {err.strip() or out.strip() or 'н/д'}"
    else:
        log_text = out

    next_tail = 600 if tail >= 600 else tail + 120
    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔎 Inspect", callback_data=f"docker:inspect:{name}"),
                InlineKeyboardButton("📜 Ещё", callback_data=f"docker:logs:{name}:{next_tail}"),
            ],
            [InlineKeyboardButton("⬅️ К списку", callback_data="docker:list")],
            [InlineKeyboardButton("⬅️ К статусу", callback_data="docker:back")],
        ]
    )
    payload = f"<b>Logs</b> (<code>{html_escape(name)}</code>, tail {tail})\n" + wrap_as_codeblock_html(_clip_text(log_text))
    await q.edit_message_text(payload, parse_mode=ParseMode.HTML, reply_markup=kb)

# ============================================================
#                       AUTH / START
# ============================================================

@require_private
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if is_authorized(update) and not is_enabled(update):
        await reply_disabled(update)
        return
    msg = update.effective_message
    if msg:
        await msg.reply_text("Меню:", reply_markup=main_menu_kb(update))

@require_private
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if is_authorized(update) and not is_enabled(update):
        await reply_disabled(update)
        return

    lines = [
        "<b>Команды</b>",
        "• /auth &lt;пароль&gt; — авторизация (роль определяется паролем)",
        "• /logout — выйти",
        "• /health — статус сервера",
        "• /ticket — создать тикет",
    ]
    if is_admin(update):
        lines += ["", "<b>Админ</b>", "• кнопка «Пользователи» — сообщения/никнеймы"]
    msg = update.effective_message
    if msg:
        await msg.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

@require_private
async def cmd_auth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    text = (msg.text if msg else "") or ""
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        if msg:
            await msg.reply_text("Формат: <b>/auth пароль</b>", parse_mode=ParseMode.HTML)
        return

    passwd = parts[1].strip()
    if not passwd:
        if msg:
            await msg.reply_text("Пустой пароль.")
        return

    role: Optional[str] = None
    if ADMIN_PASSWORD and passwd == ADMIN_PASSWORD:
        role = "admin"
    elif AUTH_PASSWORD and passwd == AUTH_PASSWORD:
        role = "user"

    if role is None:
        if msg:
            await msg.reply_text("Пароль неверный.")
        return

    u = update.effective_user
    if not u:
        if msg:
            await msg.reply_text("Ошибка: не удалось определить пользователя.")
        return

    existing = get_user_meta(u.id) or {}
    preserved_enabled = bool(existing.get("enabled", True))
    preserved_nick = existing.get("nickname")
    preserved_role = existing.get("role")
    preserved_paid = bool(existing.get("is_paid", False))

    role_to_set = preserved_role if (preserved_role and not preserved_enabled) else role

    meta = {
        "user_id": u.id,
        "role": role_to_set,
        "enabled": preserved_enabled,
        "nickname": preserved_nick,
        "username": u.username,
        "first_name": u.first_name,
        "last_name": u.last_name,
        "auth_at": datetime.now(TZ).isoformat(),
        "is_paid": preserved_paid,
    }
    await update_user_data(lambda cfg: _set_user_meta(cfg, u.id, meta))

    if not preserved_enabled:
        await reply_disabled(update)
        return

    if msg:
        await msg.reply_text("Авторизация успешна ✅", reply_markup=main_menu_kb(update))

@require_private
async def cmd_logout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if is_authorized(update) and not is_enabled(update):
        await reply_disabled(update)
        return

    uid = get_user_id(update)
    msg = update.effective_message
    if uid is None:
        return

    if str(uid) in USER_DATA.authorized_users:
        await update_user_data(lambda cfg: _remove_user(cfg, uid))
        if msg:
            await msg.reply_text("Вы удалены из списка авторизованных.")
    else:
        if msg:
            await msg.reply_text("Вы не в списке авторизованных.")

STATE_MAINT_URGENCY, STATE_MAINT_DURATION, STATE_MAINT_EXTEND = range(3)

def urgency_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔥 Срочные", callback_data="maint:urgent"),
                InlineKeyboardButton("🗓 Плановые", callback_data="maint:planned"),
            ]
        ]
    )

def parse_hhmm(text: str) -> Optional[Tuple[int, int]]:
    m = re.fullmatch(r"\s*(\d{1,3})\s*:\s*([0-5]\d)\s*", text or "")
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))

def plural_ru(n: int, one: str, few: str, many: str) -> str:
    n = abs(n) % 100
    n1 = n % 10
    if 11 <= n <= 19:
        return many
    if n1 == 1:
        return one
    if 2 <= n1 <= 4:
        return few
    return many

def humanize_hhmm(h: int, m: int) -> str:
    parts = []
    if h:
        parts.append(f"{h} {plural_ru(h, 'час', 'часа', 'часов')}")
    if m:
        parts.append(f"{m} {plural_ru(m, 'минута', 'минуты', 'минут')}")
    return " ".join(parts) if parts else "0 минут"

def format_maint(urgency: str, hh: int, mm: int, author: str) -> str:
    now = datetime.now(TZ).strftime("%d.%m.%Y %H:%M")
    urgency_label = "срочные" if urgency == "urgent" else "плановые"
    return (
        "⚠️ <b>Технические работы</b>\n"
        f"• Тип: <b>{html_escape(urgency_label)}</b>\n"
        f"• Оценка простоя: <b>{html_escape(humanize_hhmm(hh, mm))}</b>\n"
        f"• Ответственный: <b>{html_escape(author)}</b>\n"
        f"• Старт: <code>{html_escape(now)}</code> ({html_escape(TZ_NAME)})"
    )

def _hhmm_to_minutes(hh: int, mm: int) -> int:
    return max(0, (int(hh) * 60) + int(mm))

def _minutes_to_hhmm(total: int) -> Tuple[int, int]:
    total = max(0, int(total))
    return total // 60, total % 60

def _fmt_dt_short(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%d.%m.%Y %H:%M")

def _build_maint_record(urgency: str, hh: int, mm: int, author_id: Optional[int], author_name: str) -> Dict[str, Any]:
    now = datetime.now(TZ)
    duration_min = _hhmm_to_minutes(hh, mm)
    expected_end = now + timedelta(minutes=duration_min)
    maint_id = str(int(now.timestamp()))
    return {
        "id": maint_id,
        "active": True,
        "urgency": urgency,
        "duration_min": duration_min,
        "started_at": now.isoformat(),
        "expected_end": expected_end.isoformat(),
        "author_id": author_id,
        "author_name": author_name,
        "updated_at": now.isoformat(),
    }

def _maint_control_kb(maint_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Завершить", callback_data=f"maint:end:{maint_id}"),
                InlineKeyboardButton("⏳ Продлить", callback_data=f"maint:extend:{maint_id}"),
            ]
        ]
    )

def _maint_panel_text(maint: Dict[str, Any]) -> str:
    urgency_label = "срочные" if maint.get("urgency") == "urgent" else "плановые"
    duration_min = int(maint.get("duration_min", 0) or 0)
    hh, mm = _minutes_to_hhmm(duration_min)
    started_at = maint.get("started_at")
    expected_end = maint.get("expected_end")
    try:
        started_dt = datetime.fromisoformat(started_at) if started_at else None
    except Exception:
        started_dt = None
    try:
        end_dt = datetime.fromisoformat(expected_end) if expected_end else None
    except Exception:
        end_dt = None
    lines = [
        "🛠️ <b>Техработы активны</b>",
        f"• Тип: <b>{html_escape(urgency_label)}</b>",
        f"• Оценка простоя: <b>{html_escape(humanize_hhmm(hh, mm))}</b>",
    ]
    if started_dt:
        lines.append(f"• Старт: <code>{html_escape(_fmt_dt_short(started_dt))}</code> ({html_escape(TZ_NAME)})")
    if end_dt:
        lines.append(f"• Окончание: <code>{html_escape(_fmt_dt_short(end_dt))}</code> ({html_escape(TZ_NAME)})")
    lines.append("\nВыберите действие:")
    return "\n".join(lines)

def _maint_extend_notice(maint: Dict[str, Any], hh: int, mm: int, author: str) -> str:
    expected_end = maint.get("expected_end")
    end_dt = None
    try:
        end_dt = datetime.fromisoformat(expected_end) if expected_end else None
    except Exception:
        end_dt = None
    end_txt = _fmt_dt_short(end_dt) if end_dt else "-"
    return (
        "⏳ <b>Техработы продлены</b>\n"
        f"• Новый ориентир простоя: <b>{html_escape(humanize_hhmm(hh, mm))}</b>\n"
        f"• Окончание: <code>{html_escape(end_txt)}</code> ({html_escape(TZ_NAME)})\n"
        f"• Ответственный: <b>{html_escape(author)}</b>\n\n"
        "Спасибо за понимание 🙏"
    )

def _maint_end_notice(maint: Dict[str, Any], author: str) -> str:
    ended_at = datetime.now(TZ)
    return (
        "✅ <b>Техработы завершены</b>\n"
        f"• Время: <code>{html_escape(_fmt_dt_short(ended_at))}</code> ({html_escape(TZ_NAME)})\n"
        f"• Ответственный: <b>{html_escape(author)}</b>\n\n"
        "Спасибо за терпение 🙌"
    )

def _maint_due_prompt(maint: Dict[str, Any]) -> str:
    expected_end = maint.get("expected_end")
    end_dt = None
    try:
        end_dt = datetime.fromisoformat(expected_end) if expected_end else None
    except Exception:
        end_dt = None
    end_txt = _fmt_dt_short(end_dt) if end_dt else "-"
    return (
        "⏰ <b>Время простоя истекло</b>\n"
        f"• Окончание: <code>{html_escape(end_txt)}</code> ({html_escape(TZ_NAME)})\n\n"
        "Завершить техработы или продлить?"
    )

def _maint_restart_text(maint: Dict[str, Any]) -> str:
    return "♻️ <b>Бот перезапущен</b>\n\n" + _maint_panel_text(maint)

async def send_to_many(
    context: ContextTypes.DEFAULT_TYPE,
    user_ids: Iterable[int],
    text: str,
) -> Tuple[int, int]:
    ok = 0
    fail = 0
    for uid in user_ids:
        try:
            await context.bot.send_message(chat_id=uid, text=text, parse_mode=ParseMode.HTML)
            ok += 1
        except Exception as e:
            logger.warning("Не удалось отправить пользователю %s: %s", uid, e)
            fail += 1
    return ok, fail

def authorized_ids(role_filter: Optional[str] = None, exclude: Optional[Set[int]] = None) -> List[int]:
    exclude = exclude or set()
    ids: List[int] = []
    for k, meta in USER_DATA.authorized_users.items():
        try:
            uid = int(meta.get("user_id", k))
        except Exception:
            continue
        if uid in exclude:
            continue
        if not bool(meta.get("enabled", True)):
            continue
        if role_filter and meta.get("role") != role_filter:
            continue
        ids.append(uid)
    return sorted(set(ids))

 

# ============================================================
#                       MAINTENANCE (ADMIN)
# ============================================================

@require_admin
async def maint_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg:
        await msg.reply_text("Выберите тип работ:", reply_markup=urgency_kb())
    return STATE_MAINT_URGENCY

@require_admin
async def maint_urgency(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    if q.data not in ("maint:urgent", "maint:planned"):
        return ConversationHandler.END
    context.user_data["maint_urgency"] = "urgent" if q.data.endswith("urgent") else "planned"
    if q.message and q.message.chat:
        context.user_data["maint_panel_chat_id"] = q.message.chat.id
        context.user_data["maint_panel_msg_id"] = q.message.message_id
    await q.edit_message_text("Введите ожидаемое время простоя в формате ЧЧ:ММ (например, 1:35):")
    return STATE_MAINT_DURATION

@require_admin
async def maint_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    parsed = parse_hhmm((msg.text if msg else "") or "")
    if not parsed:
        if msg:
            await msg.reply_text("Некорректно. Введите ЧЧ:ММ, например 0:45 или 2:00:")
        return STATE_MAINT_DURATION

    hh, mm = parsed
    urgency = context.user_data.get("maint_urgency", "planned")
    author = display_name(update)
    author_id = get_user_id(update)
    msg_text = format_maint(urgency, hh, mm, author)

    recipients = authorized_ids(role_filter="user", exclude=set())
    if not recipients:
        if msg:
            await msg.reply_text("Нет получателей: отсутствуют авторизованные пользователи.")
        return ConversationHandler.END

    ok, fail = await send_to_many(context, recipients, msg_text)

    maint = _build_maint_record(urgency, hh, mm, author_id, author)
    maint_id = maint.get("id")
    if maint_id:
        await update_important_data(lambda cfg: _set_maintenance(cfg, maint))

    panel_text = _maint_panel_text(maint)
    panel_text = f"{panel_text}\n\nОповещены: ✅ {ok}, ❌ {fail}"

    panel_chat_id = context.user_data.get("maint_panel_chat_id")
    panel_msg_id = context.user_data.get("maint_panel_msg_id")
    if panel_chat_id and panel_msg_id:
        try:
            await context.bot.edit_message_text(
                chat_id=panel_chat_id,
                message_id=panel_msg_id,
                text=panel_text,
                parse_mode=ParseMode.HTML,
                reply_markup=_maint_control_kb(str(maint_id)),
            )
            return ConversationHandler.END
        except Exception:
            pass

    if msg:
        await msg.reply_text(panel_text, parse_mode=ParseMode.HTML, reply_markup=_maint_control_kb(str(maint_id)))
    return ConversationHandler.END

@require_admin
async def maint_extend_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    m = re.fullmatch(r"maint:extend:(\d+)", q.data or "")
    if not m:
        return ConversationHandler.END
    maint_id = m.group(1)
    maint = _get_active_maintenance()
    if not maint or str(maint.get("id")) != maint_id:
        await q.edit_message_text("Техработы не активны или уже завершены.")
        return ConversationHandler.END
    context.user_data["maint_extend_id"] = maint_id
    await q.edit_message_text(
        "⏳ Продление техработ.\nВведите новое время простоя в формате ЧЧ:ММ (например, 1:35):"
    )
    return STATE_MAINT_EXTEND

@require_admin
async def maint_extend_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    parsed = parse_hhmm((msg.text if msg else "") or "")
    if not parsed:
        if msg:
            await msg.reply_text("Некорректно. Введите ЧЧ:ММ, например 0:45 или 2:00:")
        return STATE_MAINT_EXTEND

    maint_id = context.user_data.get("maint_extend_id")
    maint = _get_active_maintenance()
    if not maint or str(maint.get("id")) != str(maint_id):
        if msg:
            await msg.reply_text("Техработы не активны или уже завершены.")
        return ConversationHandler.END

    hh, mm = parsed
    duration_min = _hhmm_to_minutes(hh, mm)
    now = datetime.now(TZ)
    expected_end = now + timedelta(minutes=duration_min)
    maint["duration_min"] = duration_min
    maint["expected_end"] = expected_end.isoformat()
    maint["updated_at"] = now.isoformat()

    await update_important_data(lambda cfg: _set_maintenance(cfg, maint))

    author = display_name(update)
    notice = _maint_extend_notice(maint, hh, mm, author)
    recipients = authorized_ids(role_filter="user", exclude=set())
    ok, fail = await send_to_many(context, recipients, notice) if recipients else (0, 0)

    panel_text = _maint_panel_text(maint)
    panel_text = f"{panel_text}\n\nОповещены: ✅ {ok}, ❌ {fail}"
    context.user_data.pop("maint_extend_id", None)
    if msg:
        await msg.reply_text(panel_text, parse_mode=ParseMode.HTML, reply_markup=_maint_control_kb(str(maint_id)))
    return ConversationHandler.END

@require_admin
async def maint_end_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"maint:end:(\d+)", q.data or "")
    if not m:
        return
    maint_id = m.group(1)
    maint = _get_active_maintenance()
    if not maint or str(maint.get("id")) != maint_id:
        await q.edit_message_text("Техработы не активны или уже завершены.")
        return

    author = display_name(update)
    notice = _maint_end_notice(maint, author)
    recipients = authorized_ids(role_filter="user", exclude=set())
    ok, fail = await send_to_many(context, recipients, notice) if recipients else (0, 0)

    await update_important_data(lambda cfg: _clear_maintenance(cfg))
    await q.edit_message_text(f"✅ Техработы завершены. Оповещены: ✅ {ok}, ❌ {fail}")

async def maint_restart_notify(context: ContextTypes.DEFAULT_TYPE) -> None:
    maint = _get_active_maintenance()
    if not maint:
        return
    maint_id = str(maint.get("id", "") or "")
    if not maint_id:
        return
    admin_ids = authorized_ids(role_filter="admin", exclude=set())
    if not admin_ids:
        return
    text = _maint_restart_text(maint)
    kb = _maint_control_kb(maint_id)
    for uid in admin_ids:
        try:
            await context.bot.send_message(chat_id=uid, text=text, parse_mode=ParseMode.HTML, reply_markup=kb)
        except Exception as e:
            logger.warning("Не удалось отправить админу %s: %s", uid, e)

TICKET_SUBJECT, TICKET_URGENCY, TICKET_TEXT, TICKET_CONFIRM = range(4)

# ============================================================
#                       TICKETS (USER FLOW)
# ============================================================

def ticket_urgency_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("P1 — критично", callback_data="ticket:p1"),
                InlineKeyboardButton("P2 — важно", callback_data="ticket:p2"),
                InlineKeyboardButton("P3 — не срочно", callback_data="ticket:p3"),
            ]
        ]
    )

def ticket_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ Отправить", callback_data="ticket:send")]])

@require_auth
async def ticket_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg:
        await msg.reply_text("Тема тикета (кратко).\nДля отмены: /cancel")
    return TICKET_SUBJECT

@require_auth
async def ticket_subject(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    subj = ((msg.text if msg else "") or "").strip()
    if len(subj) < 3:
        if msg:
            await msg.reply_text("Тема слишком короткая. Введите минимум 3 символа.")
        return TICKET_SUBJECT

    context.user_data["ticket_subject"] = subj
    if msg:
        await msg.reply_text("Срочность:", reply_markup=ticket_urgency_kb())
    return TICKET_URGENCY

@require_auth
async def ticket_urgency(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    if q.data not in ("ticket:p1", "ticket:p2", "ticket:p3"):
        return ConversationHandler.END
    context.user_data["ticket_urgency"] = q.data.split(":")[1]
    await q.edit_message_text("Опишите проблему (лучше одним сообщением). Для отмены: /cancel")
    return TICKET_TEXT

@require_auth
async def ticket_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    text = ((msg.text if msg else "") or "").strip()
    if len(text) < 10:
        if msg:
            await msg.reply_text("Описание слишком короткое. Дайте больше деталей (>= 10 символов).")
        return TICKET_TEXT

    context.user_data["ticket_text"] = text

    subj = context.user_data.get("ticket_subject", "-")
    urg = str(context.user_data.get("ticket_urgency", "p3")).upper()

    preview = (
        "<b>Проверьте тикет</b>\n"
        f"• Тема: <b>{html_escape(subj)}</b>\n"
        f"• Срочность: <b>{html_escape(urg)}</b>\n"
        f"• Текст:\n<blockquote>{html_escape(text)}</blockquote>"
    )
    if msg:
        await msg.reply_text(preview, parse_mode=ParseMode.HTML, reply_markup=ticket_confirm_kb())
    return TICKET_CONFIRM

@require_auth
async def ticket_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    if q.data != "ticket:send":
        return ConversationHandler.END

    def _next_ticket(cfg: ImportantData) -> int:
        cfg.tickets_seq += 1
        return cfg.tickets_seq

    ticket_id = await update_important_data(_next_ticket)

    u = update.effective_user
    uid = u.id if u else None

    subj = context.user_data.get("ticket_subject", "-")
    urg = str(context.user_data.get("ticket_urgency", "p3")).upper()
    txt = context.user_data.get("ticket_text", "-")
    created = datetime.now(TZ).strftime("%d.%m.%Y %H:%M")

    meta = get_user_meta(uid) if isinstance(uid, int) else None
    author_name = display_name_from_meta(meta) if meta else display_name(update)

    msg_text = (
        f"🎫 <b>Новый тикет #{ticket_id}</b>\n"
        f"• От: <b>{html_escape(author_name)}</b> (<code>{html_escape(str(uid) if uid is not None else '-')}</code>)\n"
        f"• Срочность: <b>{html_escape(urg)}</b>\n"
        f"• Тема: <b>{html_escape(subj)}</b>\n"
        f"• Время: <code>{html_escape(created)}</code> ({html_escape(TZ_NAME)})\n"
        f"\n<b>Описание</b>\n<blockquote>{html_escape(txt)}</blockquote>"
    )

    admins = authorized_ids(role_filter="admin")
    if not admins:
        await q.edit_message_text("Тикет не отправлен: нет авторизованных администраторов.")
        return ConversationHandler.END

    ok, fail = await send_to_many(context, admins, msg_text)
    await q.edit_message_text(f"Тикет отправлен админам ✅ (ok={ok}, fail={fail})")
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg:
        await msg.reply_text("Действие отменено.")
    return ConversationHandler.END

(
    ADMIN_PICK,
    ADMIN_ALL_MENU,
    ADMIN_ALL_MSG_TEXT,
    ADMIN_USER_MENU,
    ADMIN_USER_MSG_TEXT,
    ADMIN_USER_NICK_TEXT,
    ADMIN_USER_CFG_TEXT,
) = range(7)

# ============================================================
#                       ADMIN: USERS UI (INLINE)
# ============================================================

def users_list_kb() -> InlineKeyboardMarkup:
    buttons: List[List[InlineKeyboardButton]] = []
    buttons.append([InlineKeyboardButton("📣 Все пользователи", callback_data="users:all")])

    items: List[Tuple[str, bool, bool, str, int, str]] = []
    for k, meta in USER_DATA.authorized_users.items():
        try:
            uid = int(meta.get("user_id", k))
        except Exception:
            continue
        name = display_name_from_meta(meta)
        role = meta.get("role", "user")
        enabled = bool(meta.get("enabled", True))
        is_paid = bool(meta.get("is_paid", False))
        items.append((role, enabled, is_paid, name.lower(), uid, name))

    items.sort(key=lambda x: (0 if x[0] == "user" else 1, x[3], x[4]))

    row: List[InlineKeyboardButton] = []
    for role, enabled, is_paid, _, uid, name in items:
        prefix = ""
        if not enabled:
            prefix += "⛔ "
        if role == "admin":
            prefix += "👑⭐ " if is_paid else "👑 "
        else:
            if is_paid:
                prefix += "⭐ "
        label = f"{prefix}{name}"
        row.append(InlineKeyboardButton(label[:50], callback_data=f"users:user:{uid}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("⬅️ В меню", callback_data="users:main")])
    return InlineKeyboardMarkup(buttons)

def users_all_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✉️ Сообщение всем", callback_data="users:allmsg")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="users:back")],
        ]
    )

def user_card_kb(uid: int) -> InlineKeyboardMarkup:
    meta = get_user_meta(uid) or {}
    enabled = bool(meta.get("enabled", True))
    role = meta.get("role", "user")

    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton("✉️ Написать сообщение", callback_data=f"users:msg:{uid}")],
        [InlineKeyboardButton("🏷 Добавить/изменить ник", callback_data=f"users:nick:{uid}")],
        [InlineKeyboardButton("⭐ Переключить оплату", callback_data=f"users:paid:{uid}")],
        [InlineKeyboardButton("📦 Отправить конфигурацию", callback_data=f"users:cfg:{uid}")],
    ]

    if role != "admin":
        rows.append([InlineKeyboardButton("🚫 Забанить" if enabled else "✅ Разбанить", callback_data=f"users:toggle:{uid}")])

    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="users:back")])
    return InlineKeyboardMarkup(rows)

def format_user_card(meta: Dict[str, Any]) -> str:
    uid = meta.get("user_id", "-")
    role = meta.get("role", "user")
    nick = meta.get("nickname") or "-"
    uname = meta.get("username")
    nm = " ".join([x for x in [meta.get("first_name"), meta.get("last_name")] if x]) or "-"
    auth_at = meta.get("auth_at") or "-"
    status = "enabled" if meta.get("enabled", True) else "disabled"
    return (
        "<b>Пользователь</b>\n"
        f"• ID: <code>{html_escape(str(uid))}</code>\n"
        f"• Роль: <b>{html_escape(str(role))}</b>\n"
        f"• Статус: <b>{html_escape(status)}</b>\n"
        f"• Подписка: <b>{'оплачена' if bool(meta.get('is_paid', False)) else 'не оплачена'}</b>\n"
        f"• Ник: <b>{html_escape(str(nick))}</b>\n"
        f"• Username: <b>{html_escape(('@' + uname) if uname else '-')}</b>\n"
        f"• Имя: <b>{html_escape(str(nm))}</b>\n"
        f"• Авторизация: <code>{html_escape(str(auth_at))}</code>"
    )

@require_admin
async def users_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg:
        await msg.reply_text("Выберите пользователя:", reply_markup=users_list_kb())
    return ADMIN_PICK

@require_admin
async def users_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    data = q.data or ""

    if data == "users:main":

        try:
            await q.edit_message_text("Меню:", reply_markup=None)
        except Exception:
            pass
        chat_id = q.message.chat.id if q.message and q.message.chat else None
        if chat_id is not None:
            await context.bot.send_message(chat_id=chat_id, text="Меню:", reply_markup=main_menu_kb(update))
        return ConversationHandler.END

    if data == "users:all":
        await q.edit_message_text("Все пользователи:", reply_markup=users_all_kb())
        return ADMIN_ALL_MENU

    m = re.fullmatch(r"users:user:(\d+)", data)
    if m:
        uid = int(m.group(1))
        meta = get_user_meta(uid)
        if not meta:
            await q.edit_message_text("Пользователь не найден (возможно, удалён из списка).", reply_markup=users_list_kb())
            return ADMIN_PICK
        context.user_data["selected_uid"] = uid
        await q.edit_message_text(format_user_card(meta), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
        return ADMIN_USER_MENU

    if data == "users:back":
        await q.edit_message_text("Выберите пользователя:", reply_markup=users_list_kb())
        return ADMIN_PICK

    await q.edit_message_text("Выберите пользователя:", reply_markup=users_list_kb())
    return ADMIN_PICK

@require_admin
async def users_all_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()

    if q.data == "users:back":
        await q.edit_message_text("Выберите пользователя:", reply_markup=users_list_kb())
        return ADMIN_PICK
    if q.data == "users:allmsg":
        await q.edit_message_text("Введите текст сообщения всем пользователям:")
        return ADMIN_ALL_MSG_TEXT

    await q.edit_message_text("Все пользователи:", reply_markup=users_all_kb())
    return ADMIN_ALL_MENU

@require_admin
async def users_all_msg_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    text = ((msg.text if msg else "") or "").strip()
    if not text:
        if msg:
            await msg.reply_text("Пустой текст. Введите сообщение:")
        return ADMIN_ALL_MSG_TEXT

    sender = get_user_id(update)
    recipients = authorized_ids(role_filter=None, exclude={sender} if sender else set())
    if not recipients:
        if msg:
            await msg.reply_text("Нет получателей.")
        return ADMIN_PICK

    payload = f"📩 <b>Сообщение администратора</b>\n\n{html_escape(text)}"
    ok, fail = await send_to_many(context, recipients, payload)
    if msg:
        await msg.reply_text(f"Отправлено всем: ✅ {ok}, ❌ {fail}")
        await msg.reply_text("Выберите пользователя:", reply_markup=users_list_kb())
    return ADMIN_PICK

@require_admin
async def users_user_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    data = q.data or ""

    if data == "users:back":
        await q.edit_message_text("Выберите пользователя:", reply_markup=users_list_kb())
        return ADMIN_PICK

    m_toggle = re.fullmatch(r"users:toggle:(\d+)", data)
    if m_toggle:
        uid = int(m_toggle.group(1))
        meta = get_user_meta(uid)
        if not meta:
            await q.edit_message_text("Пользователь не найден.", reply_markup=users_list_kb())
            return ADMIN_PICK

        if meta.get("role") == "admin":
            await q.edit_message_text(
                format_user_card(meta) + "\n\n<b>Администраторов банить нельзя.</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=user_card_kb(uid),
            )
            return ADMIN_USER_MENU

        meta["enabled"] = not bool(meta.get("enabled", True))
        updated = await update_user_data(lambda cfg: _set_user_meta(cfg, uid, meta))
        await q.edit_message_text(format_user_card(updated), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
        return ADMIN_USER_MENU

    m_paid = re.fullmatch(r"users:paid:(\d+)", data)
    if m_paid:
        uid = int(m_paid.group(1))
        meta = get_user_meta(uid)
        if not meta:
            await q.edit_message_text("Пользователь не найден.", reply_markup=users_list_kb())
            return ADMIN_PICK
        meta["is_paid"] = not bool(meta.get("is_paid", False))
        updated = await update_user_data(lambda cfg: _set_user_meta(cfg, uid, meta))
        await q.edit_message_text(format_user_card(updated), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
        return ADMIN_USER_MENU

    m_msg = re.fullmatch(r"users:msg:(\d+)", data)
    if m_msg:
        context.user_data["selected_uid"] = int(m_msg.group(1))
        await q.edit_message_text("Введите текст личного сообщения пользователю:")
        return ADMIN_USER_MSG_TEXT

    m_nick = re.fullmatch(r"users:nick:(\d+)", data)
    if m_nick:
        context.user_data["selected_uid"] = int(m_nick.group(1))
        await q.edit_message_text("Введите никнейм (как должен отображаться в списке):")
        return ADMIN_USER_NICK_TEXT

    m_cfg = re.fullmatch(r"users:cfg:(\d+)", data)
    if m_cfg:
        uid = int(m_cfg.group(1))
        context.user_data["selected_uid"] = uid
        await q.edit_message_text(
            "Вставьте конфигурацию одним сообщением. Она будет отправлена пользователю как <b>кодовый блок</b>."
            "\n\nПодсказка: можно вставлять vless/URL/JSON без изменений.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data=f"users:user:{uid}")]]),
        )
        return ADMIN_USER_CFG_TEXT

    uid = context.user_data.get("selected_uid")
    meta = get_user_meta(uid) if isinstance(uid, int) else None
    if meta:
        await q.edit_message_text(format_user_card(meta), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
        return ADMIN_USER_MENU

    await q.edit_message_text("Выберите пользователя:", reply_markup=users_list_kb())
    return ADMIN_PICK

@require_admin
async def users_user_msg_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data.get("selected_uid")
    msg = update.effective_message
    if not isinstance(uid, int):
        if msg:
            await msg.reply_text("Ошибка: пользователь не выбран.")
            await msg.reply_text("Выберите пользователя:", reply_markup=users_list_kb())
        return ADMIN_PICK

    meta = get_user_meta(uid)
    if not meta:
        if msg:
            await msg.reply_text("Пользователь не найден.")
            await msg.reply_text("Выберите пользователя:", reply_markup=users_list_kb())
        return ADMIN_PICK

    text = ((msg.text if msg else "") or "").strip()
    if not text:
        if msg:
            await msg.reply_text("Пустой текст. Введите сообщение:")
        return ADMIN_USER_MSG_TEXT

    payload = f"📩 <b>Сообщение от администратора</b>\n\n{html_escape(text)}"
    try:
        await context.bot.send_message(chat_id=uid, text=payload, parse_mode=ParseMode.HTML)
        if msg:
            await msg.reply_text("Отправлено ✅")
    except Exception as e:
        logger.warning("Не удалось отправить пользователю %s: %s", uid, e)
        if msg:
            await msg.reply_text("Не удалось отправить (пользователь мог заблокировать бота).")

    if msg:
        await msg.reply_text(format_user_card(meta), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
    return ADMIN_USER_MENU

@require_admin
async def users_user_nick_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data.get("selected_uid")
    msg = update.effective_message
    if not isinstance(uid, int):
        if msg:
            await msg.reply_text("Ошибка: пользователь не выбран.")
            await msg.reply_text("Выберите пользователя:", reply_markup=users_list_kb())
        return ADMIN_PICK

    meta = get_user_meta(uid)
    if not meta:
        if msg:
            await msg.reply_text("Пользователь не найден.")
            await msg.reply_text("Выберите пользователя:", reply_markup=users_list_kb())
        return ADMIN_PICK

    nick = ((msg.text if msg else "") or "").strip()
    if len(nick) < 2:
        if msg:
            await msg.reply_text("Ник слишком короткий. Введите минимум 2 символа:")
        return ADMIN_USER_NICK_TEXT

    meta["nickname"] = nick
    await update_user_data(lambda cfg: _set_user_meta(cfg, uid, meta))

    if msg:
        await msg.reply_text("Никнейм сохранён ✅")
        await msg.reply_text(format_user_card(meta), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
    return ADMIN_USER_MENU

@require_admin
async def users_user_cfg_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = context.user_data.get("selected_uid")
    msg = update.effective_message
    if not isinstance(uid, int):
        if msg:
            await msg.reply_text("Пользователь не выбран.")
        return ADMIN_PICK

    cfg = (msg.text if msg else "") or ""
    if not cfg.strip():
        if msg:
            await msg.reply_text("Пустая конфигурация. Вставьте текст одним сообщением.")
        return ADMIN_USER_CFG_TEXT

    meta = get_user_meta(uid)
    if not meta:
        if msg:
            await msg.reply_text("Пользователь не найден (возможно, удалён из списка).")
        return ADMIN_PICK

    header = "📦 <b>Конфигурация от администратора</b>\n\n"
    payload = header + wrap_as_codeblock_html(cfg)

    try:
        await context.bot.send_message(chat_id=uid, text=payload, parse_mode=ParseMode.HTML)
        if msg:
            await msg.reply_text("Отправлено ✅")
    except Exception as e:
        logger.warning("Не удалось отправить конфигурацию пользователю %s: %s", uid, e)
        if msg:
            await msg.reply_text("Не удалось отправить (пользователь мог заблокировать бота).")

    if msg:
        await msg.reply_text(format_user_card(meta), parse_mode=ParseMode.HTML, reply_markup=user_card_kb(uid))
    return ADMIN_USER_MENU

# ============================================================
#                       MENU ROUTER (REPLY BUTTONS)
# ============================================================

@require_private
async def menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_authorized(update) and not is_enabled(update):
        await reply_disabled(update)
        return ConversationHandler.END

    msg = update.effective_message
    txt = ((msg.text if msg else "") or "").strip()

    if txt == MENU_STATUS:
        await cmd_health(update, context)
        return ConversationHandler.END

    if txt == MENU_TICKET:
        return await ticket_start(update, context)

    if txt == MENU_MAINT:
        if not is_admin(update):
            if msg:
                await msg.reply_text("Доступ только для администратора.")
            return ConversationHandler.END
        return await maint_start(update, context)

    if txt == MENU_USERS:
        if not is_admin(update):
            if msg:
                await msg.reply_text("Доступ только для администратора.")
            return ConversationHandler.END
        return await users_entry(update, context)

    return ConversationHandler.END

# ============================================================
#                       ERROR HANDLING
# ============================================================

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:

    logger.exception("Unhandled exception in handler: %s", context.error)

# ============================================================
#                       APP
# ============================================================

def build_app() -> Application:
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN в .env")
    if not AUTH_PASSWORD and not ADMIN_PASSWORD:
        logger.warning("Не заданы AUTH_PASSWORD и ADMIN_PASSWORD: авторизация невозможна.")

    app: Application = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("auth", cmd_auth))
    app.add_handler(CommandHandler("logout", cmd_logout))
    app.add_handler(CommandHandler("health", cmd_health))

    maint_conv = ConversationHandler(
        entry_points=[
            CommandHandler("maint", maint_start),
            MessageHandler(filters.ChatType.PRIVATE & filters.Regex(rf"^{re.escape(MENU_MAINT)}$"), maint_start),
            CallbackQueryHandler(maint_extend_cb, pattern=r"^maint:extend:\d+$"),
        ],
        states={
            STATE_MAINT_URGENCY: [CallbackQueryHandler(maint_urgency, pattern=r"^maint:(urgent|planned)$")],
            STATE_MAINT_DURATION: [
                MessageHandler(PRIVATE_TEXT, maint_duration)
            ],
            STATE_MAINT_EXTEND: [
                MessageHandler(PRIVATE_TEXT, maint_extend_duration)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="maint_flow",
        persistent=False,
    )
    app.add_handler(maint_conv)
    app.add_handler(CallbackQueryHandler(maint_end_cb, pattern=r"^maint:end:\d+$"))

    ticket_conv = ConversationHandler(
        entry_points=[
            CommandHandler("ticket", ticket_start),
            MessageHandler(filters.ChatType.PRIVATE & filters.Regex(rf"^{re.escape(MENU_TICKET)}$"), ticket_start),
        ],
        states={
            TICKET_SUBJECT: [
                MessageHandler(PRIVATE_TEXT, ticket_subject)
            ],
            TICKET_URGENCY: [CallbackQueryHandler(ticket_urgency, pattern=r"^ticket:(p1|p2|p3)$")],
            TICKET_TEXT: [MessageHandler(PRIVATE_TEXT, ticket_text)],
            TICKET_CONFIRM: [CallbackQueryHandler(ticket_confirm, pattern=r"^ticket:send$")],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="ticket_flow",
        persistent=False,
    )
    app.add_handler(ticket_conv)

    users_conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.ChatType.PRIVATE & filters.Regex(rf"^{re.escape(MENU_USERS)}$"), users_entry),
            CommandHandler("users", users_entry),
        ],
        states={
            ADMIN_PICK: [
                CallbackQueryHandler(users_pick, pattern=r"^users:(all|main|user:\d+|back)$"),
            ],
            ADMIN_ALL_MENU: [
                CallbackQueryHandler(users_all_menu, pattern=r"^users:(allmsg|back)$"),
            ],
            ADMIN_ALL_MSG_TEXT: [
                MessageHandler(PRIVATE_TEXT, users_all_msg_text),
            ],
            ADMIN_USER_MENU: [
                CallbackQueryHandler(
                    users_user_menu,
                    pattern=r"^users:(msg:\d+|nick:\d+|cfg:\d+|toggle:\d+|paid:\d+|back)$",
                ),
            ],
            ADMIN_USER_MSG_TEXT: [
                MessageHandler(PRIVATE_TEXT, users_user_msg_text),
            ],
            ADMIN_USER_NICK_TEXT: [
                MessageHandler(PRIVATE_TEXT, users_user_nick_text),
            ],
            ADMIN_USER_CFG_TEXT: [
                MessageHandler(PRIVATE_TEXT, users_user_cfg_text),
                CallbackQueryHandler(users_pick, pattern=r"^users:user:\d+$"),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="users_flow",
        persistent=False,
    )
    app.add_handler(users_conv)

    app.add_handler(CallbackQueryHandler(dns_check_cb, pattern=r"^dns:check$"))
    app.add_handler(CallbackQueryHandler(dns_back_cb, pattern=r"^dns:back$"))
    app.add_handler(CallbackQueryHandler(docker_list_menu, pattern=r"^docker:list$"))
    app.add_handler(CallbackQueryHandler(docker_back_to_status, pattern=r"^docker:back$"))
    app.add_handler(CallbackQueryHandler(docker_show, pattern=r"^docker:show:[a-zA-Z0-9_.\-]{1,64}$"))
    app.add_handler(CallbackQueryHandler(docker_inspect, pattern=r"^docker:inspect:[a-zA-Z0-9_.\-]{1,64}$"))
    app.add_handler(CallbackQueryHandler(docker_logs, pattern=r"^docker:logs:[a-zA-Z0-9_.\-]{1,64}:\d{1,3}$"))

    app.add_handler(CommandHandler("fail2ban", fail2ban_menu))
    app.add_handler(CallbackQueryHandler(f2b_menu_cb, pattern=r"^f2b:menu$"))
    app.add_handler(CallbackQueryHandler(f2b_tail_cb, pattern=r"^f2b:tail:\d{1,5}$"))
    app.add_handler(CallbackQueryHandler(f2b_digest_cb, pattern=r"^f2b:digest$"))
    app.add_handler(CallbackQueryHandler(f2b_back_cb, pattern=r"^f2b:back$"))

    if app.job_queue:
        hh, mm = 12, 0
        mt = re.fullmatch(r"(\d{1,2}):(\d{2})", FAIL2BAN_DAILY_AT)
        if mt:
            hh = max(0, min(23, int(mt.group(1))))
            mm = max(0, min(59, int(mt.group(2))))
        app.job_queue.run_daily(
            fail2ban_daily_digest,
            time=dtime(hour=hh, minute=mm, tzinfo=TZ),
            name="fail2ban_digest",
        )
        app.job_queue.run_once(maint_restart_notify, when=2, name="maint_restart_notify")
    else:
        logger.warning("JobQueue недоступен: для ежедневной выжимки установите python-telegram-bot[job-queue].")

    app.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE
            & filters.Regex(MENU_PATTERN),
            menu_router,
        )
    )

    app.add_error_handler(on_error)
    return app

def main() -> None:
    app = build_app()
    logger.info("Bot started")
    app.run_polling()

if __name__ == "__main__":
    main()
