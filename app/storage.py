import asyncio
import copy
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Optional, TypeVar

import aiofiles

from .config import IMPORTANT_DATA_PATH, LEGACY_CONFIG_PATH, USER_DATA_PATH, logger

T = TypeVar("T")
USER_DATA_SCHEMA_VERSION = 1
IMPORTANT_DATA_SCHEMA_VERSION = 1


def _normalize_bool(value: Any, truthy: set[str]) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in truthy
    return bool(value)


async def _write_json_atomic(path: str, data: Dict[str, Any]) -> None:
    p = Path(path)
    await asyncio.to_thread(p.parent.mkdir, parents=True, exist_ok=True)
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    tmp = p.with_suffix(p.suffix + ".tmp")
    async with aiofiles.open(tmp, "w", encoding="utf-8") as f:
        await f.write(payload)
    await asyncio.to_thread(tmp.replace, p)
    await asyncio.to_thread(_tighten_file_permissions, p)


def _tighten_file_permissions(path: Path) -> None:
    if os.name == "nt":
        return
    try:
        path.chmod(0o600)
    except Exception:
        pass


@dataclass
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

    @staticmethod
    def _needs_rewrite(raw: Dict[str, Any]) -> bool:
        if raw.get("schema_version") != USER_DATA_SCHEMA_VERSION:
            return True
        allowed_keys = {"schema_version", "authorized_users"}
        return any(k not in allowed_keys for k in raw.keys())

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
                    if pth != path or cls._needs_rewrite(raw):
                        try:
                            data.save(path)
                        except Exception:
                            pass
                    return data
            except Exception as e:
                logger.error("Не удалось прочитать %s: %s", pth, e)
        return cls()

    def save(self, path: str) -> None:
        payload = {"schema_version": USER_DATA_SCHEMA_VERSION, "authorized_users": self.authorized_users}
        tmp_path = Path(path)
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = tmp_path.with_suffix(tmp_path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(tmp_path)
        _tighten_file_permissions(tmp_path)

    async def save_async(self, path: str) -> None:
        await _write_json_atomic(path, {"schema_version": USER_DATA_SCHEMA_VERSION, "authorized_users": self.authorized_users})


@dataclass
class ImportantData:
    tickets_seq: int = 0
    tickets: Dict[str, Any] = field(default_factory=dict)
    maintenance: Dict[str, Any] = field(default_factory=dict)
    scheduled_maintenance: Dict[str, Any] = field(default_factory=dict)
    dns_status: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def _migrate(raw: Dict[str, Any]) -> "ImportantData":
        tickets_seq = int(raw.get("tickets_seq", 0) or 0)
        tickets = raw.get("tickets", {})
        if not isinstance(tickets, dict):
            tickets = {}
        maintenance = raw.get("maintenance", {})
        if not isinstance(maintenance, dict):
            maintenance = {}
        scheduled_maintenance = raw.get("scheduled_maintenance", {})
        if not isinstance(scheduled_maintenance, dict):
            scheduled_maintenance = {}
        dns_status = raw.get("dns_status", {})
        if not isinstance(dns_status, dict):
            dns_status = {}
        return ImportantData(
            tickets_seq=tickets_seq,
            tickets=tickets,
            maintenance=maintenance,
            scheduled_maintenance=scheduled_maintenance,
            dns_status=dns_status,
        )

    @staticmethod
    def _needs_rewrite(raw: Dict[str, Any]) -> bool:
        if raw.get("schema_version") != IMPORTANT_DATA_SCHEMA_VERSION:
            return True
        allowed_keys = {"schema_version", "tickets_seq", "tickets", "maintenance", "scheduled_maintenance", "dns_status"}
        return any(k not in allowed_keys for k in raw.keys())

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
                    if pth != path or cls._needs_rewrite(raw):
                        try:
                            data.save(path)
                        except Exception:
                            pass
                    return data
            except Exception as e:
                logger.error("Не удалось прочитать %s: %s", pth, e)
        return cls()

    def save(self, path: str) -> None:
        payload = {
            "schema_version": IMPORTANT_DATA_SCHEMA_VERSION,
            "tickets_seq": self.tickets_seq,
            "tickets": self.tickets,
            "maintenance": self.maintenance,
            "scheduled_maintenance": self.scheduled_maintenance,
            "dns_status": self.dns_status,
        }
        tmp_path = Path(path)
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = tmp_path.with_suffix(tmp_path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(tmp_path)
        _tighten_file_permissions(tmp_path)

    async def save_async(self, path: str) -> None:
        await _write_json_atomic(
            path,
            {
                "schema_version": IMPORTANT_DATA_SCHEMA_VERSION,
                "tickets_seq": self.tickets_seq,
                "tickets": self.tickets,
                "maintenance": self.maintenance,
                "scheduled_maintenance": self.scheduled_maintenance,
                "dns_status": self.dns_status,
            },
        )


USER_DATA = UserData.load(USER_DATA_PATH, legacy_path=LEGACY_CONFIG_PATH)
IMPORTANT_DATA = ImportantData.load(IMPORTANT_DATA_PATH, legacy_path=LEGACY_CONFIG_PATH)
USER_DATA_LOCK = asyncio.Lock()
IMPORTANT_DATA_LOCK = asyncio.Lock()
USER_DATA_SNAPSHOT: Dict[str, Dict[str, Any]] = {}
IMPORTANT_DATA_SNAPSHOT: Dict[str, Any] = {}


def _refresh_user_snapshot() -> None:
    global USER_DATA_SNAPSHOT
    USER_DATA_SNAPSHOT = {
        k: dict(v) for k, v in getattr(USER_DATA, "authorized_users", {}).items() if isinstance(v, dict)
    }


def _refresh_important_snapshot() -> None:
    global IMPORTANT_DATA_SNAPSHOT
    IMPORTANT_DATA_SNAPSHOT = {
        "tickets_seq": int(getattr(IMPORTANT_DATA, "tickets_seq", 0) or 0),
        "tickets": dict(getattr(IMPORTANT_DATA, "tickets", {}) or {}),
        "maintenance": dict(getattr(IMPORTANT_DATA, "maintenance", {}) or {}),
        "scheduled_maintenance": dict(getattr(IMPORTANT_DATA, "scheduled_maintenance", {}) or {}),
        "dns_status": dict(getattr(IMPORTANT_DATA, "dns_status", {}) or {}),
    }


_refresh_user_snapshot()
_refresh_important_snapshot()


async def update_user_data(update_fn: Callable[[UserData], T]) -> T:
    async with USER_DATA_LOCK:
        prev_authorized_users = copy.deepcopy(USER_DATA.authorized_users)
        try:
            result = update_fn(USER_DATA)
            await USER_DATA.save_async(USER_DATA_PATH)
        except Exception:
            USER_DATA.authorized_users = prev_authorized_users
            logger.exception("Не удалось обновить user_data")
            raise
        _refresh_user_snapshot()
    return result


async def update_important_data(update_fn: Callable[[ImportantData], T]) -> T:
    async with IMPORTANT_DATA_LOCK:
        prev_tickets_seq = IMPORTANT_DATA.tickets_seq
        prev_tickets = copy.deepcopy(IMPORTANT_DATA.tickets)
        prev_maintenance = copy.deepcopy(IMPORTANT_DATA.maintenance)
        prev_scheduled_maintenance = copy.deepcopy(IMPORTANT_DATA.scheduled_maintenance)
        prev_dns_status = copy.deepcopy(IMPORTANT_DATA.dns_status)
        try:
            result = update_fn(IMPORTANT_DATA)
            await IMPORTANT_DATA.save_async(IMPORTANT_DATA_PATH)
        except Exception:
            IMPORTANT_DATA.tickets_seq = prev_tickets_seq
            IMPORTANT_DATA.tickets = prev_tickets
            IMPORTANT_DATA.maintenance = prev_maintenance
            IMPORTANT_DATA.scheduled_maintenance = prev_scheduled_maintenance
            IMPORTANT_DATA.dns_status = prev_dns_status
            logger.exception("Не удалось обновить important_data")
            raise
        _refresh_important_snapshot()
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


def _set_scheduled_maintenance(cfg: ImportantData, payload: Dict[str, Any]) -> Dict[str, Any]:
    cfg.scheduled_maintenance = payload
    return payload


def _clear_scheduled_maintenance(cfg: ImportantData) -> None:
    cfg.scheduled_maintenance = {}


def _set_dns_status(cfg: ImportantData, server_key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    cur = dict(getattr(cfg, "dns_status", {}) or {})
    cur[str(server_key)] = dict(payload or {})
    cfg.dns_status = cur
    return dict(cur[str(server_key)])


def _set_ticket(cfg: ImportantData, ticket_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    cur = dict(getattr(cfg, "tickets", {}) or {})
    cur[str(ticket_id)] = dict(payload or {})
    cfg.tickets = cur
    return dict(cur[str(ticket_id)])


def get_user_meta_copy(uid: int) -> Optional[Dict[str, Any]]:
    meta = USER_DATA_SNAPSHOT.get(str(uid))
    return dict(meta) if isinstance(meta, dict) else None


def authorized_users_snapshot() -> Dict[str, Dict[str, Any]]:
    return {k: dict(v) for k, v in USER_DATA_SNAPSHOT.items()}


async def upsert_user_meta(uid: int, meta: Dict[str, Any]) -> Dict[str, Any]:
    return await update_user_data(lambda cfg: _set_user_meta(cfg, uid, meta))


async def remove_user_meta(uid: int) -> Optional[Dict[str, Any]]:
    return await update_user_data(lambda cfg: _remove_user(cfg, uid))


async def set_maintenance_record(payload: Dict[str, Any]) -> Dict[str, Any]:
    return await update_important_data(lambda cfg: _set_maintenance(cfg, payload))


async def clear_maintenance_record() -> None:
    await update_important_data(lambda cfg: _clear_maintenance(cfg))


def get_active_maintenance() -> Optional[Dict[str, Any]]:
    m = IMPORTANT_DATA_SNAPSHOT.get("maintenance")
    if isinstance(m, dict) and m.get("active"):
        return dict(m)
    return None


def get_scheduled_maintenance() -> Optional[Dict[str, Any]]:
    m = IMPORTANT_DATA_SNAPSHOT.get("scheduled_maintenance")
    if isinstance(m, dict) and m.get("id"):
        return dict(m)
    return None


def get_ticket_copy(ticket_id: int) -> Optional[Dict[str, Any]]:
    tickets = IMPORTANT_DATA_SNAPSHOT.get("tickets")
    if not isinstance(tickets, dict):
        return None
    item = tickets.get(str(ticket_id))
    return dict(item) if isinstance(item, dict) else None


def get_dns_status_cache(server_key: str) -> Optional[Dict[str, Any]]:
    dns = IMPORTANT_DATA_SNAPSHOT.get("dns_status")
    if not isinstance(dns, dict):
        return None
    item = dns.get(str(server_key))
    return dict(item) if isinstance(item, dict) else None


async def set_dns_status_cache(server_key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return await update_important_data(lambda cfg: _set_dns_status(cfg, server_key, payload))


async def set_scheduled_maintenance_record(payload: Dict[str, Any]) -> Dict[str, Any]:
    return await update_important_data(lambda cfg: _set_scheduled_maintenance(cfg, payload))


async def clear_scheduled_maintenance_record() -> None:
    await update_important_data(lambda cfg: _clear_scheduled_maintenance(cfg))


async def next_ticket_seq() -> int:
    def _next_ticket(cfg: ImportantData) -> int:
        cfg.tickets_seq += 1
        return cfg.tickets_seq

    return await update_important_data(_next_ticket)


async def set_ticket_record(ticket_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    return await update_important_data(lambda cfg: _set_ticket(cfg, ticket_id, payload))
