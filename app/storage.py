import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Optional, TypeVar

import aiofiles

from .config import IMPORTANT_DATA_PATH, LEGACY_CONFIG_PATH, USER_DATA_PATH, logger

T = TypeVar("T")


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
        try:
            payload = {"authorized_users": self.authorized_users}
            tmp_path = Path(path)
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = tmp_path.with_suffix(tmp_path.suffix + ".tmp")
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(tmp_path)
        except Exception as e:
            logger.error("Не удалось сохранить %s: %s", path, e)

    async def save_async(self, path: str) -> None:
        try:
            await _write_json_atomic(path, {"authorized_users": self.authorized_users})
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
        try:
            payload = {"tickets_seq": self.tickets_seq, "maintenance": self.maintenance}
            tmp_path = Path(path)
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = tmp_path.with_suffix(tmp_path.suffix + ".tmp")
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(tmp_path)
        except Exception as e:
            logger.error("Не удалось сохранить %s: %s", path, e)

    async def save_async(self, path: str) -> None:
        try:
            await _write_json_atomic(path, {"tickets_seq": self.tickets_seq, "maintenance": self.maintenance})
        except Exception as e:
            logger.error("Не удалось сохранить %s: %s", path, e)


USER_DATA = UserData.load(USER_DATA_PATH, legacy_path=LEGACY_CONFIG_PATH)
IMPORTANT_DATA = ImportantData.load(IMPORTANT_DATA_PATH, legacy_path=LEGACY_CONFIG_PATH)
USER_DATA_LOCK = asyncio.Lock()
IMPORTANT_DATA_LOCK = asyncio.Lock()


async def update_user_data(update_fn: Callable[[UserData], T]) -> T:
    async with USER_DATA_LOCK:
        result = update_fn(USER_DATA)
        await USER_DATA.save_async(USER_DATA_PATH)
    return result


async def update_important_data(update_fn: Callable[[ImportantData], T]) -> T:
    async with IMPORTANT_DATA_LOCK:
        result = update_fn(IMPORTANT_DATA)
        await IMPORTANT_DATA.save_async(IMPORTANT_DATA_PATH)
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
