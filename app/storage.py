import asyncio
import contextlib
import copy
import json
import os
import shutil
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any, TypeVar

from .config import IMPORTANT_DATA_PATH, LEGACY_CONFIG_PATH, USER_DATA_PATH, logger
from .constants import IMPORTANT_DATA_SCHEMA_VERSION, USER_DATA_SCHEMA_VERSION
from .staff import (
    STAFF_DISPLAY_TITLE,
    is_owner_meta,
    normalize_staff_alias,
    normalize_staff_display_mode,
    normalize_staff_title,
    staff_public_signature,
)

T = TypeVar("T")
ACCESS_STATES = {"pending", "approved", "blocked", "logged_out", "rejected"}
SERVICE_TIERS = {"basic", "subscriber", "unlimited_trial"}
ADMIN_LEVELS = {"admin", "owner"}
SERVICE_REQUEST_KINDS = {"trial", "purchase", "renewal"}
SERVICE_REQUEST_STATUSES = {
    "pending",
    "claimed",
    "awaiting_link",
    "requisites_sent",
    "payment_reported",
    "approved",
    "rejected",
    "cancelled",
}


class UpdateAborted(Exception):
    """Прерывает update_*_data без записи на диск; не считается ошибкой хранилища."""


def _normalize_bool(value: Any, truthy: set[str]) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in truthy
    return bool(value)


def _optional_text(value: Any, *, limit: int = 4096) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text[:limit] if text else None


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed > 0 else None


def _normalize_product_settings(raw: Any = None) -> dict[str, Any]:
    source = dict(raw) if isinstance(raw, dict) else {}
    return {
        "payment_bank": _optional_text(source.get("payment_bank"), limit=160),
        "payment_recipient": _optional_text(source.get("payment_recipient"), limit=160),
        "payment_phone": _optional_text(source.get("payment_phone"), limit=80),
        "current_period_end": _optional_text(source.get("current_period_end"), limit=80),
        "next_period_end": _optional_text(source.get("next_period_end"), limit=80),
        "period_setup_reminder_for": _optional_text(source.get("period_setup_reminder_for"), limit=80),
        "period_missing_notice_for": _optional_text(source.get("period_missing_notice_for"), limit=80),
    }


def _normalize_service_requests(raw: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(raw, dict):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for raw_id, raw_request in raw.items():
        if not isinstance(raw_request, dict):
            continue
        try:
            request_id = int(str(raw_request.get("id", raw_id)))
            user_id = int(raw_request.get("user_id", 0))
        except (TypeError, ValueError, OverflowError):
            continue
        kind = str(raw_request.get("kind") or "")
        status = str(raw_request.get("status") or "pending")
        if request_id <= 0 or user_id <= 0 or kind not in SERVICE_REQUEST_KINDS:
            continue
        if status not in SERVICE_REQUEST_STATUSES:
            status = "pending"
        item = copy.deepcopy(raw_request)
        item.pop("used_app", None)
        item.pop("used_application", None)
        item.update({"id": request_id, "user_id": user_id, "kind": kind, "status": status})
        resume_status = str(item.get("resume_status") or "")
        item["resume_status"] = resume_status if resume_status in {"pending", "payment_reported"} else None
        for key in (
            "created_at",
            "updated_at",
            "comment",
            "claimed_at",
            "reviewed_at",
            "target_end_at",
            "payment_reported_at",
            "decision_reason",
        ):
            item[key] = _optional_text(item.get(key), limit=3200 if key == "comment" else 500)
        for key in ("claimed_by_id", "reviewed_by_id"):
            try:
                item[key] = int(item[key]) if item.get(key) not in (None, "") else None
            except (TypeError, ValueError, OverflowError):
                item[key] = None
        normalized[str(request_id)] = item
    return normalized


def _normalize_audit_log(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    result: list[dict[str, Any]] = []
    for item in raw[-2000:]:
        if not isinstance(item, dict):
            continue
        action = _optional_text(item.get("action"), limit=100)
        ts = _optional_text(item.get("ts"), limit=80)
        if not action or not ts:
            continue
        clean: dict[str, Any] = {
            "ts": ts,
            "action": action,
            "actor_id": item.get("actor_id"),
            "actor_public": _optional_text(item.get("actor_public"), limit=160),
            "actor_internal": _optional_text(item.get("actor_internal"), limit=240),
            "target_user_id": item.get("target_user_id"),
            "details": copy.deepcopy(item.get("details")) if isinstance(item.get("details"), dict) else {},
        }
        result.append(clean)
    return result[-2000:]


def _normalize_outbox(raw: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(raw, dict):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for raw_id, raw_event in raw.items():
        if not isinstance(raw_event, dict):
            continue
        event_id = str(raw_event.get("id") or raw_id).strip()
        payload = raw_event.get("payload")
        recipients = raw_event.get("recipients")
        if not event_id or not isinstance(payload, dict) or not isinstance(recipients, dict):
            continue
        clean_recipients: dict[str, dict[str, Any]] = {}
        for raw_uid, raw_state in recipients.items():
            try:
                uid = int(raw_uid)
            except (TypeError, ValueError):
                continue
            if uid <= 0:
                continue
            state = dict(raw_state) if isinstance(raw_state, dict) else {}
            status = str(state.get("status") or "pending")
            if status not in {"pending", "delivered", "terminal"}:
                status = "pending"
            try:
                attempts = max(0, int(state.get("attempts", 0) or 0))
            except (TypeError, ValueError):
                attempts = 0
            try:
                part_index = max(0, int(state.get("part_index", 0) or 0))
            except (TypeError, ValueError):
                part_index = 0
            clean_recipients[str(uid)] = {
                "status": status,
                "attempts": attempts,
                "part_index": part_index,
                "next_attempt_at": str(state.get("next_attempt_at") or ""),
                "last_error": str(state.get("last_error") or "")[:500],
                "delivered_at": str(state.get("delivered_at") or ""),
            }
        if not clean_recipients:
            continue
        normalized[event_id] = {
            "id": event_id,
            "kind": str(raw_event.get("kind") or "message")[:100],
            "created_at": str(raw_event.get("created_at") or ""),
            "payload": copy.deepcopy(payload),
            "recipients": clean_recipients,
            "completion": copy.deepcopy(raw_event.get("completion"))
            if isinstance(raw_event.get("completion"), dict)
            else {},
        }
    return normalized


def make_outbox_event(
    *,
    kind: str,
    recipient_ids: list[int] | tuple[int, ...] | set[int],
    payload: dict[str, Any],
    event_id: str | None = None,
) -> dict[str, Any]:
    valid_ids: set[int] = set()
    for raw_uid in recipient_ids:
        try:
            uid = int(raw_uid)
        except (TypeError, ValueError):
            continue
        if uid > 0:
            valid_ids.add(uid)
    unique_ids = sorted(valid_ids)
    if not unique_ids:
        raise ValueError("outbox event requires at least one recipient")
    eid = (event_id or uuid.uuid4().hex).strip()
    now = datetime.now(timezone.utc).isoformat()
    return {
        "id": eid,
        "kind": str(kind or "message")[:100],
        "created_at": now,
        "payload": copy.deepcopy(payload),
        "recipients": {
            str(uid): {
                "status": "pending",
                "attempts": 0,
                "part_index": 0,
                "next_attempt_at": now,
                "last_error": "",
                "delivered_at": "",
            }
            for uid in unique_ids
        },
    }


def enqueue_user_outbox(cfg: "UserData", event: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_outbox({str(event.get("id") or ""): event})
    if not normalized:
        raise ValueError("invalid user outbox event")
    event_id, clean = next(iter(normalized.items()))
    if event_id in cfg.outbox:
        raise ValueError(f"duplicate user outbox event id: {event_id}")
    cfg.outbox[event_id] = clean
    return copy.deepcopy(clean)


def enqueue_important_outbox(cfg: "ImportantData", event: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_outbox({str(event.get("id") or ""): event})
    if not normalized:
        raise ValueError("invalid important outbox event")
    event_id, clean = next(iter(normalized.items()))
    if event_id in cfg.outbox:
        raise ValueError(f"duplicate important outbox event id: {event_id}")
    cfg.outbox[event_id] = clean
    return copy.deepcopy(clean)


def _write_json_atomic_sync(path: str, data: dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(data, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    tmp = p.with_name(f".{p.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    fd = os.open(tmp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, p)
        _tighten_file_permissions(p)
        if os.name != "nt":
            try:
                dir_fd = os.open(p.parent, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError as exc:
                # The replace has already committed and the file itself was
                # fsynced. Raising now would roll memory back while disk keeps
                # the new state, so report reduced directory durability only.
                logger.warning("Не удалось fsync каталога %s после atomic replace: %s", p.parent, exc)
    finally:
        with contextlib.suppress(FileNotFoundError):
            tmp.unlink()


async def _write_json_atomic(path: str, data: dict[str, Any]) -> None:
    await asyncio.to_thread(_write_json_atomic_sync, path, data)


def _tighten_file_permissions(path: Path) -> None:
    if os.name == "nt":
        return
    with contextlib.suppress(Exception):
        path.chmod(0o600)


class _InterprocessFileLock(AbstractContextManager["_InterprocessFileLock"]):
    def __init__(self, data_path: str) -> None:
        self.path = Path(data_path).with_suffix(Path(data_path).suffix + ".lock")
        self._handle: IO[str] | None = None

    def __enter__(self) -> "_InterprocessFileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        if os.name != "nt":
            with contextlib.suppress(OSError):
                os.chmod(self.path, 0o600)
        handle = os.fdopen(fd, "r+")
        try:
            if os.name == "nt":
                import msvcrt

                if os.fstat(handle.fileno()).st_size < 1:
                    handle.seek(0)
                    handle.write("0")
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            else:
                fcntl: Any = __import__("fcntl")

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        except Exception:
            handle.close()
            raise
        self._handle = handle
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        if self._handle is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl: Any = __import__("fcntl")

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None


def _load_raw_dict(path: str) -> dict[str, Any] | None:
    """None — файла нет; ValueError/JSONDecodeError — файл есть, но повреждён."""
    p = Path(path)
    if not p.exists():
        return None
    raw = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("ожидался JSON-объект")
    return raw


def _backup_corrupt_file(path: str) -> None:
    try:
        p = Path(path)
        if not p.exists():
            return
        suffix = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup = p.with_name(f"{p.name}.corrupt-{suffix}")
        shutil.copy2(p, backup)
        logger.error("Повреждённый файл скопирован в %s", backup)
    except Exception:
        logger.exception("Не удалось сделать резервную копию повреждённого файла %s", path)


@dataclass
class UserData:
    authorized_users: dict[str, dict[str, Any]] = field(default_factory=dict)
    outbox: dict[str, dict[str, Any]] = field(default_factory=dict)
    request_seq: int = 0
    service_requests: dict[str, dict[str, Any]] = field(default_factory=dict)
    product_settings: dict[str, Any] = field(default_factory=_normalize_product_settings)
    audit_log: list[dict[str, Any]] = field(default_factory=list)

    @staticmethod
    def _normalize_user(meta: dict[str, Any]) -> dict[str, Any]:
        meta = dict(meta) if isinstance(meta, dict) else {}
        meta.pop("used_app", None)
        meta.pop("used_application", None)

        uid_raw = meta.get("user_id")
        try:
            uid = int(uid_raw) if uid_raw is not None else None
        except Exception:
            uid = None

        role = meta.get("role", "user")
        if role not in ("user", "admin"):
            role = "user"

        enabled = _normalize_bool(meta.get("enabled", True), {"1", "true", "yes", "y", "on", "enabled"})
        state = str(meta.get("access_state") or "").strip().lower()
        if state not in ACCESS_STATES:
            state = "approved" if role == "admin" or enabled else "blocked"

        meta["user_id"] = uid
        meta["role"] = role
        meta["access_state"] = state
        meta["enabled"] = state == "approved"
        for key, raw_value, limit in (
            ("nickname", meta.get("nickname") or meta.get("nick"), 160),
            ("username", meta.get("username"), 64),
            ("first_name", meta.get("first_name"), 256),
            ("last_name", meta.get("last_name"), 256),
            ("auth_at", meta.get("auth_at"), 80),
        ):
            meta[key] = _optional_text(raw_value, limit=limit)
        for key in (
            "access_requested_at",
            "access_reviewed_at",
            "access_reviewed_by_id",
            "access_reviewed_by_name",
            "blocked_at",
            "blocked_by_id",
            "blocked_by_name",
            "blocked_reason",
            "logged_out_at",
        ):
            meta.setdefault(key, None)
        for key, limit in (
            ("access_requested_at", 80),
            ("access_reviewed_at", 80),
            ("access_reviewed_by_name", 160),
            ("blocked_at", 80),
            ("blocked_by_name", 160),
            ("blocked_reason", 500),
            ("logged_out_at", 80),
        ):
            meta[key] = _optional_text(meta.get(key), limit=limit)
        for key in ("access_reviewed_by_id", "blocked_by_id"):
            meta[key] = _optional_int(meta.get(key))

        is_admin = role == "admin"
        admin_level = str(meta.get("admin_level") or "admin") if is_admin else "none"
        if admin_level not in ADMIN_LEVELS:
            admin_level = "admin" if is_admin else "none"
        meta["admin_level"] = admin_level
        meta["staff_title"] = (
            normalize_staff_title(meta.get("staff_title"), owner=admin_level == "owner") if is_admin else None
        )
        meta["staff_alias"] = normalize_staff_alias(meta.get("staff_alias")) if is_admin else None
        display_mode = normalize_staff_display_mode(meta.get("staff_display_mode")) if is_admin else STAFF_DISPLAY_TITLE
        meta["staff_display_mode"] = display_mode if meta["staff_alias"] else STAFF_DISPLAY_TITLE

        tier = "subscriber" if is_admin else str(meta.get("service_tier") or "basic")
        if tier not in SERVICE_TIERS:
            tier = "basic"
        meta["service_tier"] = tier
        meta["is_paid"] = _normalize_bool(meta.get("is_paid", False), {"1", "true", "yes", "y", "on", "paid"})
        if tier != "subscriber":
            meta["is_paid"] = False

        connection = meta.get("connection_url")
        if connection in (None, ""):
            connection = meta.get("subscription_text")
        meta["connection_url"] = _optional_text(connection, limit=1_000_000)
        meta.pop("subscription_text", None)

        for key in (
            "subscription_updated_at",
            "subscription_updated_by_id",
            "subscription_updated_by_name",
            "paid_at",
            "payment_confirmed_by_id",
            "payment_confirmed_by_name",
            "subscription_end_at",
            "trial_issued_at",
            "trial_issued_by_id",
            "trial_issued_by_name",
            "last_auto_payment_reminder_at",
            "last_auto_payment_reminder_type",
            "last_manual_payment_reminder_at",
            "last_manual_payment_reminder_by_id",
            "last_manual_payment_reminder_by_name",
            "service_tier_updated_at",
            "service_tier_updated_by_id",
            "service_tier_updated_by_name",
        ):
            meta.setdefault(key, None)
        for key, limit in (
            ("subscription_updated_at", 80),
            ("subscription_updated_by_name", 160),
            ("paid_at", 80),
            ("payment_confirmed_by_name", 160),
            ("subscription_end_at", 80),
            ("trial_issued_at", 80),
            ("trial_issued_by_name", 160),
            ("last_auto_payment_reminder_at", 80),
            ("last_auto_payment_reminder_type", 40),
            ("last_manual_payment_reminder_at", 80),
            ("last_manual_payment_reminder_by_name", 160),
            ("service_tier_updated_at", 80),
            ("service_tier_updated_by_name", 160),
        ):
            meta[key] = _optional_text(meta.get(key), limit=limit)
        for key in (
            "subscription_updated_by_id",
            "payment_confirmed_by_id",
            "trial_issued_by_id",
            "last_manual_payment_reminder_by_id",
            "service_tier_updated_by_id",
        ):
            meta[key] = _optional_int(meta.get(key))
        if tier == "unlimited_trial":
            meta["subscription_end_at"] = None

        raw_reminders = meta.get("payment_auto_reminders")
        meta["payment_auto_reminders"] = (
            {
                str(key)[:180]: str(value)[:80]
                for key, value in list(raw_reminders.items())[-200:]
                if str(key).strip() and str(value).strip()
            }
            if isinstance(raw_reminders, dict)
            else {}
        )
        return meta

    @staticmethod
    def _migrate(raw: dict[str, Any]) -> "UserData":
        authorized_users: dict[str, dict[str, Any]] = {}
        try:
            schema_version = int(raw.get("schema_version", 0) or 0)
        except (TypeError, ValueError):
            schema_version = 0

        if isinstance(raw.get("authorized_users"), dict):
            for k, meta in raw["authorized_users"].items():
                if not isinstance(meta, dict):
                    continue
                try:
                    uid = int(meta.get("user_id", k))
                except (TypeError, ValueError, OverflowError):
                    continue
                candidate = {**meta, "user_id": uid}
                if schema_version < 3 and "service_tier" not in candidate:
                    candidate["service_tier"] = "subscriber"
                authorized_users[str(uid)] = UserData._normalize_user(candidate)
        else:
            allowed = raw.get("allowed_user_ids", [])
            if isinstance(allowed, list):
                for uid in allowed:
                    try:
                        uid_i = int(uid)
                    except (TypeError, ValueError, OverflowError):
                        continue
                    authorized_users[str(uid_i)] = UserData._normalize_user(
                        {"user_id": uid_i, "role": "user", "service_tier": "subscriber"}
                    )

        outbox = _normalize_outbox(raw.get("outbox"))
        owners = [meta for meta in authorized_users.values() if is_owner_meta(meta)]
        if len(owners) > 1:
            raise ValueError("в пользовательских данных найдено несколько руководителей сервиса")
        try:
            request_seq = max(0, int(raw.get("request_seq", 0) or 0))
        except (TypeError, ValueError):
            request_seq = 0
        service_requests = _normalize_service_requests(raw.get("service_requests"))
        if service_requests:
            request_seq = max(request_seq, max(int(key) for key in service_requests))
        return UserData(
            authorized_users=authorized_users,
            outbox=outbox,
            request_seq=request_seq,
            service_requests=service_requests,
            product_settings=_normalize_product_settings(raw.get("product_settings")),
            audit_log=_normalize_audit_log(raw.get("audit_log")),
        )

    @staticmethod
    def _needs_rewrite(raw: dict[str, Any]) -> bool:
        if raw.get("schema_version") != USER_DATA_SCHEMA_VERSION:
            return True
        allowed_keys = {
            "schema_version",
            "authorized_users",
            "outbox",
            "request_seq",
            "service_requests",
            "product_settings",
            "audit_log",
        }
        return any(k not in allowed_keys for k in raw)

    @classmethod
    def load(cls, path: str, legacy_path: str | None = None) -> "UserData":
        invalid_paths: list[str] = []
        for pth in [path, legacy_path]:
            if not pth:
                continue
            p = Path(pth)
            if not p.exists():
                continue
            try:
                raw = json.loads(p.read_text(encoding="utf-8"))
                if not isinstance(raw, dict):
                    raise ValueError("ожидался JSON-объект")
                data = cls._migrate(raw)
            except Exception as e:
                logger.error("Не удалось прочитать %s: %s", pth, e)
                _backup_corrupt_file(pth)
                invalid_paths.append(str(p))
                continue
            if pth != path or cls._needs_rewrite(raw):
                try:
                    data.save(path)
                except Exception as e:
                    raise RuntimeError(f"Не удалось сохранить миграцию пользовательских данных в {path}") from e
            return data
        if invalid_paths:
            raise RuntimeError(
                "Пользовательские данные повреждены; запуск остановлен. "
                f"Восстановите JSON из резервной копии: {', '.join(invalid_paths)}"
            )
        return cls()

    def save(self, path: str) -> None:
        payload = {
            "schema_version": USER_DATA_SCHEMA_VERSION,
            "authorized_users": self.authorized_users,
            "outbox": self.outbox,
            "request_seq": self.request_seq,
            "service_requests": self.service_requests,
            "product_settings": self.product_settings,
            "audit_log": self.audit_log,
        }
        _write_json_atomic_sync(path, payload)

    async def save_async(self, path: str) -> None:
        await _write_json_atomic(
            path,
            {
                "schema_version": USER_DATA_SCHEMA_VERSION,
                "authorized_users": self.authorized_users,
                "outbox": self.outbox,
                "request_seq": self.request_seq,
                "service_requests": self.service_requests,
                "product_settings": self.product_settings,
                "audit_log": self.audit_log,
            },
        )


@dataclass
class ImportantData:
    tickets_seq: int = 0
    tickets: dict[str, Any] = field(default_factory=dict)
    maintenance: dict[str, Any] = field(default_factory=dict)
    scheduled_maintenance: dict[str, Any] = field(default_factory=dict)
    dns_status: dict[str, Any] = field(default_factory=dict)
    daily_node_status: dict[str, Any] = field(default_factory=dict)
    outbox: dict[str, dict[str, Any]] = field(default_factory=dict)
    fail2ban_cursors: dict[str, dict[str, Any]] = field(default_factory=dict)

    @staticmethod
    def _migrate(raw: dict[str, Any]) -> "ImportantData":
        try:
            tickets_seq = max(0, int(raw.get("tickets_seq", 0) or 0))
        except (TypeError, ValueError):
            tickets_seq = 0
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
        daily_node_status = raw.get("daily_node_status", {})
        if not isinstance(daily_node_status, dict):
            daily_node_status = {}
        outbox = _normalize_outbox(raw.get("outbox"))
        raw_cursors = raw.get("fail2ban_cursors", {})
        fail2ban_cursors = (
            {str(key): copy.deepcopy(value) for key, value in raw_cursors.items() if isinstance(value, dict)}
            if isinstance(raw_cursors, dict)
            else {}
        )
        return ImportantData(
            tickets_seq=tickets_seq,
            tickets=tickets,
            maintenance=maintenance,
            scheduled_maintenance=scheduled_maintenance,
            dns_status=dns_status,
            daily_node_status=daily_node_status,
            outbox=outbox,
            fail2ban_cursors=fail2ban_cursors,
        )

    @staticmethod
    def _needs_rewrite(raw: dict[str, Any]) -> bool:
        if raw.get("schema_version") != IMPORTANT_DATA_SCHEMA_VERSION:
            return True
        allowed_keys = {
            "schema_version",
            "tickets_seq",
            "tickets",
            "maintenance",
            "scheduled_maintenance",
            "dns_status",
            "daily_node_status",
            "outbox",
            "fail2ban_cursors",
        }
        return any(k not in allowed_keys for k in raw)

    @classmethod
    def load(cls, path: str, legacy_path: str | None = None) -> "ImportantData":
        invalid_paths: list[str] = []
        for pth in [path, legacy_path]:
            if not pth:
                continue
            p = Path(pth)
            if not p.exists():
                continue
            try:
                raw = json.loads(p.read_text(encoding="utf-8"))
                if not isinstance(raw, dict):
                    raise ValueError("ожидался JSON-объект")
                data = cls._migrate(raw)
            except Exception as e:
                logger.error("Не удалось прочитать %s: %s", pth, e)
                _backup_corrupt_file(pth)
                invalid_paths.append(str(p))
                continue
            if pth != path or cls._needs_rewrite(raw):
                try:
                    data.save(path)
                except Exception as e:
                    raise RuntimeError(f"Не удалось сохранить миграцию важных данных в {path}") from e
            return data
        if invalid_paths:
            raise RuntimeError(
                "Важные данные повреждены; запуск остановлен. "
                f"Восстановите JSON из резервной копии: {', '.join(invalid_paths)}"
            )
        return cls()

    def save(self, path: str) -> None:
        payload = {
            "schema_version": IMPORTANT_DATA_SCHEMA_VERSION,
            "tickets_seq": self.tickets_seq,
            "tickets": self.tickets,
            "maintenance": self.maintenance,
            "scheduled_maintenance": self.scheduled_maintenance,
            "dns_status": self.dns_status,
            "daily_node_status": self.daily_node_status,
            "outbox": self.outbox,
            "fail2ban_cursors": self.fail2ban_cursors,
        }
        _write_json_atomic_sync(path, payload)

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
                "daily_node_status": self.daily_node_status,
                "outbox": self.outbox,
                "fail2ban_cursors": self.fail2ban_cursors,
            },
        )


USER_DATA = UserData.load(USER_DATA_PATH, legacy_path=LEGACY_CONFIG_PATH)
IMPORTANT_DATA = ImportantData.load(IMPORTANT_DATA_PATH, legacy_path=LEGACY_CONFIG_PATH)
_USER_DATA_LOCK: asyncio.Lock | None = None
_IMPORTANT_DATA_LOCK: asyncio.Lock | None = None
USER_DATA_SNAPSHOT: dict[str, dict[str, Any]] = {}
IMPORTANT_DATA_SNAPSHOT: dict[str, Any] = {}
USER_OUTBOX_SNAPSHOT: dict[str, dict[str, Any]] = {}
USER_SERVICE_REQUESTS_SNAPSHOT: dict[str, dict[str, Any]] = {}
USER_PRODUCT_SETTINGS_SNAPSHOT: dict[str, Any] = {}
USER_AUDIT_LOG_SNAPSHOT: list[dict[str, Any]] = []


def _get_user_data_lock() -> asyncio.Lock:
    global _USER_DATA_LOCK
    if _USER_DATA_LOCK is None:
        _USER_DATA_LOCK = asyncio.Lock()
    return _USER_DATA_LOCK


def _get_important_data_lock() -> asyncio.Lock:
    global _IMPORTANT_DATA_LOCK
    if _IMPORTANT_DATA_LOCK is None:
        _IMPORTANT_DATA_LOCK = asyncio.Lock()
    return _IMPORTANT_DATA_LOCK


def _refresh_user_snapshot() -> None:
    global USER_AUDIT_LOG_SNAPSHOT, USER_DATA_SNAPSHOT, USER_OUTBOX_SNAPSHOT
    global USER_PRODUCT_SETTINGS_SNAPSHOT, USER_SERVICE_REQUESTS_SNAPSHOT
    USER_DATA_SNAPSHOT = {
        k: copy.deepcopy(v) for k, v in getattr(USER_DATA, "authorized_users", {}).items() if isinstance(v, dict)
    }
    USER_OUTBOX_SNAPSHOT = copy.deepcopy(getattr(USER_DATA, "outbox", {}) or {})
    USER_SERVICE_REQUESTS_SNAPSHOT = copy.deepcopy(getattr(USER_DATA, "service_requests", {}) or {})
    USER_PRODUCT_SETTINGS_SNAPSHOT = copy.deepcopy(getattr(USER_DATA, "product_settings", {}) or {})
    USER_AUDIT_LOG_SNAPSHOT = copy.deepcopy(getattr(USER_DATA, "audit_log", []) or [])


def _refresh_important_snapshot() -> None:
    global IMPORTANT_DATA_SNAPSHOT
    IMPORTANT_DATA_SNAPSHOT = {
        "tickets_seq": int(getattr(IMPORTANT_DATA, "tickets_seq", 0) or 0),
        "tickets": copy.deepcopy(getattr(IMPORTANT_DATA, "tickets", {}) or {}),
        "maintenance": copy.deepcopy(getattr(IMPORTANT_DATA, "maintenance", {}) or {}),
        "scheduled_maintenance": copy.deepcopy(getattr(IMPORTANT_DATA, "scheduled_maintenance", {}) or {}),
        "dns_status": copy.deepcopy(getattr(IMPORTANT_DATA, "dns_status", {}) or {}),
        "daily_node_status": copy.deepcopy(getattr(IMPORTANT_DATA, "daily_node_status", {}) or {}),
        "outbox": copy.deepcopy(getattr(IMPORTANT_DATA, "outbox", {}) or {}),
        "fail2ban_cursors": copy.deepcopy(getattr(IMPORTANT_DATA, "fail2ban_cursors", {}) or {}),
    }


_refresh_user_snapshot()
_refresh_important_snapshot()


async def _reload_user_data_from_disk() -> None:
    # Если файл отсутствует или повреждён — НЕ затираем данные в памяти:
    # текущее состояние будет сохранено на диск следующей записью.
    try:
        raw = await asyncio.to_thread(_load_raw_dict, USER_DATA_PATH)
    except Exception as e:
        logger.error("user_data повреждён, оставляем данные в памяти: %s", e)
        return
    if raw is None:
        return
    latest = UserData._migrate(raw)
    USER_DATA.authorized_users = copy.deepcopy(latest.authorized_users)
    USER_DATA.outbox = copy.deepcopy(latest.outbox)
    USER_DATA.request_seq = int(latest.request_seq or 0)
    USER_DATA.service_requests = copy.deepcopy(latest.service_requests)
    USER_DATA.product_settings = copy.deepcopy(latest.product_settings)
    USER_DATA.audit_log = copy.deepcopy(latest.audit_log)


async def _reload_important_data_from_disk() -> None:
    try:
        raw = await asyncio.to_thread(_load_raw_dict, IMPORTANT_DATA_PATH)
    except Exception as e:
        logger.error("important_data повреждён, оставляем данные в памяти: %s", e)
        return
    if raw is None:
        return
    latest = ImportantData._migrate(raw)
    IMPORTANT_DATA.tickets_seq = int(latest.tickets_seq or 0)
    IMPORTANT_DATA.tickets = copy.deepcopy(latest.tickets)
    IMPORTANT_DATA.maintenance = copy.deepcopy(latest.maintenance)
    IMPORTANT_DATA.scheduled_maintenance = copy.deepcopy(latest.scheduled_maintenance)
    IMPORTANT_DATA.dns_status = copy.deepcopy(latest.dns_status)
    IMPORTANT_DATA.daily_node_status = copy.deepcopy(latest.daily_node_status)
    IMPORTANT_DATA.outbox = copy.deepcopy(latest.outbox)
    IMPORTANT_DATA.fail2ban_cursors = copy.deepcopy(latest.fail2ban_cursors)


async def update_user_data(update_fn: Callable[[UserData], T]) -> T:
    async with _get_user_data_lock():
        file_lock = _InterprocessFileLock(USER_DATA_PATH)
        # The process-wide lock is already held by the launcher, so this
        # cross-process lock is uncontended in normal operation. Acquiring it
        # synchronously avoids leaking a background lock waiter on cancellation.
        file_lock.__enter__()
        try:
            await _reload_user_data_from_disk()
            prev_authorized_users = copy.deepcopy(USER_DATA.authorized_users)
            prev_outbox = copy.deepcopy(USER_DATA.outbox)
            prev_request_seq = USER_DATA.request_seq
            prev_service_requests = copy.deepcopy(USER_DATA.service_requests)
            prev_product_settings = copy.deepcopy(USER_DATA.product_settings)
            prev_audit_log = copy.deepcopy(USER_DATA.audit_log)
            try:
                result = update_fn(USER_DATA)
                await USER_DATA.save_async(USER_DATA_PATH)
            except UpdateAborted:
                USER_DATA.authorized_users = prev_authorized_users
                USER_DATA.outbox = prev_outbox
                USER_DATA.request_seq = prev_request_seq
                USER_DATA.service_requests = prev_service_requests
                USER_DATA.product_settings = prev_product_settings
                USER_DATA.audit_log = prev_audit_log
                raise
            except Exception:
                USER_DATA.authorized_users = prev_authorized_users
                USER_DATA.outbox = prev_outbox
                USER_DATA.request_seq = prev_request_seq
                USER_DATA.service_requests = prev_service_requests
                USER_DATA.product_settings = prev_product_settings
                USER_DATA.audit_log = prev_audit_log
                logger.exception("Не удалось обновить user_data")
                raise
            _refresh_user_snapshot()
        finally:
            file_lock.__exit__(None, None, None)
    return result


async def update_important_data(update_fn: Callable[[ImportantData], T]) -> T:
    async with _get_important_data_lock():
        file_lock = _InterprocessFileLock(IMPORTANT_DATA_PATH)
        file_lock.__enter__()
        try:
            await _reload_important_data_from_disk()
            prev_tickets_seq = IMPORTANT_DATA.tickets_seq
            prev_tickets = copy.deepcopy(IMPORTANT_DATA.tickets)
            prev_maintenance = copy.deepcopy(IMPORTANT_DATA.maintenance)
            prev_scheduled_maintenance = copy.deepcopy(IMPORTANT_DATA.scheduled_maintenance)
            prev_dns_status = copy.deepcopy(IMPORTANT_DATA.dns_status)
            prev_daily_node_status = copy.deepcopy(IMPORTANT_DATA.daily_node_status)
            prev_outbox = copy.deepcopy(IMPORTANT_DATA.outbox)
            prev_fail2ban_cursors = copy.deepcopy(IMPORTANT_DATA.fail2ban_cursors)
            try:
                result = update_fn(IMPORTANT_DATA)
                await IMPORTANT_DATA.save_async(IMPORTANT_DATA_PATH)
            except Exception:
                IMPORTANT_DATA.tickets_seq = prev_tickets_seq
                IMPORTANT_DATA.tickets = prev_tickets
                IMPORTANT_DATA.maintenance = prev_maintenance
                IMPORTANT_DATA.scheduled_maintenance = prev_scheduled_maintenance
                IMPORTANT_DATA.dns_status = prev_dns_status
                IMPORTANT_DATA.daily_node_status = prev_daily_node_status
                IMPORTANT_DATA.outbox = prev_outbox
                IMPORTANT_DATA.fail2ban_cursors = prev_fail2ban_cursors
                raise
            _refresh_important_snapshot()
        except UpdateAborted:
            raise
        except Exception:
            logger.exception("Не удалось обновить important_data")
            raise
        finally:
            file_lock.__exit__(None, None, None)
    return result


def _set_user_meta(cfg: UserData, uid: int, meta: dict[str, Any]) -> dict[str, Any]:
    normalized = UserData._normalize_user(meta)
    normalized["user_id"] = int(uid)
    cfg.authorized_users[str(uid)] = normalized
    return normalized


def _remove_user(cfg: UserData, uid: int) -> dict[str, Any] | None:
    return cfg.authorized_users.pop(str(uid), None)


def _set_maintenance(cfg: ImportantData, payload: dict[str, Any]) -> dict[str, Any]:
    cfg.maintenance = payload
    return payload


def _clear_maintenance(cfg: ImportantData) -> None:
    cfg.maintenance = {}


def _set_scheduled_maintenance(cfg: ImportantData, payload: dict[str, Any]) -> dict[str, Any]:
    cfg.scheduled_maintenance = payload
    return payload


def _clear_scheduled_maintenance(cfg: ImportantData) -> None:
    cfg.scheduled_maintenance = {}


def _set_dns_status(cfg: ImportantData, server_key: str, payload: dict[str, Any]) -> dict[str, Any]:
    cur = dict(getattr(cfg, "dns_status", {}) or {})
    cur[str(server_key)] = dict(payload or {})
    cfg.dns_status = cur
    return dict(cur[str(server_key)])


def _set_daily_node_status(cfg: ImportantData, server_key: str, payload: dict[str, Any]) -> dict[str, Any]:
    cur = dict(getattr(cfg, "daily_node_status", {}) or {})
    cur[str(server_key)] = dict(payload or {})
    cfg.daily_node_status = cur
    return dict(cur[str(server_key)])


def _set_ticket(cfg: ImportantData, ticket_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    cur = dict(getattr(cfg, "tickets", {}) or {})
    cur[str(ticket_id)] = dict(payload or {})
    cfg.tickets = cur
    return dict(cur[str(ticket_id)])


def get_user_meta_copy(uid: int) -> dict[str, Any] | None:
    meta = USER_DATA_SNAPSHOT.get(str(uid))
    return copy.deepcopy(meta) if isinstance(meta, dict) else None


def authorized_users_snapshot() -> dict[str, dict[str, Any]]:
    return copy.deepcopy(USER_DATA_SNAPSHOT)


def service_requests_snapshot() -> dict[str, dict[str, Any]]:
    return copy.deepcopy(USER_SERVICE_REQUESTS_SNAPSHOT)


def product_settings_snapshot() -> dict[str, Any]:
    return copy.deepcopy(USER_PRODUCT_SETTINGS_SNAPSHOT)


def audit_log_snapshot() -> list[dict[str, Any]]:
    return copy.deepcopy(USER_AUDIT_LOG_SNAPSHOT)


def get_owner_meta_copy() -> dict[str, Any] | None:
    for meta in USER_DATA_SNAPSHOT.values():
        if is_owner_meta(meta):
            return copy.deepcopy(meta)
    return None


def get_user_audit_entries(uid: int, *, limit: int = 5) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for item in reversed(USER_AUDIT_LOG_SNAPSHOT):
        try:
            target_uid = int(item.get("target_user_id", 0) or 0)
        except (TypeError, ValueError):
            continue
        if target_uid == int(uid):
            result.append(copy.deepcopy(item))
            if len(result) >= max(1, int(limit)):
                break
    return result


def next_service_request_id(cfg: UserData) -> int:
    cfg.request_seq = max(0, int(cfg.request_seq or 0)) + 1
    return cfg.request_seq


def append_audit_entry(
    cfg: UserData,
    *,
    action: str,
    actor_meta: dict[str, Any] | None,
    target_user_id: int | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    safe_details = copy.deepcopy(details) if isinstance(details, dict) else {}
    for key in tuple(safe_details):
        lowered = str(key).lower()
        if any(secret_part in lowered for secret_part in ("password", "token", "connection", "url")):
            safe_details[key] = "<скрыто>"
    actor_id = (actor_meta or {}).get("user_id")
    real_name = " ".join(
        str(part).strip()
        for part in ((actor_meta or {}).get("first_name"), (actor_meta or {}).get("last_name"))
        if str(part or "").strip()
    )
    username = str((actor_meta or {}).get("username") or "").strip().lstrip("@")
    internal_name = (
        f"{real_name} (@{username})"
        if real_name and username
        else (real_name or (f"@{username}" if username else "система"))
    )
    internal = f"{internal_name}, ID {actor_id}" if actor_id not in (None, "") else internal_name
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "action": str(action or "unknown")[:100],
        "actor_id": actor_id,
        "actor_public": staff_public_signature(actor_meta) if actor_meta else "Система",
        "actor_internal": internal[:240],
        "target_user_id": int(target_user_id) if target_user_id is not None else None,
        "details": safe_details,
    }
    cfg.audit_log = [*list(cfg.audit_log or []), entry][-2000:]
    return copy.deepcopy(entry)


async def upsert_user_meta(uid: int, meta: dict[str, Any]) -> dict[str, Any]:
    return await update_user_data(lambda cfg: _set_user_meta(cfg, uid, meta))


async def mutate_user_meta(uid: int, mutate_fn: Callable[[dict[str, Any]], dict[str, Any]]) -> dict[str, Any] | None:
    """Атомарно изменяет запись пользователя: mutate_fn получает актуальную копию
    meta под локом (защита от last-write-wins при параллельных правках).
    Возвращает обновлённую запись или None, если пользователь не найден."""

    def _apply(cfg: UserData) -> dict[str, Any]:
        cur = cfg.authorized_users.get(str(uid))
        if not isinstance(cur, dict):
            raise UpdateAborted()
        return _set_user_meta(cfg, uid, mutate_fn(dict(cur)))

    try:
        return await update_user_data(_apply)
    except UpdateAborted:
        return None


async def remove_user_meta(uid: int) -> dict[str, Any] | None:
    return await update_user_data(lambda cfg: _remove_user(cfg, uid))


async def set_maintenance_record(payload: dict[str, Any]) -> dict[str, Any]:
    return await update_important_data(lambda cfg: _set_maintenance(cfg, payload))


async def clear_maintenance_record() -> None:
    await update_important_data(lambda cfg: _clear_maintenance(cfg))


def get_active_maintenance() -> dict[str, Any] | None:
    m = IMPORTANT_DATA_SNAPSHOT.get("maintenance")
    if isinstance(m, dict) and m.get("active"):
        return copy.deepcopy(m)
    return None


def get_scheduled_maintenance() -> dict[str, Any] | None:
    m = IMPORTANT_DATA_SNAPSHOT.get("scheduled_maintenance")
    if isinstance(m, dict) and m.get("id"):
        return copy.deepcopy(m)
    return None


def get_ticket_copy(ticket_id: int) -> dict[str, Any] | None:
    tickets = IMPORTANT_DATA_SNAPSHOT.get("tickets")
    if not isinstance(tickets, dict):
        return None
    item = tickets.get(str(ticket_id))
    return copy.deepcopy(item) if isinstance(item, dict) else None


def get_user_open_tickets(uid: int) -> list:
    tickets = IMPORTANT_DATA_SNAPSHOT.get("tickets")
    if not isinstance(tickets, dict):
        return []
    result: list[dict[str, Any]] = []
    for ticket in tickets.values():
        if not isinstance(ticket, dict):
            continue
        try:
            owner_id = int(ticket.get("user_id", 0) or 0)
        except (TypeError, ValueError):
            continue
        if owner_id == uid and str(ticket.get("status", "open")) != "closed":
            result.append(copy.deepcopy(ticket))
    return result


def get_all_tickets_snapshot() -> dict[str, dict[str, Any]]:
    tickets = IMPORTANT_DATA_SNAPSHOT.get("tickets")
    if not isinstance(tickets, dict):
        return {}
    return {k: copy.deepcopy(v) for k, v in tickets.items() if isinstance(v, dict)}


def get_admin_name_by_id(admin_id: int) -> str | None:
    meta = USER_DATA_SNAPSHOT.get(str(admin_id))
    if (
        not isinstance(meta, dict)
        or meta.get("role") != "admin"
        or meta.get("access_state", "approved") != "approved"
        or not bool(meta.get("enabled", True))
    ):
        return None
    return staff_public_signature(meta)


def get_dns_status_cache(server_key: str) -> dict[str, Any] | None:
    dns = IMPORTANT_DATA_SNAPSHOT.get("dns_status")
    if not isinstance(dns, dict):
        return None
    item = dns.get(str(server_key))
    return copy.deepcopy(item) if isinstance(item, dict) else None


async def set_dns_status_cache(server_key: str, payload: dict[str, Any]) -> dict[str, Any]:
    return await update_important_data(lambda cfg: _set_dns_status(cfg, server_key, payload))


def get_daily_node_status_cache(server_key: str) -> dict[str, Any] | None:
    daily = IMPORTANT_DATA_SNAPSHOT.get("daily_node_status")
    if not isinstance(daily, dict):
        return None
    item = daily.get(str(server_key))
    return copy.deepcopy(item) if isinstance(item, dict) else None


def outbox_snapshot() -> list[tuple[str, dict[str, Any]]]:
    events: list[tuple[str, dict[str, Any]]] = []
    events.extend(("user", copy.deepcopy(event)) for event in USER_OUTBOX_SNAPSHOT.values())
    important = IMPORTANT_DATA_SNAPSHOT.get("outbox")
    if isinstance(important, dict):
        events.extend(("important", copy.deepcopy(event)) for event in important.values() if isinstance(event, dict))
    events.sort(key=lambda item: str(item[1].get("created_at") or ""))
    return events


async def mutate_outbox_event(
    source: str,
    event_id: str,
    mutate_fn: Callable[[dict[str, Any]], dict[str, Any] | None],
) -> dict[str, Any] | None:
    def _apply_user(cfg: UserData) -> dict[str, Any] | None:
        current = cfg.outbox.get(event_id)
        if not isinstance(current, dict):
            raise UpdateAborted()
        updated = mutate_fn(copy.deepcopy(current))
        if updated is None:
            cfg.outbox.pop(event_id, None)
            return None
        clean = _normalize_outbox({event_id: updated}).get(event_id)
        if not clean:
            raise ValueError("outbox mutation produced an invalid event")
        cfg.outbox[event_id] = clean
        return copy.deepcopy(clean)

    def _apply_important(cfg: ImportantData) -> dict[str, Any] | None:
        current = cfg.outbox.get(event_id)
        if not isinstance(current, dict):
            raise UpdateAborted()
        updated = mutate_fn(copy.deepcopy(current))
        if updated is None:
            cfg.outbox.pop(event_id, None)
            return None
        clean = _normalize_outbox({event_id: updated}).get(event_id)
        if not clean:
            raise ValueError("outbox mutation produced an invalid event")
        cfg.outbox[event_id] = clean
        return copy.deepcopy(clean)

    try:
        if source == "user":
            return await update_user_data(_apply_user)
        if source == "important":
            return await update_important_data(_apply_important)
        raise ValueError(f"unknown outbox source: {source}")
    except UpdateAborted:
        return None


async def finalize_outbox_event(source: str, event_id: str, *, success: bool) -> None:
    def _finish_user(cfg: UserData) -> None:
        cfg.outbox.pop(event_id, None)

    def _finish_important(cfg: ImportantData) -> None:
        event = cfg.outbox.get(event_id)
        if not isinstance(event, dict):
            return
        completion = event.get("completion")
        if success and isinstance(completion, dict) and completion.get("type") == "fail2ban_cursor":
            server_key = str(completion.get("server_key") or "")
            cursor = completion.get("cursor")
            if server_key and isinstance(cursor, dict):
                cfg.fail2ban_cursors[server_key] = copy.deepcopy(cursor)
        cfg.outbox.pop(event_id, None)

    if source == "user":
        await update_user_data(_finish_user)
    elif source == "important":
        await update_important_data(_finish_important)
    else:
        raise ValueError(f"unknown outbox source: {source}")


def get_fail2ban_cursor(server_key: str) -> dict[str, Any] | None:
    cursors = IMPORTANT_DATA_SNAPSHOT.get("fail2ban_cursors")
    if not isinstance(cursors, dict):
        return None
    cursor = cursors.get(str(server_key))
    return copy.deepcopy(cursor) if isinstance(cursor, dict) else None


async def set_fail2ban_cursor(server_key: str, cursor: dict[str, Any]) -> dict[str, Any]:
    def _set(cfg: ImportantData) -> dict[str, Any]:
        clean = copy.deepcopy(cursor)
        cfg.fail2ban_cursors[str(server_key)] = clean
        return clean

    return await update_important_data(_set)


async def set_daily_node_status_cache(server_key: str, payload: dict[str, Any]) -> dict[str, Any]:
    return await update_important_data(lambda cfg: _set_daily_node_status(cfg, server_key, payload))


async def set_scheduled_maintenance_record(payload: dict[str, Any]) -> dict[str, Any]:
    return await update_important_data(lambda cfg: _set_scheduled_maintenance(cfg, payload))


async def clear_scheduled_maintenance_record() -> None:
    await update_important_data(lambda cfg: _clear_scheduled_maintenance(cfg))


async def next_ticket_seq() -> int:
    def _next_ticket(cfg: ImportantData) -> int:
        cfg.tickets_seq += 1
        return cfg.tickets_seq

    return await update_important_data(_next_ticket)


async def set_ticket_record(ticket_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    return await update_important_data(lambda cfg: _set_ticket(cfg, ticket_id, payload))
