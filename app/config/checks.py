"""Read-only production configuration and split-storage preflight checks."""

from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path


def _nearest_existing_parent(path: Path) -> Path | None:
    current = path
    while not current.exists():
        parent = current.parent
        if parent == current:
            return None
        current = parent
    return current


def _writable_parent(path_value: str, field_name: str) -> list[str]:
    path = Path(path_value)
    parent = path.parent
    existing = _nearest_existing_parent(parent)
    errors: list[str] = []
    if existing is None:
        errors.append(f"{field_name}: не найден существующий родительский каталог для {parent}")
    elif not existing.is_dir():
        errors.append(f"{field_name}: родительский путь не является каталогом: {existing}")
    elif not os.access(existing, os.W_OK | os.X_OK):
        errors.append(f"{field_name}: каталог недоступен для создания или записи: {existing}")
    if path.exists() and not path.is_file():
        errors.append(f"{field_name}: путь не является обычным файлом: {path}")
    elif path.exists() and not os.access(path, os.R_OK | os.W_OK):
        errors.append(f"{field_name}: файл недоступен для чтения и записи: {path}")
    return list(dict.fromkeys(errors))


def _readable_file(path_value: str, field_name: str, *, private: bool = False) -> list[str]:
    path = Path(path_value)
    if not path.is_file():
        return [f"{field_name}: файл не найден: {path}"]
    if not os.access(path, os.R_OK):
        return [f"{field_name}: файл недоступен для чтения: {path}"]
    if private and os.name != "nt":
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode & 0o077:
            return [f"{field_name}: приватный ключ должен иметь права 0600 или строже: {path}"]
    return []


def _check_private_data_permissions(path_value: str, field_name: str) -> list[str]:
    if os.name == "nt":
        return []
    path = Path(path_value)
    errors: list[str] = []
    if path.exists() and path.is_file() and stat.S_IMODE(path.stat().st_mode) & 0o077:
        errors.append(f"{field_name}: файл с данными должен иметь права 0600: {path}")
    parent = path if path.is_dir() else path.parent
    if parent.exists() and stat.S_IMODE(parent.stat().st_mode) & 0o007:
        errors.append(f"{field_name}: каталог данных не должен быть доступен остальным пользователям: {parent}")
    return errors


def _check_json_object(path_value: str, field_name: str) -> list[str]:
    """Validate a JSON object without rewriting it."""
    path = Path(path_value)
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return [f"{field_name}: существующий файл содержит некорректный JSON: {path} ({exc})"]
    if not isinstance(raw, dict):
        return [f"{field_name}: корнем JSON должен быть объект: {path}"]
    return []


def _check_split_storage(data_dir: str) -> list[str]:
    from ..persistence.backend import SplitJsonBackend
    from ..persistence.errors import PersistenceError
    from ..persistence.layout import LAYOUT_FILE, STORE_SPECS

    root = Path(data_dir)
    if not root.exists():
        return [f"DATA_DIR: каталог не найден: {root}"]
    if not root.is_dir():
        return [f"DATA_DIR: путь не является каталогом: {root}"]
    if not os.access(root, os.R_OK | os.W_OK | os.X_OK):
        return [f"DATA_DIR: каталог недоступен для чтения и записи: {root}"]

    backend = SplitJsonBackend(root)
    if not backend.exists():
        return [
            f"DATA_DIR: split-layout v1 не найден ({root / LAYOUT_FILE}); "
            "сначала выполните python -m app.persistence.migration"
        ]
    try:
        snapshot = backend.inspect()
    except (PersistenceError, OSError, UnicodeError, ValueError) as exc:
        return [f"DATA_DIR: split-layout v1 повреждён или требует recovery: {exc}"]

    grants = snapshot.data("access.grants")
    owners = sum(
        1
        for meta in grants.values()
        if isinstance(meta, dict) and meta.get("role") == "admin" and meta.get("admin_level") == "owner"
    )
    errors: list[str] = []
    if owners > 1:
        errors.append("DATA_DIR: найдено несколько руководителей сервиса")
    errors.extend(_check_private_data_permissions(str(root / LAYOUT_FILE), "DATA_DIR"))
    for spec in STORE_SPECS.values():
        errors.extend(_check_private_data_permissions(str(root / spec.relative_path), spec.name))
    return list(dict.fromkeys(errors))


def validate_configuration() -> list[str]:
    # Resolve the public package at call time so diagnostic tests and one-shot
    # deployment checks can safely override settings.
    from . import (
        BOT_TOKEN,
        DATA_DIR,
        INSTANCE_LOCK_PATH,
        PRIVILEGED_HELPER_BIN,
        PTB_PERSISTENCE_PATH,
        SERVERS,
        SSH_IDENTITY_FILE,
        SSH_KNOWN_HOSTS_FILE,
        SSH_STRICT_HOST_KEY_CHECKING,
        SUDO_BIN,
    )
    from .locations import ENV_FILE, SECRETS_ENV_FILE

    errors: list[str] = []
    if ":" not in BOT_TOKEN or len(BOT_TOKEN) < 20:
        errors.append("BOT_TOKEN: значение не похоже на токен Telegram")
    if not SERVERS:
        errors.append("не настроено ни одного сервера")
    for key, server in SERVERS.items():
        if server.mode == "ssh" and not server.ssh_target:
            errors.append(f"SERVERS[{key}]: отсутствует SSH target")

    has_ssh_servers = any(server.mode == "ssh" for server in SERVERS.values())
    if has_ssh_servers:
        if SSH_STRICT_HOST_KEY_CHECKING != "yes":
            errors.append("SSH_STRICT_HOST_KEY_CHECKING должен быть yes для удалённых серверов")
        if not SSH_KNOWN_HOSTS_FILE:
            errors.append("SSH_KNOWN_HOSTS_FILE обязателен для удалённых серверов")
        else:
            errors.extend(_readable_file(SSH_KNOWN_HOSTS_FILE, "SSH_KNOWN_HOSTS_FILE"))
        if not SSH_IDENTITY_FILE:
            errors.append("SSH_IDENTITY_FILE обязателен для удалённых серверов")
        else:
            errors.extend(_readable_file(SSH_IDENTITY_FILE, "SSH_IDENTITY_FILE", private=True))

    if os.name != "nt":
        helper = Path(PRIVILEGED_HELPER_BIN)
        if not helper.is_absolute() or not helper.is_file() or not os.access(helper, os.X_OK):
            errors.append(f"PRIVILEGED_HELPER_BIN: helper не найден или не исполняемый: {helper}")
        if not SUDO_BIN:
            errors.append("sudo не найден, безопасный privileged helper недоступен")

    errors.extend(_check_split_storage(DATA_DIR))
    for value, name in (
        (PTB_PERSISTENCE_PATH, "PTB_PERSISTENCE_PATH"),
        (INSTANCE_LOCK_PATH, "INSTANCE_LOCK_PATH"),
    ):
        errors.extend(_writable_parent(value, name))
    errors.extend(_check_private_data_permissions(PTB_PERSISTENCE_PATH, "PTB_PERSISTENCE_PATH"))

    if os.name != "nt":
        for path, name in ((ENV_FILE, "ENV_PATH"), (SECRETS_ENV_FILE, "SECRETS_ENV_PATH")):
            if path.exists() and stat.S_IMODE(path.stat().st_mode) & 0o007:
                errors.append(f"{name}: env-файл не должен быть доступен остальным пользователям: {path}")
    return list(dict.fromkeys(errors))


def main() -> int:
    try:
        errors = validate_configuration()
    except Exception as exc:
        print(f"Ошибка конфигурации: {exc}", file=sys.stderr)
        return 1
    if errors:
        for error in errors:
            print(f"Ошибка конфигурации: {error}", file=sys.stderr)
        return 1
    print("Конфигурация MaintBot корректна")
    return 0


__all__ = [
    "main",
    "validate_configuration",
]
