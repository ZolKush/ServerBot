from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path


def _writable_parent(path_value: str, field_name: str) -> list[str]:
    path = Path(path_value)
    parent = path.parent
    errors: list[str] = []
    if not parent.exists():
        errors.append(f"{field_name}: каталог не существует: {parent}")
    elif not parent.is_dir():
        errors.append(f"{field_name}: родительский путь не является каталогом: {parent}")
    elif not os.access(parent, os.W_OK | os.X_OK):
        errors.append(f"{field_name}: каталог недоступен для записи: {parent}")
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
    if path.exists() and stat.S_IMODE(path.stat().st_mode) & 0o077:
        errors.append(f"{field_name}: файл с пользовательскими данными должен иметь права 0600: {path}")
    parent = path.parent
    if parent.exists() and stat.S_IMODE(parent.stat().st_mode) & 0o007:
        errors.append(f"{field_name}: каталог данных не должен быть доступен остальным пользователям: {parent}")
    return errors


def _check_json_object(path_value: str, field_name: str) -> list[str]:
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


def _check_state_schema(
    path_value: str,
    field_name: str,
    *,
    supported_version: int,
    check_single_owner: bool = False,
) -> list[str]:
    path = Path(path_value)
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return []  # Подробную синтаксическую ошибку уже вернёт _check_json_object.
    if not isinstance(raw, dict):
        return []
    errors: list[str] = []
    try:
        schema_version = int(raw.get("schema_version", 0) or 0)
    except (TypeError, ValueError, OverflowError):
        schema_version = 0
    if schema_version > supported_version:
        errors.append(
            f"{field_name}: версия схемы {schema_version} новее поддерживаемой {supported_version}; "
            "не запускайте старую версию бота с новыми данными"
        )
    if check_single_owner:
        users = raw.get("authorized_users")
        if isinstance(users, dict):
            owners = sum(
                1
                for meta in users.values()
                if isinstance(meta, dict) and meta.get("role") == "admin" and meta.get("admin_level") == "owner"
            )
            if owners > 1:
                errors.append(f"{field_name}: найдено несколько руководителей сервиса")
    return errors


def validate_configuration() -> list[str]:
    from .config import (
        BOT_TOKEN,
        IMPORTANT_DATA_PATH,
        INSTANCE_LOCK_PATH,
        PRIVILEGED_HELPER_BIN,
        PTB_PERSISTENCE_PATH,
        SERVERS,
        SSH_IDENTITY_FILE,
        SSH_KNOWN_HOSTS_FILE,
        SSH_STRICT_HOST_KEY_CHECKING,
        SUDO_BIN,
        USER_DATA_PATH,
    )
    from .constants import IMPORTANT_DATA_SCHEMA_VERSION, USER_DATA_SCHEMA_VERSION
    from .settings import ENV_FILE, SECRETS_ENV_FILE

    errors: list[str] = []
    if ":" not in BOT_TOKEN or len(BOT_TOKEN) < 20:
        errors.append("BOT_TOKEN: значение не похоже на токен Telegram")
    if Path(USER_DATA_PATH).resolve() == Path(IMPORTANT_DATA_PATH).resolve():
        errors.append("USER_DATA_PATH и IMPORTANT_DATA_PATH должны указывать на разные файлы")
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
    for value, name in (
        (USER_DATA_PATH, "USER_DATA_PATH"),
        (IMPORTANT_DATA_PATH, "IMPORTANT_DATA_PATH"),
        (PTB_PERSISTENCE_PATH, "PTB_PERSISTENCE_PATH"),
        (INSTANCE_LOCK_PATH, "INSTANCE_LOCK_PATH"),
    ):
        errors.extend(_writable_parent(value, name))
    for value, name in (
        (USER_DATA_PATH, "USER_DATA_PATH"),
        (IMPORTANT_DATA_PATH, "IMPORTANT_DATA_PATH"),
        (PTB_PERSISTENCE_PATH, "PTB_PERSISTENCE_PATH"),
    ):
        errors.extend(_check_private_data_permissions(value, name))
    errors.extend(_check_json_object(USER_DATA_PATH, "USER_DATA_PATH"))
    errors.extend(_check_json_object(IMPORTANT_DATA_PATH, "IMPORTANT_DATA_PATH"))
    errors.extend(
        _check_state_schema(
            USER_DATA_PATH,
            "USER_DATA_PATH",
            supported_version=USER_DATA_SCHEMA_VERSION,
            check_single_owner=True,
        )
    )
    errors.extend(
        _check_state_schema(
            IMPORTANT_DATA_PATH,
            "IMPORTANT_DATA_PATH",
            supported_version=IMPORTANT_DATA_SCHEMA_VERSION,
        )
    )
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


if __name__ == "__main__":
    raise SystemExit(main())
