from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

_REDACTED = "[REDACTED]"


def _secret_values(values: Iterable[str] | None) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(value) for value in values or () if value))


def _redact(text: str, secrets: tuple[str, ...]) -> str:
    result = text
    for secret in secrets:
        result = result.replace(secret, _REDACTED)
    return result


class SecretRedactingFormatter(logging.Formatter):
    """Plain-text formatter that removes known runtime secrets last."""

    def __init__(self, fmt: str, *, secrets: Iterable[str] | None = None) -> None:
        super().__init__(fmt)
        self._secrets = _secret_values(secrets)

    def format(self, record: logging.LogRecord) -> str:
        return _redact(super().format(record), self._secrets)


class JsonLogFormatter(logging.Formatter):
    def __init__(self, *, secrets: Iterable[str] | None = None) -> None:
        super().__init__()
        self._secrets = _secret_values(secrets)

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": _redact(record.getMessage(), self._secrets),
        }
        if record.exc_info:
            payload["exc"] = _redact(self.formatException(record.exc_info), self._secrets)
        for key in (
            "user_id",
            "chat_id",
            "server_key",
            "action",
            "source",
            "duration_ms",
            "total",
            "ok",
            "problems",
            "cache_age_sec",
            "primary_port",
            "effective_port",
        ):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(
    *,
    level: str | None = None,
    use_json: bool | None = None,
    force: bool = False,
    secrets: Iterable[str] | None = None,
) -> None:
    root = logging.getLogger()
    if getattr(root, "_maintbot_configured", False) and not force:
        return
    use_json = (
        use_json if use_json is not None else os.getenv("LOG_JSON", "").strip().lower() in {"1", "true", "yes", "on"}
    )
    level = (level if level is not None else os.getenv("LOG_LEVEL", "INFO")).strip().upper() or "INFO"
    handler = logging.StreamHandler()
    if use_json:
        handler.setFormatter(JsonLogFormatter(secrets=secrets))
    else:
        handler.setFormatter(
            SecretRedactingFormatter(
                "%(asctime)s %(levelname)s %(name)s: %(message)s",
                secrets=secrets,
            )
        )
    root.handlers[:] = [handler]
    try:
        root.setLevel(level)
    except ValueError:
        root.setLevel("INFO")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    root._maintbot_configured = True  # type: ignore[attr-defined]


logger = logging.getLogger("maint-bot")

__all__ = [
    "JsonLogFormatter",
    "SecretRedactingFormatter",
    "configure_logging",
    "logger",
]
