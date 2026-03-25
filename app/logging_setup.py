import json
import logging
import os
from datetime import datetime, timezone
from typing import Any


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        for key in ("user_id", "chat_id", "server_key", "action"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(*, level: str | None = None, use_json: bool | None = None, force: bool = False) -> None:
    root = logging.getLogger()
    if getattr(root, "_maintbot_configured", False) and not force:
        return
    use_json = (
        use_json
        if use_json is not None
        else os.getenv("LOG_JSON", "").strip().lower() in {"1", "true", "yes", "on"}
    )
    level = (level if level is not None else os.getenv("LOG_LEVEL", "INFO")).strip().upper() or "INFO"
    handler = logging.StreamHandler()
    if use_json:
        handler.setFormatter(JsonLogFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root.handlers[:] = [handler]
    try:
        root.setLevel(level)
    except ValueError:
        root.setLevel("INFO")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    root._maintbot_configured = True  # type: ignore[attr-defined]


configure_logging()
logger = logging.getLogger("maint-bot")
