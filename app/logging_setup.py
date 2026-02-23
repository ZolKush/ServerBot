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


def configure_logging() -> None:
    root = logging.getLogger()
    if getattr(root, "_maintbot_configured", False):
        return
    use_json = os.getenv("LOG_JSON", "").strip().lower() in {"1", "true", "yes", "on"}
    level = os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO"
    handler = logging.StreamHandler()
    if use_json:
        handler.setFormatter(JsonLogFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root.handlers[:] = [handler]
    root.setLevel(level)
    root._maintbot_configured = True  # type: ignore[attr-defined]


configure_logging()
logger = logging.getLogger("maint-bot")
