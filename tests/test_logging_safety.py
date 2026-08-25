from __future__ import annotations

import json
import logging
import sys

from app.runtime.logging import JsonLogFormatter, SecretRedactingFormatter


def _record_with_secret(secret: str) -> logging.LogRecord:
    try:
        raise RuntimeError(f"failed with {secret}")
    except RuntimeError:
        return logging.LogRecord(
            name="maint-bot",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="request token=%s",
            args=(secret,),
            exc_info=sys.exc_info(),
        )


def test_plain_logging_redacts_secrets_from_message_and_exception() -> None:
    secret = "123456:SECRET_RUNTIME_TOKEN"
    rendered = SecretRedactingFormatter("%(message)s %(exc_text)s", secrets=[secret]).format(
        _record_with_secret(secret)
    )

    assert secret not in rendered
    assert rendered.count("[REDACTED]") >= 2


def test_json_logging_redacts_secrets_from_message_and_exception() -> None:
    secret = "very-sensitive-password"
    payload = json.loads(JsonLogFormatter(secrets=[secret]).format(_record_with_secret(secret)))

    assert secret not in payload["msg"]
    assert secret not in payload["exc"]
    assert "[REDACTED]" in payload["msg"]
    assert "[REDACTED]" in payload["exc"]
