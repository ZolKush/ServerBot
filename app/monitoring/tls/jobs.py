from __future__ import annotations

import asyncio
from typing import Any

from ...config import logger
from .service import evaluate_tls_deadlines, refresh_tls_certificates

TLS_NETWORK_REFRESH_INTERVAL_SEC = 7 * 24 * 60 * 60
TLS_NETWORK_STARTUP_DELAY_SEC = 10
TLS_DEADLINE_EVALUATION_INTERVAL_SEC = 24 * 60 * 60
TLS_DEADLINE_EVALUATION_STARTUP_DELAY_SEC = 60


async def tls_certificate_check_job(context: Any) -> None:
    job_name = str(getattr(getattr(context, "job", None), "name", "") or "")
    source = "startup" if "startup" in job_name else "scheduled"
    try:
        await refresh_tls_certificates(source=source)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Scheduled TLS certificate refresh failed", extra={"action": "tls_refresh_failed"})


async def tls_deadline_evaluation_job(context: Any) -> None:
    del context
    try:
        await evaluate_tls_deadlines()
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception(
            "Scheduled local TLS deadline evaluation failed",
            extra={"action": "tls_deadline_evaluation_failed"},
        )


__all__ = [
    "TLS_DEADLINE_EVALUATION_INTERVAL_SEC",
    "TLS_DEADLINE_EVALUATION_STARTUP_DELAY_SEC",
    "TLS_NETWORK_REFRESH_INTERVAL_SEC",
    "TLS_NETWORK_STARTUP_DELAY_SEC",
    "tls_certificate_check_job",
    "tls_deadline_evaluation_job",
]
