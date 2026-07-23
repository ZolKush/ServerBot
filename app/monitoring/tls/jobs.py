from __future__ import annotations

import asyncio
from typing import Any

from ...config import logger
from .service import refresh_tls_certificates


async def tls_certificate_check_job(context: Any) -> None:
    del context
    try:
        await refresh_tls_certificates()
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Scheduled TLS certificate refresh failed", extra={"action": "tls_refresh_failed"})


__all__ = ["tls_certificate_check_job"]
