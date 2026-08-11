from __future__ import annotations

import asyncio
import time
from datetime import datetime

import httpx

from ...config import (
    REMNAWAVE_HIDDEN_UUIDS,
    REMNAWAVE_METRICS_CACHE_TTL_SEC,
    REMNAWAVE_METRICS_MAX_BYTES,
    REMNAWAVE_METRICS_PASS,
    REMNAWAVE_METRICS_TIMEOUT_SEC,
    REMNAWAVE_METRICS_URL,
    REMNAWAVE_METRICS_USER,
    TZ,
    logger,
)
from .models import MetricsSnapshot
from .parser import build_nodes, parse_prometheus_text

_CACHE_LOCK: asyncio.Lock | None = None
_FETCH_LOCK: asyncio.Lock | None = None
_CACHED_SNAPSHOT: MetricsSnapshot | None = None
_CACHED_AT_MONOTONIC = 0.0
_HTTP_CLIENT: httpx.AsyncClient | None = None
_LAST_SUCCESS_AT: datetime | None = None


def _get_cache_lock() -> asyncio.Lock:
    global _CACHE_LOCK
    if _CACHE_LOCK is None:
        _CACHE_LOCK = asyncio.Lock()
    return _CACHE_LOCK


def _get_fetch_lock() -> asyncio.Lock:
    global _FETCH_LOCK
    if _FETCH_LOCK is None:
        _FETCH_LOCK = asyncio.Lock()
    return _FETCH_LOCK


async def _fetch_metrics_text() -> str:
    global _HTTP_CLIENT
    if not REMNAWAVE_METRICS_URL:
        raise RuntimeError("REMNAWAVE_METRICS_URL is not configured")
    auth = None
    if REMNAWAVE_METRICS_USER or REMNAWAVE_METRICS_PASS:
        auth = (REMNAWAVE_METRICS_USER, REMNAWAVE_METRICS_PASS)
    if _HTTP_CLIENT is None or _HTTP_CLIENT.is_closed:
        timeout_value = float(REMNAWAVE_METRICS_TIMEOUT_SEC)
        _HTTP_CLIENT = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_value),
            follow_redirects=False,
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=2, keepalive_expiry=30),
        )
    chunks: list[bytes] = []
    total = 0
    async with _HTTP_CLIENT.stream("GET", REMNAWAVE_METRICS_URL, auth=auth) as response:
        response.raise_for_status()
        content_length = response.headers.get("content-length")
        if content_length:
            try:
                if int(content_length) > REMNAWAVE_METRICS_MAX_BYTES:
                    raise RuntimeError("metrics response exceeds configured size limit")
            except ValueError:
                pass
        async for chunk in response.aiter_bytes():
            total += len(chunk)
            if total > REMNAWAVE_METRICS_MAX_BYTES:
                raise RuntimeError("metrics response exceeds configured size limit")
            chunks.append(chunk)
    return b"".join(chunks).decode("utf-8", errors="replace")


async def close_metrics_client() -> None:
    global _HTTP_CLIENT
    if _HTTP_CLIENT is not None and not _HTTP_CLIENT.is_closed:
        await _HTTP_CLIENT.aclose()
    _HTTP_CLIENT = None


async def _do_fetch_and_build_snapshot() -> MetricsSnapshot:
    global _LAST_SUCCESS_AT
    try:
        text = await _fetch_metrics_text()
    except httpx.HTTPStatusError as exc:
        message = f"HTTP {exc.response.status_code}"
        logger.warning("RemnaWave metrics request failed: %s", message)
        return MetricsSnapshot(error=message, fetched_at=_LAST_SUCCESS_AT)
    except (httpx.HTTPError, OSError) as exc:
        message = f"{exc.__class__.__name__}: {str(exc).strip() or 'connection error'}"
        logger.warning("RemnaWave metrics fetch failed: %s", message)
        return MetricsSnapshot(error=message, fetched_at=_LAST_SUCCESS_AT)
    except RuntimeError as exc:
        message = str(exc).strip() or "invalid metrics response"
        logger.warning("RemnaWave metrics response rejected: %s", message)
        return MetricsSnapshot(error=message, fetched_at=_LAST_SUCCESS_AT)
    except Exception as exc:
        message = f"{exc.__class__.__name__}: {str(exc).strip() or 'unknown error'}"
        logger.exception("RemnaWave metrics: unexpected error")
        return MetricsSnapshot(error=message, fetched_at=_LAST_SUCCESS_AT)

    try:
        nodes = build_nodes(parse_prometheus_text(text))
    except Exception as exc:
        message = f"parse error: {exc}"
        logger.exception("RemnaWave metrics parse failed")
        return MetricsSnapshot(error=message, fetched_at=_LAST_SUCCESS_AT)

    if REMNAWAVE_HIDDEN_UUIDS:
        hidden = {uuid for uuid in REMNAWAVE_HIDDEN_UUIDS if uuid}
        nodes = {key: value for key, value in nodes.items() if key not in hidden}

    _LAST_SUCCESS_AT = datetime.now(TZ)
    return MetricsSnapshot(nodes=nodes, fetched_at=_LAST_SUCCESS_AT)


async def get_metrics_snapshot(*, force_refresh: bool = False) -> MetricsSnapshot:
    global _CACHED_SNAPSHOT, _CACHED_AT_MONOTONIC

    ttl = max(1, int(REMNAWAVE_METRICS_CACHE_TTL_SEC))
    async with _get_cache_lock():
        now = time.monotonic()
        if not force_refresh and _CACHED_SNAPSHOT is not None and (now - _CACHED_AT_MONOTONIC) < ttl:
            return _CACHED_SNAPSHOT

    async with _get_fetch_lock():
        async with _get_cache_lock():
            now = time.monotonic()
            if not force_refresh and _CACHED_SNAPSHOT is not None and (now - _CACHED_AT_MONOTONIC) < ttl:
                return _CACHED_SNAPSHOT
        snapshot = await _do_fetch_and_build_snapshot()
        async with _get_cache_lock():
            _CACHED_SNAPSHOT = snapshot
            _CACHED_AT_MONOTONIC = time.monotonic()
        return snapshot


__all__ = ["close_metrics_client", "get_metrics_snapshot"]
