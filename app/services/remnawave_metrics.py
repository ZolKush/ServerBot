import asyncio
import re
import time
from dataclasses import dataclass, field
from datetime import datetime

import httpx

from ..config import (
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


@dataclass(frozen=True)
class NodeMetrics:
    uuid: str
    status: int | None
    online_users: int | None
    uptime_s: float | None
    mem_total: int | None
    mem_free: int | None
    cpu_count: int | None
    network_rx_per_sec: float | None
    network_tx_per_sec: float | None
    node_name: str = ""
    country_emoji: str = ""

    @property
    def is_online(self) -> bool:
        return self.status == 1

    @property
    def mem_used(self) -> int | None:
        if self.mem_total is None or self.mem_free is None:
            return None
        return max(self.mem_total - self.mem_free, 0)


@dataclass
class MetricsSnapshot:
    nodes: dict[str, NodeMetrics] = field(default_factory=dict)
    # Время последнего УСПЕШНОГО получения метрик; None — успехов ещё не было.
    fetched_at: datetime | None = None
    error: str = ""

    @property
    def ok(self) -> bool:
        return not self.error

    def get(self, uuid: str) -> NodeMetrics | None:
        if not uuid:
            return None
        return self.nodes.get(uuid)


_METRIC_LINE_RE = re.compile(r"^([A-Za-z_:][A-Za-z0-9_:]*)(?:\{([^}]*)\})?\s+([^\s]+)\s*$")
_LABEL_RE = re.compile(r'([A-Za-z_][A-Za-z0-9_]*)="((?:[^"\\]|\\.)*)"')

_HOT_METRIC_NAMES = {
    "remnawave_node_status",
    "remnawave_node_online_users",
    "remnawave_node_uptime_seconds",
    "remnawave_node_memory_total_bytes",
    "remnawave_node_memory_free_bytes",
    "remnawave_node_cpu_count",
    "remnawave_node_network_rx_bytes_per_sec",
    "remnawave_node_network_tx_bytes_per_sec",
    "remnawave_node_basic_info",
}


def _unescape_label_value(raw: str) -> str:
    # Prometheus экранирует в значениях лейблов только \\, \" и \n.
    # unicode_escape здесь нельзя: он ломает многобайтные UTF-8 символы (эмодзи).
    out: list[str] = []
    i = 0
    while i < len(raw):
        ch = raw[i]
        if ch == "\\" and i + 1 < len(raw):
            nxt = raw[i + 1]
            if nxt == "n":
                out.append("\n")
                i += 2
                continue
            if nxt in ('"', "\\"):
                out.append(nxt)
                i += 2
                continue
        out.append(ch)
        i += 1
    return "".join(out)


def _parse_labels(raw: str) -> dict[str, str]:
    if not raw:
        return {}
    out: dict[str, str] = {}
    for m in _LABEL_RE.finditer(raw):
        out[m.group(1)] = _unescape_label_value(m.group(2))
    return out


def _parse_value(raw: str) -> float | None:
    s = (raw or "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_prometheus_text(text: str) -> dict[str, dict[str, tuple[dict[str, str], float]]]:
    """
    Парсит текст /metrics. Возвращает:
    { metric_name: { uuid: (labels, value) } } для интересующих нас метрик.
    """
    grouped: dict[str, dict[str, tuple[dict[str, str], float]]] = {}
    for line in (text or "").splitlines():
        if not line or line.startswith("#"):
            continue
        m = _METRIC_LINE_RE.match(line)
        if not m:
            continue
        name = m.group(1)
        if name not in _HOT_METRIC_NAMES:
            continue
        labels = _parse_labels(m.group(2) or "")
        value = _parse_value(m.group(3) or "")
        if value is None:
            continue
        uuid = labels.get("node_uuid", "")
        if not uuid:
            continue
        grouped.setdefault(name, {})[uuid] = (labels, value)
    return grouped


def _build_snapshot(grouped: dict[str, dict[str, tuple[dict[str, str], float]]]) -> dict[str, NodeMetrics]:
    uuids: set[str] = set()
    for per_metric in grouped.values():
        uuids.update(per_metric.keys())

    nodes: dict[str, NodeMetrics] = {}

    def _int(metric_name: str, uuid: str) -> int | None:
        rec = grouped.get(metric_name, {}).get(uuid)
        return int(rec[1]) if rec else None

    def _float(metric_name: str, uuid: str) -> float | None:
        rec = grouped.get(metric_name, {}).get(uuid)
        return float(rec[1]) if rec else None

    for uuid in uuids:
        info = grouped.get("remnawave_node_basic_info", {}).get(uuid)
        info_labels = info[0] if info else {}
        node_name = (info_labels.get("node_name") or "").strip()
        country_emoji = (info_labels.get("node_country_emoji") or "").strip()

        nodes[uuid] = NodeMetrics(
            uuid=uuid,
            status=_int("remnawave_node_status", uuid),
            online_users=_int("remnawave_node_online_users", uuid),
            uptime_s=_float("remnawave_node_uptime_seconds", uuid),
            mem_total=_int("remnawave_node_memory_total_bytes", uuid),
            mem_free=_int("remnawave_node_memory_free_bytes", uuid),
            cpu_count=_int("remnawave_node_cpu_count", uuid),
            network_rx_per_sec=_float("remnawave_node_network_rx_bytes_per_sec", uuid),
            network_tx_per_sec=_float("remnawave_node_network_tx_bytes_per_sec", uuid),
            node_name=node_name,
            country_emoji=country_emoji,
        )
    return nodes


_CACHE_LOCK: asyncio.Lock | None = None
_FETCH_LOCK: asyncio.Lock | None = None
_CACHED_SNAPSHOT: MetricsSnapshot | None = None
_CACHED_AT_MONOTONIC: float = 0.0
_HTTP_CLIENT: httpx.AsyncClient | None = None


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


_LAST_SUCCESS_AT: datetime | None = None


async def _do_fetch_and_build_snapshot() -> MetricsSnapshot:
    global _LAST_SUCCESS_AT
    try:
        text = await _fetch_metrics_text()
    except httpx.HTTPStatusError as e:
        msg = f"HTTP {e.response.status_code}"
        logger.warning("RemnaWave metrics: %s for %s", msg, REMNAWAVE_METRICS_URL)
        return MetricsSnapshot(error=msg, fetched_at=_LAST_SUCCESS_AT)
    except (httpx.HTTPError, OSError) as e:
        msg = f"{e.__class__.__name__}: {str(e).strip() or 'connection error'}"
        logger.warning("RemnaWave metrics fetch failed: %s", msg)
        return MetricsSnapshot(error=msg, fetched_at=_LAST_SUCCESS_AT)
    except RuntimeError as e:
        msg = str(e).strip() or "invalid metrics response"
        logger.warning("RemnaWave metrics response rejected: %s", msg)
        return MetricsSnapshot(error=msg, fetched_at=_LAST_SUCCESS_AT)
    except Exception as e:
        msg = f"{e.__class__.__name__}: {str(e).strip() or 'unknown error'}"
        logger.exception("RemnaWave metrics: unexpected error")
        return MetricsSnapshot(error=msg, fetched_at=_LAST_SUCCESS_AT)

    try:
        grouped = parse_prometheus_text(text)
        nodes = _build_snapshot(grouped)
    except Exception as e:
        msg = f"parse error: {e}"
        logger.exception("RemnaWave metrics parse failed")
        return MetricsSnapshot(error=msg, fetched_at=_LAST_SUCCESS_AT)

    if REMNAWAVE_HIDDEN_UUIDS:
        hidden = {u for u in REMNAWAVE_HIDDEN_UUIDS if u}
        nodes = {k: v for k, v in nodes.items() if k not in hidden}

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
        snap = await _do_fetch_and_build_snapshot()
        async with _get_cache_lock():
            _CACHED_SNAPSHOT = snap
            _CACHED_AT_MONOTONIC = time.monotonic()
        return snap


def format_uptime_seconds(seconds: float | None) -> str:
    if seconds is None:
        return "н/д"
    s = int(seconds)
    days, rem = divmod(s, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60
    parts: list[str] = []
    if days:
        parts.append(f"{days} д")
    if hours:
        parts.append(f"{hours} ч")
    if minutes or not parts:
        parts.append(f"{minutes} м")
    return " ".join(parts)


def format_memory_bytes(used: int | None, total: int | None) -> str:
    if used is None or total is None or total <= 0:
        return "н/д"
    used_mib = int(round(used / (1024 * 1024)))
    total_mib = int(round(total / (1024 * 1024)))
    return f"{used_mib} / {total_mib} MiB"
