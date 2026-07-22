from __future__ import annotations

import asyncio
import contextlib
import html
import ipaddress
import ssl
from datetime import datetime, timedelta, timezone
from typing import Any

from cryptography import x509
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import hashes
from cryptography.x509.oid import NameOID

from ..config import SERVERS, logger
from ..storage import (
    ImportantData,
    authorized_users_snapshot,
    enqueue_important_outbox,
    make_outbox_event,
    tls_certificates_snapshot,
    update_important_data,
)
from .outbox import message_payload

TLS_PORT = 443
TLS_CHECK_TIMEOUT_SEC = 10
TLS_EXPIRY_WARNING = timedelta(days=3)
TLS_CHECK_CONCURRENCY = 6


def configured_tls_endpoints() -> dict[tuple[str, int], list[str]]:
    endpoints: dict[tuple[str, int], list[str]] = {}
    for server_key, server in SERVERS.items():
        for raw_domain in server.check_a_domains:
            domain = str(raw_domain or "").strip().lower().rstrip(".")
            if not domain:
                continue
            try:
                ascii_domain = domain.encode("idna").decode("ascii")
            except UnicodeError:
                logger.warning("TLS certificate check skipped invalid domain=%s", domain)
                continue
            endpoint = (ascii_domain, TLS_PORT)
            endpoints.setdefault(endpoint, []).append(server_key)
    return {endpoint: list(dict.fromkeys(server_keys)) for endpoint, server_keys in endpoints.items()}


async def _open_tls(domain: str, port: int, context: ssl.SSLContext) -> bytes | None:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(domain, port, ssl=context, server_hostname=domain),
        timeout=TLS_CHECK_TIMEOUT_SEC,
    )
    del reader
    try:
        ssl_object = writer.get_extra_info("ssl_object")
        if ssl_object is None:
            raise RuntimeError("TLS handshake completed without an SSL object")
        return ssl_object.getpeercert(binary_form=True)
    finally:
        writer.close()
        with contextlib.suppress(TimeoutError, OSError, ssl.SSLError):
            await asyncio.wait_for(writer.wait_closed(), timeout=2)


async def _fetch_der_certificate(domain: str, port: int) -> bytes:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    certificate = await _open_tls(domain, port, context)
    if not certificate:
        raise RuntimeError("server did not return a certificate")
    return certificate


async def _verify_certificate_trust(domain: str, port: int) -> tuple[bool, str | None]:
    context = ssl.create_default_context()
    context.check_hostname = False
    try:
        await _open_tls(domain, port, context)
    except ssl.SSLCertVerificationError as exc:
        return False, str(exc.verify_message or exc)
    except (TimeoutError, OSError, ssl.SSLError) as exc:
        return False, str(exc)
    return True, None


def _dns_pattern_matches(pattern: str, hostname: str) -> bool:
    pattern = pattern.strip().lower().rstrip(".")
    hostname = hostname.strip().lower().rstrip(".")
    if not pattern or not hostname:
        return False
    if "*" not in pattern:
        return pattern == hostname
    if not pattern.startswith("*.") or pattern.count("*") != 1:
        return False
    suffix = pattern[2:]
    host_labels = hostname.split(".")
    suffix_labels = suffix.split(".")
    return len(host_labels) == len(suffix_labels) + 1 and host_labels[1:] == suffix_labels


def _hostname_matches(certificate: x509.Certificate, domain: str) -> bool:
    try:
        expected_ip = ipaddress.ip_address(domain)
    except ValueError:
        expected_ip = None

    try:
        san = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    except x509.ExtensionNotFound:
        san = None
    if san is not None:
        if expected_ip is not None:
            return expected_ip in san.get_values_for_type(x509.IPAddress)
        dns_names = san.get_values_for_type(x509.DNSName)
        if dns_names:
            return any(isinstance(name, str) and _dns_pattern_matches(name, domain) for name in dns_names)

    if expected_ip is not None:
        return False
    common_names = certificate.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
    for attribute in common_names:
        value = attribute.value
        if isinstance(value, str) and _dns_pattern_matches(value, domain):
            return True
    return False


def _certificate_time(certificate: x509.Certificate, attribute: str) -> datetime:
    value = getattr(certificate, f"{attribute}_utc", None)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    legacy = getattr(certificate, attribute)
    return legacy.replace(tzinfo=timezone.utc) if legacy.tzinfo is None else legacy.astimezone(timezone.utc)


async def check_tls_endpoint(domain: str, port: int, server_keys: list[str]) -> dict[str, Any]:
    checked_at = datetime.now(timezone.utc)
    base: dict[str, Any] = {
        "domain": domain,
        "port": port,
        "servers": list(server_keys),
        "checked_at": checked_at.isoformat(),
        "status": "error",
        "not_before": None,
        "not_after": None,
        "fingerprint": None,
        "issuer": None,
        "error": None,
        "hostname_valid": False,
        "trust_valid": False,
        "remaining_seconds": 0,
    }
    try:
        certificate_der = await _fetch_der_certificate(domain, port)
        certificate = x509.load_der_x509_certificate(certificate_der)
        not_before = _certificate_time(certificate, "not_valid_before")
        not_after = _certificate_time(certificate, "not_valid_after")
        remaining = not_after - checked_at
        trust_valid, trust_error = await _verify_certificate_trust(domain, port)
        hostname_valid = _hostname_matches(certificate, domain)
        if remaining <= timedelta(0):
            status = "expired"
        elif checked_at < not_before or not hostname_valid or not trust_valid:
            status = "invalid"
        elif remaining <= TLS_EXPIRY_WARNING:
            status = "expiring"
        else:
            status = "ok"
        errors: list[str] = []
        if checked_at < not_before:
            errors.append("срок действия сертификата ещё не начался")
        if not hostname_valid:
            errors.append("имя домена отсутствует в сертификате")
        if trust_error:
            errors.append(f"проверка цепочки: {trust_error}")
        base.update(
            {
                "status": status,
                "not_before": not_before.isoformat(),
                "not_after": not_after.isoformat(),
                "fingerprint": certificate.fingerprint(hashes.SHA256()).hex(),
                "issuer": certificate.issuer.rfc4514_string(),
                "error": "; ".join(errors) or None,
                "hostname_valid": hostname_valid,
                "trust_valid": trust_valid,
                "remaining_seconds": int(remaining.total_seconds()),
            }
        )
    except (TimeoutError, OSError, ssl.SSLError, ValueError, UnsupportedAlgorithm) as exc:
        base["error"] = f"{exc.__class__.__name__}: {exc}"[:1000]
    except Exception as exc:
        logger.exception("Unexpected TLS certificate check error domain=%s port=%s", domain, port)
        base["error"] = f"{exc.__class__.__name__}: {exc}"[:1000]
    return base


def _approved_admin_ids() -> list[int]:
    result: list[int] = []
    for key, meta in authorized_users_snapshot().items():
        if meta.get("role") != "admin" or meta.get("access_state") != "approved" or not bool(meta.get("enabled", True)):
            continue
        try:
            result.append(int(meta.get("user_id", key)))
        except (TypeError, ValueError, OverflowError):
            continue
    return sorted(set(uid for uid in result if uid > 0))


def _alert_text(item: dict[str, Any], level: str) -> str:
    domain = str(item.get("domain") or "-")
    port = int(item.get("port", TLS_PORT) or TLS_PORT)
    not_after = str(item.get("not_after") or "-")
    try:
        end = datetime.fromisoformat(not_after).astimezone(timezone.utc)
        end_text = end.strftime("%d.%m.%Y %H:%M UTC")
    except (TypeError, ValueError):
        end_text = not_after
    if level == "expired":
        heading = "‼️‼️‼️ СЕРТИФИКАТ ПРОСРОЧЕН ‼️‼️‼️"
        detail = "TLS-сертификат уже недействителен. Требуется немедленное обновление."
    else:
        heading = "‼️‼️‼️ СЕРТИФИКАТ ИСТЕКАЕТ ‼️‼️‼️"
        remaining_hours = max(0, int(item.get("remaining_seconds", 0) or 0) // 3600)
        detail = f"До окончания TLS-сертификата осталось не более 3 суток ({remaining_hours} ч.)."
    servers = ", ".join(str(server) for server in item.get("servers", []) if str(server).strip()) or "-"
    return (
        f"<b>{heading}</b>\n\n"
        f"{detail}\n\n"
        f"• Домен: <code>{html.escape(domain)}:{port}</code>\n"
        f"• Серверы: <code>{html.escape(servers)}</code>\n"
        f"• Действителен до: <code>{html.escape(end_text)}</code>"
    )


async def refresh_tls_certificates() -> dict[str, dict[str, Any]]:
    endpoints = configured_tls_endpoints()
    semaphore = asyncio.Semaphore(TLS_CHECK_CONCURRENCY)

    async def _check(endpoint: tuple[str, int], server_keys: list[str]) -> tuple[str, dict[str, Any]]:
        domain, port = endpoint
        async with semaphore:
            result = await check_tls_endpoint(domain, port, server_keys)
        return f"{domain}:{port}", result

    checked = await asyncio.gather(*(_check(endpoint, keys) for endpoint, keys in endpoints.items()))
    fresh = dict(checked)
    admin_ids = _approved_admin_ids()

    def _save(cfg: ImportantData) -> dict[str, dict[str, Any]]:
        previous = dict(cfg.tls_certificates or {})
        updated: dict[str, dict[str, Any]] = {}
        for key, raw_item in fresh.items():
            item = dict(raw_item)
            old_value = previous.get(key)
            old: dict[str, Any] = dict(old_value) if isinstance(old_value, dict) else {}
            fingerprint = str(item.get("fingerprint") or "")
            old_notified_fingerprint = str(old.get("notified_fingerprint") or "")
            if fingerprint and fingerprint == old_notified_fingerprint:
                notified_levels = [
                    str(level) for level in old.get("notified_levels", []) if str(level) in {"expiring", "expired"}
                ]
            elif not fingerprint:
                item["notified_fingerprint"] = old.get("notified_fingerprint")
                notified_levels = list(old.get("notified_levels", []) or [])
            else:
                notified_levels = []

            remaining_seconds = int(item.get("remaining_seconds", 0) or 0)
            has_expiry = bool(item.get("not_after"))
            level = (
                "expired"
                if has_expiry and remaining_seconds <= 0
                else ("expiring" if has_expiry and remaining_seconds <= int(TLS_EXPIRY_WARNING.total_seconds()) else "")
            )
            if level in {"expiring", "expired"} and level not in notified_levels and admin_ids and fingerprint:
                enqueue_important_outbox(
                    cfg,
                    make_outbox_event(
                        kind=f"tls_certificate_{level}",
                        recipient_ids=admin_ids,
                        payload=message_payload(_alert_text(item, level)),
                    ),
                )
                notified_levels.append(level)
                item["notified_fingerprint"] = fingerprint
            elif fingerprint:
                item["notified_fingerprint"] = (
                    old_notified_fingerprint if fingerprint == old_notified_fingerprint else None
                )
            item["notified_levels"] = list(dict.fromkeys(notified_levels))
            updated[key] = item
        cfg.tls_certificates = updated
        return {key: dict(value) for key, value in updated.items()}

    result = await update_important_data(_save)
    counts: dict[str, int] = {}
    for item in result.values():
        status = str(item.get("status") or "error")
        counts[status] = counts.get(status, 0) + 1
    logger.info(
        "TLS certificates refreshed: total=%s statuses=%s", len(result), counts, extra={"action": "tls_refresh"}
    )
    return result


async def tls_certificate_check_job(context) -> None:
    del context
    try:
        await refresh_tls_certificates()
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Scheduled TLS certificate refresh failed", extra={"action": "tls_refresh_failed"})


def tls_snapshot_for_server(server_key: str) -> list[dict[str, Any]]:
    items = [
        item
        for item in tls_certificates_snapshot().values()
        if server_key in [str(value) for value in item.get("servers", [])]
    ]
    return sorted(items, key=lambda item: (str(item.get("status") or ""), str(item.get("domain") or "")))


__all__ = [
    "TLS_CHECK_TIMEOUT_SEC",
    "TLS_EXPIRY_WARNING",
    "check_tls_endpoint",
    "configured_tls_endpoints",
    "refresh_tls_certificates",
    "tls_certificate_check_job",
    "tls_snapshot_for_server",
]
