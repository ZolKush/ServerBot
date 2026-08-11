from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import ssl
from datetime import datetime, timedelta, timezone
from typing import Any

from cryptography import x509
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import hashes
from cryptography.x509.oid import NameOID

from ...config import logger

TLS_PORT = 443
TLS_CHECK_TIMEOUT_SEC = 10
TLS_EXPIRY_WARNING = timedelta(days=3)


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
    result: dict[str, Any] = {
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
        "failure_kind": "",
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
        result.update(
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
    except (TimeoutError, OSError, ssl.SSLError) as exc:
        result["failure_kind"] = "transport"
        result["error"] = f"{exc.__class__.__name__}: {exc}"[:1000]
    except (ValueError, UnsupportedAlgorithm) as exc:
        result["failure_kind"] = "certificate"
        result["error"] = f"{exc.__class__.__name__}: {exc}"[:1000]
    except Exception as exc:
        logger.exception("Unexpected TLS certificate check error domain=%s port=%s", domain, port)
        result["failure_kind"] = "internal"
        result["error"] = f"{exc.__class__.__name__}: {exc}"[:1000]
    return result


__all__ = [
    "TLS_CHECK_TIMEOUT_SEC",
    "TLS_EXPIRY_WARNING",
    "TLS_PORT",
    "check_tls_endpoint",
]
