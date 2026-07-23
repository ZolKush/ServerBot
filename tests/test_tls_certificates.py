from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from app import storage
from app.monitoring.tls import checks as tls_checks
from app.monitoring.tls import service as tls_certificates


def _certificate_der(domain: str, *, not_before: datetime, not_after: datetime) -> bytes:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, domain)])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(domain)]), critical=False)
        .sign(key, hashes.SHA256())
    )
    return certificate.public_bytes(serialization.Encoding.DER)


def test_configured_tls_endpoints_are_normalized_and_deduplicated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        tls_certificates,
        "SERVERS",
        {
            "main": SimpleNamespace(check_a_domains=["VPN.Example.COM.", "shared.example.com"]),
            "edge": SimpleNamespace(check_a_domains=["vpn.example.com", "shared.example.com"]),
        },
    )

    assert tls_certificates.configured_tls_endpoints() == {
        ("vpn.example.com", 443): ["main", "edge"],
        ("shared.example.com", 443): ["main", "edge"],
    }


@pytest.mark.asyncio
async def test_tls_endpoint_distinguishes_expiring_and_not_yet_valid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)

    async def _trusted(domain: str, port: int) -> tuple[bool, None]:
        return True, None

    monkeypatch.setattr(tls_checks, "_verify_certificate_trust", _trusted)
    expiring_der = _certificate_der(
        "vpn.example.com",
        not_before=now - timedelta(days=1),
        not_after=now + timedelta(days=2),
    )

    async def _fetch_expiring(domain: str, port: int) -> bytes:
        return expiring_der

    monkeypatch.setattr(tls_checks, "_fetch_der_certificate", _fetch_expiring)
    expiring = await tls_checks.check_tls_endpoint("vpn.example.com", 443, ["main"])

    assert expiring["status"] == "expiring"
    assert expiring["hostname_valid"] is True
    assert expiring["trust_valid"] is True

    future_der = _certificate_der(
        "vpn.example.com",
        not_before=now + timedelta(days=1),
        not_after=now + timedelta(days=10),
    )

    async def _fetch_future(domain: str, port: int) -> bytes:
        return future_der

    monkeypatch.setattr(tls_checks, "_fetch_der_certificate", _fetch_future)
    future = await tls_checks.check_tls_endpoint("vpn.example.com", 443, ["main"])

    assert future["status"] == "invalid"
    assert "ещё не начался" in future["error"]


@pytest.mark.asyncio
async def test_tls_alerts_are_sent_once_per_certificate_and_threshold(
    isolated_storage: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _seed_users(cfg: storage.UserData) -> None:
        cfg.authorized_users = {
            "1": storage.UserData._normalize_user(
                {"user_id": 1, "role": "admin", "admin_level": "owner", "access_state": "approved"}
            ),
            "2": storage.UserData._normalize_user(
                {"user_id": 2, "role": "admin", "admin_level": "admin", "access_state": "approved"}
            ),
            "3": storage.UserData._normalize_user({"user_id": 3, "role": "user", "access_state": "approved"}),
        }

    await storage.update_user_data(_seed_users)
    monkeypatch.setattr(
        tls_certificates,
        "configured_tls_endpoints",
        lambda: {("vpn.example.com", 443): ["main"]},
    )
    state = {"status": "expiring", "remaining": 2 * 86400}

    async def _check(domain: str, port: int, servers: list[str]) -> dict:
        now = datetime.now(timezone.utc)
        return {
            "domain": domain,
            "port": port,
            "servers": servers,
            "checked_at": now.isoformat(),
            "status": state["status"],
            "not_before": (now - timedelta(days=80)).isoformat(),
            "not_after": (now + timedelta(seconds=state["remaining"])).isoformat(),
            "fingerprint": "same-certificate",
            "issuer": "CN=Test CA",
            "error": None,
            "hostname_valid": True,
            "trust_valid": True,
            "remaining_seconds": state["remaining"],
        }

    monkeypatch.setattr(tls_certificates, "check_tls_endpoint", _check)

    await tls_certificates.refresh_tls_certificates()
    await tls_certificates.refresh_tls_certificates()
    state.update(status="expired", remaining=-60)
    await tls_certificates.refresh_tls_certificates()
    await tls_certificates.refresh_tls_certificates()

    events = [event for _, event in storage.outbox_snapshot()]
    assert [event["kind"] for event in events].count("tls_certificate_expiring") == 1
    assert [event["kind"] for event in events].count("tls_certificate_expired") == 1
    assert all(set(event["recipients"]) == {"1", "2"} for event in events)
    assert all(event["payload"]["text"].count("‼️") >= 3 for event in events)
    persisted = storage.tls_certificates_snapshot()["vpn.example.com:443"]
    assert persisted["status"] == "expired"
    assert persisted["notified_levels"] == ["expiring", "expired"]
    assert tls_certificates.tls_snapshot_for_server("main")[0]["domain"] == "vpn.example.com"
