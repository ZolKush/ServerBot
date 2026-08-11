from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from app import storage
from app.config.servers import ServerTarget, TLSEndpoint
from app.monitoring.tls import checks as tls_checks
from app.monitoring.tls import policy as tls_policy
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
        tls_policy,
        "SERVERS",
        {
            "main": ServerTarget(
                key="main",
                label="Main",
                flag="",
                mode="local",
                expected_a_ip="",
                check_a_domains=[],
                monitor_containers=[],
                fail2ban_log_path="/var/log/fail2ban.log",
                tls_endpoints=(
                    TLSEndpoint("vpn.example.com", 443, (8443,)),
                    TLSEndpoint("shared.example.com"),
                ),
            ),
            "edge": ServerTarget(
                key="edge",
                label="Edge",
                flag="",
                mode="ssh",
                expected_a_ip="",
                check_a_domains=[],
                monitor_containers=[],
                fail2ban_log_path="/var/log/fail2ban.log",
                ssh_target="maintbot@example.com",
                tls_endpoints=(TLSEndpoint("vpn.example.com"), TLSEndpoint("shared.example.com")),
            ),
        },
    )

    assert tls_certificates.configured_tls_endpoints() == [
        tls_certificates.ConfiguredTLSEndpoint("shared.example.com", 443, (), ("main", "edge")),
        tls_certificates.ConfiguredTLSEndpoint("vpn.example.com", 443, (8443,), ("main", "edge")),
    ]


@pytest.mark.asyncio
async def test_tls_fallback_is_used_only_after_transport_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []

    async def _check(domain: str, port: int, servers: list[str]) -> dict:
        calls.append(port)
        if port == 443:
            return {
                "domain": domain,
                "port": port,
                "servers": servers,
                "status": "error",
                "failure_kind": "transport",
                "error": "SSLError: TLSV1_UNRECOGNIZED_NAME",
            }
        return {
            "domain": domain,
            "port": port,
            "servers": servers,
            "status": "ok",
            "failure_kind": "",
            "error": None,
        }

    monkeypatch.setattr(tls_policy, "check_tls_endpoint", _check)
    target = tls_certificates.ConfiguredTLSEndpoint("example.com", 443, (8443,), ("nl",))

    result = await tls_certificates.check_tls_with_fallback(target)

    assert calls == [443, 8443]
    assert result["primary_port"] == 443
    assert result["effective_port"] == 8443
    assert result["used_fallback"] is True
    assert len(result["attempt_errors"]) == 1


@pytest.mark.asyncio
async def test_tls_fallback_never_masks_invalid_certificate(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []

    async def _check(domain: str, port: int, servers: list[str]) -> dict:
        calls.append(port)
        return {
            "domain": domain,
            "port": port,
            "servers": servers,
            "status": "invalid",
            "failure_kind": "",
            "error": "certificate hostname mismatch",
        }

    monkeypatch.setattr(tls_policy, "check_tls_endpoint", _check)
    target = tls_certificates.ConfiguredTLSEndpoint("example.com", 443, (8443,), ("nl",))

    result = await tls_certificates.check_tls_with_fallback(target)

    assert calls == [443]
    assert result["status"] == "invalid"
    assert result["effective_port"] == 443
    assert result["used_fallback"] is False


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
        lambda: [tls_certificates.ConfiguredTLSEndpoint("vpn.example.com", 443, (), ("main",))],
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
            "failure_kind": "",
        }

    monkeypatch.setattr(tls_policy, "check_tls_endpoint", _check)

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


@pytest.mark.asyncio
async def test_transient_tls_failure_preserves_last_good_certificate(
    isolated_storage: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tls_certificates.ConfiguredTLSEndpoint("vpn.example.com", 443, (), ("main",))
    monkeypatch.setattr(tls_certificates, "configured_tls_endpoints", lambda: [target])
    now = datetime.now(timezone.utc)
    state = {"failure": False}

    async def _check(domain: str, port: int, servers: list[str]) -> dict:
        if state["failure"]:
            return {
                "domain": domain,
                "port": port,
                "servers": servers,
                "checked_at": (now + timedelta(hours=1)).isoformat(),
                "status": "error",
                "failure_kind": "transport",
                "error": "TimeoutError: timed out",
            }
        return {
            "domain": domain,
            "port": port,
            "servers": servers,
            "checked_at": now.isoformat(),
            "status": "ok",
            "not_before": (now - timedelta(days=1)).isoformat(),
            "not_after": (now + timedelta(days=60)).isoformat(),
            "fingerprint": "last-good",
            "issuer": "CN=Test CA",
            "error": None,
            "hostname_valid": True,
            "trust_valid": True,
            "remaining_seconds": 60 * 86400,
            "failure_kind": "",
        }

    monkeypatch.setattr(tls_policy, "check_tls_endpoint", _check)
    first = await tls_certificates.refresh_tls_certificates(source="startup")
    state["failure"] = True
    second = await tls_certificates.refresh_tls_certificates(source="scheduled")

    assert first["vpn.example.com:443"]["status"] == "ok"
    failed = second["vpn.example.com:443"]
    assert failed["status"] == "error"
    assert failed["fingerprint"] == "last-good"
    assert failed["not_after"] == (now + timedelta(days=60)).isoformat()
    assert failed["last_success_at"] == now.isoformat()
    assert failed["last_attempt_at"] == (now + timedelta(hours=1)).isoformat()


@pytest.mark.asyncio
async def test_local_deadline_evaluation_never_opens_tls_connection(
    isolated_storage: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    await storage.set_tls_certificates_snapshot(
        {
            "vpn.example.com:443": {
                "domain": "vpn.example.com",
                "port": 443,
                "primary_port": 443,
                "servers": ["main"],
                "status": "ok",
                "not_after": (now + timedelta(days=2)).isoformat(),
                "fingerprint": "persisted",
                "hostname_valid": True,
                "trust_valid": True,
            }
        }
    )

    async def _unexpected_network(*_args, **_kwargs):
        raise AssertionError("local deadline evaluation must not use the network")

    monkeypatch.setattr(tls_policy, "check_tls_endpoint", _unexpected_network)

    result = await tls_certificates.evaluate_tls_deadlines(source="test-local")

    item = result["vpn.example.com:443"]
    assert item["status"] == "expiring"
    assert 0 < item["remaining_seconds"] <= 2 * 86400


@pytest.mark.asyncio
async def test_tls_logs_only_final_endpoint_problem_after_fallbacks(
    isolated_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    target = tls_certificates.ConfiguredTLSEndpoint("zeronet.example.com", 443, (8443,), ("nl",))
    monkeypatch.setattr(tls_certificates, "configured_tls_endpoints", lambda: [target])
    fallback_fails = False

    async def _check(domain: str, port: int, servers: list[str]) -> dict:
        if port == 443 or fallback_fails:
            return {
                "domain": domain,
                "port": port,
                "servers": servers,
                "status": "error",
                "failure_kind": "transport",
                "error": "SSLError: handshake failed",
            }
        return {
            "domain": domain,
            "port": port,
            "servers": servers,
            "status": "ok",
            "failure_kind": "",
            "error": None,
        }

    monkeypatch.setattr(tls_policy, "check_tls_endpoint", _check)
    caplog.set_level(logging.INFO, logger="maint-bot")

    await tls_certificates.refresh_tls_certificates(source="test")

    assert any("TLS fallback succeeded" in record.message for record in caplog.records)
    assert not any("TLS endpoint problem" in record.message for record in caplog.records)

    caplog.clear()
    fallback_fails = True
    await tls_certificates.refresh_tls_certificates(source="test")

    warnings = [record.message for record in caplog.records if "TLS endpoint problem" in record.message]
    assert len(warnings) == 1
    assert "zeronet.example.com" in warnings[0]
    assert "effective_port=8443" in warnings[0]
