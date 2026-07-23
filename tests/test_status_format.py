"""Golden-тесты редизайна статус-экрана."""

from app.bot.ui import SEP
from app.monitoring.status.models import DockerContainerView, StatusSnapshot, TLSCertificateView
from app.monitoring.status.views import format_status_message, format_ufw_message


def make_snapshot(**kw) -> StatusSnapshot:
    base = dict(
        title="Статус",
        server_label="Germany",
        server_flag="🇩🇪",
        now_text="12.06.2026 14:30:05",
        uptime_text="12 дней",
        memory_raw="2150 / 4096 MiB",
        disk_raw="18G / 40G (avail 22G, 45%) mount /",
        ufw_state="active",
        dns_ok_domains=8,
        dns_total_domains=8,
        containers=[
            DockerContainerView("remnanode", True, "Up 2 days", "0"),
            DockerContainerView("caddy", True, "Up 2 days", "0"),
        ],
        admin_mode=True,
        online_users=5,
    )
    base.update(kw)
    return StatusSnapshot(**base)


def test_status_message_structure() -> None:
    text = format_status_message(make_snapshot())
    assert SEP in text
    assert "▰" in text  # прогресс-бары
    assert "52%" in text  # RAM 2150/4096
    assert "45%" in text  # диск из disk_raw
    assert "🌐 DNS 🟢 8/8" in text
    assert "🛡 UFW 🟢" in text
    assert "🐳 Docker 🟢 работают 2 · остановлены 0 · unhealthy 0 · всего 2" in text
    assert "обновлено 12.06.2026 14:30:05" in text
    assert len(text) < 4096


def test_status_message_escapes_injection() -> None:
    text = format_status_message(
        make_snapshot(
            server_label="<b>haxor</b> & co",
            uptime_text="<i>up</i>",
            memory_raw="нет данных <script>",
        )
    )
    assert "<b>haxor</b>" not in text
    assert "&lt;b&gt;haxor&lt;/b&gt; &amp; co" in text
    assert "<script>" not in text


def test_status_message_unparseable_metrics_fall_back() -> None:
    text = format_status_message(make_snapshot(memory_raw="н/д", disk_raw=""))
    assert "RAM: н/д" in text
    assert "Диск: н/д" in text


def test_status_message_dns_problems() -> None:
    text = format_status_message(
        make_snapshot(
            dns_ok_domains=6,
            dns_bad_domains=2,
            dns_total_domains=8,
            dns_error_details=[f"• <code>dom{i}.com</code>: 🔴 ожидался x" for i in range(15)],
        )
    )
    assert "🌐 DNS 🔴 6/8" in text
    assert "ошибки: 2" in text
    assert "… ещё 5" in text  # 15 деталей, показываем 10


def test_offline_message() -> None:
    text = format_status_message(make_snapshot(source_mode="mixed", node_online=False))
    assert "🔴 офлайн" in text
    assert SEP in text
    assert "обновлено 12.06.2026 14:30:05" in text


def test_online_header_mixed_mode() -> None:
    text = format_status_message(make_snapshot(source_mode="mixed", node_online=True))
    assert "🟢 онлайн" in text


def test_ufw_message() -> None:
    text = format_ufw_message(make_snapshot(ufw_allow=["22/tcp ALLOW"], ufw_deny=[], ufw_reject=[]))
    assert "🛡 <b>UFW — Germany</b> · 🟢 ACTIVE" in text
    assert "22/tcp ALLOW" in text
    assert SEP in text


def test_docker_summary_counts_stopped_unhealthy_and_missing() -> None:
    text = format_status_message(
        make_snapshot(
            containers=[
                DockerContainerView("healthy", True, "Up 2 days (healthy)", "0"),
                DockerContainerView("unhealthy", True, "Up 1 hour (unhealthy)", "0"),
                DockerContainerView("stopped", False, "Exited (1) 2 minutes ago", "0"),
                DockerContainerView("expected", False, "не найден", "-"),
            ]
        )
    )
    assert "🐳 Docker 🔴 работают 2 · остановлены 1 · unhealthy 1 · всего 3 · не найдены 1" in text
    assert "🔴 stopped" in text


def test_docker_zero_containers_is_available_not_warning() -> None:
    text = format_status_message(make_snapshot(containers=[]))
    assert "🐳 Docker 🟢 работают 0 · остановлены 0 · unhealthy 0 · всего 0" in text
    assert "Docker доступен, контейнеров нет" in text


def test_docker_details_are_admin_only() -> None:
    user_text = format_status_message(
        make_snapshot(
            admin_mode=False,
            containers=[
                DockerContainerView("healthy", True, "Up 2 days (healthy)", "0"),
                DockerContainerView("private-service", False, "Exited (1) 2 minutes ago", "0"),
            ],
        )
    )

    assert "🐳 Docker 🔴" in user_text
    assert "работают 1" not in user_text
    assert "всего 2" not in user_text
    assert "Контейнеры" not in user_text
    assert "private-service" not in user_text


def test_tls_details_are_admin_only() -> None:
    certificate = TLSCertificateView(
        domain="vpn.example.com",
        port=443,
        status="expiring",
        not_after="2026-06-14T12:00:00+00:00",
        remaining_seconds=2 * 86400,
        hostname_valid=True,
        trust_valid=True,
    )
    admin_text = format_status_message(make_snapshot(tls_certificates=[certificate], admin_mode=True))
    user_text = format_status_message(make_snapshot(tls_certificates=[], admin_mode=False))

    assert "🔐 TLS ⚠️ исправны 0 · проблемы 1 · всего 1" in admin_text
    assert "vpn.example.com:443" in admin_text
    assert "TLS-сертификаты" not in user_text
    assert "🔐 TLS" not in user_text
