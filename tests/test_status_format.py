"""Golden-тесты редизайна статус-экрана (status_format.py)."""

from app.handlers.status_format import format_status_message, format_ufw_message
from app.handlers.status_models import DockerContainerView, StatusSnapshot
from app.handlers.ui import SEP


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
    assert "🐳 Docker 🟢 2/2" in text
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
