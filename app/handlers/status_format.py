import re

from .common import html_escape
from .status_models import StatusSnapshot
from .ui import SEP, extract_percent, footer_updated, header, metric_line, section, used_total_percent

MAX_DNS_DETAIL_LINES = 10


def _normalize_memory_display(raw: str) -> str:
    return (raw or "").strip() or "н/д"


def _normalize_disk_display(raw: str) -> str:
    s = (raw or "").strip()
    if " (" in s:
        s = s.split(" (", 1)[0].strip()
    if " mount" in s:
        s = s.split(" mount", 1)[0].strip()
    return s or "н/д"


def _memory_percent(raw: str) -> int | None:
    m = re.match(r"\s*([\d.,]+)\s*/\s*([\d.,]+)", raw or "")
    if not m:
        return None
    try:
        used = float(m.group(1).replace(",", "."))
        total = float(m.group(2).replace(",", "."))
    except ValueError:
        return None
    return used_total_percent(used, total)


def _fmt_ufw_list(items: list[str]) -> list[str]:
    if not items:
        return ["<code>  —</code>"]
    out: list[str] = []
    visible = items[:40]
    for item in visible:
        out.append(f"<code>  • {html_escape(item)}</code>")
    if len(items) > len(visible):
        out.append(f"<i>  … ещё {len(items) - len(visible)} правил</i>")
    return out


def _ufw_emoji(state: str) -> str:
    s = (state or "").strip().lower()
    if s == "active":
        return "🟢"
    if s in ("inactive", "disabled"):
        return "🔴"
    return "⚠️"


def _dns_chip(snapshot: StatusSnapshot) -> str:
    total = int(snapshot.dns_total_domains or 0)
    ok = int(snapshot.dns_ok_domains or 0)
    bad = int(snapshot.dns_bad_domains or 0)
    unknown = int(snapshot.dns_unknown_domains or 0)
    if total <= 0:
        return "🌐 DNS ⚠️"
    if ok == 0 and bad == 0 and unknown == 0:
        return "🌐 DNS ⚠️"
    emoji = "🟢" if (bad == 0 and unknown == 0) else ("🔴" if bad else "⚠️")
    return f"🌐 DNS {emoji} {ok}/{total}"


def _dns_detail_line(snapshot: StatusSnapshot) -> str | None:
    total = int(snapshot.dns_total_domains or 0)
    ok = int(snapshot.dns_ok_domains or 0)
    bad = int(snapshot.dns_bad_domains or 0)
    unknown = int(snapshot.dns_unknown_domains or 0)
    if total <= 0:
        return "🌐 DNS: проверка не настроена"
    if ok == 0 and bad == 0 and unknown == 0:
        return f"🌐 DNS: нет свежих данных ({total} доменов)"
    if bad == 0 and unknown == 0:
        return None
    parts = []
    if bad:
        parts.append(f"ошибки: {bad}")
    if unknown:
        parts.append(f"нет ответа: {unknown}")
    return "🌐 DNS: " + ", ".join(parts)


def _docker_chip(snapshot: StatusSnapshot) -> str:
    if not snapshot.containers:
        return "🐳 Docker ⚠️"
    up_count = sum(1 for c in snapshot.containers if c.is_up)
    total = len(snapshot.containers)
    emoji = "🟢" if up_count == total else "🔴"
    return f"🐳 Docker {emoji} {up_count}/{total}"


def _summary_chips_line(snapshot: StatusSnapshot) -> str:
    ufw = f"🛡 UFW {_ufw_emoji(snapshot.ufw_state)}"
    return f"{_dns_chip(snapshot)}   {ufw}   {_docker_chip(snapshot)}"


def _dns_error_block(snapshot: StatusSnapshot) -> list[str]:
    if not snapshot.dns_error_details:
        return []
    lines: list[str] = ["", section("DNS — проблемы", "🌐")]
    details = list(snapshot.dns_error_details)
    lines.extend(details[:MAX_DNS_DETAIL_LINES])
    hidden = len(details) - MAX_DNS_DETAIL_LINES
    if hidden > 0:
        lines.append(f"… ещё {hidden}")
    return lines


def _suffix_updated(text: str) -> str:
    s = (text or "").strip()
    if not s:
        return ""
    return f" <i>(обновлено {html_escape(s)})</i>"


def _format_offline_message(snapshot: StatusSnapshot) -> str:
    lines: list[str] = []
    lines.append(header(snapshot.server_flag or "🖥", snapshot.server_label, "🔴 офлайн"))
    lines.append(SEP)
    lines.append("🔴 <b>Нода офлайн</b>")
    if snapshot.last_seen_text:
        lines.append(f"Последнее обновление: <code>{html_escape(snapshot.last_seen_text)}</code>")
    lines.append(SEP)
    lines.append(_summary_chips_line(snapshot))
    dns_line = _dns_detail_line(snapshot)
    if dns_line:
        lines.append(dns_line)
    lines.extend(_dns_error_block(snapshot))
    lines.append("")
    lines.append(footer_updated(snapshot.now_text))
    return "\n".join(lines)


def _format_metrics_error_message(snapshot: StatusSnapshot) -> str:
    lines = [
        header(snapshot.server_flag or "🖥", snapshot.server_label, "⚠️ статус неизвестен"),
        SEP,
        "📡 <b>Панель метрик недоступна или вернула неполные данные</b>",
        f"<i>{html_escape(snapshot.metrics_error or 'неизвестная ошибка')}</i>",
    ]
    if snapshot.last_seen_text:
        lines.append(f"Последнее успешное обновление: <code>{html_escape(snapshot.last_seen_text)}</code>")
    lines.extend([SEP, _summary_chips_line(snapshot)])
    dns_line = _dns_detail_line(snapshot)
    if dns_line:
        lines.append(dns_line)
    lines.extend(_dns_error_block(snapshot))
    lines.extend(["", footer_updated(snapshot.now_text)])
    return "\n".join(lines)


def format_status_message(snapshot: StatusSnapshot) -> str:
    if snapshot.source_mode == "mixed" and snapshot.metrics_error:
        return _format_metrics_error_message(snapshot)
    if snapshot.source_mode == "mixed" and snapshot.node_online is False:
        return _format_offline_message(snapshot)

    status_text = ""
    if snapshot.source_mode == "mixed" and snapshot.node_online is True:
        status_text = "🟢 онлайн"

    lines: list[str] = []
    lines.append(header(snapshot.server_flag or "🖥", snapshot.server_label, status_text))
    lines.append(SEP)
    lines.append(section("Ресурсы", "📊"))

    mem_display = _normalize_memory_display(snapshot.memory_raw)
    lines.append(metric_line("RAM", _memory_percent(snapshot.memory_raw), mem_display))

    disk_display = _normalize_disk_display(snapshot.disk_raw)
    disk_line = metric_line("Диск", extract_percent(snapshot.disk_raw), disk_display)
    if snapshot.source_mode == "mixed":
        disk_line += _suffix_updated(snapshot.disk_updated_at_text)
    lines.append(disk_line)

    lines.append(f"Аптайм: <b>{html_escape(snapshot.uptime_text)}</b>")
    lines.append(SEP)
    lines.append(_summary_chips_line(snapshot))

    dns_line = _dns_detail_line(snapshot)
    if dns_line:
        lines.append(dns_line)

    ufw_state_text = (snapshot.ufw_state or "н/д").upper()
    ufw_suffix = _suffix_updated(snapshot.ufw_updated_at_text) if snapshot.source_mode == "mixed" else ""
    if (snapshot.ufw_state or "").strip().lower() != "active" or ufw_suffix:
        lines.append(f"🛡 UFW: <b>{html_escape(ufw_state_text)}</b>{ufw_suffix}")

    if snapshot.admin_mode and snapshot.online_users is not None:
        lines.append(f"👥 Онлайн пользователей: <b>{int(snapshot.online_users)}</b>")

    lines.extend(_dns_error_block(snapshot))

    if snapshot.show_containers_block:
        lines.append("")
        lines.append(section("Контейнеры", "🐳"))
        if snapshot.containers:
            for c in snapshot.containers:
                emoji = "🟢" if c.is_up else "🔴"
                lines.append(f"{emoji} {html_escape(c.name)}")
        else:
            lines.append("⚠️ Контейнеры не настроены")

    lines.append("")
    lines.append(footer_updated(snapshot.now_text))

    return "\n".join(lines)


def format_ufw_message(snapshot: StatusSnapshot) -> str:
    lines: list[str] = []
    state_text = (snapshot.ufw_state or "н/д").upper()
    lines.append(
        f"🛡 <b>UFW — {html_escape(snapshot.server_label)}</b> · {_ufw_emoji(snapshot.ufw_state)} {html_escape(state_text)}"
    )
    if snapshot.ufw_updated_at_text:
        lines.append(f"<i>обновлено {html_escape(snapshot.ufw_updated_at_text)}</i>")
    lines.append(SEP)
    if snapshot.admin_mode and (snapshot.ufw_state or "").lower() == "active":
        lines.append("ALLOW:")
        lines.extend(_fmt_ufw_list(snapshot.ufw_allow))
        lines.append("DENY:")
        lines.extend(_fmt_ufw_list(snapshot.ufw_deny))
        lines.append("REJECT:")
        lines.extend(_fmt_ufw_list(snapshot.ufw_reject))
    else:
        lines.append("• Дополнительные правила UFW недоступны.")
    return "\n".join(lines)
