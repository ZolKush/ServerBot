from typing import List

from .common import breadcrumbs, html_escape
from .status_models import StatusSnapshot


def _normalize_memory_display(raw: str) -> str:
    s = (raw or "").strip()
    if s.lower().startswith("ram:"):
        s = s.split(":", 1)[1].strip()
    if ";" in s:
        s = s.split(";", 1)[0].strip()
    if " (" in s:
        s = s.split(" (", 1)[0].strip()
    return s or "н/д"


def _normalize_disk_display(raw: str) -> str:
    s = (raw or "").strip()
    if " (" in s:
        s = s.split(" (", 1)[0].strip()
    if " mount" in s:
        s = s.split(" mount", 1)[0].strip()
    return s or "н/д"


def _fmt_ufw_list(items: List[str]) -> List[str]:
    if not items:
        return ["<code>  —</code>"]
    out: List[str] = []
    for item in items:
        out.append(f"<code>  • {html_escape(item)}</code>")
    return out


def _dns_summary_line(snapshot: StatusSnapshot) -> str:
    total = int(snapshot.dns_total_domains or 0)
    ok = int(snapshot.dns_ok_domains or 0)
    bad = int(snapshot.dns_bad_domains or 0)
    unknown = int(snapshot.dns_unknown_domains or 0)
    if total <= 0:
        return "🌐 DNS: ⚠️ не настроено"
    if ok == 0 and bad == 0 and unknown == 0:
        return f"🌐 DNS: ⚠️ нет свежих данных ({total} доменов)"
    if bad == 0 and unknown == 0:
        return f"🌐 DNS: ✅ {ok}/{total} доменов OK"
    parts = [f"ok={ok}/{total}"]
    if bad:
        parts.append(f"ошибки={bad}")
    if unknown:
        parts.append(f"нет ответа={unknown}")
    return "🌐 DNS: ❌ " + ", ".join(parts)


def _docker_summary_lines(snapshot: StatusSnapshot) -> List[str]:
    lines: List[str] = []
    if not snapshot.containers:
        return ["🐳 Docker: ⚠️ контейнеры не настроены"]
    up_count = sum(1 for c in snapshot.containers if c.is_up)
    total = len(snapshot.containers)
    lines.append(f"🐳 Docker: {'✅' if up_count == total else '❌'} {up_count}/{total} OK")
    for c in snapshot.containers:
        emoji = "🟢" if c.is_up else "🔴"
        lines.append(f"{emoji} {html_escape(c.name)}")
    return lines


def format_status_message(snapshot: StatusSnapshot) -> str:
    lines: List[str] = []
    lines.append(f"<b>{html_escape(breadcrumbs('Статус', snapshot.server_label))}</b>")
    lines.append(f"{snapshot.server_flag} <b>{html_escape(snapshot.server_label)}</b>")
    lines.append("")
    lines.append("<b>Краткий статус</b>")
    lines.append(f"⏰ Время: <code>{html_escape(snapshot.now_text)}</code>")
    lines.append(f"⏳ Uptime: <b>{html_escape(snapshot.uptime_text)}</b>")
    lines.append(f"🧠 RAM: <b>{html_escape(_normalize_memory_display(snapshot.memory_raw))}</b>")
    lines.append(f"💾 Disk: <b>{html_escape(_normalize_disk_display(snapshot.disk_raw))}</b>")
    lines.append(f"🛡️ UFW: <b>{html_escape((snapshot.ufw_state or 'н/д').upper())}</b>")
    lines.append(_dns_summary_line(snapshot))
    if snapshot.dns_error_details:
        lines.append("")
        lines.append("<b>DNS details (errors)</b>")
        lines.extend(snapshot.dns_error_details)
    lines.append("")
    lines.append("<b>Контейнеры</b>")
    lines.extend(_docker_summary_lines(snapshot))

    lines.append("")
    lines.append("Действия: кнопки ниже ↓")

    return "\n".join(lines)


def format_ufw_message(snapshot: StatusSnapshot) -> str:
    lines: List[str] = []
    lines.append(f"<b>{html_escape(breadcrumbs('Статус', snapshot.server_label, 'UFW'))}</b>")
    lines.append("")
    lines.append(f"🛡️ UFW status: <b>{html_escape((snapshot.ufw_state or 'н/д').upper())}</b>")
    if snapshot.admin_mode and (snapshot.ufw_state or "").lower() == "active":
        lines.append("")
        lines.append("ALLOW:")
        lines.extend(_fmt_ufw_list(snapshot.ufw_allow))
        lines.append("DENY:")
        lines.extend(_fmt_ufw_list(snapshot.ufw_deny))
        lines.append("REJECT:")
        lines.extend(_fmt_ufw_list(snapshot.ufw_reject))
    else:
        lines.append("")
        lines.append("• Дополнительные правила UFW недоступны.")
    lines.append("")
    lines.append("Действия: кнопки ниже ↓")
    return "\n".join(lines)
