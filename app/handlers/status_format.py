from typing import List

from .common import html_escape
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
        return ["<code>    —</code>"]
    out: List[str] = []
    for i, item in enumerate(items):
        suffix = "," if i < (len(items) - 1) else ""
        out.append(f"<code>    {html_escape(item)}{suffix}</code>")
    return out


def format_status_message(snapshot: StatusSnapshot) -> str:
    lines: List[str] = []
    lines.append(f"<b>{html_escape(snapshot.title)}</b>")
    lines.append(f"<b>🌍 Сервер:</b> {snapshot.server_flag} {html_escape(snapshot.server_label)}")
    lines.append(f"<b>⏰ Время:</b> {html_escape(snapshot.now_text)}")
    lines.append(f"<b>⏳ Uptime:</b> {html_escape(snapshot.uptime_text)}")
    lines.append(f"<b>🧠 RAM:</b> {html_escape(_normalize_memory_display(snapshot.memory_raw))}")
    lines.append(f"<b>💾 ROM:</b> {html_escape(_normalize_disk_display(snapshot.disk_raw))}")
    lines.append(f"<b>🛡 UFW status:</b> <b>{html_escape((snapshot.ufw_state or 'н/д').upper())}</b>")

    if snapshot.admin_mode and (snapshot.ufw_state or "").lower() == "active":
        lines.append("    ALLOW:")
        lines.extend(_fmt_ufw_list(snapshot.ufw_allow))
        lines.append("    DENY:")
        lines.extend(_fmt_ufw_list(snapshot.ufw_deny))
        lines.append("    REJECT:")
        lines.extend(_fmt_ufw_list(snapshot.ufw_reject))

    lines.append("")
    lines.append("<b>🐳 Docker контейнеры:</b>")
    for c in snapshot.containers:
        emoji = "🟢" if c.is_up else "🔴"
        lines.append(
            f"{emoji} {html_escape(c.name)} — {html_escape(c.status_text)} (restarts: {html_escape(c.restarts)})"
        )
    return "\n".join(lines)
