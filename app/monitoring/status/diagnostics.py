"""Presentation for one-off SSH diagnostic results."""

from __future__ import annotations

from ...bot.ui import html_escape, now_str, ui_error_text
from ...config import ServerTarget


def format_diagnostic_report(
    server: ServerTarget,
    payload: dict[str, object],
) -> str:
    lines = [
        f"<b>SSH-диагностика — {html_escape(server.label)}</b>",
        f"⏰ Время: <code>{html_escape(now_str())}</code>",
    ]
    if not payload.get("ok"):
        lines.extend(
            [
                "",
                ui_error_text(f"SSH ошибка: {html_escape(str(payload.get('error', 'н/д')))}"),
            ]
        )
        return "\n".join(lines)

    lines.extend(
        [
            "",
            f"⏳ Uptime: <b>{html_escape(str(payload.get('uptime') or 'н/д'))}</b>",
            f"🧠 RAM: <b>{html_escape(str(payload.get('memory') or 'н/д'))}</b>",
            f"💾 Disk: <b>{html_escape(str(payload.get('disk_raw') or 'н/д'))}</b>",
            f"🛡️ UFW: <b>{html_escape(str(payload.get('ufw_state') or 'н/д').upper())}</b>",
        ]
    )
    raw_containers = payload.get("containers") or []
    containers: list[tuple[str, bool, str]] = []
    if isinstance(raw_containers, list):
        for item in raw_containers:
            if isinstance(item, (list, tuple)) and len(item) >= 3:
                containers.append((str(item[0]), bool(item[1]), str(item[2])))
    if containers:
        up_count = sum(1 for _, is_up, _ in containers if is_up)
        lines.extend(["", f"<b>Контейнеры:</b> {up_count}/{len(containers)}"])
        for name, is_up, status in containers:
            emoji = "🟢" if is_up else "🔴"
            lines.append(f"{emoji} <code>{html_escape(name)}</code> — {html_escape(status)}")
    lines.extend(["", "<i>Эта диагностика видна только вам.</i>"])
    return "\n".join(lines)


__all__ = ["format_diagnostic_report"]
