import asyncio
from typing import Dict, List, Optional, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..config import (
    CHECK_A_DOMAINS,
    DNS_RESOLVERS,
    EXPECTED_A_IP,
    MONITOR_CONTAINERS,
    TZ_NAME,
)
from ..services.docker_service import docker_containers
from ..services.system_service import (
    check_uptime,
    disk_root,
    meminfo,
    resolve_a_record,
    ufw_status_basic,
    ufw_summary_for_admin,
)
from .common import html_escape, is_admin, now_str, require_auth


@require_auth
async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text, markup = await build_status_message(update)
    msg = update.effective_message
    if msg:
        await msg.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


async def build_status_message(update: Update) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    up = await check_uptime()
    mem = await meminfo()
    disk = await disk_root()

    if is_admin(update):
        ufw_s, allow, deny, reject = await ufw_summary_for_admin()
    else:
        ufw_s = await ufw_status_basic()
        allow, deny, reject = [], [], []

    cont = await docker_containers(MONITOR_CONTAINERS)

    # ping and loadavg aren't part of the requested status layout

    mem_clean = mem
    if mem_clean.lower().startswith("ram:"):
        mem_clean = mem_clean.split(":", 1)[1].strip()
    if ";" in mem_clean:
        mem_clean = mem_clean.split(";", 1)[0].strip()

    disk_clean = disk.strip()
    if " (" in disk_clean:
        disk_clean = disk_clean.split(" (", 1)[0].strip()
    if " mount" in disk_clean:
        disk_clean = disk_clean.split(" mount", 1)[0].strip()

    ufw_state = ufw_s.upper()

    def fmt_ufw_list(items: List[str]) -> List[str]:
        if not items:
            return ["<code>    —</code>"]
        out: List[str] = []
        for i, item in enumerate(items):
            suffix = "," if i < (len(items) - 1) else ""
            out.append(f"<code>    {html_escape(item)}{suffix}</code>")
        return out

    lines: List[str] = []
    lines.append("<b>🧭 Статус сервера</b>")
    lines.append(f"<b>⏰ Время:</b> {html_escape(now_str())}")
    lines.append(f"<b>⏳ Uptime:</b> {html_escape(up)}")
    lines.append(f"<b>🧠 RAM:</b> {html_escape(mem_clean)}")
    lines.append(f"<b>💾 ROM:</b> {html_escape(disk_clean)}")
    lines.append(f"<b>🛡 UFW status:</b> <b>{html_escape(ufw_state)}</b>")
    if is_admin(update) and ufw_s == "active":
        lines.append("    ALLOW:")
        lines.extend(fmt_ufw_list(allow))
        lines.append("    DENY:")
        lines.extend(fmt_ufw_list(deny))
        lines.append("    REJECT:")
        lines.extend(fmt_ufw_list(reject))

    lines.append("")
    lines.append("<b>🐳 Docker контейнеры:</b>")
    for name, upb, st, rst in cont:
        emoji = "🟢" if upb else "🔴"
        lines.append(f"{emoji} {html_escape(name)} — {html_escape(st)} (restarts: {html_escape(rst)})")

    rows: List[List[InlineKeyboardButton]] = []
    if is_admin(update):
        rows.append([InlineKeyboardButton("🐳 Docker: inspect/logs", callback_data="docker:list")])
        rows.append([InlineKeyboardButton("🛡️ Fail2ban: logs", callback_data="f2b:menu")])
    rows.append([InlineKeyboardButton("DNS проверка", callback_data="dns:check")])
    markup = InlineKeyboardMarkup(rows)
    return "\n".join(lines), markup


async def build_dns_status_message() -> str:
    domains = CHECK_A_DOMAINS
    expected_ip = EXPECTED_A_IP
    dns_resolvers = DNS_RESOLVERS
    dns_map: Dict[str, Dict[str, List[str]]] = {}

    for d in domains:
        ips_by = await asyncio.gather(*[resolve_a_record(d, resolver=r) for r in dns_resolvers])
        dns_map[d] = {r: ips for r, ips in zip(dns_resolvers, ips_by)}

    lines: List[str] = []
    lines.append("<b>DNS A-записи</b>")
    lines.append(f"• Ожидаемый IP: <code>{html_escape(expected_ip)}</code>")
    for dom in domains:
        lines.append(f"• <code>{html_escape(dom)}</code>")
        per = dns_map.get(dom, {})
        for r in dns_resolvers:
            ips = per.get(r, []) or []
            ips_s = ", ".join(ips) if ips else "н/д"
            ok = bool(ips) and (expected_ip in ips)
            flag = "✅" if ok else ("⚠️" if not ips else "❌")
            lines.append(f"  {flag} <code>{html_escape(r)}</code> → <code>{html_escape(ips_s)}</code>")

    return "\n".join(lines)


@require_auth
async def dns_check_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer("Проверяю...")
    text = await build_dns_status_message()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Назад к статусу", callback_data="dns:back")]])
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


@require_auth
async def dns_back_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    text, markup = await build_status_message(update)
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
