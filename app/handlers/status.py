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
    PING_COUNT,
    PING_TIMEOUT_SEC,
    TZ_NAME,
)
from ..services.docker_service import docker_containers
from ..services.system_service import (
    check_uptime,
    disk_root,
    loadavg,
    meminfo,
    ping_host,
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
    la = await loadavg()
    mem = await meminfo()
    disk = await disk_root()

    if is_admin(update):
        ufw_s, allow, deny, reject = await ufw_summary_for_admin()
    else:
        ufw_s = await ufw_status_basic()
        allow, deny, reject = [], [], []

    cont = await docker_containers(MONITOR_CONTAINERS)

    expected_ip = EXPECTED_A_IP
    ok_ping_expected, rtt_expected = await ping_host(expected_ip, count=PING_COUNT, timeout_sec=PING_TIMEOUT_SEC)

    lines: List[str] = []
    lines.append("<b>Статус сервера</b>")
    lines.append(f"• Время: <code>{html_escape(now_str())}</code> ({html_escape(TZ_NAME)})")
    lines.append(f"• Uptime: <code>{html_escape(up)}</code>")
    lines.append(f"• Load average (1/5/15): <code>{html_escape(la)}</code>")
    lines.append(f"• Память: <code>{html_escape(mem)}</code>")
    lines.append(f"• Диск /: <code>{html_escape(disk)}</code>")
    lines.append(f"• UFW: <code>{html_escape(ufw_s)}</code>")

    if is_admin(update) and ufw_s == "active":

        def join_short(xs: List[str]) -> str:
            if not xs:
                return "—"
            s = ", ".join(xs)
            return s if len(s) <= 200 else (s[:200] + "…")

        lines.append(f"  ALLOW: <code>{html_escape(join_short(allow))}</code>")
        lines.append(f"  DENY: <code>{html_escape(join_short(deny))}</code>")
        lines.append(f"  REJECT: <code>{html_escape(join_short(reject))}</code>")

    lines.append("\n<b>Docker контейнеры</b>")
    for name, upb, st, rst in cont:
        emoji = "🟢" if upb else "🔴"
        lines.append(f"• {emoji} <code>{html_escape(name)}</code> — {html_escape(st)} (restarts: {html_escape(rst)})")

    lines.append("\n<b>Сеть (ICMP)</b>")
    if ok_ping_expected:
        rtt_s = f"{rtt_expected:.1f} ms" if rtt_expected is not None else "ok"
        lines.append(f"• ping <code>{html_escape(expected_ip)}</code> — ok (avg {html_escape(rtt_s)})")
    else:
        lines.append(f"• ping <code>{html_escape(expected_ip)}</code> — fail/timeout")

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
