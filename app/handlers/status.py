import asyncio
import re
from typing import Dict, List, Optional, Tuple, cast

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..config import DNS_RESOLVERS, SERVERS, ServerTarget
from ..services.docker_service import docker_containers
from ..services.remote_service import (
    remote_check_uptime,
    remote_docker_containers,
    remote_disk_root,
    remote_meminfo,
    remote_resolve_a_record_system,
    remote_ufw_status_basic,
    remote_ufw_summary_for_admin,
)
from ..services.system_service import (
    disk_root,
    dns_supports_custom_resolver,
    meminfo,
    resolve_a_record,
    ufw_status_basic,
    ufw_summary_for_admin,
    check_uptime,
)
from .common import html_escape, is_admin, now_str, require_auth


def _server_keys() -> List[str]:
    return list(SERVERS.keys())


def _first_server_key() -> str:
    keys = _server_keys()
    return keys[0] if keys else "local"


def get_server_target(server_key: Optional[str]) -> Optional[ServerTarget]:
    key = (server_key or "").strip().lower()
    if key and key in SERVERS:
        return SERVERS[key]
    first = _first_server_key()
    return SERVERS.get(first)


def _status_pick_kb() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for key in _server_keys():
        srv = SERVERS[key]
        rows.append([InlineKeyboardButton(f"🖥 {srv.label}", callback_data=f"status:show:{srv.key}")])
    return InlineKeyboardMarkup(rows)


def _status_pick_text() -> str:
    return "<b>Выберите сервер</b>\nКакой статус показать?"


def _status_actions_kb(admin_mode: bool, server_key: str) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if admin_mode:
        rows.append([InlineKeyboardButton("🐳 Docker: inspect/logs", callback_data=f"docker:list:{server_key}")])
        rows.append([InlineKeyboardButton("🛡️ Fail2ban: logs", callback_data=f"f2b:menu:{server_key}")])
    rows.append([InlineKeyboardButton("DNS проверка", callback_data=f"dns:check:{server_key}")])
    if len(SERVERS) > 1:
        rows.append([InlineKeyboardButton("⬅️ К выбору сервера", callback_data="status:pick")])
    return InlineKeyboardMarkup(rows)


def _resolve_server_key_from_callback(data: str, prefix: str) -> Optional[str]:
    m = re.fullmatch(prefix + r":([a-z0-9_-]{1,12})", data or "")
    return m.group(1) if m else None


@require_auth
async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    if len(SERVERS) > 1:
        await msg.reply_text(_status_pick_text(), parse_mode=ParseMode.HTML, reply_markup=_status_pick_kb())
        return
    server_key = _first_server_key()
    text, markup = await build_status_message(update, server_key=server_key)
    await msg.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


@require_auth
async def status_pick_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    await q.edit_message_text(_status_pick_text(), parse_mode=ParseMode.HTML, reply_markup=_status_pick_kb())


@require_auth
async def status_show_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    server_key = _resolve_server_key_from_callback(q.data or "", r"status:show")
    if not get_server_target(server_key):
        await q.edit_message_text("Сервер не найден.", reply_markup=_status_pick_kb())
        return
    text, markup = await build_status_message(update, server_key=server_key)
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


async def _build_status_payload_local(admin_mode: bool, server: ServerTarget):
    up, mem, disk, cont, ufw_data = await asyncio.gather(
        check_uptime(),
        meminfo(),
        disk_root(),
        docker_containers(server.monitor_containers),
        ufw_summary_for_admin() if admin_mode else ufw_status_basic(),
    )
    if admin_mode:
        ufw_s, allow, deny, reject = cast(Tuple[str, List[str], List[str], List[str]], ufw_data)
    else:
        ufw_s = str(ufw_data)
        allow, deny, reject = [], [], []
    return up, mem, disk, cont, ufw_s, allow, deny, reject


async def _build_status_payload_remote(admin_mode: bool, server: ServerTarget):
    ssh_target = server.ssh_target
    up, mem, disk, cont, ufw_data = await asyncio.gather(
        remote_check_uptime(ssh_target),
        remote_meminfo(ssh_target),
        remote_disk_root(ssh_target),
        remote_docker_containers(ssh_target, server.monitor_containers),
        remote_ufw_summary_for_admin(ssh_target) if admin_mode else remote_ufw_status_basic(ssh_target),
    )
    if admin_mode:
        ufw_s, allow, deny, reject = cast(Tuple[str, List[str], List[str], List[str]], ufw_data)
    else:
        ufw_s = str(ufw_data)
        allow, deny, reject = [], [], []
    return up, mem, disk, cont, ufw_s, allow, deny, reject


async def build_status_message(update: Update, server_key: Optional[str] = None) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    server = get_server_target(server_key)
    if not server:
        return "Сервер не настроен.", _status_pick_kb() if len(SERVERS) > 1 else None

    admin_mode = is_admin(update)
    if server.mode == "ssh":
        up, mem, disk, cont, ufw_s, allow, deny, reject = await _build_status_payload_remote(admin_mode, server)
    else:
        up, mem, disk, cont, ufw_s, allow, deny, reject = await _build_status_payload_local(admin_mode, server)

    mem_clean = mem
    if mem_clean.lower().startswith("ram:"):
        mem_clean = mem_clean.split(":", 1)[1].strip()
    if ";" in mem_clean:
        mem_clean = mem_clean.split(";", 1)[0].strip()
    if " (" in mem_clean:
        mem_clean = mem_clean.split(" (", 1)[0].strip()

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
    lines.append(f"<b>🌍 Сервер:</b> {html_escape(server.label)}")
    lines.append(f"<b>⏰ Время:</b> {html_escape(now_str())}")
    lines.append(f"<b>⏳ Uptime:</b> {html_escape(up)}")
    lines.append(f"<b>🧠 RAM:</b> {html_escape(mem_clean)}")
    lines.append(f"<b>💾 ROM:</b> {html_escape(disk_clean)}")
    lines.append(f"<b>🛡 UFW status:</b> <b>{html_escape(ufw_state)}</b>")
    if admin_mode and ufw_s == "active":
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

    markup = _status_actions_kb(admin_mode=admin_mode, server_key=server.key)
    return "\n".join(lines), markup


async def build_dns_status_message(server_key: Optional[str]) -> str:
    server = get_server_target(server_key)
    if not server:
        return "Сервер не найден."

    domains = list(server.check_a_domains)
    expected_ip = server.expected_a_ip
    dns_map: Dict[str, Dict[str, List[str]]] = {}
    lines: List[str] = []
    lines.append(f"<b>DNS A-записи ({html_escape(server.label)})</b>")
    if expected_ip:
        lines.append(f"• Ожидаемый IP: <code>{html_escape(expected_ip)}</code>")

    if server.mode == "ssh":
        lines.append(f"• Режим проверки: <code>system resolver via SSH ({html_escape(server.ssh_target)})</code>")
        for d in domains:
            dns_map[d] = {"system": await remote_resolve_a_record_system(server.ssh_target, d)}
        resolver_labels = ["system"]
    else:
        custom_resolvers_supported = dns_supports_custom_resolver()
        if custom_resolvers_supported:
            for d in domains:
                ips_by = await asyncio.gather(*[resolve_a_record(d, resolver=r) for r in DNS_RESOLVERS])
                dns_map[d] = {r: ips for r, ips in zip(DNS_RESOLVERS, ips_by)}
            resolver_labels = list(DNS_RESOLVERS)
        else:
            lines.append("• Режим проверки: <code>system resolver fallback</code> (aiodns не установлен)")
            for d in domains:
                dns_map[d] = {"system": await resolve_a_record(d, resolver=None)}
            resolver_labels = ["system"]

    if not domains:
        lines.append("• Домены для проверки не настроены.")
        return "\n".join(lines)

    for dom in domains:
        lines.append(f"• <code>{html_escape(dom)}</code>")
        per = dns_map.get(dom, {})
        for r in resolver_labels:
            ips = per.get(r, []) or []
            ips_s = ", ".join(ips) if ips else "н/д"
            ok = bool(ips) and (expected_ip in ips if expected_ip else True)
            flag = "✅" if ok else ("⚠️" if not ips else "❌")
            lines.append(f"  {flag} <code>{html_escape(r)}</code> → <code>{html_escape(ips_s)}</code>")

    return "\n".join(lines)


@require_auth
async def dns_check_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer("Проверяю...")
    server_key = _resolve_server_key_from_callback(q.data or "", r"dns:check")
    if not server_key:
        server_key = _first_server_key()
    text = await build_dns_status_message(server_key)
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Назад к статусу", callback_data=f"dns:back:{server_key}")]])
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


@require_auth
async def dns_back_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    server_key = _resolve_server_key_from_callback(q.data or "", r"dns:back")
    if not server_key:
        server_key = _first_server_key()
    text, markup = await build_status_message(update, server_key=server_key)
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
