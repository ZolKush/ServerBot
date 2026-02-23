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
from .common import breadcrumbs, html_escape, is_admin, now_str, require_auth, ui_error_text
from .status_format import format_status_message
from .status_models import DockerContainerView, StatusSnapshot


def _server_keys() -> List[str]:
    return list(SERVERS.keys())


def _first_server_key() -> str:
    keys = _server_keys()
    return keys[0] if keys else "local"


def _default_server_target() -> Optional[ServerTarget]:
    return SERVERS.get(_first_server_key())


def get_server_target(server_key: Optional[str]) -> Optional[ServerTarget]:
    key = (server_key or "").strip().lower()
    return SERVERS.get(key) if key else None


def _status_pick_kb() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for key in _server_keys():
        srv = SERVERS[key]
        rows.append([InlineKeyboardButton(f"{_server_flag(srv)} {srv.label}", callback_data=f"status:show:{srv.key}")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def _status_pick_text() -> str:
    return "<b>Выберите сервер</b>\nКакой статус показать?"


def _server_flag(server: ServerTarget) -> str:
    key = (server.key or "").lower()
    label = (server.label or "").lower()
    if key == "de" or "germany" in label:
        return "🇩🇪"
    if key == "nl" or "netherlands" in label:
        return "🇳🇱"
    return "🖥"


def _status_actions_kb(admin_mode: bool, server_key: str, *, detailed: bool = False) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton("🔄 Обновить", callback_data=f"status:show:{server_key}")])
    rows.append(
        [
            InlineKeyboardButton(
                "📖 Скрыть детали" if detailed else "📖 Подробнее",
                callback_data=f"status:detail:{server_key}:{'brief' if detailed else 'full'}",
            )
        ]
    )
    if admin_mode:
        rows.append([InlineKeyboardButton("🐳 Docker: inspect/logs", callback_data=f"docker:list:{server_key}")])
        rows.append([InlineKeyboardButton("🛡️ Fail2ban: logs", callback_data=f"f2b:menu:{server_key}")])
    rows.append([InlineKeyboardButton("DNS проверка", callback_data=f"dns:check:{server_key}")])
    if len(SERVERS) > 1:
        rows.append([InlineKeyboardButton("⬅️ К выбору сервера", callback_data="status:pick")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def _resolve_server_key_from_callback(data: str, prefix: str) -> Optional[str]:
    m = re.fullmatch(prefix + r":([a-z0-9_-]{1,12})", data or "")
    return m.group(1) if m else None


def _parse_status_detail_callback(data: str) -> Optional[Tuple[str, bool]]:
    m = re.fullmatch(r"status:detail:([a-z0-9_-]{1,12}):(full|brief)", data or "")
    if not m:
        return None
    return m.group(1), (m.group(2) == "full")


def _exc_brief(value: object) -> str:
    if not isinstance(value, Exception):
        return "н/д"
    name = value.__class__.__name__
    text = str(value).strip()
    return f"{name}: {text}" if text else name


@require_auth
async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    msg = update.effective_message
    if not msg:
        return
    if q:
        await q.answer()
    if len(SERVERS) > 1:
        if q:
            await q.edit_message_text(_status_pick_text(), parse_mode=ParseMode.HTML, reply_markup=_status_pick_kb())
        else:
            await msg.reply_text(_status_pick_text(), parse_mode=ParseMode.HTML, reply_markup=_status_pick_kb())
        return
    server_key = _first_server_key()
    text, markup = await build_status_message(update, server_key=server_key)
    if q:
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
    else:
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
        await q.edit_message_text(ui_error_text("сервер не найден."), reply_markup=_status_pick_kb())
        return
    text, markup = await build_status_message(update, server_key=server_key, detailed=False)
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


@require_auth
async def status_detail_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    parsed = _parse_status_detail_callback(q.data or "")
    if not parsed:
        return
    server_key, detailed = parsed
    if not get_server_target(server_key):
        await q.edit_message_text(ui_error_text("сервер не найден."), reply_markup=_status_pick_kb())
        return
    text, markup = await build_status_message(update, server_key=server_key, detailed=detailed)
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


async def _build_status_payload_local(admin_mode: bool, server: ServerTarget):
    up, mem, disk, cont, ufw_data = await asyncio.gather(
        check_uptime(),
        meminfo(),
        disk_root(),
        docker_containers(server.monitor_containers),
        ufw_summary_for_admin() if admin_mode else ufw_status_basic(),
        return_exceptions=True,
    )
    if isinstance(up, Exception):
        up = "н/д"
    if isinstance(mem, Exception):
        mem = "н/д"
    if isinstance(disk, Exception):
        disk = "н/д"
    if isinstance(cont, Exception):
        cont = [(n, False, f"ошибка: {_exc_brief(cont)}", "-") for n in server.monitor_containers]
    if isinstance(ufw_data, Exception):
        ufw_data = ("н/д", [], [], []) if admin_mode else "н/д"
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
        return_exceptions=True,
    )
    if isinstance(up, Exception):
        up = "н/д"
    if isinstance(mem, Exception):
        mem = "н/д"
    if isinstance(disk, Exception):
        disk = "н/д"
    if isinstance(cont, Exception):
        cont = [(n, False, f"ошибка: {_exc_brief(cont)}", "-") for n in server.monitor_containers]
    if isinstance(ufw_data, Exception):
        ufw_data = ("н/д", [], [], []) if admin_mode else "н/д"
    if admin_mode:
        ufw_s, allow, deny, reject = cast(Tuple[str, List[str], List[str], List[str]], ufw_data)
    else:
        ufw_s = str(ufw_data)
        allow, deny, reject = [], [], []
    return up, mem, disk, cont, ufw_s, allow, deny, reject


async def _build_dns_summary(server: ServerTarget) -> Tuple[int, int, int]:
    domains = list(server.check_a_domains)
    if not domains:
        return 0, 0, 0
    expected_ip = (server.expected_a_ip or "").strip()
    ok = bad = unknown = 0
    custom_resolvers_supported = dns_supports_custom_resolver()
    if custom_resolvers_supported and DNS_RESOLVERS:
        for dom in domains:
            results = await asyncio.gather(
                *[resolve_a_record(dom, resolver=r) for r in DNS_RESOLVERS],
                return_exceptions=True,
            )
            ip_lists = [r for r in results if isinstance(r, list)]
            merged: List[str] = []
            for ips in ip_lists:
                for ip in ips:
                    if ip not in merged:
                        merged.append(ip)
            if not merged:
                unknown += 1
            elif expected_ip and expected_ip not in merged:
                bad += 1
            else:
                ok += 1
    else:
        for dom in domains:
            try:
                ips = await resolve_a_record(dom, resolver=None)
            except Exception:
                ips = []
            if not ips:
                unknown += 1
            elif expected_ip and expected_ip not in ips:
                bad += 1
            else:
                ok += 1
    return ok, bad, unknown


async def _build_status_snapshot(update: Update, server: ServerTarget) -> StatusSnapshot:
    admin_mode = is_admin(update)
    if server.mode == "ssh":
        status_task = _build_status_payload_remote(admin_mode, server)
    else:
        status_task = _build_status_payload_local(admin_mode, server)
    (up, mem, disk, cont, ufw_s, allow, deny, reject), dns_summary = await asyncio.gather(
        status_task,
        _build_dns_summary(server),
    )
    dns_ok, dns_bad, dns_unknown = dns_summary

    containers = [
        DockerContainerView(name=name, is_up=upb, status_text=st, restarts=rst)
        for name, upb, st, rst in cont
    ]
    return StatusSnapshot(
        title="🧭 Статус сервера",
        server_label=server.label,
        server_flag=_server_flag(server),
        now_text=now_str(),
        uptime_text=str(up),
        memory_raw=str(mem),
        disk_raw=str(disk),
        ufw_state=str(ufw_s),
        dns_ok_domains=dns_ok,
        dns_total_domains=len(list(server.check_a_domains)),
        dns_bad_domains=dns_bad,
        dns_unknown_domains=dns_unknown,
        ufw_allow=list(allow),
        ufw_deny=list(deny),
        ufw_reject=list(reject),
        containers=containers,
        admin_mode=admin_mode,
    )


async def build_status_message(
    update: Update,
    server_key: Optional[str] = None,
    *,
    detailed: bool = False,
) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    server = get_server_target(server_key) if server_key else _default_server_target()
    if not server:
        return "Сервер не настроен.", _status_pick_kb() if len(SERVERS) > 1 else None

    snapshot = await _build_status_snapshot(update, server)
    markup = _status_actions_kb(admin_mode=snapshot.admin_mode, server_key=server.key, detailed=detailed)
    return format_status_message(snapshot, detailed=detailed), markup


async def build_dns_status_message(server_key: Optional[str]) -> str:
    server = get_server_target(server_key) if server_key else _default_server_target()
    if not server:
        return "Сервер не найден."

    domains = list(server.check_a_domains)
    expected_ip = server.expected_a_ip
    dns_map: Dict[str, Dict[str, List[str]]] = {}
    lines: List[str] = []
    lines.append(f"<b>{html_escape(breadcrumbs('Статус', server.label, 'DNS'))}</b>")
    if expected_ip:
        lines.append(f"• Ожидаемый IP: <code>{html_escape(expected_ip)}</code>")

    custom_resolvers_supported = dns_supports_custom_resolver()
    ok_domains = 0
    bad_domains = 0
    unknown_domains = 0

    if custom_resolvers_supported:
        for d in domains:
            ips_by = await asyncio.gather(
                *[resolve_a_record(d, resolver=r) for r in DNS_RESOLVERS],
                return_exceptions=True,
            )
            normalized: Dict[str, List[str]] = {}
            for r, ips in zip(DNS_RESOLVERS, ips_by):
                normalized[r] = ips if isinstance(ips, list) else []
            dns_map[d] = normalized
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
        per = dns_map.get(dom, {})
        merged: List[str] = []
        for r in resolver_labels:
            for ip in per.get(r, []) or []:
                if ip not in merged:
                    merged.append(ip)
        if not merged:
            unknown_domains += 1
        elif expected_ip and expected_ip not in merged:
            bad_domains += 1
        else:
            ok_domains += 1

    if bad_domains == 0 and unknown_domains == 0:
        lines.append(f"• Итог: ✅ <b>{ok_domains}/{len(domains)}</b> доменов совпадают")
    else:
        lines.append(
            f"• Итог: ❌ <b>{ok_domains}/{len(domains)}</b> OK, "
            f"<b>{bad_domains}</b> неверный IP, <b>{unknown_domains}</b> без ответа"
        )
    lines.append("")

    for dom in domains:
        lines.append(f"• <b><code>{html_escape(dom)}</code></b>")
        per = dns_map.get(dom, {})
        for r in resolver_labels:
            ips = per.get(r, []) or []
            ips_s = ", ".join(ips) if ips else "н/д"
            ok = bool(ips) and (expected_ip in ips if expected_ip else True)
            flag = "✅" if ok else ("⚠️" if not ips else "❌")
            lines.append(f"  {flag} <code>{html_escape(r)}</code> | <code>{html_escape(ips_s)}</code>")

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
    kb = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔄 Повторить", callback_data=f"dns:check:{server_key}")],
            [InlineKeyboardButton("⬅️ Назад к статусу", callback_data=f"dns:back:{server_key}")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )
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
    text, markup = await build_status_message(update, server_key=server_key, detailed=False)
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
