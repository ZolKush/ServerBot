import asyncio
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, cast

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..config import DNS_RESOLVERS, SERVERS, ServerTarget, TZ, logger
from ..services.docker_service import docker_containers
from ..services.remote_service import (
    remote_status_bundle,
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
from ..storage import get_dns_status_cache, set_dns_status_cache
from .common import breadcrumbs, html_escape, is_admin, now_str, require_admin, require_auth, ui_error_text, ui_info_text
from .status_format import format_status_message, format_ufw_message
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
    return (
        "<b>Выберите сервер</b>\n"
        "Какой статус показать?\n\n"
        "ℹ️ Нажмите кнопку сервера один раз и подождите загрузку."
    )


def _server_flag(server: ServerTarget) -> str:
    return server.flag or "🖥"


def _status_actions_kb(admin_mode: bool, server_key: str) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton("🔄 Обновить", callback_data=f"status:show:{server_key}")])
    rows.append([InlineKeyboardButton("🌐 Обновить DNS статус", callback_data=f"status:dnsrefresh:{server_key}")])
    if admin_mode:
        rows.append([InlineKeyboardButton("🛡️ UFW", callback_data=f"status:ufw:{server_key}")])
    if admin_mode:
        rows.append([InlineKeyboardButton("🐳 Docker: inspect/logs", callback_data=f"docker:list:{server_key}")])
        rows.append([InlineKeyboardButton("🛡️ Fail2ban: logs", callback_data=f"f2b:menu:{server_key}")])
    if len(SERVERS) > 1:
        rows.append([InlineKeyboardButton("⬅️ К выбору сервера", callback_data="status:pick")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def _resolve_server_key_from_callback(data: str, prefix: str) -> Optional[str]:
    m = re.fullmatch(prefix + r":([a-z0-9_-]{1,12})", data or "")
    return m.group(1) if m else None


def _parse_status_ufw_callback(data: str) -> Optional[str]:
    m = re.fullmatch(r"status:ufw:([a-z0-9_-]{1,12})", data or "")
    return m.group(1) if m else None


def _parse_status_dnsrefresh_callback(data: str) -> Optional[str]:
    m = re.fullmatch(r"status:dnsrefresh:([a-z0-9_-]{1,12})", data or "")
    return m.group(1) if m else None


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
    text, markup = await build_status_message(update, server_key=server_key)
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


def _ufw_actions_kb(server_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔄 Обновить UFW", callback_data=f"status:ufw:{server_key}")],
            [InlineKeyboardButton("⬅️ Назад к статусу", callback_data=f"status:show:{server_key}")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


@require_admin
async def status_ufw_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    server_key = _parse_status_ufw_callback(q.data or "")
    if not server_key or not get_server_target(server_key):
        await q.edit_message_text(ui_error_text("сервер не найден."), reply_markup=_status_pick_kb())
        return
    snapshot, server = await _build_status_snapshot_and_server(update, server_key)
    if not snapshot or not server:
        await q.edit_message_text(ui_error_text("сервер не найден."), reply_markup=_status_pick_kb())
        return
    await q.edit_message_text(
        format_ufw_message(snapshot),
        parse_mode=ParseMode.HTML,
        reply_markup=_ufw_actions_kb(server.key),
    )


@require_auth
async def status_dns_refresh_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer("Обновляю DNS...")
    server_key = _parse_status_dnsrefresh_callback(q.data or "")
    if not server_key or not get_server_target(server_key):
        await q.edit_message_text(ui_error_text("сервер не найден."), reply_markup=_status_pick_kb())
        return
    server = get_server_target(server_key)
    if not server:
        await q.edit_message_text(ui_error_text("сервер не найден."), reply_markup=_status_pick_kb())
        return
    payload = await _build_dns_status_payload_live(server)
    await set_dns_status_cache(server.key, payload)
    logger.info(
        "DNS status refreshed manually for server=%s ok=%s bad=%s unknown=%s total=%s",
        server.key,
        payload.get("ok"),
        payload.get("bad"),
        payload.get("unknown"),
        payload.get("total"),
    )
    text, markup = await build_status_message(update, server_key=server.key)
    await q.edit_message_text(
        text + "\n\n" + ui_info_text("DNS статус обновлён в реальном времени."),
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
    )


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
    try:
        return await remote_status_bundle(server.ssh_target, server.monitor_containers, admin_mode=admin_mode)
    except Exception as e:
        return (
            "н/д",
            "н/д",
            "н/д",
            [(n, False, f"ошибка: {_exc_brief(e)}", "-") for n in server.monitor_containers],
            "н/д",
            [],
            [],
            [],
        )


async def _build_dns_status_payload_live(server: ServerTarget) -> Dict[str, object]:
    domains = list(server.check_a_domains)
    if not domains:
        return {
            "server_key": server.key,
            "updated_at": datetime.now(TZ).isoformat(),
            "total": 0,
            "ok": 0,
            "bad": 0,
            "unknown": 0,
            "details": [],
        }
    expected_ip = (server.expected_a_ip or "").strip()
    ok = bad = unknown = 0
    details: List[str] = []
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
                details.append(f"• <code>{html_escape(dom)}</code>: ⚠️ нет ответа")
            elif expected_ip and expected_ip not in merged:
                bad += 1
                details.append(
                    f"• <code>{html_escape(dom)}</code>: ❌ ожидался <code>{html_escape(expected_ip)}</code>, "
                    f"получено <code>{html_escape(', '.join(merged))}</code>"
                )
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
                details.append(f"• <code>{html_escape(dom)}</code>: ⚠️ нет ответа")
            elif expected_ip and expected_ip not in ips:
                bad += 1
                details.append(
                    f"• <code>{html_escape(dom)}</code>: ❌ ожидался <code>{html_escape(expected_ip)}</code>, "
                    f"получено <code>{html_escape(', '.join(ips))}</code>"
                )
            else:
                ok += 1
    return {
        "server_key": server.key,
        "updated_at": datetime.now(TZ).isoformat(),
        "total": len(domains),
        "ok": ok,
        "bad": bad,
        "unknown": unknown,
        "details": details,
    }


def _dns_payload_from_cache_or_empty(server: ServerTarget) -> Dict[str, object]:
    raw = get_dns_status_cache(server.key) or {}
    details = raw.get("details", [])
    if not isinstance(details, list):
        details = []
    total = int(raw.get("total", 0) or 0)
    ok = int(raw.get("ok", 0) or 0)
    bad = int(raw.get("bad", 0) or 0)
    unknown = int(raw.get("unknown", 0) or 0)
    if total <= 0 and server.check_a_domains:
        return {
            "total": len(server.check_a_domains),
            "ok": 0,
            "bad": 0,
            "unknown": 0,
            "details": [f"• {html_escape(ui_info_text('DNS статус ещё не обновлялся. Нажмите «Обновить DNS статус».'))}"],
        }
    return {
        "total": total,
        "ok": ok,
        "bad": bad,
        "unknown": unknown,
        "details": [str(x) for x in details],
    }


async def _build_status_snapshot(update: Update, server: ServerTarget) -> StatusSnapshot:
    admin_mode = is_admin(update)
    if server.mode == "ssh":
        status_task = _build_status_payload_remote(admin_mode, server)
    else:
        status_task = _build_status_payload_local(admin_mode, server)
    up, mem, disk, cont, ufw_s, allow, deny, reject = await status_task
    dns_payload = _dns_payload_from_cache_or_empty(server)
    dns_ok = int(dns_payload.get("ok", 0) or 0)
    dns_bad = int(dns_payload.get("bad", 0) or 0)
    dns_unknown = int(dns_payload.get("unknown", 0) or 0)
    dns_error_details = [str(x) for x in (dns_payload.get("details", []) or [])]

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
        dns_total_domains=int(dns_payload.get("total", len(list(server.check_a_domains))) or 0),
        dns_bad_domains=dns_bad,
        dns_unknown_domains=dns_unknown,
        dns_error_details=dns_error_details,
        ufw_allow=list(allow),
        ufw_deny=list(deny),
        ufw_reject=list(reject),
        containers=containers,
        admin_mode=admin_mode,
    )


async def _build_status_snapshot_and_server(update: Update, server_key: Optional[str]) -> Tuple[Optional[StatusSnapshot], Optional[ServerTarget]]:
    server = get_server_target(server_key) if server_key else _default_server_target()
    if not server:
        return None, None
    return await _build_status_snapshot(update, server), server


async def dns_daily_refresh(context: ContextTypes.DEFAULT_TYPE) -> None:
    for server in SERVERS.values():
        try:
            payload = await _build_dns_status_payload_live(server)
            await set_dns_status_cache(server.key, payload)
            logger.info(
                "DNS status refreshed (scheduled) for server=%s ok=%s bad=%s unknown=%s total=%s",
                server.key,
                payload.get("ok"),
                payload.get("bad"),
                payload.get("unknown"),
                payload.get("total"),
            )
        except Exception:
            # Do not break the loop if one server fails.
            logger.exception("DNS status refresh failed for server=%s", server.key)


async def build_status_message(
    update: Update,
    server_key: Optional[str] = None,
) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    server = get_server_target(server_key) if server_key else _default_server_target()
    if not server:
        return "Сервер не настроен.", _status_pick_kb() if len(SERVERS) > 1 else None

    snapshot = await _build_status_snapshot(update, server)
    markup = _status_actions_kb(admin_mode=snapshot.admin_mode, server_key=server.key)
    return format_status_message(snapshot), markup


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
