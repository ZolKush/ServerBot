import asyncio
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, cast

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..config import (
    BOT_MODE,
    DNS_RESOLVERS,
    SERVER_KEY_PATTERN,
    SERVERS,
    ServerTarget,
    TZ,
    logger,
)
from ..services.docker_service import docker_containers
from ..services.remnawave_metrics import (
    NodeMetrics,
    format_memory_bytes,
    format_uptime_seconds,
    get_metrics_snapshot,
)
from ..services.remote_service import (
    remote_status_bundle,
)
from ..services.system_dns import dns_supports_custom_resolver, resolve_a_record
from ..services.system_metrics import check_uptime, disk_root, meminfo
from ..services.system_ufw import ufw_status_basic, ufw_summary_for_admin
from ..storage import (
    get_daily_node_status_cache,
    get_dns_status_cache,
    set_daily_node_status_cache,
    set_dns_status_cache,
)
from .common import html_escape, is_admin, now_str, require_admin, require_auth, ui_error_text, ui_info_text
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


def _status_actions_kb(
    admin_mode: bool,
    server_key: str,
    *,
    show_ssh_diag: bool = False,
    show_ssh_refresh: bool = False,
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton("🔄 Обновить", callback_data=f"status:show:{server_key}")])
    rows.append([InlineKeyboardButton("🌐 Обновить DNS статус", callback_data=f"status:dnsrefresh:{server_key}")])
    if admin_mode:
        rows.append([InlineKeyboardButton("🛡️ UFW", callback_data=f"status:ufw:{server_key}")])
    if admin_mode and show_ssh_refresh:
        rows.append([InlineKeyboardButton("🔧 Обновить disk/UFW по SSH", callback_data=f"status:sshrefresh:{server_key}")])
    if admin_mode and show_ssh_diag:
        rows.append([InlineKeyboardButton("🔧 Проверить через SSH (только вам)", callback_data=f"status:sshdiag:{server_key}")])
    if admin_mode:
        rows.append([InlineKeyboardButton("🐳 Docker: inspect/logs", callback_data=f"docker:list:{server_key}")])
        rows.append([InlineKeyboardButton("🛡️ Fail2ban: logs", callback_data=f"f2b:menu:{server_key}")])
    if len(SERVERS) > 1:
        rows.append([InlineKeyboardButton("⬅️ К выбору сервера", callback_data="status:pick")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def _confirm_kb(server_key: str, action: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Да", callback_data=f"status:{action}:confirm:{server_key}"),
                InlineKeyboardButton("❌ Отмена", callback_data=f"status:show:{server_key}"),
            ]
        ]
    )


def _resolve_server_key_from_callback(data: str, prefix: str) -> Optional[str]:
    m = re.fullmatch(prefix + rf":({SERVER_KEY_PATTERN})", data or "")
    return m.group(1) if m else None


def _parse_status_ufw_callback(data: str) -> Optional[str]:
    m = re.fullmatch(rf"status:ufw:({SERVER_KEY_PATTERN})", data or "")
    return m.group(1) if m else None


def _parse_status_dnsrefresh_callback(data: str) -> Optional[str]:
    m = re.fullmatch(rf"status:dnsrefresh:({SERVER_KEY_PATTERN})", data or "")
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
    if not SERVERS:
        no_servers_kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]])
        text = ui_error_text("Сервер не настроен.")
        if q:
            await q.edit_message_text(text, reply_markup=no_servers_kb)
        else:
            await msg.reply_text(text, reply_markup=no_servers_kb)
        return
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
    server = get_server_target(server_key) if server_key else None
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
        cont_err = _exc_brief(cont)
        cont = [(n, False, f"ошибка: {cont_err}", "-") for n in server.monitor_containers]
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
                    f"• <code>{html_escape(dom)}</code>: 🔴 ожидался <code>{html_escape(expected_ip)}</code>, "
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
                    f"• <code>{html_escape(dom)}</code>: 🔴 ожидался <code>{html_escape(expected_ip)}</code>, "
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


def _server_uses_metrics(server: ServerTarget) -> bool:
    return BOT_MODE == "mixed" and bool((server.remnawave_uuid or "").strip())


def _format_iso_short(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        return raw
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    else:
        dt = dt.astimezone(TZ)
    return dt.strftime("%d.%m %H:%M")


def _daily_cache_for(server: ServerTarget) -> Tuple[str, str, str, List[str], List[str], List[str], str, str]:
    """
    Возвращает (disk_raw, ufw_state, ufw_updated_text, allow, deny, reject, disk_updated_text, raw_updated_iso).
    Если кэша нет — возвращаем 'н/д' и пустые списки.
    """
    raw = get_daily_node_status_cache(server.key) or {}
    disk = str(raw.get("disk_raw") or "").strip() or "н/д"
    ufw_state = str(raw.get("ufw_state") or "").strip() or "н/д"
    allow = [str(x) for x in (raw.get("ufw_allow") or []) if str(x).strip()]
    deny = [str(x) for x in (raw.get("ufw_deny") or []) if str(x).strip()]
    reject = [str(x) for x in (raw.get("ufw_reject") or []) if str(x).strip()]
    updated_text = _format_iso_short(raw.get("updated_at"))
    return disk, ufw_state, updated_text, allow, deny, reject, updated_text, str(raw.get("updated_at") or "")


async def _ssh_collect_disk_ufw(server: ServerTarget, *, admin_mode: bool) -> Dict[str, object]:
    """
    Собирает disk/UFW с конкретной ноды по SSH. Используется и в дневном джобе,
    и в админской ручной кнопке.
    """
    if server.mode == "ssh":
        try:
            up, mem, disk, cont, ufw_s, allow, deny, reject = await remote_status_bundle(
                server.ssh_target,
                server.monitor_containers,
                admin_mode=True,
            )
        except Exception as e:
            logger.warning("SSH refresh failed for server=%s: %s", server.key, _exc_brief(e))
            return {
                "ok": False,
                "error": _exc_brief(e),
                "updated_at": datetime.now(TZ).isoformat(),
            }
        return {
            "ok": True,
            "disk_raw": str(disk),
            "ufw_state": str(ufw_s),
            "ufw_allow": list(allow),
            "ufw_deny": list(deny),
            "ufw_reject": list(reject),
            "updated_at": datetime.now(TZ).isoformat(),
        }
    # local node
    try:
        disk, ufw_data = await asyncio.gather(
            disk_root(),
            ufw_summary_for_admin(),
            return_exceptions=True,
        )
    except Exception as e:
        return {
            "ok": False,
            "error": _exc_brief(e),
            "updated_at": datetime.now(TZ).isoformat(),
        }
    if isinstance(disk, Exception):
        disk = "н/д"
    if isinstance(ufw_data, Exception):
        ufw_data = ("н/д", [], [], [])
    ufw_s, allow, deny, reject = cast(Tuple[str, List[str], List[str], List[str]], ufw_data)
    return {
        "ok": True,
        "disk_raw": str(disk),
        "ufw_state": str(ufw_s),
        "ufw_allow": list(allow),
        "ufw_deny": list(deny),
        "ufw_reject": list(reject),
        "updated_at": datetime.now(TZ).isoformat(),
    }


async def _build_status_snapshot_mixed(
    update: Update,
    server: ServerTarget,
) -> StatusSnapshot:
    admin_mode = is_admin(update)
    snap = await get_metrics_snapshot()
    node: Optional[NodeMetrics] = snap.get(server.remnawave_uuid)

    dns_payload = _dns_payload_from_cache_or_empty(server)
    dns_ok = int(dns_payload.get("ok", 0) or 0)
    dns_bad = int(dns_payload.get("bad", 0) or 0)
    dns_unknown = int(dns_payload.get("unknown", 0) or 0)
    dns_error_details = [str(x) for x in (dns_payload.get("details", []) or [])]
    dns_total = int(dns_payload.get("total", len(list(server.check_a_domains))) or 0)

    daily_disk, daily_ufw, ufw_upd, allow, deny, reject, disk_upd, raw_upd_iso = _daily_cache_for(server)

    metrics_error = "" if snap.ok else snap.error
    last_seen_text = ""
    if metrics_error:
        last_seen_text = snap.fetched_at.strftime("%d.%m %H:%M") if snap.fetched_at else ""

    if metrics_error or node is None:
        return StatusSnapshot(
            title="🧭 Статус сервера",
            server_label=server.label,
            server_flag=_server_flag(server),
            now_text=now_str(),
            uptime_text="н/д",
            memory_raw="н/д",
            disk_raw="н/д",
            ufw_state="н/д",
            dns_ok_domains=dns_ok,
            dns_total_domains=dns_total,
            dns_bad_domains=dns_bad,
            dns_unknown_domains=dns_unknown,
            dns_error_details=dns_error_details,
            admin_mode=admin_mode,
            source_mode="mixed",
            node_online=None if metrics_error else False,
            online_users=None,
            last_seen_text=last_seen_text or _format_iso_short(raw_upd_iso),
            metrics_error=metrics_error,
            disk_updated_at_text=disk_upd,
            ufw_updated_at_text=ufw_upd,
            show_containers_block=False,
        )

    if not node.is_online:
        return StatusSnapshot(
            title="🧭 Статус сервера",
            server_label=server.label,
            server_flag=_server_flag(server),
            now_text=now_str(),
            uptime_text="н/д",
            memory_raw="н/д",
            disk_raw="н/д",
            ufw_state="н/д",
            dns_ok_domains=dns_ok,
            dns_total_domains=dns_total,
            dns_bad_domains=dns_bad,
            dns_unknown_domains=dns_unknown,
            dns_error_details=dns_error_details,
            admin_mode=admin_mode,
            source_mode="mixed",
            node_online=False,
            online_users=None,
            last_seen_text=_format_iso_short(raw_upd_iso),
            disk_updated_at_text=disk_upd,
            ufw_updated_at_text=ufw_upd,
            show_containers_block=False,
        )

    return StatusSnapshot(
        title="🧭 Статус сервера",
        server_label=server.label,
        server_flag=_server_flag(server),
        now_text=now_str(),
        uptime_text=format_uptime_seconds(node.uptime_s),
        memory_raw=format_memory_bytes(node.mem_used, node.mem_total),
        disk_raw=daily_disk,
        ufw_state=daily_ufw,
        dns_ok_domains=dns_ok,
        dns_total_domains=dns_total,
        dns_bad_domains=dns_bad,
        dns_unknown_domains=dns_unknown,
        dns_error_details=dns_error_details,
        ufw_allow=list(allow),
        ufw_deny=list(deny),
        ufw_reject=list(reject),
        admin_mode=admin_mode,
        source_mode="mixed",
        node_online=True,
        online_users=node.online_users,
        last_seen_text="",
        disk_updated_at_text=disk_upd,
        ufw_updated_at_text=ufw_upd,
        show_containers_block=False,
    )


async def _build_status_snapshot(update: Update, server: ServerTarget) -> StatusSnapshot:
    if _server_uses_metrics(server):
        return await _build_status_snapshot_mixed(update, server)

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
        source_mode="ssh",
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
        if not SERVERS:
            no_servers_kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]])
            return ui_error_text("Сервер не настроен."), no_servers_kb
        if len(SERVERS) > 1:
            return ui_error_text("Сервер не найден."), _status_pick_kb()
        return ui_error_text("Сервер не найден."), InlineKeyboardMarkup(
            [[InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]]
        )

    snapshot = await _build_status_snapshot(update, server)
    show_ssh_diag = snapshot.source_mode == "mixed" and (
        bool(snapshot.metrics_error) or snapshot.node_online is False
    )
    show_ssh_refresh = snapshot.source_mode == "mixed" and snapshot.node_online is True
    markup = _status_actions_kb(
        admin_mode=snapshot.admin_mode,
        server_key=server.key,
        show_ssh_diag=show_ssh_diag,
        show_ssh_refresh=show_ssh_refresh,
    )
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


async def daily_node_status_refresh(context: ContextTypes.DEFAULT_TYPE) -> None:
    if BOT_MODE != "mixed":
        return
    for server in SERVERS.values():
        try:
            payload = await _ssh_collect_disk_ufw(server, admin_mode=True)
            await set_daily_node_status_cache(server.key, payload)
            logger.info(
                "Daily node status refreshed for server=%s ok=%s",
                server.key,
                payload.get("ok"),
            )
        except Exception:
            logger.exception("Daily node status refresh failed for server=%s", server.key)


@require_admin
async def status_ssh_refresh_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Шаг 1: показать экран подтверждения для планового SSH-обновления disk/UFW.
    Этот результат попадает в общий кэш, виден всем пользователям.
    """
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(rf"status:sshrefresh:({SERVER_KEY_PATTERN})", q.data or "")
    if not m:
        return
    server_key = m.group(1)
    server = get_server_target(server_key)
    if not server:
        await q.edit_message_text(ui_error_text("сервер не найден."), reply_markup=_status_pick_kb())
        return
    text = (
        f"⚠️ <b>Подтверждение</b>\n\n"
        f"Будет выполнено SSH-подключение к ноде <b>{html_escape(server.label)}</b> "
        f"для обновления disk/UFW.\n\n"
        f"Результат сохранится в общий кэш и будет виден всем пользователям.\n\n"
        f"Продолжить?"
    )
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=_confirm_kb(server.key, "sshrefresh"))


@require_admin
async def status_ssh_refresh_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer("Запускаю SSH...")
    m = re.fullmatch(rf"status:sshrefresh:confirm:({SERVER_KEY_PATTERN})", q.data or "")
    if not m:
        return
    server_key = m.group(1)
    server = get_server_target(server_key)
    if not server:
        await q.edit_message_text(ui_error_text("сервер не найден."), reply_markup=_status_pick_kb())
        return
    payload = await _ssh_collect_disk_ufw(server, admin_mode=True)
    await set_daily_node_status_cache(server.key, payload)
    text, markup = await build_status_message(update, server_key=server.key)
    note = (
        ui_info_text("Disk/UFW обновлены через SSH.")
        if payload.get("ok")
        else ui_error_text(f"SSH ошибка: {html_escape(str(payload.get('error', 'н/д')))}")
    )
    await q.edit_message_text(text + "\n\n" + note, parse_mode=ParseMode.HTML, reply_markup=markup)


@require_admin
async def status_ssh_diag_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Шаг 1: показать экран подтверждения для эфемерной SSH-диагностики.
    Результат покажется только этому админу, в общий кэш не сохранится.
    """
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(rf"status:sshdiag:({SERVER_KEY_PATTERN})", q.data or "")
    if not m:
        return
    server_key = m.group(1)
    server = get_server_target(server_key)
    if not server:
        await q.edit_message_text(ui_error_text("сервер не найден."), reply_markup=_status_pick_kb())
        return
    text = (
        f"⚠️ <b>Подтверждение</b>\n\n"
        f"Будет выполнено SSH-подключение к ноде <b>{html_escape(server.label)}</b>.\n\n"
        f"Результат будет показан <b>только вам</b> и не сохранится в общий кэш.\n\n"
        f"Продолжить?"
    )
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=_confirm_kb(server.key, "sshdiag"))


def _format_ssh_diag_report(server: ServerTarget, payload: Dict[str, object]) -> str:
    lines: List[str] = []
    lines.append(f"<b>SSH-диагностика — {html_escape(server.label)}</b>")
    lines.append(f"⏰ Время: <code>{html_escape(now_str())}</code>")
    if not payload.get("ok"):
        lines.append("")
        lines.append(ui_error_text(f"SSH ошибка: {html_escape(str(payload.get('error', 'н/д')))}"))
        return "\n".join(lines)
    up = str(payload.get("uptime") or "н/д")
    mem = str(payload.get("memory") or "н/д")
    disk = str(payload.get("disk_raw") or "н/д")
    ufw_s = str(payload.get("ufw_state") or "н/д").upper()
    cont_raw = payload.get("containers") or []
    lines.append("")
    lines.append(f"⏳ Uptime: <b>{html_escape(up)}</b>")
    lines.append(f"🧠 RAM: <b>{html_escape(mem)}</b>")
    lines.append(f"💾 Disk: <b>{html_escape(disk)}</b>")
    lines.append(f"🛡️ UFW: <b>{html_escape(ufw_s)}</b>")
    cont_items: List[Tuple[str, bool, str]] = []
    if isinstance(cont_raw, list):
        for item in cont_raw:
            if isinstance(item, (list, tuple)) and len(item) >= 3:
                cont_items.append((str(item[0]), bool(item[1]), str(item[2])))
    if cont_items:
        up_count = sum(1 for _, is_up, _ in cont_items if is_up)
        total = len(cont_items)
        lines.append("")
        lines.append(f"<b>Контейнеры:</b> {up_count}/{total}")
        for name, is_up, status_text in cont_items:
            emoji = "🟢" if is_up else "🔴"
            lines.append(f"{emoji} <code>{html_escape(name)}</code> — {html_escape(status_text)}")
    lines.append("")
    lines.append("<i>Эта диагностика видна только вам.</i>")
    return "\n".join(lines)


async def _ssh_full_diag(server: ServerTarget) -> Dict[str, object]:
    if server.mode == "ssh":
        try:
            up, mem, disk, cont, ufw_s, allow, deny, reject = await remote_status_bundle(
                server.ssh_target,
                server.monitor_containers,
                admin_mode=True,
            )
        except Exception as e:
            return {"ok": False, "error": _exc_brief(e)}
        return {
            "ok": True,
            "uptime": str(up),
            "memory": str(mem),
            "disk_raw": str(disk),
            "ufw_state": str(ufw_s),
            "containers": [(n, bool(u), str(s), str(r)) for n, u, s, r in cont],
        }
    # local
    results = await asyncio.gather(
        check_uptime(),
        meminfo(),
        disk_root(),
        docker_containers(server.monitor_containers),
        ufw_summary_for_admin(),
        return_exceptions=True,
    )
    up_r, mem_r, disk_r, cont_r, ufw_r = results
    up_v = "н/д" if isinstance(up_r, Exception) else str(up_r)
    mem_v = "н/д" if isinstance(mem_r, Exception) else str(mem_r)
    disk_v = "н/д" if isinstance(disk_r, Exception) else str(disk_r)
    if isinstance(cont_r, Exception):
        cont_v = [(n, False, f"ошибка: {_exc_brief(cont_r)}", "-") for n in server.monitor_containers]
    else:
        cont_v = list(cont_r)
    if isinstance(ufw_r, Exception) or not isinstance(ufw_r, tuple):
        ufw_state = "н/д"
    else:
        ufw_state = str(ufw_r[0])
    return {
        "ok": True,
        "uptime": up_v,
        "memory": mem_v,
        "disk_raw": disk_v,
        "ufw_state": ufw_state,
        "containers": [(n, bool(u), str(s), str(r)) for n, u, s, r in cont_v],
    }


@require_admin
async def status_ssh_diag_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer("Запускаю SSH...")
    m = re.fullmatch(rf"status:sshdiag:confirm:({SERVER_KEY_PATTERN})", q.data or "")
    if not m:
        return
    server_key = m.group(1)
    server = get_server_target(server_key)
    if not server:
        await q.edit_message_text(ui_error_text("сервер не найден."), reply_markup=_status_pick_kb())
        return
    payload = await _ssh_full_diag(server)
    report = _format_ssh_diag_report(server, payload)
    # Возвращаем экран статуса как был, а отчёт отправляем отдельным эфемерным сообщением админу.
    text, markup = await build_status_message(update, server_key=server.key)
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
    if q.message:
        await q.message.reply_text(report, parse_mode=ParseMode.HTML)
