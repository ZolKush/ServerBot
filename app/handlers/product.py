from __future__ import annotations

import contextlib
import re
from datetime import datetime, timedelta
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes, ConversationHandler

from ..config import TZ, logger
from ..services.outbox import message_payload
from ..staff import (
    REGULAR_STAFF_TITLES,
    STAFF_DISPLAY_TITLE,
    STAFF_DISPLAY_TITLE_ALIAS,
    STAFF_TITLE_LABELS,
    is_admin_meta,
    is_lead_or_owner_meta,
    is_owner_meta,
    normalize_staff_alias,
    staff_internal_identity,
    staff_public_signature,
    staff_title_label,
)
from ..storage import (
    UserData,
    append_audit_entry,
    authorized_users_snapshot,
    enqueue_user_outbox,
    get_user_meta_copy,
    make_outbox_event,
    next_service_request_id,
    product_settings_snapshot,
    service_requests_snapshot,
    update_user_data,
)
from .common import (
    clip_html,
    format_dt_human,
    get_user_id,
    html_escape,
    main_menu_inline_kb,
    require_admin,
    require_auth,
    show_main_menu,
    ui_ok_text,
    ui_warn_text,
)
from .subscription import (
    CONNECTION_URL_KEY,
    connection_outbox_payload,
    has_connection,
    is_valid_connection_url,
)

PRODUCT_INPUT, PRODUCT_CONFIRM = range(2)

PLAN_MONTHS = 3
PLAN_MONTHLY_RUB = 100
PLAN_TOTAL_RUB = PLAN_MONTHS * PLAN_MONTHLY_RUB
REQUEST_CLAIM_TIMEOUT = timedelta(minutes=15)
ACTIVE_REQUEST_STATUSES = {
    "pending",
    "claimed",
    "awaiting_link",
    "requisites_sent",
    "payment_reported",
}

_CTX_ACTION = "product_input_action"
_CTX_REQUEST_ID = "product_request_id"
_CTX_TARGET_UID = "product_target_uid"
_CTX_PENDING = "product_pending_change"


def _now() -> datetime:
    return datetime.now(TZ)


def _context_data(context: ContextTypes.DEFAULT_TYPE) -> dict[str, Any]:
    data = context.user_data
    if data is None:
        raise RuntimeError("Telegram user_data is unavailable")
    return data


def _now_iso() -> str:
    return _now().isoformat()


def _parse_dt(value: object) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    with contextlib.suppress(ValueError):
        parsed = datetime.fromisoformat(raw)
        return parsed.replace(tzinfo=TZ) if parsed.tzinfo is None else parsed.astimezone(TZ)
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y %H:%M:%S"):
        with contextlib.suppress(ValueError):
            return datetime.strptime(raw, fmt).replace(tzinfo=TZ)
    return None


def _parse_input_dt(value: str) -> datetime | None:
    raw = " ".join(str(value or "").strip().split())
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y %H:%M:%S"):
        with contextlib.suppress(ValueError):
            return datetime.strptime(raw, fmt).replace(tzinfo=TZ)
    return None


def _dt_text(value: object) -> str:
    return format_dt_human(value, empty="не указана")


def _service_tier_label(value: object) -> str:
    return {
        "basic": "Базовый доступ",
        "subscriber": "Подписчик",
        "unlimited_trial": "Безлимитный тестовый доступ",
    }.get(str(value or ""), "Неизвестный уровень")


def _request_kind_label(value: object) -> str:
    return {
        "trial": "Тестовый доступ",
        "purchase": "Покупка подписки",
        "renewal": "Продление подписки",
    }.get(str(value or ""), "Заявка")


def _request_status_label(value: object) -> str:
    return {
        "pending": "ожидает решения",
        "claimed": "обрабатывается",
        "awaiting_link": "ожидает ссылку",
        "requisites_sent": "реквизиты отправлены",
        "payment_reported": "пользователь сообщил об оплате",
        "approved": "одобрена",
        "rejected": "отклонена",
        "cancelled": "отменена",
    }.get(str(value or ""), "неизвестно")


def _clear_product_context(context: ContextTypes.DEFAULT_TYPE) -> None:
    data = _context_data(context)
    for key in (_CTX_ACTION, _CTX_REQUEST_ID, _CTX_TARGET_UID, _CTX_PENDING):
        data.pop(key, None)


def _approved_admin_ids(cfg: UserData) -> list[int]:
    result: list[int] = []
    for key, meta in cfg.authorized_users.items():
        if not isinstance(meta, dict) or meta.get("role") != "admin":
            continue
        if meta.get("access_state") != "approved" or not bool(meta.get("enabled", True)):
            continue
        with contextlib.suppress(TypeError, ValueError):
            result.append(int(meta.get("user_id", key)))
    return sorted(set(result))


def _owner_meta_from_cfg(cfg: UserData) -> dict[str, Any] | None:
    for meta in cfg.authorized_users.values():
        if isinstance(meta, dict) and is_owner_meta(meta):
            return meta
    return None


def _queue_message(
    cfg: UserData,
    *,
    recipient_ids: list[int],
    kind: str,
    text: str,
    reply_markup: list[list[dict[str, str]]] | None = None,
) -> None:
    if not recipient_ids:
        return
    enqueue_user_outbox(
        cfg,
        make_outbox_event(
            kind=kind,
            recipient_ids=recipient_ids,
            payload=message_payload(text, reply_markup=reply_markup),
        ),
    )


def _active_request(
    cfg: UserData,
    *,
    user_id: int,
    kind: str,
) -> dict[str, Any] | None:
    for request in cfg.service_requests.values():
        if not isinstance(request, dict):
            continue
        if int(request.get("user_id", 0) or 0) != user_id or request.get("kind") != kind:
            continue
        if request.get("status") in ACTIVE_REQUEST_STATUSES:
            return request
    return None


def _new_request(
    cfg: UserData,
    *,
    kind: str,
    user_id: int,
    status: str = "pending",
    comment: str | None = None,
    target_end_at: str | None = None,
) -> dict[str, Any]:
    request_id = next_service_request_id(cfg)
    now = _now_iso()
    request = {
        "id": request_id,
        "kind": kind,
        "status": status,
        "user_id": user_id,
        "created_at": now,
        "updated_at": now,
        "comment": comment,
        "target_end_at": target_end_at,
        "claimed_by_id": None,
        "claimed_at": None,
        "reviewed_by_id": None,
        "reviewed_at": None,
        "payment_reported_at": now if status == "payment_reported" else None,
    }
    cfg.service_requests[str(request_id)] = request
    return request


def _cancel_active_requests(
    cfg: UserData,
    *,
    user_id: int,
    reason: str,
    exclude_request_id: int | None = None,
    kinds: set[str] | None = None,
) -> int:
    cancelled = 0
    for key, request in list(cfg.service_requests.items()):
        if not isinstance(request, dict):
            continue
        if int(request.get("user_id", 0) or 0) != user_id or request.get("status") not in ACTIVE_REQUEST_STATUSES:
            continue
        if kinds is not None and str(request.get("kind") or "") not in kinds:
            continue
        if exclude_request_id is not None and int(request.get("id", 0) or 0) == exclude_request_id:
            continue
        updated = dict(request)
        updated.update(
            {
                "status": "cancelled",
                "decision_reason": reason,
                "updated_at": _now_iso(),
                "claimed_by_id": None,
                "claimed_at": None,
            }
        )
        cfg.service_requests[key] = updated
        cancelled += 1
    return cancelled


def _real_user_name(meta: dict[str, Any]) -> str:
    name = " ".join(
        str(part).strip()[:80] for part in (meta.get("first_name"), meta.get("last_name")) if str(part or "").strip()
    )
    return name[:160] or "не указано"


def _request_card(request: dict[str, Any], meta: dict[str, Any]) -> str:
    username = str(meta.get("username") or "").strip().lstrip("@")
    comment = str(request.get("comment") or "").strip()
    lines = [
        f"📥 <b>{html_escape(_request_kind_label(request.get('kind')))}</b> · #{request.get('id')}",
        "",
        f"• Статус: <b>{html_escape(_request_status_label(request.get('status')))}</b>",
        f"• Пользователь: <b>{html_escape(_real_user_name(meta))}</b>",
        f"• Username: <code>{html_escape('@' + username if username else '-')}</code>",
        f"• Telegram ID: <code>{html_escape(str(meta.get('user_id') or request.get('user_id')))}</code>",
        f"• Допущен: <code>{html_escape(_dt_text(meta.get('auth_at')))}</code>",
        f"• Уровень: <b>{html_escape(_service_tier_label(meta.get('service_tier')))}</b>",
        f"• Оплата: <b>{'подтверждена' if meta.get('is_paid') else 'не подтверждена'}</b>",
        f"• Тест ранее: <b>{'выдавался' if meta.get('trial_issued_at') else 'не выдавался'}</b>",
        f"• Ссылка: <b>{'назначена' if has_connection(meta) else 'не назначена'}</b>",
    ]
    if request.get("target_end_at"):
        lines.append(f"• Доступ до: <code>{html_escape(_dt_text(request.get('target_end_at')))}</code>")
    claimed_by = int(request.get("claimed_by_id", 0) or 0)
    if claimed_by:
        claimed_meta = get_user_meta_copy(claimed_by)
        claimed_identity = staff_internal_identity(claimed_meta) if claimed_meta else f"ID {claimed_by}"
        lines.append(f"• Обрабатывает: <code>{html_escape(claimed_identity)}</code>")
    if comment:
        lines.extend(["", "<b>Комментарий пользователя:</b>", clip_html(comment, limit=1400)])
    return "\n".join(lines)


def _request_markup(request: dict[str, Any], actor_meta: dict[str, Any]) -> InlineKeyboardMarkup:
    request_id = int(request.get("id", 0) or 0)
    user_id = int(request.get("user_id", 0) or 0)
    kind = str(request.get("kind") or "")
    status = str(request.get("status") or "")
    rows: list[list[InlineKeyboardButton]] = []
    if kind == "trial" and status in {"pending", "claimed", "awaiting_link"}:
        rows.append(
            [
                InlineKeyboardButton("✅ Одобрить", callback_data=f"product:req:approve:{request_id}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"product:req:reject:{request_id}"),
            ]
        )
    elif kind == "purchase" and status == "pending":
        rows.append(
            [
                InlineKeyboardButton("💳 Отправить реквизиты", callback_data=f"product:req:requisites:{request_id}"),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"product:req:reject:{request_id}"),
            ]
        )
    elif kind == "purchase" and status == "requisites_sent":
        rows.append([InlineKeyboardButton("❌ Отклонить", callback_data=f"product:req:reject:{request_id}")])
    if kind in {"purchase", "renewal"} and status == "payment_reported" and is_owner_meta(actor_meta):
        rows.append(
            [
                InlineKeyboardButton("✅ Подтвердить оплату", callback_data=f"product:req:confirm:{request_id}"),
                InlineKeyboardButton("🔎 Платёж не найден", callback_data=f"product:req:notfound:{request_id}"),
            ]
        )
    elif (
        kind in {"purchase", "renewal"}
        and status == "awaiting_link"
        and is_owner_meta(actor_meta)
        and int(request.get("claimed_by_id", 0) or 0) == int(actor_meta.get("user_id", 0) or 0)
    ):
        rows.append(
            [InlineKeyboardButton("🔗 Продолжить ввод ссылки", callback_data=f"product:req:confirm:{request_id}")]
        )
    rows.append([InlineKeyboardButton("👤 Открыть профиль", callback_data=f"users:user:{user_id}")])
    rows.append([InlineKeyboardButton("⬅️ К заявкам", callback_data="product:requests")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def _payment_target(settings: dict[str, Any], *, after: datetime | None = None) -> datetime | None:
    floor = after or _now()
    next_end = _parse_dt(settings.get("next_period_end"))
    if next_end and next_end > floor:
        return next_end
    current_end = _parse_dt(settings.get("current_period_end"))
    if current_end and current_end > floor:
        return current_end
    return None


def _payment_profile_ready(settings: dict[str, Any]) -> bool:
    return all(str(settings.get(key) or "").strip() for key in ("payment_bank", "payment_recipient", "payment_phone"))


def _payment_message(settings: dict[str, Any], request: dict[str, Any]) -> str:
    return (
        "💳 <b>Оплата подписки</b>\n\n"
        f"• Период: <b>{PLAN_MONTHS} месяца</b>\n"
        f"• Стоимость: <b>{PLAN_TOTAL_RUB} ₽</b>\n"
        f"• Доступ до: <code>{html_escape(_dt_text(request.get('target_end_at')))}</code>\n\n"
        f"• Банк: <b>{html_escape(str(settings.get('payment_bank') or '-'))}</b>\n"
        f"• Получатель: <b>{html_escape(str(settings.get('payment_recipient') or '-'))}</b>\n"
        f"• Телефон: <code>{html_escape(str(settings.get('payment_phone') or '-'))}</code>\n\n"
        "После перевода нажмите «Я оплатил». Если возникнут вопросы, создайте тикет в поддержку."
    )


def _payment_markup(request_id: int) -> list[list[dict[str, str]]]:
    return [
        [{"text": "✅ Я оплатил", "callback_data": f"subscription:paid:{request_id}"}],
        [{"text": "🎫 Создать тикет", "callback_data": "menu:ticket"}],
    ]


def _renewal_markup() -> list[list[dict[str, str]]]:
    return [
        [{"text": "✅ Я оплатил продление", "callback_data": "subscription:renew"}],
        [{"text": "🎫 Создать тикет", "callback_data": "menu:ticket"}],
    ]


def _actor_meta(update: Update) -> dict[str, Any] | None:
    uid = get_user_id(update)
    return get_user_meta_copy(uid) if uid is not None else None


def _user_profile_text(meta: dict[str, Any]) -> str:
    username = str(meta.get("username") or "").strip().lstrip("@")
    lines = [
        "👤 <b>Профиль</b>",
        "",
        f"• Имя: <b>{html_escape(_real_user_name(meta))}</b>",
        f"• Username: <code>{html_escape('@' + username if username else '-')}</code>",
        f"• Уровень: <b>{html_escape(_service_tier_label(meta.get('service_tier')))}</b>",
        f"• Оплата: <b>{'подтверждена' if meta.get('is_paid') else 'не подтверждена'}</b>",
        f"• Дата оплаты: <code>{html_escape(_dt_text(meta.get('paid_at')))}</code>",
        f"• Доступ до: <code>{html_escape(_dt_text(meta.get('subscription_end_at')))}</code>",
        f"• Персональная ссылка: <b>{'назначена' if has_connection(meta) else 'не назначена'}</b>",
        f"• Тестовый доступ: <b>{'выдавался' if meta.get('trial_issued_at') else 'не выдавался'}</b>",
    ]
    return "\n".join(lines)


@require_auth
async def product_profile_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    meta = _actor_meta(update)
    if not query or not meta:
        return
    await query.answer()
    await query.edit_message_text(
        _user_profile_text(meta),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]]),
    )


@require_auth
async def trial_request_start_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    meta = _actor_meta(update)
    if not query or not meta:
        return ConversationHandler.END
    await query.answer()
    if meta.get("role") == "admin" or meta.get("service_tier") != "basic":
        await query.edit_message_text("Тестовый доступ предназначен для пользователей с базовым доступом.")
        return ConversationHandler.END
    if meta.get("trial_issued_at"):
        await query.edit_message_text("Тестовый доступ уже выдавался ранее.")
        return ConversationHandler.END
    uid = int(meta.get("user_id") or 0)
    if any(
        int(item.get("user_id", 0) or 0) == uid
        and item.get("kind") == "trial"
        and item.get("status") in ACTIVE_REQUEST_STATUSES
        for item in service_requests_snapshot().values()
    ):
        await query.edit_message_text("Заявка на тестовый доступ уже ожидает решения.")
        return ConversationHandler.END
    _clear_product_context(context)
    _context_data(context)[_CTX_ACTION] = "trial_comment"
    await query.edit_message_text(
        "🧪 <b>Запрос тестового доступа</b>\n\nКоротко опишите запрос. Приложение указывать не нужно — используется Happ.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Отмена", callback_data="menu:home")]]),
    )
    return PRODUCT_INPUT


@require_auth
async def purchase_show_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    meta = _actor_meta(update)
    if not query or not meta:
        return
    await query.answer()
    if meta.get("role") == "admin" or meta.get("service_tier") != "basic":
        await query.edit_message_text("Покупка доступна пользователям с базовым доступом.")
        return
    settings = product_settings_snapshot()
    target = _payment_target(settings)
    if not _payment_profile_ready(settings) or target is None:
        await query.edit_message_text(
            "Покупка через бот временно недоступна: руководитель ещё не настроил реквизиты или дату периода. Создайте тикет в поддержку.",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🎫 Создать тикет", callback_data="menu:ticket")],
                    [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
                ]
            ),
        )
        return
    text = (
        "💳 <b>Покупка подписки</b>\n\n"
        f"• Период: <b>{PLAN_MONTHS} месяца</b>\n"
        f"• Стоимость: <b>{PLAN_TOTAL_RUB} ₽</b>\n"
        f"• Доступ до: <code>{html_escape(_dt_text(target.isoformat()))}</code>\n\n"
        "После создания заявки сотрудник отправит реквизиты."
    )
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("✅ Создать заявку", callback_data="subscription:buyconfirm")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="menu:subscription")],
            ]
        ),
    )


@require_auth
async def purchase_create_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = _actor_meta(update)
    if not query or not actor:
        return
    await query.answer()
    uid = int(actor.get("user_id") or 0)
    if actor.get("role") == "admin" or actor.get("service_tier") != "basic":
        await query.edit_message_text("Заявка больше недоступна для вашего уровня доступа.")
        return

    def _create(cfg: UserData) -> tuple[str, int | None]:
        current = cfg.authorized_users.get(str(uid))
        if not isinstance(current, dict) or current.get("service_tier") != "basic":
            return "denied", None
        existing = _active_request(cfg, user_id=uid, kind="purchase")
        if existing:
            return "exists", int(existing.get("id", 0) or 0)
        target = _payment_target(cfg.product_settings)
        if not _payment_profile_ready(cfg.product_settings) or target is None:
            return "not_configured", None
        request = _new_request(cfg, kind="purchase", user_id=uid, target_end_at=target.isoformat())
        request_id = int(request["id"])
        markup = [
            [
                {"text": "💳 Отправить реквизиты", "callback_data": f"product:req:requisites:{request_id}"},
                {"text": "❌ Отклонить", "callback_data": f"product:req:reject:{request_id}"},
            ],
            [{"text": "👤 Профиль", "callback_data": f"users:user:{uid}"}],
        ]
        _queue_message(
            cfg,
            recipient_ids=_approved_admin_ids(cfg),
            kind="purchase_request",
            text=_request_card(request, current),
            reply_markup=markup,
        )
        return "created", request_id

    outcome, _request_id = await update_user_data(_create)
    texts = {
        "created": "✅ Заявка на покупку создана. Сотрудник отправит реквизиты после проверки.",
        "exists": "Заявка на покупку уже находится в обработке.",
        "not_configured": "Платёжные реквизиты или дата периода пока не настроены.",
        "denied": "Заявка больше недоступна для вашего уровня доступа.",
    }
    await query.edit_message_text(
        texts[outcome],
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]]),
    )


@require_admin
async def product_requests_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer()
    requests = [
        request for request in service_requests_snapshot().values() if request.get("status") in ACTIVE_REQUEST_STATUSES
    ]
    requests.sort(key=lambda item: (str(item.get("created_at") or ""), int(item.get("id", 0) or 0)))
    lines = ["📥 <b>Заявки</b>", "", f"Активных заявок: <b>{len(requests)}</b>"]
    rows: list[list[InlineKeyboardButton]] = []
    for request in requests[-40:]:
        request_id = int(request.get("id", 0) or 0)
        user_id = int(request.get("user_id", 0) or 0)
        meta = get_user_meta_copy(user_id) or {}
        icon = {"trial": "🧪", "purchase": "💳", "renewal": "🔄"}.get(str(request.get("kind")), "📄")
        label = f"{icon} #{request_id} {_real_user_name(meta)} · {_request_status_label(request.get('status'))}"
        rows.append([InlineKeyboardButton(label[:60], callback_data=f"product:req:view:{request_id}")])
    if not requests:
        lines.extend(["", "Новых заявок нет."])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    await query.edit_message_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(rows))


@require_admin
async def product_request_view_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    match = re.fullmatch(r"product:req:view:(\d+)", query.data or "")
    if not match:
        return
    request = service_requests_snapshot().get(match.group(1))
    if not isinstance(request, dict):
        await query.answer("Заявка не найдена.", show_alert=True)
        return
    await query.answer()
    meta = get_user_meta_copy(int(request.get("user_id", 0) or 0)) or {}
    actor = _actor_meta(update) or {}
    await query.edit_message_text(
        _request_card(request, meta),
        parse_mode=ParseMode.HTML,
        reply_markup=_request_markup(request, actor),
    )


def _payment_report_notification(request: dict[str, Any], meta: dict[str, Any]) -> str:
    return (
        "💰 <b>Пользователь сообщил об оплате</b>\n\n"
        + _request_card(request, meta)
        + "\n\nПодтвердить поступление может только руководитель сервиса."
    )


@require_auth
async def payment_reported_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = _actor_meta(update)
    if not query or not actor:
        return
    match = re.fullmatch(r"subscription:paid:(\d+)", query.data or "")
    if not match:
        return
    await query.answer()
    request_id = int(match.group(1))
    uid = int(actor.get("user_id") or 0)

    def _report(cfg: UserData) -> str:
        request = cfg.service_requests.get(str(request_id))
        if not isinstance(request, dict) or int(request.get("user_id", 0) or 0) != uid:
            return "missing"
        status = str(request.get("status") or "")
        if status == "payment_reported":
            return "already"
        if status != "requisites_sent":
            return "stale"
        request = dict(request)
        request.update({"status": "payment_reported", "payment_reported_at": _now_iso(), "updated_at": _now_iso()})
        cfg.service_requests[str(request_id)] = request
        owner = _owner_meta_from_cfg(cfg)
        if owner:
            _queue_message(
                cfg,
                recipient_ids=[int(owner.get("user_id") or 0)],
                kind="payment_reported",
                text=_payment_report_notification(request, actor),
                reply_markup=[
                    [
                        {"text": "✅ Подтвердить", "callback_data": f"product:req:confirm:{request_id}"},
                        {"text": "🔎 Не найден", "callback_data": f"product:req:notfound:{request_id}"},
                    ],
                    [{"text": "👤 Профиль", "callback_data": f"users:user:{uid}"}],
                ],
            )
        return "reported"

    outcome = await update_user_data(_report)
    texts = {
        "reported": "✅ Информация об оплате отправлена руководителю сервиса. Ожидайте подтверждения.",
        "already": "Оплата уже ожидает проверки руководителем сервиса.",
        "missing": "Заявка не найдена.",
        "stale": "Эта платёжная кнопка больше неактивна.",
    }
    await query.edit_message_text(
        texts[outcome],
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]]),
    )


@require_auth
async def renewal_reported_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = _actor_meta(update)
    if not query or not actor:
        return
    await query.answer()
    uid = int(actor.get("user_id") or 0)
    if actor.get("service_tier") != "subscriber" or not actor.get("is_paid"):
        await query.edit_message_text("Продление недоступно для текущего уровня доступа.")
        return
    actor_end = _parse_dt(actor.get("subscription_end_at"))
    if actor_end is None or not (timedelta(0) <= actor_end - _now() <= timedelta(days=3)):
        await query.edit_message_text("Сообщить о продлении можно начиная за 3 дня до окончания доступа.")
        return

    def _create(cfg: UserData) -> tuple[str, int | None]:
        current = cfg.authorized_users.get(str(uid))
        if not isinstance(current, dict) or current.get("service_tier") != "subscriber" or not current.get("is_paid"):
            return "denied", None
        current_end = _parse_dt(current.get("subscription_end_at"))
        if current_end is None or not (timedelta(0) <= current_end - _now() <= timedelta(days=3)):
            return "not_due", None
        existing = _active_request(cfg, user_id=uid, kind="renewal")
        if existing:
            return "exists", int(existing.get("id", 0) or 0)
        target = _payment_target(cfg.product_settings, after=current_end or _now())
        if target is None or not _payment_profile_ready(cfg.product_settings):
            return "not_configured", None
        request = _new_request(
            cfg,
            kind="renewal",
            user_id=uid,
            status="payment_reported",
            target_end_at=target.isoformat(),
        )
        request_id = int(request["id"])
        owner = _owner_meta_from_cfg(cfg)
        if owner:
            _queue_message(
                cfg,
                recipient_ids=[int(owner.get("user_id") or 0)],
                kind="renewal_payment_reported",
                text=_payment_report_notification(request, current),
                reply_markup=[
                    [
                        {"text": "✅ Подтвердить", "callback_data": f"product:req:confirm:{request_id}"},
                        {"text": "🔎 Не найден", "callback_data": f"product:req:notfound:{request_id}"},
                    ],
                    [{"text": "👤 Профиль", "callback_data": f"users:user:{uid}"}],
                ],
            )
        return "created", request_id

    outcome, _request_id = await update_user_data(_create)
    texts = {
        "created": "✅ Информация о продлении отправлена руководителю сервиса.",
        "exists": "Продление уже ожидает проверки.",
        "not_configured": "Следующий платёжный период или реквизиты ещё не настроены. Создайте тикет.",
        "not_due": "Сообщить о продлении можно начиная за 3 дня до окончания доступа.",
        "denied": "Продление недоступно для текущего уровня доступа.",
    }
    await query.edit_message_text(
        texts[outcome],
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("🎫 Создать тикет", callback_data="menu:ticket")],
                [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
            ]
        ),
    )


def _finalize_trial(
    cfg: UserData, request: dict[str, Any], actor: dict[str, Any], connection_url: str | None
) -> dict[str, Any]:
    if not is_admin_meta(actor):
        raise ValueError("admin_required")
    uid = int(request.get("user_id", 0) or 0)
    current = cfg.authorized_users.get(str(uid))
    if not isinstance(current, dict):
        raise ValueError("user_missing")
    if current.get("role") == "admin" or current.get("service_tier") != "basic":
        raise ValueError("tier_changed")
    if current.get("trial_issued_at"):
        raise ValueError("already_issued")
    updated = dict(current)
    if connection_url:
        updated[CONNECTION_URL_KEY] = connection_url
        updated["subscription_updated_at"] = _now_iso()
        updated["subscription_updated_by_id"] = actor.get("user_id")
        updated["subscription_updated_by_name"] = staff_public_signature(actor)
    updated["trial_issued_at"] = _now_iso()
    updated["trial_issued_by_id"] = actor.get("user_id")
    updated["trial_issued_by_name"] = staff_public_signature(actor)
    updated = UserData._normalize_user(updated)
    cfg.authorized_users[str(uid)] = updated
    finished = dict(request)
    finished.update(
        {
            "status": "approved",
            "reviewed_by_id": actor.get("user_id"),
            "reviewed_at": _now_iso(),
            "updated_at": _now_iso(),
            "claimed_by_id": None,
            "claimed_at": None,
        }
    )
    cfg.service_requests[str(request["id"])] = finished
    _queue_message(
        cfg,
        recipient_ids=[uid],
        kind="trial_approved",
        text=(
            "🧪 <b>Тестовый доступ одобрен</b>\n\n"
            "Для вашей учётной записи подготовлена персональная ссылка подключения. "
            "Тест не меняет базовый уровень доступа в боте."
        ),
    )
    enqueue_user_outbox(
        cfg,
        make_outbox_event(
            kind="trial_connection",
            recipient_ids=[uid],
            payload=connection_outbox_payload(updated),
        ),
    )
    append_audit_entry(
        cfg,
        action="trial_approved",
        actor_meta=actor,
        target_user_id=uid,
        details={"request_id": request.get("id")},
    )
    return updated


def _finalize_payment(
    cfg: UserData,
    request: dict[str, Any],
    actor: dict[str, Any],
    *,
    connection_url: str | None = None,
) -> dict[str, Any]:
    if not is_owner_meta(actor):
        raise ValueError("owner_required")
    uid = int(request.get("user_id", 0) or 0)
    current = cfg.authorized_users.get(str(uid))
    if not isinstance(current, dict):
        raise ValueError("user_missing")
    target = _parse_dt(request.get("target_end_at"))
    if target is None or target <= _now():
        raise ValueError("invalid_target")
    updated = dict(current)
    if connection_url:
        updated[CONNECTION_URL_KEY] = connection_url
        updated["subscription_updated_at"] = _now_iso()
        updated["subscription_updated_by_id"] = actor.get("user_id")
        updated["subscription_updated_by_name"] = staff_public_signature(actor)
    if not str(updated.get(CONNECTION_URL_KEY) or "").strip():
        raise ValueError("connection_missing")
    now = _now_iso()
    updated.update(
        {
            "service_tier": "subscriber",
            "is_paid": True,
            "paid_at": now,
            "payment_confirmed_by_id": actor.get("user_id"),
            "payment_confirmed_by_name": staff_public_signature(actor, allow_alias=False),
            "subscription_end_at": target.isoformat(),
            "payment_auto_reminders": {},
            "service_tier_updated_at": now,
            "service_tier_updated_by_id": actor.get("user_id"),
            "service_tier_updated_by_name": staff_public_signature(actor, allow_alias=False),
        }
    )
    updated = UserData._normalize_user(updated)
    cfg.authorized_users[str(uid)] = updated
    finished = dict(request)
    finished.update(
        {
            "status": "approved",
            "reviewed_by_id": actor.get("user_id"),
            "reviewed_at": now,
            "updated_at": now,
            "claimed_by_id": None,
            "claimed_at": None,
        }
    )
    cfg.service_requests[str(request["id"])] = finished
    _cancel_active_requests(
        cfg,
        user_id=uid,
        reason="payment_activated",
        exclude_request_id=int(request.get("id", 0) or 0),
    )
    _queue_message(
        cfg,
        recipient_ids=[uid],
        kind="payment_approved",
        text=(
            "✅ <b>Доступ к сервису активирован</b>\n\n"
            "Оплата подтверждена. Для вашей учётной записи открыт полный доступ к функциям сервиса.\n\n"
            f"Доступ оплачен до: <code>{html_escape(_dt_text(target.isoformat()))}</code>"
        ),
    )
    if connection_url:
        enqueue_user_outbox(
            cfg,
            make_outbox_event(
                kind="payment_connection",
                recipient_ids=[uid],
                payload=connection_outbox_payload(updated),
            ),
        )
    append_audit_entry(
        cfg,
        action="payment_confirmed",
        actor_meta=actor,
        target_user_id=uid,
        details={"request_id": request.get("id"), "target_end_at": target.isoformat()},
    )
    return updated


@require_admin
async def product_request_action_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    actor = _actor_meta(update)
    if not query or not actor:
        return ConversationHandler.END
    match = re.fullmatch(r"product:req:(approve|reject|requisites|confirm|notfound):(\d+)", query.data or "")
    if not match:
        return ConversationHandler.END
    action, request_id_text = match.groups()
    request_id = int(request_id_text)
    await query.answer()

    if action == "approve":

        def _approve(cfg: UserData) -> tuple[str, dict[str, Any] | None]:
            request = cfg.service_requests.get(str(request_id))
            if not isinstance(request, dict) or request.get("kind") != "trial":
                return "missing", None
            if request.get("status") not in {"pending", "claimed", "awaiting_link"}:
                return "stale", request
            if request.get("claimed_by_id") not in (None, actor.get("user_id")):
                return "claimed", request
            uid = int(request.get("user_id", 0) or 0)
            current = cfg.authorized_users.get(str(uid))
            if not isinstance(current, dict):
                return "missing", request
            if current.get("role") == "admin" or current.get("service_tier") != "basic":
                _cancel_active_requests(cfg, user_id=uid, reason="service_tier_changed", kinds={"trial"})
                return "tier_changed", request
            if current.get("trial_issued_at"):
                return "already_issued", request
            if has_connection(current):
                _finalize_trial(cfg, request, actor, None)
                return "completed", request
            updated_request = dict(request)
            updated_request.update(
                {
                    "status": "awaiting_link",
                    "resume_status": "pending",
                    "claimed_by_id": actor.get("user_id"),
                    "claimed_at": _now_iso(),
                    "updated_at": _now_iso(),
                }
            )
            cfg.service_requests[str(request_id)] = updated_request
            return "need_link", updated_request

        outcome, _request = await update_user_data(_approve)
        if outcome == "need_link":
            _clear_product_context(context)
            data = _context_data(context)
            data[_CTX_ACTION] = "request_link"
            data[_CTX_REQUEST_ID] = request_id
            await query.edit_message_text(
                "🔗 Вставьте персональную ссылку подключения одним сообщением. Поддерживаются только ссылки HTTP/HTTPS.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("❌ Отмена", callback_data="product:cancel")]]
                ),
            )
            return PRODUCT_INPUT
        messages = {
            "completed": "Тестовый доступ одобрен, существующая ссылка отправлена пользователю.",
            "claimed": "Заявку уже обрабатывает другой сотрудник.",
            "stale": "Заявка уже обработана.",
            "tier_changed": "Уровень пользователя уже изменился; заявка на тест отменена.",
            "already_issued": "Тестовый доступ уже выдавался.",
            "missing": "Заявка или пользователь не найдены.",
        }
        await query.edit_message_text(messages.get(outcome, "Заявка не обработана."))
        return ConversationHandler.END

    if action == "requisites":

        def _requisites(cfg: UserData) -> tuple[str, dict[str, Any] | None]:
            request = cfg.service_requests.get(str(request_id))
            if not isinstance(request, dict) or request.get("kind") != "purchase":
                return "missing", None
            if request.get("status") == "requisites_sent":
                return "already_sent", request
            if request.get("status") != "pending":
                return "stale", request
            uid = int(request.get("user_id", 0) or 0)
            current = cfg.authorized_users.get(str(uid))
            if not isinstance(current, dict):
                return "missing", request
            if current.get("role") == "admin" or current.get("service_tier") != "basic":
                _cancel_active_requests(cfg, user_id=uid, reason="service_tier_changed", kinds={"purchase"})
                return "tier_changed", request
            target = _parse_dt(request.get("target_end_at"))
            if target is None or target <= _now():
                target = _payment_target(cfg.product_settings)
            if target is None or target <= _now() or not _payment_profile_ready(cfg.product_settings):
                return "not_configured", request
            updated = dict(request)
            updated.update(
                {
                    "status": "requisites_sent",
                    "target_end_at": target.isoformat(),
                    "updated_at": _now_iso(),
                    "reviewed_by_id": actor.get("user_id"),
                    "reviewed_at": _now_iso(),
                }
            )
            cfg.service_requests[str(request_id)] = updated
            _queue_message(
                cfg,
                recipient_ids=[int(updated.get("user_id", 0) or 0)],
                kind="payment_requisites",
                text=_payment_message(cfg.product_settings, updated),
                reply_markup=_payment_markup(request_id),
            )
            return "sent", updated

        outcome, _request = await update_user_data(_requisites)
        messages = {
            "sent": "Реквизиты поставлены в очередь отправки пользователю.",
            "already_sent": "Реквизиты уже были отправлены пользователю.",
            "tier_changed": "Уровень пользователя уже изменился; заявка отменена.",
            "not_configured": "Реквизиты или дата периода заполнены не полностью.",
            "stale": "Заявка уже перешла на другой этап.",
            "missing": "Заявка не найдена.",
        }
        await query.edit_message_text(messages[outcome])
        return ConversationHandler.END

    if action == "reject":

        def _reject(cfg: UserData) -> str:
            request = cfg.service_requests.get(str(request_id))
            if not isinstance(request, dict) or request.get("status") not in ACTIVE_REQUEST_STATUSES:
                return "stale"
            if request.get("status") == "awaiting_link" and request.get("claimed_by_id") not in (
                None,
                actor.get("user_id"),
            ):
                return "claimed"
            if request.get("status") == "payment_reported" and not is_owner_meta(actor):
                return "owner_only"
            updated = dict(request)
            updated.update(
                {
                    "status": "rejected",
                    "updated_at": _now_iso(),
                    "reviewed_at": _now_iso(),
                    "reviewed_by_id": actor.get("user_id"),
                    "claimed_by_id": None,
                    "claimed_at": None,
                }
            )
            cfg.service_requests[str(request_id)] = updated
            text = {
                "trial": "❌ Запрос тестового доступа отклонён.",
                "purchase": "❌ Заявка на покупку подписки отклонена. При необходимости создайте тикет в поддержку.",
                "renewal": "❌ Запрос на продление подписки отклонён. При необходимости создайте тикет в поддержку.",
            }.get(str(request.get("kind") or ""), "❌ Заявка отклонена.")
            _queue_message(
                cfg,
                recipient_ids=[int(request.get("user_id", 0) or 0)],
                kind="service_request_rejected",
                text=text,
            )
            append_audit_entry(
                cfg,
                action="service_request_rejected",
                actor_meta=actor,
                target_user_id=int(request.get("user_id", 0) or 0),
                details={"request_id": request_id, "kind": request.get("kind")},
            )
            return "rejected"

        outcome = await update_user_data(_reject)
        await query.edit_message_text(
            {
                "rejected": "Заявка отклонена.",
                "owner_only": "Платёжное решение может принять только руководитель сервиса.",
                "claimed": "Заявку уже обрабатывает другой сотрудник.",
                "stale": "Заявка уже обработана.",
            }[outcome]
        )
        return ConversationHandler.END

    if not is_owner_meta(actor):
        await query.edit_message_text("Подтверждать оплату может только руководитель сервиса.")
        return ConversationHandler.END

    if action == "notfound":

        def _not_found(cfg: UserData) -> str:
            request = cfg.service_requests.get(str(request_id))
            if not isinstance(request, dict) or request.get("status") != "payment_reported":
                return "stale"
            updated = dict(request)
            updated.update(
                {
                    "status": "requisites_sent",
                    "updated_at": _now_iso(),
                    "payment_reported_at": None,
                    "reviewed_by_id": actor.get("user_id"),
                    "reviewed_at": _now_iso(),
                }
            )
            cfg.service_requests[str(request_id)] = updated
            _queue_message(
                cfg,
                recipient_ids=[int(request.get("user_id", 0) or 0)],
                kind="payment_not_found",
                text=(
                    "🔎 <b>Платёж пока не найден</b>\n\n"
                    "Проверьте реквизиты и статус перевода. После поступления платежа нажмите кнопку ещё раз. "
                    "Если возникли вопросы, создайте тикет."
                ),
                reply_markup=_payment_markup(request_id),
            )
            return "reset"

        outcome = await update_user_data(_not_found)
        await query.edit_message_text("Пользователь уведомлён." if outcome == "reset" else "Заявка уже обработана.")
        return ConversationHandler.END

    def _confirm(cfg: UserData) -> tuple[str, dict[str, Any] | None]:
        request = cfg.service_requests.get(str(request_id))
        if not isinstance(request, dict) or request.get("kind") not in {"purchase", "renewal"}:
            return "missing", None
        if request.get("status") == "awaiting_link":
            if int(request.get("claimed_by_id", 0) or 0) != int(actor.get("user_id", 0) or 0):
                return "claimed", request
            refreshed = dict(request)
            refreshed.update({"claimed_at": _now_iso(), "updated_at": _now_iso()})
            cfg.service_requests[str(request_id)] = refreshed
            return "need_link", refreshed
        if request.get("status") != "payment_reported":
            return "stale", request
        uid = int(request.get("user_id", 0) or 0)
        current = cfg.authorized_users.get(str(uid))
        if not isinstance(current, dict):
            return "missing", request
        target = _parse_dt(request.get("target_end_at"))
        if target is None or target <= _now():
            return "invalid_target", request
        if has_connection(current):
            _finalize_payment(cfg, request, actor)
            return "completed", request
        updated_request = dict(request)
        updated_request.update(
            {
                "status": "awaiting_link",
                "resume_status": "payment_reported",
                "claimed_by_id": actor.get("user_id"),
                "claimed_at": _now_iso(),
                "updated_at": _now_iso(),
            }
        )
        cfg.service_requests[str(request_id)] = updated_request
        return "need_link", updated_request

    outcome, _request = await update_user_data(_confirm)
    if outcome == "need_link":
        _clear_product_context(context)
        data = _context_data(context)
        data[_CTX_ACTION] = "payment_link"
        data[_CTX_REQUEST_ID] = request_id
        await query.edit_message_text(
            "Оплата найдена, но у пользователя нет персональной ссылки. Вставьте ссылку HTTP/HTTPS для завершения активации.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="product:cancel")]]),
        )
        return PRODUCT_INPUT
    messages = {
        "completed": "Оплата подтверждена, доступ пользователя активирован.",
        "claimed": "Ввод ссылки уже выполняет другой руководитель сервиса.",
        "invalid_target": "Дата оплачиваемого периода уже истекла. Исправьте период или зарегистрируйте оплату вручную.",
        "stale": "Заявка уже обработана.",
        "missing": "Заявка или пользователь не найдены.",
    }
    await query.edit_message_text(messages[outcome])
    return ConversationHandler.END


def _staff_profile_text(meta: dict[str, Any]) -> str:
    mode = (
        "только должность" if meta.get("staff_display_mode") != STAFF_DISPLAY_TITLE_ALIAS else "должность и псевдоним"
    )
    return (
        "👤 <b>Профиль сотрудника</b>\n\n"
        f"• Публичная подпись: <b>{html_escape(staff_public_signature(meta))}</b>\n"
        f"• Должность: <b>{html_escape(staff_title_label(meta))}</b>\n"
        f"• Псевдоним: <b>{html_escape(str(meta.get('staff_alias') or '-'))}</b>\n"
        f"• Режим: <b>{html_escape(mode)}</b>\n\n"
        f"• Внутренняя личность: <code>{html_escape(staff_internal_identity(meta))}</code>"
    )


def _staff_profile_markup(meta: dict[str, Any]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🏷 Изменить псевдоним", callback_data="staff:alias")],
        [
            InlineKeyboardButton("Только должность", callback_data="staff:mode:title"),
            InlineKeyboardButton("Должность + псевдоним", callback_data="staff:mode:title_alias"),
        ],
    ]
    if is_lead_or_owner_meta(meta):
        rows.append(
            [
                InlineKeyboardButton("📅 Массовая дата", callback_data="product:input:massdate"),
                InlineKeyboardButton("🔔 Массово напомнить", callback_data="product:input:massremind"),
            ]
        )
    if is_owner_meta(meta):
        rows.append([InlineKeyboardButton("⚙️ Настройки сервиса", callback_data="product:owner")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


@require_admin
async def staff_profile_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = _actor_meta(update)
    if not query or not actor:
        return
    await query.answer()
    await query.edit_message_text(
        _staff_profile_text(actor),
        parse_mode=ParseMode.HTML,
        reply_markup=_staff_profile_markup(actor),
    )


@require_admin
async def staff_mode_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = _actor_meta(update)
    if not query or not actor:
        return
    match = re.fullmatch(r"staff:mode:(title|title_alias)", query.data or "")
    if not match:
        return
    await query.answer()
    mode = match.group(1)
    if mode == STAFF_DISPLAY_TITLE_ALIAS and not actor.get("staff_alias"):
        await query.edit_message_text(
            "Сначала задайте псевдоним.",
            reply_markup=_staff_profile_markup(actor),
        )
        return
    uid = int(actor.get("user_id") or 0)
    updated = await update_user_data(lambda cfg: _update_staff_mode(cfg, uid=uid, mode=mode))
    await query.edit_message_text(
        _staff_profile_text(updated),
        parse_mode=ParseMode.HTML,
        reply_markup=_staff_profile_markup(updated),
    )


def _update_staff_mode(cfg: UserData, *, uid: int, mode: str) -> dict[str, Any]:
    current = cfg.authorized_users.get(str(uid))
    if not isinstance(current, dict) or current.get("role") != "admin":
        raise ValueError("admin_missing")
    old_mode = str(current.get("staff_display_mode") or STAFF_DISPLAY_TITLE)
    updated = UserData._normalize_user({**current, "staff_display_mode": mode})
    cfg.authorized_users[str(uid)] = updated
    append_audit_entry(
        cfg,
        action="staff_display_mode_changed",
        actor_meta=updated,
        target_user_id=uid,
        details={"old": old_mode, "new": updated.get("staff_display_mode")},
    )
    return updated


def _owner_panel_text(settings: dict[str, Any]) -> str:
    return (
        "⚙️ <b>Настройки сервиса</b>\n\n"
        f"• Банк: <b>{html_escape(str(settings.get('payment_bank') or '-'))}</b>\n"
        f"• Получатель: <b>{html_escape(str(settings.get('payment_recipient') or '-'))}</b>\n"
        f"• Телефон: <code>{html_escape(str(settings.get('payment_phone') or '-'))}</code>\n"
        f"• Тариф: <b>{PLAN_TOTAL_RUB} ₽ / {PLAN_MONTHS} месяца</b>\n\n"
        f"• Текущий период до: <code>{html_escape(_dt_text(settings.get('current_period_end')))}</code>\n"
        f"• Следующий период до: <code>{html_escape(_dt_text(settings.get('next_period_end')))}</code>\n\n"
        "Изменение следующего периода не продлевает пользователей автоматически."
    )


def _owner_panel_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🏦 Банк", callback_data="product:input:setting_bank"),
                InlineKeyboardButton("👤 Получатель", callback_data="product:input:setting_recipient"),
            ],
            [InlineKeyboardButton("📱 Телефон", callback_data="product:input:setting_phone")],
            [
                InlineKeyboardButton("📅 Текущий период", callback_data="product:input:setting_current"),
                InlineKeyboardButton("⏭ Следующий период", callback_data="product:input:setting_next"),
            ],
            [InlineKeyboardButton("⬅️ Профиль сотрудника", callback_data="staff:profile")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


@require_admin
async def owner_panel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = _actor_meta(update)
    if not query or not actor:
        return
    await query.answer()
    if not is_owner_meta(actor):
        await query.edit_message_text("Доступно только руководителю сервиса.")
        return
    await query.edit_message_text(
        _owner_panel_text(product_settings_snapshot()),
        parse_mode=ParseMode.HTML,
        reply_markup=_owner_panel_markup(),
    )


@require_admin
async def product_manage_user_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = _actor_meta(update)
    if not query or not actor:
        return
    match = re.fullmatch(r"product:manage:(\d+)", query.data or "")
    if not match:
        return
    await query.answer()
    uid = int(match.group(1))
    target = get_user_meta_copy(uid)
    if not target:
        await query.edit_message_text("Пользователь не найден.")
        return
    rows: list[list[InlineKeyboardButton]] = []
    if is_lead_or_owner_meta(actor):
        rows.append(
            [
                InlineKeyboardButton("📅 Изменить дату", callback_data=f"product:input:user_end:{uid}"),
                InlineKeyboardButton("🔔 Напомнить", callback_data=f"product:remind:{uid}"),
            ]
        )
    if is_owner_meta(actor):
        rows.append(
            [InlineKeyboardButton("💰 Подтвердить оплату вручную", callback_data=f"product:input:manualpay:{uid}")]
        )
        rows.append(
            [
                InlineKeyboardButton("Базовый", callback_data=f"product:tier:{uid}:basic"),
                InlineKeyboardButton("Безлимитный", callback_data=f"product:tier:{uid}:unlimited_trial"),
            ]
        )
        if is_admin_meta(target) and not is_owner_meta(target):
            rows.append([InlineKeyboardButton("🪪 Изменить должность", callback_data=f"product:titlemenu:{uid}")])
    rows.append([InlineKeyboardButton("⬅️ Профиль пользователя", callback_data=f"users:user:{uid}")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    text = (
        "⚙️ <b>Управление доступом</b>\n\n"
        f"• Пользователь: <b>{html_escape(_real_user_name(target))}</b>\n"
        f"• ID: <code>{uid}</code>\n"
        f"• Уровень: <b>{html_escape(_service_tier_label(target.get('service_tier')))}</b>\n"
        f"• Оплата: <b>{'подтверждена' if target.get('is_paid') else 'не подтверждена'}</b>\n"
        f"• Доступ до: <code>{html_escape(_dt_text(target.get('subscription_end_at')))}</code>"
    )
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(rows))


@require_admin
async def product_title_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = _actor_meta(update)
    if not query or not actor:
        return
    match = re.fullmatch(r"product:titlemenu:(\d+)", query.data or "")
    if not match:
        return
    await query.answer()
    if not is_owner_meta(actor):
        await query.edit_message_text("Доступно только руководителю сервиса.")
        return
    uid = int(match.group(1))
    target = get_user_meta_copy(uid)
    if not target or not is_admin_meta(target) or is_owner_meta(target):
        await query.edit_message_text("Должность этого пользователя изменить нельзя.")
        return
    rows = [
        [InlineKeyboardButton(STAFF_TITLE_LABELS[code], callback_data=f"product:title:{uid}:{code}")]
        for code in REGULAR_STAFF_TITLES
    ]
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"product:manage:{uid}")])
    await query.edit_message_text(
        f"🪪 <b>Должность сотрудника</b>\n\nТекущая: <b>{html_escape(staff_title_label(target))}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


@require_admin
async def product_title_apply_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = _actor_meta(update)
    if not query or not actor:
        return
    match = re.fullmatch(r"product:title:(\d+):([a-z_]+)", query.data or "")
    if not match:
        return
    await query.answer()
    if not is_owner_meta(actor):
        await query.edit_message_text("Доступно только руководителю сервиса.")
        return
    uid = int(match.group(1))
    title_code = match.group(2)
    if title_code not in REGULAR_STAFF_TITLES:
        await query.edit_message_text("Неизвестная должность.")
        return

    def _apply(cfg: UserData) -> dict[str, Any] | None:
        target = cfg.authorized_users.get(str(uid))
        if not isinstance(target, dict) or not is_admin_meta(target) or is_owner_meta(target):
            return None
        old_title = staff_title_label(target)
        updated = UserData._normalize_user({**target, "staff_title": title_code})
        cfg.authorized_users[str(uid)] = updated
        append_audit_entry(
            cfg,
            action="staff_title_changed",
            actor_meta=actor,
            target_user_id=uid,
            details={"old": old_title, "new": STAFF_TITLE_LABELS[title_code]},
        )
        return updated

    updated = await update_user_data(_apply)
    if not updated:
        await query.edit_message_text("Сотрудник не найден.")
        return
    await query.edit_message_text(
        ui_ok_text(f"Должность изменена: {STAFF_TITLE_LABELS[title_code]}"),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data=f"product:manage:{uid}")]]),
    )


@require_admin
async def product_tier_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = _actor_meta(update)
    if not query or not actor:
        return
    match = re.fullmatch(r"product:tier:(\d+):(basic|unlimited_trial)", query.data or "")
    if not match:
        return
    await query.answer()
    if not is_owner_meta(actor):
        await query.edit_message_text("Доступно только руководителю сервиса.")
        return
    uid = int(match.group(1))
    tier = match.group(2)

    def _apply(cfg: UserData) -> dict[str, Any] | None:
        current = cfg.authorized_users.get(str(uid))
        if not isinstance(current, dict) or current.get("role") == "admin":
            return None
        old = str(current.get("service_tier") or "basic")
        updated = dict(current)
        updated.update(
            {
                "service_tier": tier,
                "is_paid": False,
                "subscription_end_at": None,
                "service_tier_updated_at": _now_iso(),
                "service_tier_updated_by_id": actor.get("user_id"),
                "service_tier_updated_by_name": staff_public_signature(actor, allow_alias=False),
            }
        )
        updated = UserData._normalize_user(updated)
        cfg.authorized_users[str(uid)] = updated
        _cancel_active_requests(cfg, user_id=uid, reason="service_tier_changed")
        append_audit_entry(
            cfg,
            action="service_tier_changed",
            actor_meta=actor,
            target_user_id=uid,
            details={"old": old, "new": tier},
        )
        return updated

    updated = await update_user_data(_apply)
    if not updated:
        await query.edit_message_text("Уровень этого пользователя изменить нельзя.")
        return
    await query.edit_message_text(
        ui_ok_text(f"Назначен уровень: {_service_tier_label(tier)}"),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data=f"product:manage:{uid}")]]),
    )


@require_admin
async def product_input_start_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    actor = _actor_meta(update)
    if not query or not actor:
        return ConversationHandler.END
    data = query.data or ""
    action = ""
    target_uid: int | None = None
    prompt = ""
    if data == "staff:alias":
        action = "staff_alias"
        prompt = "Введите псевдоним длиной от 2 до 32 символов. Для удаления отправьте один дефис: <code>-</code>"
    elif data in {
        "product:input:setting_bank",
        "product:input:setting_recipient",
        "product:input:setting_phone",
        "product:input:setting_current",
        "product:input:setting_next",
    }:
        if not is_owner_meta(actor):
            await query.answer("Доступно только руководителю сервиса.", show_alert=True)
            return ConversationHandler.END
        action = data.removeprefix("product:input:")
        prompts = {
            "setting_bank": "Введите название банка:",
            "setting_recipient": "Введите имя получателя платежа:",
            "setting_phone": "Введите номер телефона для перевода:",
            "setting_current": "Введите окончание текущего периода в формате <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>:",
            "setting_next": "Введите окончание следующего периода в формате <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>:",
        }
        prompt = prompts[action]
    elif data == "product:input:massdate":
        if not is_lead_or_owner_meta(actor):
            await query.answer("Недостаточно прав.", show_alert=True)
            return ConversationHandler.END
        action = "mass_date"
        prompt = (
            "Введите новую дату в формате <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>.\n\n"
            "Для выбранных пользователей добавьте после вертикальной черты Telegram ID через запятую:\n"
            "<code>31.10.2026 23:59 | 123456, 789012</code>\n\n"
            "Без списка дата будет назначена всем оплаченным подписчикам."
        )
    elif data == "product:input:massremind":
        if not is_lead_or_owner_meta(actor):
            await query.answer("Недостаточно прав.", show_alert=True)
            return ConversationHandler.END
        action = "mass_reminder"
        prompt = (
            "Укажите получателей:\n\n"
            "• <code>все</code> — все оплаченные подписчики;\n"
            "• <code>до 31.10.2026 23:59</code> — подписчики с окончанием не позднее даты;\n"
            "• <code>123456, 789012</code> — конкретные Telegram ID."
        )
    else:
        match = re.fullmatch(r"product:input:(user_end|manualpay):(\d+)", data)
        if not match:
            return ConversationHandler.END
        action, uid_text = match.groups()
        target_uid = int(uid_text)
        if action == "user_end" and not is_lead_or_owner_meta(actor):
            await query.answer("Недостаточно прав.", show_alert=True)
            return ConversationHandler.END
        if action == "manualpay" and not is_owner_meta(actor):
            await query.answer("Оплату подтверждает только руководитель сервиса.", show_alert=True)
            return ConversationHandler.END
        prompt = (
            "Введите новую дату окончания в формате <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>:"
            if action == "user_end"
            else "Введите дату окончания оплаченного доступа в формате <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>:"
        )
    await query.answer()
    _clear_product_context(context)
    context_state = _context_data(context)
    context_state[_CTX_ACTION] = action
    if target_uid is not None:
        context_state[_CTX_TARGET_UID] = target_uid
    await query.edit_message_text(
        prompt,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="product:cancel")]]),
    )
    return PRODUCT_INPUT


def _trial_comment_apply(cfg: UserData, *, uid: int, comment: str) -> tuple[str, int | None]:
    current = cfg.authorized_users.get(str(uid))
    if not isinstance(current, dict) or current.get("service_tier") != "basic" or current.get("role") == "admin":
        return "denied", None
    if current.get("trial_issued_at"):
        return "issued", None
    existing = _active_request(cfg, user_id=uid, kind="trial")
    if existing:
        return "exists", int(existing.get("id", 0) or 0)
    request = _new_request(cfg, kind="trial", user_id=uid, comment=comment)
    request_id = int(request["id"])
    _queue_message(
        cfg,
        recipient_ids=_approved_admin_ids(cfg),
        kind="trial_request",
        text=_request_card(request, current),
        reply_markup=[
            [
                {"text": "✅ Одобрить", "callback_data": f"product:req:approve:{request_id}"},
                {"text": "❌ Отклонить", "callback_data": f"product:req:reject:{request_id}"},
            ],
            [{"text": "👤 Профиль", "callback_data": f"users:user:{uid}"}],
        ],
    )
    return "created", request_id


async def _complete_connection_input(
    *,
    action: str,
    request_id: int,
    connection_url: str,
    actor: dict[str, Any],
) -> str:
    def _apply(cfg: UserData) -> str:
        request = cfg.service_requests.get(str(request_id))
        if not isinstance(request, dict) or request.get("status") != "awaiting_link":
            return "stale"
        if int(request.get("claimed_by_id", 0) or 0) != int(actor.get("user_id", 0) or 0):
            return "claimed"
        if action == "request_link" and request.get("kind") == "trial":
            try:
                _finalize_trial(cfg, request, actor, connection_url)
            except ValueError as exc:
                code = str(exc)
                updated_request = dict(request)
                updated_request.update(
                    {
                        "status": "cancelled"
                        if code in {"tier_changed", "already_issued", "user_missing"}
                        else "pending",
                        "decision_reason": code,
                        "claimed_by_id": None,
                        "claimed_at": None,
                        "updated_at": _now_iso(),
                    }
                )
                cfg.service_requests[str(request_id)] = updated_request
                return code
            return "completed"
        if action == "payment_link" and request.get("kind") in {"purchase", "renewal"} and is_owner_meta(actor):
            try:
                _finalize_payment(cfg, request, actor, connection_url=connection_url)
            except ValueError as exc:
                code = str(exc)
                updated_request = dict(request)
                updated_request.update(
                    {
                        "status": (
                            "cancelled"
                            if code == "user_missing"
                            else str(request.get("resume_status") or "payment_reported")
                        ),
                        "decision_reason": code,
                        "claimed_by_id": None,
                        "claimed_at": None,
                        "updated_at": _now_iso(),
                    }
                )
                cfg.service_requests[str(request_id)] = updated_request
                return code
            return "completed"
        return "stale"

    return await update_user_data(_apply)


def _is_paid_subscriber(meta: dict[str, Any]) -> bool:
    return bool(meta.get("role") != "admin" and meta.get("service_tier") == "subscriber" and meta.get("is_paid"))


def _eligible_paid_subscriber(meta: dict[str, Any]) -> bool:
    return bool(
        _is_paid_subscriber(meta) and meta.get("access_state") == "approved" and bool(meta.get("enabled", True))
    )


def _parse_id_list(raw: str) -> set[int] | None:
    values = [part.strip() for part in raw.split(",") if part.strip()]
    if not values or any(not value.isdigit() for value in values):
        return None
    return {int(value) for value in values if int(value) > 0}


def _manual_reminder_text(meta: dict[str, Any], settings: dict[str, Any], actor: dict[str, Any]) -> str:
    end = _parse_dt(meta.get("subscription_end_at"))
    target = _payment_target(settings, after=end or _now())
    can_report_payment = bool(
        end and timedelta(0) <= end - _now() <= timedelta(days=3) and target and _payment_profile_ready(settings)
    )
    lines = [
        "✉️ <b>Персональное напоминание об оплате</b>",
        "",
        f"Отправитель: <b>{html_escape(staff_title_label(actor))}</b>",
        "",
        f"Текущий доступ до: <code>{html_escape(_dt_text(meta.get('subscription_end_at')))}</code>",
        f"Стоимость продления: <b>{PLAN_TOTAL_RUB} ₽ за {PLAN_MONTHS} месяца</b>",
    ]
    if target:
        lines.append(f"Следующий период до: <code>{html_escape(_dt_text(target.isoformat()))}</code>")
    if _payment_profile_ready(settings):
        lines.extend(
            [
                "",
                f"Банк: <b>{html_escape(str(settings.get('payment_bank')))}</b>",
                f"Получатель: <b>{html_escape(str(settings.get('payment_recipient')))}</b>",
                f"Телефон: <code>{html_escape(str(settings.get('payment_phone')))}</code>",
            ]
        )
    if can_report_payment:
        lines.extend(["", "После перевода нажмите «Я оплатил продление». При вопросах создайте тикет."])
    else:
        lines.extend(["", "Кнопка оплаты появится за 3 дня до окончания. При вопросах создайте тикет."])
    return "\n".join(lines)


def _queue_manual_reminders(
    cfg: UserData,
    *,
    actor: dict[str, Any],
    target_ids: list[int],
) -> tuple[int, int]:
    sent = skipped = 0
    for uid in sorted(set(target_ids)):
        current = cfg.authorized_users.get(str(uid))
        end = _parse_dt(current.get("subscription_end_at")) if isinstance(current, dict) else None
        if not isinstance(current, dict) or not _eligible_paid_subscriber(current) or end is None:
            skipped += 1
            continue
        can_report_payment = bool(
            timedelta(0) <= end - _now() <= timedelta(days=3)
            and _payment_profile_ready(cfg.product_settings)
            and _payment_target(cfg.product_settings, after=end)
        )
        markup = [[{"text": "🎫 Создать тикет", "callback_data": "menu:ticket"}]]
        if can_report_payment:
            markup.insert(0, [{"text": "✅ Я оплатил продление", "callback_data": "subscription:renew"}])
        _queue_message(
            cfg,
            recipient_ids=[uid],
            kind="manual_payment_reminder",
            text=_manual_reminder_text(current, cfg.product_settings, actor),
            reply_markup=markup,
        )
        updated = UserData._normalize_user(
            {
                **current,
                "last_manual_payment_reminder_at": _now_iso(),
                "last_manual_payment_reminder_by_id": actor.get("user_id"),
                "last_manual_payment_reminder_by_name": staff_public_signature(actor, allow_alias=False),
            }
        )
        cfg.authorized_users[str(uid)] = updated
        append_audit_entry(
            cfg,
            action="manual_payment_reminder",
            actor_meta=actor,
            target_user_id=uid,
            details={},
        )
        sent += 1
    return sent, skipped


@require_auth
async def product_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    actor = _actor_meta(update)
    if not message or not actor:
        return ConversationHandler.END
    text = (message.text or "").strip()
    data = _context_data(context)
    action = str(data.get(_CTX_ACTION) or "")
    if not text:
        await message.reply_text("Пустое значение. Повторите ввод.")
        return PRODUCT_INPUT

    if action == "trial_comment":
        if len(text) > 1000:
            await message.reply_text("Комментарий слишком длинный. Максимум 1000 символов.")
            return PRODUCT_INPUT
        outcome, _request_id = await update_user_data(
            lambda cfg: _trial_comment_apply(cfg, uid=int(actor.get("user_id") or 0), comment=text)
        )
        _clear_product_context(context)
        await message.reply_text(
            {
                "created": "✅ Заявка на тестовый доступ отправлена.",
                "exists": "Заявка уже ожидает решения.",
                "issued": "Тестовый доступ уже выдавался ранее.",
                "denied": "Запрос недоступен для текущего уровня.",
            }[outcome],
            reply_markup=main_menu_inline_kb(update),
        )
        return ConversationHandler.END

    if action in {"request_link", "payment_link"}:
        request_id = int(data.get(_CTX_REQUEST_ID, 0) or 0)
        if not is_valid_connection_url(text):
            await message.reply_text(
                "Некорректная ссылка. Вставьте полную ссылку, начинающуюся с http:// или https://."
            )
            return PRODUCT_INPUT
        outcome = await _complete_connection_input(
            action=action,
            request_id=request_id,
            connection_url=text,
            actor=actor,
        )
        _clear_product_context(context)
        await message.reply_text(
            {
                "completed": "✅ Ссылка сохранена, заявка завершена и уведомление поставлено в очередь.",
                "claimed": "Заявку уже обрабатывает другой сотрудник.",
                "stale": "Заявка уже обработана или отменена.",
                "invalid_target": "Дата оплачиваемого периода уже истекла. Настройте период и повторите подтверждение.",
                "user_missing": "Пользователь больше не найден.",
                "connection_missing": "Не удалось сохранить персональную ссылку.",
                "tier_changed": "Уровень пользователя уже изменился; заявка на тест отменена.",
                "already_issued": "Тестовый доступ уже был выдан ранее.",
            }.get(outcome, "Заявку не удалось завершить. Откройте её заново."),
            reply_markup=main_menu_inline_kb(update),
        )
        return ConversationHandler.END

    if not is_admin_meta(actor):
        _clear_product_context(context)
        await message.reply_text("Административное действие больше недоступно.")
        return ConversationHandler.END

    if action == "staff_alias":
        cleaned_alias = " ".join(text.split())
        if cleaned_alias != "-" and len(cleaned_alias) > 32:
            await message.reply_text("Псевдоним слишком длинный. Максимум 32 символа.")
            return PRODUCT_INPUT
        alias = None if cleaned_alias == "-" else normalize_staff_alias(cleaned_alias)
        if alias is not None and len(alias) < 2:
            await message.reply_text("Псевдоним слишком короткий. Минимум 2 символа.")
            return PRODUCT_INPUT
        uid = int(actor.get("user_id") or 0)

        def _alias(cfg: UserData) -> dict[str, Any]:
            current = cfg.authorized_users.get(str(uid))
            if not isinstance(current, dict) or current.get("role") != "admin":
                raise ValueError("admin_missing")
            old_alias = current.get("staff_alias")
            mode = current.get("staff_display_mode")
            if alias is None:
                mode = STAFF_DISPLAY_TITLE
            updated = UserData._normalize_user({**current, "staff_alias": alias, "staff_display_mode": mode})
            cfg.authorized_users[str(uid)] = updated
            append_audit_entry(
                cfg,
                action="staff_alias_changed",
                actor_meta=updated,
                target_user_id=uid,
                details={"old": old_alias, "new": alias},
            )
            return updated

        updated = await update_user_data(_alias)
        _clear_product_context(context)
        await message.reply_text(
            _staff_profile_text(updated),
            parse_mode=ParseMode.HTML,
            reply_markup=_staff_profile_markup(updated),
        )
        return ConversationHandler.END

    if action in {"setting_bank", "setting_recipient", "setting_phone"}:
        if not is_owner_meta(actor):
            _clear_product_context(context)
            await message.reply_text("Доступно только руководителю сервиса.")
            return ConversationHandler.END
        limits = {"setting_bank": 160, "setting_recipient": 160, "setting_phone": 80}
        if len(text) > limits[action]:
            await message.reply_text("Значение слишком длинное.")
            return PRODUCT_INPUT
        key = {
            "setting_bank": "payment_bank",
            "setting_recipient": "payment_recipient",
            "setting_phone": "payment_phone",
        }[action]

        def _setting(cfg: UserData) -> dict[str, Any]:
            cfg.product_settings[key] = " ".join(text.split())
            append_audit_entry(cfg, action=f"{key}_changed", actor_meta=actor, details={"value": "обновлено"})
            return dict(cfg.product_settings)

        settings = await update_user_data(_setting)
        _clear_product_context(context)
        await message.reply_text(
            _owner_panel_text(settings), parse_mode=ParseMode.HTML, reply_markup=_owner_panel_markup()
        )
        return ConversationHandler.END

    if action == "mass_reminder":
        targets: list[int] = []
        snapshot = authorized_users_snapshot()
        lowered = text.lower()
        if lowered == "все":
            targets = [
                int(meta.get("user_id", key)) for key, meta in snapshot.items() if _eligible_paid_subscriber(meta)
            ]
        elif lowered.startswith("до "):
            cutoff = _parse_input_dt(text[3:].strip())
            if cutoff is None:
                await message.reply_text("Некорректная дата. Используйте ДД.ММ.ГГГГ ЧЧ:ММ.")
                return PRODUCT_INPUT
            for key, meta in snapshot.items():
                end = _parse_dt(meta.get("subscription_end_at"))
                if _eligible_paid_subscriber(meta) and end and end <= cutoff:
                    targets.append(int(meta.get("user_id", key)))
        else:
            parsed_ids = _parse_id_list(text)
            if parsed_ids is None:
                await message.reply_text("Введите «все», условие с датой или Telegram ID через запятую.")
                return PRODUCT_INPUT
            targets = sorted(parsed_ids)
        if not targets:
            await message.reply_text("Подходящих получателей нет. Повторите ввод.")
            return PRODUCT_INPUT
        data[_CTX_PENDING] = {"kind": "mass_reminder", "target_ids": sorted(set(targets))}
        await message.reply_text(
            f"Будет подготовлено напоминаний: <b>{len(set(targets))}</b>. Подтвердите отправку.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="product:confirm:apply")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="product:cancel")],
                ]
            ),
        )
        return PRODUCT_CONFIRM

    if action == "mass_date":
        date_part, separator, ids_part = text.partition("|")
        target_dt = _parse_input_dt(date_part.strip())
        if target_dt is None:
            await message.reply_text("Некорректная дата. Используйте ДД.ММ.ГГГГ ЧЧ:ММ.")
            return PRODUCT_INPUT
        selected_ids = _parse_id_list(ids_part.strip()) if separator else None
        if separator and selected_ids is None:
            await message.reply_text("После | укажите корректные Telegram ID через запятую.")
            return PRODUCT_INPUT
        snapshot = authorized_users_snapshot()
        candidates = [
            int(meta.get("user_id", key))
            for key, meta in snapshot.items()
            if _eligible_paid_subscriber(meta)
            and (selected_ids is None or int(meta.get("user_id", key)) in selected_ids)
        ]
        skipped = (len(selected_ids) - len(candidates)) if selected_ids is not None else 0
        if not candidates:
            await message.reply_text("Нет оплаченных подписчиков, которым можно назначить эту дату.")
            return PRODUCT_INPUT
        data[_CTX_PENDING] = {
            "kind": "mass_date",
            "target_ids": sorted(set(candidates)),
            "target_end_at": target_dt.isoformat(),
            "skipped": max(0, skipped),
        }
        await message.reply_text(
            "📅 <b>Проверка массового изменения</b>\n\n"
            f"• Новая дата: <code>{html_escape(_dt_text(target_dt.isoformat()))}</code>\n"
            f"• Будет изменено: <b>{len(set(candidates))}</b>\n"
            f"• Пропущено: <b>{max(0, skipped)}</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="product:confirm:apply")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="product:cancel")],
                ]
            ),
        )
        return PRODUCT_CONFIRM

    if action in {"setting_current", "setting_next", "user_end", "manualpay"}:
        target_dt = _parse_input_dt(text)
        if target_dt is None:
            await message.reply_text("Некорректная дата. Используйте ДД.ММ.ГГГГ ЧЧ:ММ.")
            return PRODUCT_INPUT
        if action in {"setting_current", "setting_next", "manualpay"} and target_dt <= _now():
            await message.reply_text("Для этого действия дата должна находиться в будущем.")
            return PRODUCT_INPUT
        pending: dict[str, Any] = {"kind": action, "target_end_at": target_dt.isoformat()}
        target_uid = data.get(_CTX_TARGET_UID)
        if isinstance(target_uid, int):
            pending["target_uid"] = target_uid
        if action == "user_end":
            target = get_user_meta_copy(int(target_uid or 0))
            if not target or not _is_paid_subscriber(target):
                await message.reply_text(
                    "⛔ Невозможно изменить дату окончания\n\n"
                    "Оплата пользователя не подтверждена. Сначала руководитель сервиса должен подтвердить оплату."
                )
                return PRODUCT_INPUT
        if action == "manualpay":
            target = get_user_meta_copy(int(target_uid or 0))
            if not target or target.get("role") == "admin":
                await message.reply_text("Пользователь не найден.")
                return PRODUCT_INPUT
            if not has_connection(target):
                await message.reply_text("Сначала назначьте пользователю персональную ссылку подключения.")
                return PRODUCT_INPUT
        data[_CTX_PENDING] = pending
        labels = {
            "setting_current": "Текущий период",
            "setting_next": "Следующий период",
            "user_end": "Дата окончания пользователя",
            "manualpay": "Ручное подтверждение оплаты",
        }
        await message.reply_text(
            f"<b>{html_escape(labels[action])}</b>\n\nНовое значение: <code>{html_escape(_dt_text(target_dt.isoformat()))}</code>\n\nПодтвердите изменение.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="product:confirm:apply")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="product:cancel")],
                ]
            ),
        )
        return PRODUCT_CONFIRM

    _clear_product_context(context)
    await message.reply_text("Сценарий ввода устарел. Начните действие заново.")
    return ConversationHandler.END


@require_admin
async def product_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    actor = _actor_meta(update)
    if not query or not actor:
        return ConversationHandler.END
    await query.answer()
    pending = _context_data(context).get(_CTX_PENDING)
    if not isinstance(pending, dict):
        await query.edit_message_text("Подтверждение устарело. Начните действие заново.")
        return ConversationHandler.END
    kind = str(pending.get("kind") or "")
    target_dt = _parse_dt(pending.get("target_end_at"))

    if kind in {"setting_current", "setting_next"}:
        if not is_owner_meta(actor) or target_dt is None or target_dt <= _now():
            await query.edit_message_text("Изменение больше недоступно или дата устарела.")
            _clear_product_context(context)
            return ConversationHandler.END

        def _period(cfg: UserData) -> tuple[str, dict[str, Any]]:
            current = _parse_dt(cfg.product_settings.get("current_period_end"))
            next_end = _parse_dt(cfg.product_settings.get("next_period_end"))
            if kind == "setting_current" and next_end and target_dt >= next_end:
                return "order", dict(cfg.product_settings)
            if kind == "setting_next" and current is None:
                return "missing_current", dict(cfg.product_settings)
            if kind == "setting_next" and current and target_dt <= current:
                return "order", dict(cfg.product_settings)
            key = "current_period_end" if kind == "setting_current" else "next_period_end"
            old = cfg.product_settings.get(key)
            cfg.product_settings[key] = target_dt.isoformat()
            cfg.product_settings["period_setup_reminder_for"] = None
            cfg.product_settings["period_missing_notice_for"] = None
            append_audit_entry(
                cfg,
                action=f"{key}_changed",
                actor_meta=actor,
                details={"old": old, "new": target_dt.isoformat()},
            )
            return "updated", dict(cfg.product_settings)

        outcome, settings = await update_user_data(_period)
        _clear_product_context(context)
        if outcome == "order":
            await query.edit_message_text("Следующий период должен заканчиваться позже текущего.")
        elif outcome == "missing_current":
            await query.edit_message_text("Сначала укажите дату окончания текущего периода.")
        else:
            await query.edit_message_text(
                _owner_panel_text(settings), parse_mode=ParseMode.HTML, reply_markup=_owner_panel_markup()
            )
        return ConversationHandler.END

    if kind == "user_end":
        if not is_lead_or_owner_meta(actor) or target_dt is None:
            await query.edit_message_text("Недостаточно прав или некорректная дата.")
            _clear_product_context(context)
            return ConversationHandler.END
        uid = int(pending.get("target_uid", 0) or 0)

        def _user_end(cfg: UserData) -> tuple[str, dict[str, Any] | None]:
            current = cfg.authorized_users.get(str(uid))
            if not isinstance(current, dict):
                return "missing", None
            if not _is_paid_subscriber(current):
                return "unpaid", current
            old = current.get("subscription_end_at")
            updated = UserData._normalize_user(
                {**current, "subscription_end_at": target_dt.isoformat(), "payment_auto_reminders": {}}
            )
            cfg.authorized_users[str(uid)] = updated
            append_audit_entry(
                cfg,
                action="subscription_end_changed",
                actor_meta=actor,
                target_user_id=uid,
                details={"old": old, "new": target_dt.isoformat()},
            )
            return "updated", updated

        outcome, _updated = await update_user_data(_user_end)
        _clear_product_context(context)
        if outcome == "unpaid":
            await query.edit_message_text(
                "⛔ Невозможно изменить дату окончания\n\n"
                "Оплата пользователя не подтверждена. Сначала руководитель сервиса должен подтвердить оплату."
            )
        elif outcome == "missing":
            await query.edit_message_text("Пользователь не найден.")
        else:
            await query.edit_message_text(
                ui_ok_text(f"Дата окончания изменена: {_dt_text(target_dt.isoformat())}"),
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⬅️ Назад", callback_data=f"product:manage:{uid}")]]
                ),
            )
        return ConversationHandler.END

    if kind == "manualpay":
        if not is_owner_meta(actor) or target_dt is None or target_dt <= _now():
            await query.edit_message_text("Ручное подтверждение оплаты больше недоступно.")
            _clear_product_context(context)
            return ConversationHandler.END
        uid = int(pending.get("target_uid", 0) or 0)

        def _manual_payment(cfg: UserData) -> tuple[str, dict[str, Any] | None]:
            current = cfg.authorized_users.get(str(uid))
            if not isinstance(current, dict) or current.get("role") == "admin":
                return "missing", None
            if not has_connection(current):
                return "connection_missing", current
            request = _new_request(
                cfg,
                kind="purchase",
                user_id=uid,
                status="payment_reported",
                target_end_at=target_dt.isoformat(),
                comment="Ручная регистрация оплаты руководителем",
            )
            return "updated", _finalize_payment(cfg, request, actor)

        outcome, _updated = await update_user_data(_manual_payment)
        _clear_product_context(context)
        messages = {
            "updated": "Оплата зарегистрирована, доступ пользователя активирован.",
            "connection_missing": "Сначала назначьте персональную ссылку подключения.",
            "missing": "Пользователь не найден.",
        }
        await query.edit_message_text(
            messages[outcome],
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data=f"product:manage:{uid}")]]
            ),
        )
        return ConversationHandler.END

    if kind == "mass_date":
        if not is_lead_or_owner_meta(actor) or target_dt is None:
            await query.edit_message_text("Массовое изменение больше недоступно.")
            _clear_product_context(context)
            return ConversationHandler.END
        target_ids = [int(uid) for uid in pending.get("target_ids", []) if str(uid).isdigit()]

        def _mass_date(cfg: UserData) -> tuple[int, int]:
            changed = skipped = 0
            for uid in sorted(set(target_ids)):
                current = cfg.authorized_users.get(str(uid))
                if not isinstance(current, dict) or not _is_paid_subscriber(current):
                    skipped += 1
                    continue
                old = current.get("subscription_end_at")
                cfg.authorized_users[str(uid)] = UserData._normalize_user(
                    {**current, "subscription_end_at": target_dt.isoformat(), "payment_auto_reminders": {}}
                )
                append_audit_entry(
                    cfg,
                    action="subscription_end_changed_mass",
                    actor_meta=actor,
                    target_user_id=uid,
                    details={"old": old, "new": target_dt.isoformat()},
                )
                changed += 1
            return changed, skipped

        changed, skipped = await update_user_data(_mass_date)
        _clear_product_context(context)
        await query.edit_message_text(ui_ok_text(f"Дата изменена у {changed} пользователей. Пропущено: {skipped}."))
        return ConversationHandler.END

    if kind == "mass_reminder":
        if not is_lead_or_owner_meta(actor):
            await query.edit_message_text("Массовая отправка больше недоступна.")
            _clear_product_context(context)
            return ConversationHandler.END
        target_ids = [int(uid) for uid in pending.get("target_ids", []) if str(uid).isdigit()]
        sent, skipped = await update_user_data(
            lambda cfg: _queue_manual_reminders(cfg, actor=actor, target_ids=target_ids)
        )
        _clear_product_context(context)
        await query.edit_message_text(ui_ok_text(f"Напоминания поставлены в очередь: {sent}. Пропущено: {skipped}."))
        return ConversationHandler.END

    _clear_product_context(context)
    await query.edit_message_text("Неизвестное или устаревшее подтверждение.")
    return ConversationHandler.END


@require_admin
async def product_manual_reminder_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = _actor_meta(update)
    if not query or not actor:
        return
    match = re.fullmatch(r"product:remind:(\d+)", query.data or "")
    if not match:
        return
    await query.answer()
    if not is_lead_or_owner_meta(actor):
        await query.edit_message_text("Недостаточно прав.")
        return
    uid = int(match.group(1))
    sent, skipped = await update_user_data(lambda cfg: _queue_manual_reminders(cfg, actor=actor, target_ids=[uid]))
    await query.edit_message_text(
        ui_ok_text("Напоминание поставлено в очередь.") if sent else ui_warn_text("напоминание отправить нельзя."),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data=f"product:manage:{uid}")]]),
    )
    _ = skipped


async def abandon_product_flow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Release a claimed request before an unfinished input flow is discarded."""

    actor = _actor_meta(update)
    request_id = int(_context_data(context).get(_CTX_REQUEST_ID, 0) or 0)
    try:
        if not request_id or not actor:
            return

        def _release(cfg: UserData) -> None:
            request = cfg.service_requests.get(str(request_id))
            if not isinstance(request, dict) or request.get("status") != "awaiting_link":
                return
            if int(request.get("claimed_by_id", 0) or 0) != int(actor.get("user_id", 0) or 0):
                return
            updated = dict(request)
            updated.update(
                {
                    "status": str(request.get("resume_status") or "pending"),
                    "claimed_by_id": None,
                    "claimed_at": None,
                    "updated_at": _now_iso(),
                }
            )
            cfg.service_requests[str(request_id)] = updated

        await update_user_data(_release)
    finally:
        _clear_product_context(context)


async def product_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    return_to_menu = bool(query and query.data == "menu:home")
    await abandon_product_flow(update, context)
    if return_to_menu:
        await show_main_menu(update)
    elif query:
        await query.answer()
        await query.edit_message_text(
            "Действие отменено.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]]),
        )
    elif update.effective_message:
        await update.effective_message.reply_text("Действие отменено.", reply_markup=main_menu_inline_kb(update))
    return ConversationHandler.END


def _automatic_reminder_text(meta: dict[str, Any], settings: dict[str, Any], reminder_type: str) -> str:
    heading = {
        "3d": "Срок оплаченного доступа завершится через 3 дня.",
        "1d": "Срок оплаченного доступа завершится через 1 день.",
    }[reminder_type]
    target = _payment_target(settings, after=_parse_dt(meta.get("subscription_end_at")) or _now())
    lines = [
        "🤖 <b>Системное уведомление</b>",
        "",
        heading,
        f"Текущий доступ до: <code>{html_escape(_dt_text(meta.get('subscription_end_at')))}</code>",
        f"Стоимость продления: <b>{PLAN_TOTAL_RUB} ₽ за {PLAN_MONTHS} месяца</b>",
    ]
    if target:
        lines.append(f"Следующий период до: <code>{html_escape(_dt_text(target.isoformat()))}</code>")
    if _payment_profile_ready(settings):
        lines.extend(
            [
                "",
                f"Банк: <b>{html_escape(str(settings.get('payment_bank')))}</b>",
                f"Получатель: <b>{html_escape(str(settings.get('payment_recipient')))}</b>",
                f"Телефон: <code>{html_escape(str(settings.get('payment_phone')))}</code>",
            ]
        )
    if target and _payment_profile_ready(settings):
        lines.extend(["", "После перевода нажмите «Я оплатил продление». При вопросах создайте тикет."])
    else:
        lines.extend(["", "Реквизиты или следующий платёжный период пока не настроены. Создайте тикет в поддержку."])
    return "\n".join(lines)


async def subscription_lifecycle_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    now = _now()

    def _tick(cfg: UserData) -> dict[str, int]:
        counters = {"reminders": 0, "expired": 0, "released": 0, "period_rollover": 0}
        owner = _owner_meta_from_cfg(cfg)
        settings = cfg.product_settings
        current_end = _parse_dt(settings.get("current_period_end"))
        next_end = _parse_dt(settings.get("next_period_end"))

        if current_end and now >= current_end:
            if next_end and next_end > current_end:
                settings["current_period_end"] = next_end.isoformat()
                settings["next_period_end"] = None
                settings["period_setup_reminder_for"] = None
                settings["period_missing_notice_for"] = None
                counters["period_rollover"] += 1
                if owner:
                    _queue_message(
                        cfg,
                        recipient_ids=[int(owner.get("user_id") or 0)],
                        kind="billing_period_rollover",
                        text=(
                            "🤖 <b>Платёжный период обновлён</b>\n\n"
                            f"Новый текущий период заканчивается: <code>{html_escape(_dt_text(next_end.isoformat()))}</code>.\n"
                            "Даты пользователей автоматически не изменялись. Задайте следующий период заранее."
                        ),
                    )
            elif owner and settings.get("period_missing_notice_for") != current_end.isoformat():
                _queue_message(
                    cfg,
                    recipient_ids=[int(owner.get("user_id") or 0)],
                    kind="billing_period_missing",
                    text="⚠️ Текущий платёжный период завершён, но следующая дата не настроена.",
                )
                settings["period_missing_notice_for"] = current_end.isoformat()
        elif (
            owner
            and current_end
            and not next_end
            and timedelta(0) <= current_end - now <= timedelta(days=7)
            and settings.get("period_setup_reminder_for") != current_end.isoformat()
        ):
            _queue_message(
                cfg,
                recipient_ids=[int(owner.get("user_id") or 0)],
                kind="billing_period_setup_reminder",
                text=(
                    "🤖 <b>Напоминание руководителю</b>\n\n"
                    f"Текущий период завершится: <code>{html_escape(_dt_text(current_end.isoformat()))}</code>.\n"
                    "Укажите дату следующего периода. Пользователи от этого автоматически не продлятся."
                ),
            )
            settings["period_setup_reminder_for"] = current_end.isoformat()

        for request_id, request in list(cfg.service_requests.items()):
            if not isinstance(request, dict) or request.get("status") != "awaiting_link":
                continue
            claimed_at = _parse_dt(request.get("claimed_at"))
            claimed_by = int(request.get("claimed_by_id", 0) or 0)
            claimed_meta = cfg.authorized_users.get(str(claimed_by))
            claimed_admin_active = bool(
                isinstance(claimed_meta, dict)
                and claimed_meta.get("role") == "admin"
                and claimed_meta.get("access_state") == "approved"
                and bool(claimed_meta.get("enabled", True))
            )
            if claimed_at is None or not claimed_admin_active or now - claimed_at >= REQUEST_CLAIM_TIMEOUT:
                updated_request = dict(request)
                updated_request.update(
                    {
                        "status": str(request.get("resume_status") or "pending"),
                        "claimed_by_id": None,
                        "claimed_at": None,
                        "updated_at": now.isoformat(),
                    }
                )
                cfg.service_requests[request_id] = updated_request
                counters["released"] += 1

        for key, current in list(cfg.authorized_users.items()):
            if not isinstance(current, dict) or not _is_paid_subscriber(current):
                continue
            end = _parse_dt(current.get("subscription_end_at"))
            if end is None:
                continue
            uid = int(current.get("user_id", key))
            if now >= end:
                updated = UserData._normalize_user(
                    {
                        **current,
                        "service_tier": "basic",
                        "is_paid": False,
                        "service_tier_updated_at": now.isoformat(),
                        "service_tier_updated_by_id": None,
                        "service_tier_updated_by_name": "Система",
                    }
                )
                cfg.authorized_users[key] = updated
                if current.get("access_state") == "approved" and bool(current.get("enabled", True)):
                    _queue_message(
                        cfg,
                        recipient_ids=[uid],
                        kind="subscription_expired",
                        text=(
                            "🤖 <b>Системное уведомление</b>\n\n"
                            "Срок оплаченного доступа завершён. Уровень в боте изменён на базовый. "
                            "Персональная ссылка сохранена. После подтверждения оплаты полный доступ будет восстановлен."
                        ),
                        reply_markup=[
                            [{"text": "💳 Купить подписку", "callback_data": "subscription:buy"}],
                            [{"text": "🎫 Создать тикет", "callback_data": "menu:ticket"}],
                        ],
                    )
                counters["expired"] += 1
                continue
            if current.get("access_state") != "approved" or not bool(current.get("enabled", True)):
                continue
            remaining = end - now
            reminder_type = "1d" if remaining <= timedelta(days=1) else ("3d" if remaining <= timedelta(days=3) else "")
            if not reminder_type:
                continue
            reminder_key = f"{end.isoformat()}:{reminder_type}"
            sent_map = dict(current.get("payment_auto_reminders") or {})
            if reminder_key in sent_map:
                continue
            markup = (
                _renewal_markup()
                if _payment_profile_ready(settings) and _payment_target(settings, after=end)
                else [[{"text": "🎫 Создать тикет", "callback_data": "menu:ticket"}]]
            )
            _queue_message(
                cfg,
                recipient_ids=[uid],
                kind=f"subscription_reminder_{reminder_type}",
                text=_automatic_reminder_text(current, settings, reminder_type),
                reply_markup=markup,
            )
            sent_map[reminder_key] = now.isoformat()
            cfg.authorized_users[key] = UserData._normalize_user(
                {
                    **current,
                    "payment_auto_reminders": sent_map,
                    "last_auto_payment_reminder_at": now.isoformat(),
                    "last_auto_payment_reminder_type": reminder_type,
                }
            )
            counters["reminders"] += 1
        cfg.product_settings = settings
        return counters

    counters = await update_user_data(_tick)
    if any(counters.values()):
        logger.info("Subscription lifecycle processed: %s", counters, extra={"action": "subscription_lifecycle"})
