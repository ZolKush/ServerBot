from __future__ import annotations

from datetime import datetime, timedelta
from io import BytesIO
from typing import Any
from urllib.parse import urlparse

from telegram import InputFile, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..bot.guards import get_user_id, require_auth
from ..bot.ui import format_dt_human, html_escape, ui_info_text, wrap_as_codeblock_html
from ..config import TZ
from ..messaging.outbox import document_text_payload, message_payload
from ..runtime.logging import logger
from ..storage import get_user_meta_copy, service_requests_snapshot
from ..users.staff import is_billing_exempt_meta

CONNECTION_URL_KEY = "connection_url"
CONNECTION_UPDATED_AT_KEY = "subscription_updated_at"
CONNECTION_UPDATED_BY_ID_KEY = "subscription_updated_by_id"
CONNECTION_UPDATED_BY_NAME_KEY = "subscription_updated_by_name"
_INLINE_CONNECTION_LIMIT = 3000
MAX_CONNECTION_BYTES = 1_000_000


def is_valid_connection_url(value: object) -> bool:
    raw = str(value or "").strip()
    if not raw or len(raw.encode("utf-8")) > MAX_CONNECTION_BYTES or any(char.isspace() for char in raw):
        return False
    try:
        parsed = urlparse(raw)
        _ = parsed.port  # Проверяет в том числе некорректный формат порта.
    except ValueError:
        return False
    return parsed.scheme.lower() in {"http", "https"} and bool(parsed.netloc and parsed.hostname)


def get_connection_url(meta: dict[str, Any] | None) -> str:
    if not meta:
        return ""
    return str(meta.get(CONNECTION_URL_KEY, "") or "")


def has_connection(meta: dict[str, Any] | None) -> bool:
    return bool(get_connection_url(meta).strip())


def _connection_intro(meta: dict[str, Any]) -> str:
    updated_at = str(meta.get(CONNECTION_UPDATED_AT_KEY, "") or "").strip()
    lines = [
        "🔗 <b>Ссылка подключения готова</b>",
        "",
        "Для вашей учётной записи назначена персональная ссылка подключения.",
        "Откройте её, чтобы посмотреть инструкцию, или скопируйте ссылку и добавьте её в Happ.",
    ]
    if updated_at:
        lines.extend(["", f"• Обновлена: <code>{html_escape(format_dt_human(updated_at))}</code>"])
    return "\n".join(lines)


def connection_outbox_payload(
    meta: dict[str, Any],
    *,
    title: str | None = None,
    filename_prefix: str = "connection",
) -> dict[str, Any]:
    cfg = get_connection_url(meta)
    if not cfg.strip():
        raise ValueError("connection URL is empty")
    if len(cfg.encode("utf-8")) > MAX_CONNECTION_BYTES:
        raise ValueError("connection URL exceeds 1000000 UTF-8 bytes")
    intro = title or _connection_intro(meta)
    if len(html_escape(cfg)) <= _INLINE_CONNECTION_LIMIT:
        return message_payload(
            intro + "\n\n" + wrap_as_codeblock_html(cfg, limit=_INLINE_CONNECTION_LIMIT),
            reply_markup=[[{"text": "🌐 Открыть инструкцию", "url": cfg}]],
        )
    return document_text_payload(
        cfg,
        filename=f"{filename_prefix}.txt",
        caption=intro + "\n\nПолная ссылка находится в файле.",
    )


async def send_connection_payload(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    chat_id: int,
    meta: dict[str, Any],
    title: str | None = None,
    filename_prefix: str = "connection",
) -> None:
    cfg = get_connection_url(meta)
    if not cfg.strip():
        raise ValueError("connection URL is empty")
    if len(cfg.encode("utf-8")) > MAX_CONNECTION_BYTES:
        raise ValueError("connection URL exceeds 1000000 UTF-8 bytes")

    intro = title or _connection_intro(meta)
    # Лимит Telegram считается по экранированному тексту (& -> &amp; и т.п.).
    if len(html_escape(cfg)) <= _INLINE_CONNECTION_LIMIT:
        payload = intro + "\n\n" + wrap_as_codeblock_html(cfg, limit=_INLINE_CONNECTION_LIMIT)
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup

        await context.bot.send_message(
            chat_id=chat_id,
            text=payload,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🌐 Открыть инструкцию", url=cfg)]]),
        )
        return

    await context.bot.send_message(
        chat_id=chat_id,
        text=intro + "\n\nПолная ссылка отправлена отдельным файлом.",
        parse_mode=ParseMode.HTML,
    )
    document = InputFile(BytesIO(cfg.encode("utf-8")), filename=f"{filename_prefix}.txt")
    await context.bot.send_document(chat_id=chat_id, document=document, caption="connection.txt")


def _active_request_status(uid: int, kind: str) -> str | None:
    for request in service_requests_snapshot().values():
        if (
            int(request.get("user_id", 0) or 0) == uid
            and request.get("kind") == kind
            and request.get("status") in {"pending", "claimed", "awaiting_link", "requisites_sent", "payment_reported"}
        ):
            return str(request.get("status"))
    return None


def _dashboard_markup(meta: dict[str, Any]) -> Any:
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    tier = str(meta.get("service_tier") or "basic")
    billing_exempt = is_billing_exempt_meta(meta)
    rows: list[list[InlineKeyboardButton]] = []
    if has_connection(meta):
        rows.append([InlineKeyboardButton("🔗 Моя ссылка подключения", callback_data="subscription:connection")])
    if tier == "basic" and not billing_exempt:
        if meta.get("role") != "admin" and not meta.get("trial_issued_at"):
            rows.append([InlineKeyboardButton("🧪 Запросить тестовый доступ", callback_data="subscription:trial")])
        rows.append([InlineKeyboardButton("💳 Купить подписку", callback_data="subscription:buy")])
    elif tier == "subscriber" and meta.get("is_paid") and not billing_exempt:
        end_raw = str(meta.get("subscription_end_at") or "")
        end = None
        try:
            end = datetime.fromisoformat(end_raw) if end_raw else None
            if end and end.tzinfo is None:
                end = end.replace(tzinfo=TZ)
            elif end:
                end = end.astimezone(TZ)
        except ValueError:
            end = None
        if end and timedelta(0) <= end - datetime.now(TZ) <= timedelta(days=3):
            rows.append([InlineKeyboardButton("✅ Я оплатил продление", callback_data="subscription:renew")])
    rows.append([InlineKeyboardButton("👤 Личный профиль", callback_data="profile:show")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def _dashboard_text(meta: dict[str, Any]) -> str:
    uid = int(meta.get("user_id", 0) or 0)
    tier = str(meta.get("service_tier") or "basic")
    billing_exempt = is_billing_exempt_meta(meta)
    tier_label = {
        "basic": "Базовый доступ",
        "subscriber": "Подписчик",
        "unlimited_trial": "Безлимитный тестовый доступ",
    }.get(tier, tier)
    payment_text = (
        "бессрочная — руководитель сервиса"
        if billing_exempt
        else ("подтверждена" if meta.get("is_paid") else "не подтверждена")
    )
    end_text = "бессрочно" if billing_exempt else format_dt_human(meta.get("subscription_end_at"))
    lines = [
        "🔗 <b>Подключение и подписка</b>",
        "",
        f"• Уровень: <b>{html_escape('Бессрочный оплаченный доступ — руководитель сервиса' if billing_exempt else tier_label)}</b>",
        f"• Оплата: <b>{html_escape(payment_text)}</b>",
        f"• Доступ до: <code>{html_escape(end_text)}</code>",
        f"• Персональная ссылка: <b>{'назначена' if has_connection(meta) else 'не назначена'}</b>",
    ]
    for kind, label in (("trial", "Тест"), ("purchase", "Покупка"), ("renewal", "Продление")):
        status = _active_request_status(uid, kind)
        if status:
            status_label = {
                "pending": "ожидает решения",
                "claimed": "обрабатывается",
                "awaiting_link": "ожидает ссылку",
                "requisites_sent": "реквизиты отправлены",
                "payment_reported": "оплата проверяется",
            }.get(status, status)
            lines.append(f"• {label}: <b>{html_escape(status_label)}</b>")
    if tier == "basic":
        lines.extend(
            [
                "",
                "Вы можете купить подписку."
                if meta.get("role") == "admin"
                else "Вы можете запросить тестовый доступ или купить подписку.",
            ]
        )
    elif tier == "unlimited_trial":
        lines.extend(["", "Для этого уровня оплата и дата окончания не применяются."])
    elif billing_exempt:
        lines.extend(["", "Для руководителя сервиса оплата и дата окончания не применяются."])
    return "\n".join(lines)


@require_auth
async def subscription_show(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    msg = update.effective_message
    uid = get_user_id(update)
    meta = get_user_meta_copy(uid) if uid is not None else None

    if not uid or not meta:
        if q:
            await q.answer()
        return

    if q:
        await q.answer()

    text = _dashboard_text(meta)
    markup = _dashboard_markup(meta)
    if q:
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
    elif msg:
        await msg.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


@require_auth
async def connection_show_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    uid = get_user_id(update)
    meta = get_user_meta_copy(uid) if uid is not None else None
    if not q or uid is None or not meta:
        return
    await q.answer()
    if not has_connection(meta):
        await q.edit_message_text(
            "Персональная ссылка подключения ещё не назначена.",
            reply_markup=_dashboard_markup(meta),
        )
        return
    await send_connection_payload(context, chat_id=uid, meta=meta, filename_prefix=f"connection_{uid}")
    await q.edit_message_text(
        ui_info_text("Персональная ссылка отправлена отдельным сообщением ниже."),
        reply_markup=_dashboard_markup(meta),
    )
    logger.info("Connection URL delivered to user_id=%s", uid)
