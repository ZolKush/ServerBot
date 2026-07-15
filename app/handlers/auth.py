from __future__ import annotations

import contextlib
import hmac
import re
from datetime import datetime
from time import monotonic

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..config import (
    ACCESS_REQUEST_COOLDOWN_SEC,
    ADMIN_PASSWORD,
    AUTH_FAIL_WINDOW_SEC,
    AUTH_GLOBAL_MAX_FAILS_IN_WINDOW,
    AUTH_LOCKOUT_SEC,
    AUTH_MAX_FAILS_IN_WINDOW,
    OWNER_PASSWORD,
    TZ,
    logger,
)
from ..services.outbox import message_payload
from ..staff import STAFF_TITLE_OWNER, STAFF_TITLE_SUPPORT
from ..storage import (
    UserData,
    append_audit_entry,
    enqueue_user_outbox,
    get_user_meta_copy,
    make_outbox_event,
    mutate_user_meta,
    update_user_data,
)
from .common import (
    authorized_ids,
    display_name,
    get_user_id,
    get_user_meta,
    html_escape,
    is_authorized,
    is_enabled,
    is_private,
    main_menu_inline_kb,
    reply_disabled,
    reply_need_auth,
    require_admin,
    require_private,
    show_main_menu,
    staff_title,
)

_AUTH_FAILS: dict[str, list[float]] = {}
_AUTH_LOCKED_UNTIL: dict[str, float] = {}
_AUTH_GLOBAL_FAILS: list[float] = []
_AUTH_GLOBAL_LOCKED_UNTIL = 0.0


def _auth_prune(now: float | None = None) -> None:
    global _AUTH_GLOBAL_FAILS, _AUTH_GLOBAL_LOCKED_UNTIL
    now = monotonic() if now is None else now
    active_fails: dict[str, list[float]] = {}
    for key, attempts in _AUTH_FAILS.items():
        filtered = [ts for ts in attempts if (now - ts) <= AUTH_FAIL_WINDOW_SEC]
        if filtered:
            active_fails[key] = filtered
    _AUTH_FAILS.clear()
    _AUTH_FAILS.update(active_fails)
    _AUTH_GLOBAL_FAILS = [ts for ts in _AUTH_GLOBAL_FAILS if (now - ts) <= AUTH_FAIL_WINDOW_SEC]

    for key in [key for key, until in _AUTH_LOCKED_UNTIL.items() if until <= now]:
        _AUTH_LOCKED_UNTIL.pop(key, None)
    if now >= _AUTH_GLOBAL_LOCKED_UNTIL:
        _AUTH_GLOBAL_LOCKED_UNTIL = 0.0


def _auth_actor_key(update: Update) -> str:
    user = update.effective_user
    if user:
        return f"user:{user.id}"
    chat = update.effective_chat
    return f"chat:{chat.id}" if chat else "unknown"


def _auth_lock_remaining_sec(update: Update) -> int:
    now = monotonic()
    _auth_prune(now)
    until = max(_AUTH_LOCKED_UNTIL.get(_auth_actor_key(update), 0.0), _AUTH_GLOBAL_LOCKED_UNTIL)
    return max(0, int(until - now) + 1) if until > now else 0


def _auth_register_failure(update: Update) -> None:
    global _AUTH_GLOBAL_LOCKED_UNTIL
    key = _auth_actor_key(update)
    now = monotonic()
    _auth_prune(now)
    attempts = [ts for ts in _AUTH_FAILS.get(key, []) if (now - ts) <= AUTH_FAIL_WINDOW_SEC]
    attempts.append(now)
    _AUTH_FAILS[key] = attempts
    _AUTH_GLOBAL_FAILS.append(now)
    if len(attempts) >= AUTH_MAX_FAILS_IN_WINDOW:
        _AUTH_LOCKED_UNTIL[key] = now + AUTH_LOCKOUT_SEC
        _AUTH_FAILS[key] = []
    if len(_AUTH_GLOBAL_FAILS) >= AUTH_GLOBAL_MAX_FAILS_IN_WINDOW:
        _AUTH_GLOBAL_LOCKED_UNTIL = now + AUTH_LOCKOUT_SEC
        _AUTH_GLOBAL_FAILS.clear()
        logger.warning("Global admin authentication lockout activated")


def _auth_reset_actor_limits(update: Update) -> None:
    _auth_prune()
    key = _auth_actor_key(update)
    _AUTH_FAILS.pop(key, None)
    _AUTH_LOCKED_UNTIL.pop(key, None)


async def auth_prune_task(context) -> None:
    _auth_prune()


async def _auth_delete_sensitive_message(update: Update) -> None:
    message = update.effective_message
    if message:
        with contextlib.suppress(Exception):
            await message.delete()


@require_private
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorized(update):
        meta = get_user_meta(get_user_id(update) or 0)
        if meta and meta.get("access_state") == "blocked":
            await reply_disabled(update)
        else:
            await reply_need_auth(update)
        return
    if not is_enabled(update):
        await reply_disabled(update)
        return
    await show_main_menu(update)


@require_private
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    meta = get_user_meta(get_user_id(update) or 0)
    if meta and meta.get("access_state") == "blocked":
        await reply_disabled(update)
        return

    lines = [
        "<b>Руководство по диагностике подключения</b>",
        "",
        "Если у вас возникли проблемы с подключением, выполните эти шаги до обращения к администраторам:",
        "",
        "а. Обновите подписку",
        "б. Проверьте соединение с каждым сервером",
        "в. Обновите приложение",
        "г. Попробуйте включить фрагментирование в настройках",
        "д. Сбросьте данные приложения и добавьте подписку заново",
        "е. Если ничего не помогло — создайте тикет в боте",
    ]
    query = update.callback_query
    message = update.effective_message
    if query and message:
        await query.answer()
        await query.edit_message_text(
            "\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=main_menu_inline_kb(update)
        )
    elif message:
        await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=main_menu_inline_kb(update))


async def cmd_auth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private(update):
        await _auth_delete_sensitive_message(update)
        return
    message = update.effective_message
    try:
        left = _auth_lock_remaining_sec(update)
        if left > 0:
            if message:
                await message.reply_text(f"Слишком много попыток. Повторите через {left} сек.")
            return

        text = (message.text if message else "") or ""
        parts = text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1]:
            if message:
                await message.reply_text(
                    "Эта команда предназначена только для администраторов. Формат: <b>/auth пароль</b>",
                    parse_mode=ParseMode.HTML,
                )
            return

        user = update.effective_user
        if not user:
            return
        existing = get_user_meta_copy(user.id) or {}
        if existing.get("access_state") == "blocked" and existing.get("role") != "admin":
            await reply_disabled(update)
            return

        password_ok = hmac.compare_digest(parts[1].encode("utf-8"), ADMIN_PASSWORD.encode("utf-8"))
        if not password_ok:
            _auth_register_failure(update)
            logger.warning("Admin auth failed for %s", _auth_actor_key(update), extra={"action": "auth_failed"})
            if message:
                await message.reply_text("Пароль неверный.")
            return

        meta = dict(existing)
        meta.update(
            {
                "user_id": user.id,
                "role": "admin",
                "access_state": "approved",
                "enabled": True,
                "username": user.username,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "auth_at": datetime.now(TZ).isoformat(),
                "is_paid": bool(existing.get("is_paid", False)),
                "service_tier": existing.get("service_tier") or "subscriber",
                "admin_level": existing.get("admin_level") or "admin",
                "staff_title": existing.get("staff_title") or STAFF_TITLE_SUPPORT,
                "logged_out_at": None,
            }
        )

        def _authorize(cfg: UserData) -> int:
            cfg.authorized_users[str(user.id)] = UserData._normalize_user(meta)
            return sum(
                1
                for candidate in cfg.authorized_users.values()
                if isinstance(candidate, dict) and candidate.get("access_state") == "pending"
            )

        pending_count = await update_user_data(_authorize)
        _auth_reset_actor_limits(update)
        logger.info("Administrator authenticated user_id=%s", user.id, extra={"user_id": user.id, "action": "auth_ok"})
        pending_note = (
            f"\nОжидают решения заявок: <b>{pending_count}</b>. Откройте раздел «Пользователи»."
            if pending_count
            else ""
        )
        await show_main_menu(update, text=f"Авторизация администратора успешна ✅{pending_note}\n\nМеню:")
    finally:
        # This covers malformed input and lockout branches too.
        await _auth_delete_sensitive_message(update)


def _request_markup() -> list[list[dict[str, str]]]:
    return [
        [
            {"text": "✅ Одобрить", "callback_data": "access:approve:{uid}"},
            {"text": "❌ Отклонить", "callback_data": "access:reject:{uid}"},
        ],
        [{"text": "🚫 Заблокировать", "callback_data": "access:block:{uid}"}],
    ]


@require_private
async def access_request_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    message = update.effective_message
    if query:
        await query.answer()
    user = update.effective_user
    if not user or not message:
        return

    now = datetime.now(TZ)
    admin_ids = authorized_ids(role_filter="admin")
    display = f"@{user.username}" if user.username else " ".join(x for x in (user.first_name, user.last_name) if x)
    display = display or str(user.id)
    event = None
    if admin_ids:
        markup = _request_markup()
        for row in markup:
            for button in row:
                button["callback_data"] = button["callback_data"].format(uid=user.id)
        event = make_outbox_event(
            kind="access_request",
            recipient_ids=admin_ids,
            payload=message_payload(
                "🔐 <b>Новая заявка на доступ</b>\n\n"
                f"Пользователь: <b>{html_escape(display)}</b>\n"
                f"ID: <code>{user.id}</code>",
                reply_markup=markup,
            ),
        )

    def _apply(cfg: UserData) -> str:
        current = cfg.authorized_users.get(str(user.id))
        current = dict(current) if isinstance(current, dict) else {}
        state = str(current.get("access_state") or "")
        if state == "approved":
            return "approved"
        if current.get("role") == "admin":
            return "admin"
        if state == "blocked":
            return "blocked"
        if state == "pending":
            return "pending"
        previous = str(current.get("access_requested_at") or "")
        if previous:
            with contextlib.suppress(ValueError):
                previous_dt = datetime.fromisoformat(previous)
                if previous_dt.tzinfo is None:
                    previous_dt = previous_dt.replace(tzinfo=TZ)
                if (now - previous_dt.astimezone(TZ)).total_seconds() < ACCESS_REQUEST_COOLDOWN_SEC:
                    return "cooldown"
        current.update(
            {
                "user_id": user.id,
                "role": "user",
                # Повторная заявка после /logout не должна стирать уже
                # оплаченный или безлимитный уровень пользователя.
                "service_tier": current.get("service_tier") or "basic",
                "access_state": "pending",
                "enabled": False,
                "username": user.username,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "access_requested_at": now.isoformat(),
            }
        )
        cfg.authorized_users[str(user.id)] = UserData._normalize_user(current)
        if event:
            enqueue_user_outbox(cfg, event)
        return "created"

    result = await update_user_data(_apply)
    texts = {
        "approved": "Доступ уже одобрен. Откройте /menu.",
        "blocked": "🚫 Доступ к боту отключён\n\nВы отключены от данного бота по решению администрации.",
        "admin": "Для возврата в администраторскую учётную запись используйте команду /auth.",
        "pending": "Заявка уже ожидает решения администратора.",
        "cooldown": "Повторная заявка отправлялась недавно. Попробуйте позже.",
        "created": "Заявка отправлена администраторам. Бот сообщит о решении.",
    }
    suffix = (
        "\n\nСейчас нет активного администратора; заявка сохранена." if result == "created" and not admin_ids else ""
    )
    if query:
        await query.edit_message_text(texts[result] + suffix)
    else:
        await message.reply_text(texts[result] + suffix)


@require_admin
async def access_review_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = update.effective_user
    if not query or not actor:
        return
    match = re.fullmatch(r"access:(approve|reject|block):(\d+)", query.data or "")
    if not match:
        return
    action, uid_text = match.groups()
    target_uid = int(uid_text)
    if target_uid == actor.id:
        await query.answer("Нельзя изменить собственный доступ этой кнопкой.", show_alert=True)
        return

    now = datetime.now(TZ)
    actor_name = display_name(update)
    actor_public = staff_title(update)
    labels = {"approve": "одобрена", "reject": "отклонена", "block": "заблокирована"}

    def _apply(cfg: UserData) -> tuple[str, dict[str, object] | None]:
        current = cfg.authorized_users.get(str(target_uid))
        if not isinstance(current, dict):
            return "missing", None
        current = dict(current)
        if current.get("role") == "admin":
            return "admin", current
        state = str(current.get("access_state") or "")
        desired = {"approve": "approved", "reject": "rejected", "block": "blocked"}[action]
        if state == desired:
            return "already", current
        if action in {"approve", "reject"} and state != "pending":
            return "stale", current
        current.update(
            {
                "access_state": desired,
                "enabled": desired == "approved",
                "access_reviewed_at": now.isoformat(),
                "access_reviewed_by_id": actor.id,
                "access_reviewed_by_name": actor_name,
                "auth_at": now.isoformat() if desired == "approved" else current.get("auth_at"),
                "blocked_at": now.isoformat() if desired == "blocked" else None,
                "blocked_by_id": actor.id if desired == "blocked" else None,
                "blocked_by_name": actor_name if desired == "blocked" else None,
            }
        )
        cfg.authorized_users[str(target_uid)] = UserData._normalize_user(current)
        append_audit_entry(
            cfg,
            action=f"access_{desired}",
            actor_meta=cfg.authorized_users.get(str(actor.id)),
            target_user_id=target_uid,
            details={},
        )
        user_text = {
            "approved": "✅ Ваша заявка на доступ одобрена. Используйте /menu.",
            "rejected": "❌ Ваша заявка на доступ отклонена.",
            "blocked": "🚫 Доступ к боту отключён\n\nВы отключены от данного бота по решению администрации.",
        }[desired]
        enqueue_user_outbox(
            cfg,
            make_outbox_event(
                kind=f"access_{desired}",
                recipient_ids=[target_uid],
                payload=message_payload(user_text, parse_mode=None),
            ),
        )
        return "updated", current

    outcome, _meta = await update_user_data(_apply)
    if outcome == "updated":
        logger.info(
            "Access request %s target_uid=%s by admin=%s",
            action,
            target_uid,
            actor.id,
            extra={"user_id": actor.id, "action": f"access_{action}"},
        )
        await query.answer("Решение сохранено.")
        original_text = str(getattr(query.message, "text_html", "") or "Заявка")
        await query.edit_message_text(
            original_text + f"\n\n<b>Решение:</b> {labels[action]} · {html_escape(actor_public)}",
            parse_mode=ParseMode.HTML,
        )
        return
    messages = {
        "missing": "Пользователь больше не найден.",
        "admin": "Нельзя изменить доступ администратора.",
        "already": "Это решение уже было применено.",
        "stale": "Заявка уже обработана другим администратором.",
    }
    await query.answer(messages.get(outcome, "Заявка уже обработана."), show_alert=True)


async def cmd_owner(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Однократно назначает единственного руководителя сервиса."""

    if not is_private(update):
        await _auth_delete_sensitive_message(update)
        return
    message = update.effective_message
    try:
        left = _auth_lock_remaining_sec(update)
        if left > 0:
            if message:
                await message.reply_text(f"Слишком много попыток. Повторите через {left} сек.")
            return
        uid = get_user_id(update)
        current = get_user_meta(uid or 0) if uid is not None else None
        if uid is None or not current or current.get("role") != "admin" or current.get("access_state") != "approved":
            if message:
                await message.reply_text("Сначала авторизуйтесь как администратор.")
            return
        text = (message.text if message else "") or ""
        parts = text.split(maxsplit=1)
        if len(parts) != 2 or not parts[1]:
            if message:
                await message.reply_text("Формат: <b>/owner отдельный_пароль</b>", parse_mode=ParseMode.HTML)
            return
        if not hmac.compare_digest(parts[1].encode("utf-8"), OWNER_PASSWORD.encode("utf-8")):
            _auth_register_failure(update)
            logger.warning("Owner claim failed for %s", _auth_actor_key(update), extra={"action": "owner_claim_failed"})
            if message:
                await message.reply_text("Пароль неверный.")
            return

        def _claim(cfg: UserData) -> str:
            if any(
                isinstance(meta, dict) and meta.get("role") == "admin" and meta.get("admin_level") == "owner"
                for meta in cfg.authorized_users.values()
            ):
                return "exists"
            latest = cfg.authorized_users.get(str(uid))
            if not isinstance(latest, dict) or latest.get("role") != "admin":
                return "denied"
            updated = UserData._normalize_user({**latest, "admin_level": "owner", "staff_title": STAFF_TITLE_OWNER})
            cfg.authorized_users[str(uid)] = updated
            append_audit_entry(
                cfg,
                action="owner_claimed",
                actor_meta=updated,
                target_user_id=uid,
                details={"staff_title": "Руководитель сервиса"},
            )
            return "claimed"

        outcome = await update_user_data(_claim)
        if outcome == "claimed":
            _auth_reset_actor_limits(update)
            logger.info("Service owner claimed by user_id=%s", uid, extra={"user_id": uid, "action": "owner_claimed"})
            await show_main_menu(update, text="Роль руководителя сервиса активирована ✅\n\nМеню:")
        # Если руководитель уже существует, бизнес-состояние и интерфейс не меняются.
    finally:
        await _auth_delete_sensitive_message(update)


@require_private
async def cmd_logout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = get_user_id(update)
    message = update.effective_message
    if uid is None:
        return
    now = datetime.now(TZ).isoformat()
    updated = await mutate_user_meta(
        uid,
        lambda meta: {
            **meta,
            "access_state": "logged_out",
            "enabled": False,
            "logged_out_at": now,
        },
    )
    if message:
        if updated:
            if updated.get("role") == "admin":
                await message.reply_text(
                    "Вы вышли из администраторской учётной записи. Для возврата используйте /auth."
                )
            else:
                await message.reply_text(
                    "Вы вышли из бота. Запись и ограничения доступа сохранены; для возврата отправьте новую заявку.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔐 Запросить доступ", callback_data="access:request")]]
                    ),
                )
        else:
            await message.reply_text("Активной авторизации нет.")
