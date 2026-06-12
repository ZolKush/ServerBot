import re
from datetime import datetime, timedelta
from typing import Any, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import ContextTypes, ConversationHandler

from ..config import TZ, logger
from ..storage import (
    clear_scheduled_maintenance_record,
    get_active_maintenance,
    get_scheduled_maintenance,
    set_maintenance_record,
    set_scheduled_maintenance_record,
    update_important_data,
)
from .common import authorized_ids, display_name, get_user_id, html_escape, require_admin, send_to_many
from .maint_helpers import (
    MAINT_SCOPE_ALL,
    _build_maint_record,
    _build_scheduled_maint_record,
    _maint_active_reminder_text,
    _maint_control_kb,
    _maint_end_confirm_kb,
    _maint_end_notice,
    _maint_extend_notice,
    _maint_panel_text,
    _maint_scheduled_soon_notice,
    _maint_scheduled_start_notice,
    _normalize_scope,
    _scheduled_panel_text,
    _scheduled_to_active_record,
    _scope_label,
    format_scheduled_maint,
    format_maint,
    maint_mode_kb,
    parse_clock_range,
    parse_hhmm,
    scope_kb,
    urgency_kb,
    _hhmm_to_minutes,
)

STATE_MAINT_MODE, STATE_MAINT_SCOPE, STATE_MAINT_URGENCY, STATE_MAINT_DURATION, STATE_MAINT_EXTEND, STATE_MAINT_SCHEDULE_RANGE = range(6)


def _maint_notice_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Меню", callback_data="menu:home")]])


def _clear_maint_ctx(context: ContextTypes.DEFAULT_TYPE) -> None:
    for key in (
        "maint_mode",
        "maint_scope",
        "maint_urgency",
        "maint_panel_chat_id",
        "maint_panel_msg_id",
        "maint_extend_id",
    ):
        context.user_data.pop(key, None)


async def _take_active_maintenance(maint_id: str) -> Optional[dict[str, Any]]:
    def _take(cfg):
        current = getattr(cfg, "maintenance", {})
        if not isinstance(current, dict) or not current.get("active"):
            return None
        if str(current.get("id") or "") != str(maint_id):
            return None
        prev = dict(current)
        cfg.maintenance = {}
        return prev

    taken = await update_important_data(_take)
    if isinstance(taken, dict):
        return taken
    return None


async def _send_maint_notice_with_admin_copy(
    context: ContextTypes.DEFAULT_TYPE,
    author_id: int | None,
    text: str,
) -> tuple[tuple[int, int], tuple[int, int]]:
    user_ids = authorized_ids(role_filter="user", exclude=set())
    admin_ids = authorized_ids(role_filter="admin", exclude={author_id} if author_id else set())
    menu_kb = _maint_notice_menu_kb()
    user_res = await send_to_many(context, user_ids, text, reply_markup=menu_kb) if user_ids else None
    admin_res = await send_to_many(context, admin_ids, text, reply_markup=menu_kb) if admin_ids else None
    users_ok = int(user_res.ok) if user_res is not None else 0
    users_fail = int(user_res.fail) if user_res is not None else 0
    admins_ok = int(admin_res.ok) if admin_res is not None else 0
    admins_fail = int(admin_res.fail) if admin_res is not None else 0
    return (users_ok, users_fail), (admins_ok, admins_fail)


def _maint_delivery_status(users_ok: int, users_fail: int, admins_ok: int, admins_fail: int) -> str:
    return (
        "Статус отправки:\n"
        f"• Пользователи: ✅ {users_ok}, ❌ {users_fail}\n"
        f"• Админы (кроме инициатора): ✅ {admins_ok}, ❌ {admins_fail}"
    )


def _maint_menu_text(scheduled: Optional[dict[str, Any]] = None) -> str:
    lines = [
        "<b>Техработы</b>",
        "",
        "Выберите действие:",
    ]
    if scheduled:
        lines.extend(["", _scheduled_panel_text(scheduled)])
    return "\n".join(lines)


@require_admin
async def maint_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    active = get_active_maintenance()
    if active and str(active.get("id") or ""):
        _clear_maint_ctx(context)
        msg = update.effective_message
        if q and msg:
            await q.answer()
            await q.edit_message_text(
                _maint_panel_text(active),
                parse_mode=ParseMode.HTML,
                reply_markup=_maint_control_kb(str(active["id"])),
            )
        elif msg:
            await msg.reply_text(
                _maint_panel_text(active),
                parse_mode=ParseMode.HTML,
                reply_markup=_maint_control_kb(str(active["id"])),
            )
        return ConversationHandler.END

    scheduled = get_scheduled_maintenance()
    msg = update.effective_message
    if q and msg:
        await q.answer()
        await q.edit_message_text(_maint_menu_text(scheduled), parse_mode=ParseMode.HTML, reply_markup=maint_mode_kb())
    elif msg:
        await msg.reply_text(_maint_menu_text(scheduled), parse_mode=ParseMode.HTML, reply_markup=maint_mode_kb())
    return STATE_MAINT_MODE


@require_admin
async def maint_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    m = re.fullmatch(r"maint:mode:(announce|schedule)", q.data or "")
    if not m:
        return ConversationHandler.END
    context.user_data["maint_mode"] = m.group(1)
    await q.edit_message_text("Выберите область техработ:", reply_markup=scope_kb())
    return STATE_MAINT_SCOPE


@require_admin
async def maint_scope(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    m = re.fullmatch(r"maint:scope:([a-z0-9_-]{1,12})", q.data or "")
    if not m:
        return ConversationHandler.END
    scope = _normalize_scope(m.group(1))
    context.user_data["maint_scope"] = scope
    if q.message and q.message.chat:
        context.user_data["maint_panel_chat_id"] = q.message.chat.id
        context.user_data["maint_panel_msg_id"] = q.message.message_id
    maint_mode = str(context.user_data.get("maint_mode", "announce"))
    if maint_mode == "schedule":
        await q.edit_message_text(
            f"Область: {html_escape(_scope_label(scope))}\n\n"
            "Введите интервал в формате ЧЧ:ММ - ЧЧ:ММ.\n"
            "Если время начала уже прошло сегодня, план будет создан на завтра.",
            parse_mode=ParseMode.HTML,
        )
        return STATE_MAINT_SCHEDULE_RANGE
    await q.edit_message_text(
        f"Область: {html_escape(_scope_label(scope))}\n\nВыберите тип работ:",
        parse_mode=ParseMode.HTML,
        reply_markup=urgency_kb(),
    )
    return STATE_MAINT_URGENCY


@require_admin
async def maint_urgency(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    if q.data not in ("maint:urgency:urgent", "maint:urgency:planned"):
        return ConversationHandler.END
    context.user_data["maint_urgency"] = "urgent" if q.data.endswith("urgent") else "planned"
    await q.edit_message_text("Введите ожидаемое время простоя в формате ЧЧ:ММ (например, 1:35):")
    return STATE_MAINT_DURATION


@require_admin
async def maint_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    parsed = parse_hhmm((msg.text if msg else "") or "")
    if not parsed:
        if msg:
            await msg.reply_text("Некорректно. Введите ЧЧ:ММ, например 0:45 или 2:00:")
        return STATE_MAINT_DURATION

    hh, mm = parsed
    scope = _normalize_scope(str(context.user_data.get("maint_scope", MAINT_SCOPE_ALL)))
    urgency = str(context.user_data.get("maint_urgency", "planned"))
    author = display_name(update)
    author_id = get_user_id(update)

    # Сначала сохраняем запись, потом рассылаем: иначе при сбое сохранения
    # пользователи получат уведомление о несуществующих техработах.
    maint = _build_maint_record(scope, urgency, hh, mm, author_id, author)
    maint_id = maint.get("id")
    await set_maintenance_record(maint)
    logger.info("Maintenance started by user_id=%s scope=%s urgency=%s duration_min=%s", author_id, scope, urgency, _hhmm_to_minutes(hh, mm))

    msg_text = format_maint(scope, urgency, hh, mm, author)
    (users_ok, users_fail), (admins_ok, admins_fail) = await _send_maint_notice_with_admin_copy(
        context,
        author_id=author_id,
        text=msg_text,
    )

    panel_text = f"{_maint_panel_text(maint)}\n\n{_maint_delivery_status(users_ok, users_fail, admins_ok, admins_fail)}"
    panel_chat_id = context.user_data.get("maint_panel_chat_id")
    panel_msg_id = context.user_data.get("maint_panel_msg_id")
    if panel_chat_id and panel_msg_id:
        try:
            await context.bot.edit_message_text(
                chat_id=panel_chat_id,
                message_id=panel_msg_id,
                text=panel_text,
                parse_mode=ParseMode.HTML,
                reply_markup=_maint_control_kb(str(maint_id)),
            )
            _clear_maint_ctx(context)
            return ConversationHandler.END
        except BadRequest as e:
            if "message is not modified" in str(e).lower():
                _clear_maint_ctx(context)
                return ConversationHandler.END
            logger.warning("Не удалось обновить панель техработ (%s), отправляю новое сообщение", e)
        except Exception as e:
            logger.warning("Не удалось обновить панель техработ (%s), отправляю новое сообщение", e)

    if msg:
        await msg.reply_text(panel_text, parse_mode=ParseMode.HTML, reply_markup=_maint_control_kb(str(maint_id)))
    _clear_maint_ctx(context)
    return ConversationHandler.END


@require_admin
async def maint_schedule_range(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    parsed = parse_clock_range((msg.text if msg else "") or "")
    if not parsed:
        if msg:
            await msg.reply_text("Некорректно. Введите интервал в формате ЧЧ:ММ - ЧЧ:ММ, например 17:00 - 18:00.")
        return STATE_MAINT_SCHEDULE_RANGE

    sh, sm, eh, em = parsed
    now = datetime.now(TZ)
    start_at = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
    end_at = now.replace(hour=eh, minute=em, second=0, microsecond=0)
    if start_at <= now:
        start_at += timedelta(days=1)
        end_at += timedelta(days=1)
    if end_at <= start_at:
        if msg:
            await msg.reply_text("Окончание должно быть позже начала в рамках одного дня, например 17:00 - 18:00.")
        return STATE_MAINT_SCHEDULE_RANGE

    scope = _normalize_scope(str(context.user_data.get("maint_scope", MAINT_SCOPE_ALL)))
    author = display_name(update)
    author_id = get_user_id(update)
    scheduled = _build_scheduled_maint_record(scope, start_at, end_at, author_id, author)
    await set_scheduled_maintenance_record(scheduled)
    logger.info(
        "Maintenance scheduled by user_id=%s scope=%s start=%s end=%s",
        author_id,
        scope,
        scheduled.get("scheduled_start"),
        scheduled.get("scheduled_end"),
    )

    panel_text = (
        format_scheduled_maint(scope, start_at, end_at, author)
        + "\n\nУведомления пользователям будут отправлены за 30 минут и в момент начала."
    )
    panel_chat_id = context.user_data.get("maint_panel_chat_id")
    panel_msg_id = context.user_data.get("maint_panel_msg_id")
    if panel_chat_id and panel_msg_id:
        try:
            await context.bot.edit_message_text(
                chat_id=panel_chat_id,
                message_id=panel_msg_id,
                text=panel_text,
                parse_mode=ParseMode.HTML,
                reply_markup=_maint_notice_menu_kb(),
            )
            _clear_maint_ctx(context)
            return ConversationHandler.END
        except BadRequest as e:
            if "message is not modified" in str(e).lower():
                _clear_maint_ctx(context)
                return ConversationHandler.END
            logger.warning("Не удалось обновить панель техработ (%s), отправляю новое сообщение", e)
        except Exception as e:
            logger.warning("Не удалось обновить панель техработ (%s), отправляю новое сообщение", e)

    if msg:
        await msg.reply_text(panel_text, parse_mode=ParseMode.HTML, reply_markup=_maint_notice_menu_kb())
    _clear_maint_ctx(context)
    return ConversationHandler.END


@require_admin
async def maint_extend_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    m = re.fullmatch(r"maint:extend:([0-9a-f]+)", q.data or "")
    if not m:
        _clear_maint_ctx(context)
        return ConversationHandler.END
    maint_id = m.group(1)
    maint = get_active_maintenance()
    if not maint or str(maint.get("id")) != maint_id:
        await q.edit_message_text("Техработы не активны или уже завершены.")
        _clear_maint_ctx(context)
        return ConversationHandler.END
    context.user_data["maint_extend_id"] = maint_id
    await q.edit_message_text("⏳ Продление техработ.\nВведите новое время простоя в формате ЧЧ:ММ (например, 1:35):")
    return STATE_MAINT_EXTEND


@require_admin
async def maint_extend_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    parsed = parse_hhmm((msg.text if msg else "") or "")
    if not parsed:
        if msg:
            await msg.reply_text("Некорректно. Введите ЧЧ:ММ, например 0:45 или 2:00:")
        return STATE_MAINT_EXTEND

    maint_id = context.user_data.get("maint_extend_id")
    maint = get_active_maintenance()
    if not maint or str(maint.get("id")) != str(maint_id):
        if msg:
            await msg.reply_text("Техработы не активны или уже завершены.")
        _clear_maint_ctx(context)
        return ConversationHandler.END

    hh, mm = parsed
    duration_min = _hhmm_to_minutes(hh, mm)

    def _extend_current(cfg):
        current = getattr(cfg, "maintenance", {})
        if not isinstance(current, dict) or not current.get("active"):
            raise RuntimeError("maintenance_not_active")
        if str(current.get("id")) != str(maint_id):
            raise RuntimeError("maintenance_changed")
        now = datetime.now(TZ)
        updated = dict(current)
        updated["duration_min"] = duration_min
        updated["expected_end"] = (now + timedelta(minutes=duration_min)).isoformat()
        updated["updated_at"] = now.isoformat()
        cfg.maintenance = updated
        return updated

    try:
        maint = await update_important_data(_extend_current)
    except RuntimeError:
        if msg:
            await msg.reply_text("Техработы не активны или уже завершены.")
        _clear_maint_ctx(context)
        return ConversationHandler.END

    author = display_name(update)
    logger.info("Maintenance extended by user_id=%s duration_min=%s maint_id=%s", get_user_id(update), duration_min, maint_id)
    notice = _maint_extend_notice(maint, hh, mm, author)
    author_id = get_user_id(update)
    (users_ok, users_fail), (admins_ok, admins_fail) = await _send_maint_notice_with_admin_copy(
        context,
        author_id=author_id,
        text=notice,
    )

    panel_text = f"{_maint_panel_text(maint)}\n\n{_maint_delivery_status(users_ok, users_fail, admins_ok, admins_fail)}"
    context.user_data.pop("maint_extend_id", None)
    if msg:
        await msg.reply_text(panel_text, parse_mode=ParseMode.HTML, reply_markup=_maint_control_kb(str(maint_id)))
    _clear_maint_ctx(context)
    return ConversationHandler.END


@require_admin
async def maint_end_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"maint:end:([0-9a-f]+)", q.data or "")
    if not m:
        return
    maint_id = m.group(1)
    maint = await _take_active_maintenance(maint_id)
    if not maint:
        await q.edit_message_text("Техработы не активны или уже завершены.")
        return

    author = display_name(update)
    ended_at = datetime.now(TZ)
    notice = _maint_end_notice(maint, author, ended_at=ended_at)
    author_id = get_user_id(update)
    (users_ok, users_fail), (admins_ok, admins_fail) = await _send_maint_notice_with_admin_copy(
        context,
        author_id=author_id,
        text=notice,
    )

    logger.info("Maintenance ended by user_id=%s maint_id=%s", get_user_id(update), maint_id)
    await q.edit_message_text(
        "✅ Техработы завершены.\n\n"
        + _maint_delivery_status(users_ok, users_fail, admins_ok, admins_fail)
    )


@require_admin
async def maint_end_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"maint:endconfirm:([0-9a-f]+)", q.data or "")
    if not m:
        return
    maint_id = m.group(1)
    maint = get_active_maintenance()
    if not maint or str(maint.get("id")) != maint_id:
        await q.edit_message_text("Техработы не активны или уже завершены.")
        return
    text = _maint_panel_text(maint) + "\n\n<b>Подтвердить завершение?</b>"
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=_maint_end_confirm_kb(maint_id))


@require_admin
async def maint_cancel_end_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"maint:cancelend:([0-9a-f]+)", q.data or "")
    if not m:
        return
    maint_id = m.group(1)
    maint = get_active_maintenance()
    if not maint or str(maint.get("id")) != maint_id:
        await q.edit_message_text("Техработы не активны или уже завершены.")
        return
    await q.edit_message_text(
        _maint_panel_text(maint),
        parse_mode=ParseMode.HTML,
        reply_markup=_maint_control_kb(maint_id),
    )


async def maint_restart_notify(context: ContextTypes.DEFAULT_TYPE) -> None:
    maint = get_active_maintenance()
    if not maint:
        return
    if not str(maint.get("id", "") or ""):
        return
    admin_ids = authorized_ids(role_filter="admin", exclude=set())
    if not admin_ids:
        return
    text = _maint_active_reminder_text(maint) + "\n\nОткройте «/maint» для управления."
    await send_to_many(context, admin_ids, text, reply_markup=_maint_notice_menu_kb())


async def maint_schedule_tick(context: ContextTypes.DEFAULT_TYPE) -> None:
    scheduled = get_scheduled_maintenance()
    if not scheduled:
        return

    try:
        start_at = datetime.fromisoformat(str(scheduled.get("scheduled_start") or ""))
        end_at = datetime.fromisoformat(str(scheduled.get("scheduled_end") or ""))
    except Exception:
        logger.warning("Scheduled maintenance has invalid timestamps, clearing record")
        await clear_scheduled_maintenance_record()
        return

    if start_at.tzinfo is None:
        start_at = start_at.replace(tzinfo=TZ)
    if end_at.tzinfo is None:
        end_at = end_at.replace(tzinfo=TZ)

    now = datetime.now(TZ)

    if now >= end_at:
        # Окно плановых работ полностью прошло (бот был выключен или активация
        # откладывалась из-за других работ) — задним числом не активируем.
        logger.warning(
            "Scheduled maintenance %s expired (end=%s), clearing",
            scheduled.get("id"),
            scheduled.get("scheduled_end"),
        )
        await clear_scheduled_maintenance_record()
        admin_ids = authorized_ids(role_filter="admin")
        if admin_ids:
            await send_to_many(
                context,
                admin_ids,
                "⚠️ <b>Запланированные техработы отменены</b>\n"
                "Окно работ прошло, пока они не были запущены (бот был недоступен или шли другие работы).",
                reply_markup=_maint_notice_menu_kb(),
            )
        return

    if now >= (start_at - timedelta(minutes=30)) and now < start_at and not bool(scheduled.get("notified_before", False)):
        # Сначала фиксируем отметку, потом рассылаем — иначе сбой записи
        # приведёт к повторной рассылке на каждом тике.
        def _mark_before(cfg):
            cur = dict(getattr(cfg, "scheduled_maintenance", {}) or {})
            if str(cur.get("id") or "") != str(scheduled.get("id") or ""):
                return None
            cur["notified_before"] = True
            cur["updated_at"] = now.isoformat()
            cfg.scheduled_maintenance = cur
            return cur

        marked = await update_important_data(_mark_before)
        if marked:
            notice = _maint_scheduled_soon_notice(scheduled)
            author_id = scheduled.get("author_id")
            await _send_maint_notice_with_admin_copy(context, author_id=author_id if isinstance(author_id, int) else None, text=notice)
        scheduled = get_scheduled_maintenance() or scheduled

    if now >= start_at and not bool(scheduled.get("notified_start", False)):
        active = get_active_maintenance()
        if active:
            logger.info(
                "Scheduled maintenance %s deferred: another maintenance %s is active",
                scheduled.get("id"),
                active.get("id"),
            )
            return

        def _activate_scheduled(cfg):
            cur = dict(getattr(cfg, "scheduled_maintenance", {}) or {})
            if str(cur.get("id") or "") != str(scheduled.get("id") or ""):
                return None
            existing = getattr(cfg, "maintenance", {}) or {}
            if isinstance(existing, dict) and existing.get("active"):
                return None
            cfg.maintenance = _scheduled_to_active_record(cur)
            cfg.scheduled_maintenance = {}
            return dict(cfg.maintenance)

        activated = await update_important_data(_activate_scheduled)
        if not activated:
            return
        notice = _maint_scheduled_start_notice(scheduled)
        author_id = scheduled.get("author_id")
        await _send_maint_notice_with_admin_copy(context, author_id=author_id if isinstance(author_id, int) else None, text=notice)
        logger.info(
            "Scheduled maintenance activated id=%s start=%s end=%s",
            scheduled.get("id"),
            scheduled.get("scheduled_start"),
            scheduled.get("scheduled_end"),
        )
