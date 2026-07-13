import re
from datetime import date, datetime, timedelta
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import ContextTypes, ConversationHandler

from ..config import TZ, logger
from ..services.outbox import message_payload
from ..storage import (
    ImportantData,
    enqueue_important_outbox,
    get_active_maintenance,
    get_scheduled_maintenance,
    make_outbox_event,
    update_important_data,
)
from .common import authorized_ids, display_name, get_user_id, html_escape, require_admin
from .maint_helpers import (
    MAINT_SCOPE_ALL,
    MAINT_WARN_THRESHOLDS_MIN,
    _build_maint_record,
    _build_scheduled_maint_record,
    _due_thresholds,
    _hhmm_to_minutes,
    _initial_notified_thresholds,
    _maint_active_reminder_text,
    _maint_control_kb,
    _maint_end_confirm_kb,
    _maint_end_notice,
    _maint_extend_notice,
    _maint_panel_text,
    _maint_scheduled_cancel_notice,
    _maint_scheduled_soon_notice,
    _maint_scheduled_start_notice,
    _normalize_scope,
    _scheduled_cancel_confirm_kb,
    _scheduled_control_kb,
    _scheduled_panel_text,
    _scheduled_to_active_record,
    _scope_label,
    format_maint,
    format_scheduled_maint,
    maint_mode_kb,
    parse_clock_range,
    parse_hhmm,
    schedule_calendar_kb,
    scope_kb,
    urgency_kb,
)

(
    STATE_MAINT_MODE,
    STATE_MAINT_SCOPE,
    STATE_MAINT_URGENCY,
    STATE_MAINT_DURATION,
    STATE_MAINT_EXTEND,
    STATE_MAINT_SCHEDULE_RANGE,
    STATE_MAINT_SCHEDULE_DATE,
) = range(7)


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
        "maint_sched_date",
        "maint_base_scheduled_id",
    ):
        context.user_data.pop(key, None)


def _make_maint_notice_event(
    *,
    author_id: int | None,
    text: str,
    kind: str,
) -> tuple[dict[str, Any] | None, int, int]:
    user_ids = authorized_ids(role_filter="user", exclude=set())
    admin_ids = authorized_ids(role_filter="admin", exclude={author_id} if author_id else set())
    recipient_ids = user_ids + admin_ids
    if not recipient_ids:
        return None, 0, 0
    event = make_outbox_event(
        kind=kind,
        recipient_ids=recipient_ids,
        payload=message_payload(
            text,
            reply_markup=[[{"text": "🏠 Меню", "callback_data": "menu:home"}]],
        ),
    )
    return event, len(user_ids), len(admin_ids)


def _enqueue_if_present(cfg: ImportantData, event: dict[str, Any] | None) -> None:
    if event:
        enqueue_important_outbox(cfg, event)


def _maint_delivery_status(users_count: int, admins_count: int) -> str:
    return (
        "Уведомления сохранены в надёжной очереди:\n"
        f"• Пользователи: {users_count}\n"
        f"• Админы (кроме инициатора): {admins_count}"
    )


def _maint_menu_text(scheduled: dict[str, Any] | None = None) -> str:
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
    if scheduled and str(scheduled.get("id") or ""):
        text = _scheduled_panel_text(scheduled)
        kb = _scheduled_control_kb(str(scheduled["id"]))
        if q and msg:
            await q.answer()
            await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        elif msg:
            await msg.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return STATE_MAINT_MODE
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
    current_schedule = get_scheduled_maintenance() or {}
    context.user_data["maint_base_scheduled_id"] = str(current_schedule.get("id") or "")
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
        today = datetime.now(TZ).date()
        await q.edit_message_text(
            f"Область: {html_escape(_scope_label(scope))}\n\nВыберите дату техработ:",
            parse_mode=ParseMode.HTML,
            reply_markup=schedule_calendar_kb(today.year, today.month, today=today),
        )
        return STATE_MAINT_SCHEDULE_DATE
    await q.edit_message_text(
        f"Область: {html_escape(_scope_label(scope))}\n\nВыберите тип работ:",
        parse_mode=ParseMode.HTML,
        reply_markup=urgency_kb(),
    )
    return STATE_MAINT_URGENCY


@require_admin
async def maint_cal_noop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q:
        await q.answer()
    return STATE_MAINT_SCHEDULE_DATE


@require_admin
async def maint_cal_nav(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    m = re.fullmatch(r"maint:cal:nav:(\d{4})-(\d{2})", q.data or "")
    if not m:
        return STATE_MAINT_SCHEDULE_DATE
    year, month = int(m.group(1)), int(m.group(2))
    today = datetime.now(TZ).date()
    # Клампинг: не раньше текущего месяца
    if (year, month) < (today.year, today.month):
        year, month = today.year, today.month
    await q.edit_message_text(
        "Выберите дату техработ:",
        reply_markup=schedule_calendar_kb(year, month, today=today),
    )
    return STATE_MAINT_SCHEDULE_DATE


@require_admin
async def maint_cal_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return ConversationHandler.END
    await q.answer()
    m = re.fullmatch(r"maint:cal:day:(\d{4})-(\d{2})-(\d{2})", q.data or "")
    if not m:
        return STATE_MAINT_SCHEDULE_DATE
    chosen = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    today = datetime.now(TZ).date()
    if chosen < today:
        await q.edit_message_text(
            "Дата уже прошла. Выберите дату техработ:",
            reply_markup=schedule_calendar_kb(today.year, today.month, today=today),
        )
        return STATE_MAINT_SCHEDULE_DATE
    context.user_data["maint_sched_date"] = chosen.isoformat()
    if q.message and q.message.chat:
        context.user_data["maint_panel_chat_id"] = q.message.chat.id
        context.user_data["maint_panel_msg_id"] = q.message.message_id
    await q.edit_message_text(
        f"Дата: <code>{html_escape(chosen.strftime('%d.%m.%Y'))}</code>\n\n"
        "Введите интервал в формате ЧЧ:ММ - ЧЧ:ММ (например, 17:00 - 18:00).",
        parse_mode=ParseMode.HTML,
    )
    return STATE_MAINT_SCHEDULE_RANGE


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

    maint = _build_maint_record(scope, urgency, hh, mm, author_id, author)
    maint_id = maint.get("id")
    msg_text = format_maint(scope, urgency, hh, mm, author)
    event, users_count, admins_count = _make_maint_notice_event(
        author_id=author_id,
        text=msg_text,
        kind="maintenance_started",
    )
    expected_schedule_id = str(context.user_data.get("maint_base_scheduled_id") or "")

    def _start(cfg: ImportantData) -> bool:
        active = cfg.maintenance if isinstance(cfg.maintenance, dict) else {}
        if active.get("active"):
            return False
        current_schedule = cfg.scheduled_maintenance if isinstance(cfg.scheduled_maintenance, dict) else {}
        if str(current_schedule.get("id") or "") != expected_schedule_id:
            return False
        cfg.maintenance = dict(maint)
        # Immediate announcement intentionally supersedes a schedule, but both
        # actions happen under one compare-and-set transaction.
        cfg.scheduled_maintenance = {}
        _enqueue_if_present(cfg, event)
        return True

    if not await update_important_data(_start):
        if msg:
            await msg.reply_text("Другой администратор уже запустил техработы. Откройте /maint заново.")
        _clear_maint_ctx(context)
        return ConversationHandler.END
    logger.info(
        "Maintenance started by user_id=%s scope=%s urgency=%s duration_min=%s",
        author_id,
        scope,
        urgency,
        _hhmm_to_minutes(hh, mm),
    )

    panel_text = f"{_maint_panel_text(maint)}\n\n{_maint_delivery_status(users_count, admins_count)}"
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
    date_iso = str(context.user_data.get("maint_sched_date") or "")
    try:
        chosen = date.fromisoformat(date_iso)
    except ValueError:
        chosen = now.date()
    start_at = now.replace(
        year=chosen.year,
        month=chosen.month,
        day=chosen.day,
        hour=sh,
        minute=sm,
        second=0,
        microsecond=0,
    )
    end_at = start_at.replace(hour=eh, minute=em)
    if start_at <= now:
        if msg:
            await msg.reply_text("Время начала уже прошло. Выберите более позднее время.")
        return STATE_MAINT_SCHEDULE_RANGE
    if end_at <= start_at:
        if msg:
            await msg.reply_text("Окончание должно быть позже начала в рамках одного дня, например 17:00 - 18:00.")
        return STATE_MAINT_SCHEDULE_RANGE

    scope = _normalize_scope(str(context.user_data.get("maint_scope", MAINT_SCOPE_ALL)))
    author = display_name(update)
    author_id = get_user_id(update)
    scheduled = _build_scheduled_maint_record(scope, start_at, end_at, author_id, author)

    def _schedule(cfg: ImportantData) -> bool:
        active = cfg.maintenance if isinstance(cfg.maintenance, dict) else {}
        existing = cfg.scheduled_maintenance if isinstance(cfg.scheduled_maintenance, dict) else {}
        if active.get("active") or existing.get("id"):
            return False
        cfg.scheduled_maintenance = dict(scheduled)
        return True

    if not await update_important_data(_schedule):
        if msg:
            await msg.reply_text("Другой администратор уже создал план или запустил работы. Откройте /maint заново.")
        _clear_maint_ctx(context)
        return ConversationHandler.END
    logger.info(
        "Maintenance scheduled by user_id=%s scope=%s start=%s end=%s",
        author_id,
        scope,
        scheduled.get("scheduled_start"),
        scheduled.get("scheduled_end"),
    )

    panel_text = (
        format_scheduled_maint(scope, start_at, end_at, author)
        + "\n\nУведомления будут поставлены в очередь за 3 суток, 12 часов и 30 минут, а также в момент начала."
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
    author = display_name(update)
    author_id = get_user_id(update)
    users_count = admins_count = 0

    def _extend_current(cfg):
        nonlocal users_count, admins_count
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
        notice = _maint_extend_notice(updated, hh, mm, author)
        event, users_count, admins_count = _make_maint_notice_event(
            author_id=author_id,
            text=notice,
            kind="maintenance_extended",
        )
        _enqueue_if_present(cfg, event)
        return updated

    try:
        maint = await update_important_data(_extend_current)
    except RuntimeError:
        if msg:
            await msg.reply_text("Техработы не активны или уже завершены.")
        _clear_maint_ctx(context)
        return ConversationHandler.END

    logger.info(
        "Maintenance extended by user_id=%s duration_min=%s maint_id=%s", get_user_id(update), duration_min, maint_id
    )
    panel_text = f"{_maint_panel_text(maint)}\n\n{_maint_delivery_status(users_count, admins_count)}"
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
    author = display_name(update)
    ended_at = datetime.now(TZ)
    author_id = get_user_id(update)
    users_count = admins_count = 0

    def _end(cfg: ImportantData) -> dict[str, Any] | None:
        nonlocal users_count, admins_count
        current = cfg.maintenance if isinstance(cfg.maintenance, dict) else {}
        if not current.get("active") or str(current.get("id") or "") != maint_id:
            return None
        previous = dict(current)
        notice = _maint_end_notice(previous, author, ended_at=ended_at)
        event, users_count, admins_count = _make_maint_notice_event(
            author_id=author_id,
            text=notice,
            kind="maintenance_ended",
        )
        cfg.maintenance = {}
        reminder_kind = f"maintenance_admin_reminder_{maint_id}"
        cfg.outbox = {
            event_id: pending
            for event_id, pending in cfg.outbox.items()
            if not (isinstance(pending, dict) and pending.get("kind") == reminder_kind)
        }
        _enqueue_if_present(cfg, event)
        return previous

    maint = await update_important_data(_end)
    if not isinstance(maint, dict):
        await q.edit_message_text("Техработы не активны или уже завершены.")
        return

    logger.info("Maintenance ended by user_id=%s maint_id=%s", get_user_id(update), maint_id)
    await q.edit_message_text("✅ Техработы завершены.\n\n" + _maint_delivery_status(users_count, admins_count))


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


@require_admin
async def maint_sched_cancel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"maint:schedcancel:([0-9a-f]+)", q.data or "")
    if not m:
        return
    sched_id = m.group(1)
    scheduled = get_scheduled_maintenance()
    if not scheduled or str(scheduled.get("id") or "") != sched_id:
        await q.edit_message_text("Запланированные техработы не найдены или уже неактуальны.")
        return
    text = _scheduled_panel_text(scheduled) + "\n\n<b>Отменить запланированные техработы?</b>"
    await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=_scheduled_cancel_confirm_kb(sched_id))


@require_admin
async def maint_sched_cancel_back_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"maint:schedcancelback:([0-9a-f]+)", q.data or "")
    if not m:
        return
    sched_id = m.group(1)
    scheduled = get_scheduled_maintenance()
    if not scheduled or str(scheduled.get("id") or "") != sched_id:
        await q.edit_message_text("Запланированные техработы не найдены или уже неактуальны.")
        return
    await q.edit_message_text(
        _scheduled_panel_text(scheduled),
        parse_mode=ParseMode.HTML,
        reply_markup=_scheduled_control_kb(sched_id),
    )


@require_admin
async def maint_sched_cancel_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    m = re.fullmatch(r"maint:schedcancelconfirm:([0-9a-f]+)", q.data or "")
    if not m:
        return
    sched_id = m.group(1)
    actor_id = get_user_id(update)
    users_count = admins_count = 0
    queued_notice = False

    def _cancel(cfg: ImportantData) -> dict[str, Any] | None:
        nonlocal users_count, admins_count, queued_notice
        current = cfg.scheduled_maintenance if isinstance(cfg.scheduled_maintenance, dict) else {}
        if str(current.get("id") or "") != sched_id:
            return None
        previous = dict(current)
        announced = previous.get("announced_thresholds")
        already_warned = (
            bool(announced) or bool(previous.get("notified_start")) or bool(previous.get("notified_before"))
        )
        cfg.scheduled_maintenance = {}
        if already_warned:
            event, users_count, admins_count = _make_maint_notice_event(
                author_id=actor_id,
                text=_maint_scheduled_cancel_notice(previous),
                kind="maintenance_schedule_cancelled",
            )
            _enqueue_if_present(cfg, event)
            queued_notice = event is not None
        return previous

    scheduled = await update_important_data(_cancel)
    if not isinstance(scheduled, dict):
        await q.edit_message_text("Запланированные техработы не найдены или уже неактуальны.")
        return

    logger.info("Scheduled maintenance cancelled by user_id=%s sched_id=%s", get_user_id(update), sched_id)
    if queued_notice:
        await q.edit_message_text(
            "✅ Запланированные техработы отменены; уведомление сохранено в очереди.\n\n"
            + _maint_delivery_status(users_count, admins_count)
        )
    else:
        await q.edit_message_text("✅ Запланированные техработы отменены.")


async def maint_restart_notify(context: ContextTypes.DEFAULT_TYPE) -> None:
    maint = get_active_maintenance()
    if not maint:
        return
    if not str(maint.get("id", "") or ""):
        return
    admin_ids = authorized_ids(role_filter="admin", exclude=set())
    if not admin_ids:
        return
    maint_id = str(maint.get("id") or "")
    reminder_kind = f"maintenance_admin_reminder_{maint_id}"
    text = _maint_active_reminder_text(maint) + "\n\nОткройте «/maint» для управления."
    event = make_outbox_event(
        kind=reminder_kind,
        recipient_ids=admin_ids,
        payload=message_payload(
            text,
            reply_markup=[[{"text": "🏠 Меню", "callback_data": "menu:home"}]],
        ),
    )

    def _queue_reminder(cfg: ImportantData) -> bool:
        current = cfg.maintenance if isinstance(cfg.maintenance, dict) else {}
        if not current.get("active") or str(current.get("id") or "") != maint_id:
            return False
        if any(isinstance(pending, dict) and pending.get("kind") == reminder_kind for pending in cfg.outbox.values()):
            return False
        enqueue_important_outbox(cfg, event)
        return True

    await update_important_data(_queue_reminder)


async def maint_schedule_tick(context: ContextTypes.DEFAULT_TYPE) -> None:
    scheduled = get_scheduled_maintenance()
    if not scheduled:
        return

    try:
        start_at = datetime.fromisoformat(str(scheduled.get("scheduled_start") or ""))
        end_at = datetime.fromisoformat(str(scheduled.get("scheduled_end") or ""))
    except Exception:
        logger.warning("Scheduled maintenance has invalid timestamps, clearing record")
        schedule_id = str(scheduled.get("id") or "")

        def _clear_invalid(cfg: ImportantData) -> None:
            current = cfg.scheduled_maintenance if isinstance(cfg.scheduled_maintenance, dict) else {}
            if str(current.get("id") or "") == schedule_id:
                cfg.scheduled_maintenance = {}

        await update_important_data(_clear_invalid)
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
        admin_ids = authorized_ids(role_filter="admin")
        announced = scheduled.get("announced_thresholds")
        users_were_notified = bool(announced) or bool(scheduled.get("notified_before"))
        user_ids = authorized_ids(role_filter="user") if users_were_notified else []
        expiry_recipients = sorted(set(admin_ids + user_ids))
        expiry_event = (
            make_outbox_event(
                kind="maintenance_schedule_expired",
                recipient_ids=expiry_recipients,
                payload=message_payload(
                    "⚠️ <b>Запланированные техработы отменены</b>\n"
                    "Окно работ прошло, пока они не были запущены (бот был недоступен или шли другие работы).",
                    reply_markup=[[{"text": "🏠 Меню", "callback_data": "menu:home"}]],
                ),
            )
            if expiry_recipients
            else None
        )

        def _expire(cfg: ImportantData) -> bool:
            current = cfg.scheduled_maintenance if isinstance(cfg.scheduled_maintenance, dict) else {}
            if str(current.get("id") or "") != str(scheduled.get("id") or ""):
                return False
            cfg.scheduled_maintenance = {}
            _enqueue_if_present(cfg, expiry_event)
            return True

        await update_important_data(_expire)
        return

    notified_raw = scheduled.get("notified_thresholds")
    if isinstance(notified_raw, list):
        notified = [int(x) for x in notified_raw if isinstance(x, int)]
    elif scheduled.get("notified_before"):
        notified = list(MAINT_WARN_THRESHOLDS_MIN)
    else:
        notified = _initial_notified_thresholds(int((start_at - now).total_seconds() // 60))

    remaining_min = int((start_at - now).total_seconds() // 60)
    due = _due_thresholds(notified, remaining_min)
    if due and now < start_at:
        updated_notified = sorted(set(notified) | set(due))
        notice = _maint_scheduled_soon_notice(scheduled, remaining_min)
        author_id = scheduled.get("author_id")
        event, _users_count, _admins_count = _make_maint_notice_event(
            author_id=author_id if isinstance(author_id, int) else None,
            text=notice,
            kind="maintenance_schedule_warning",
        )

        def _mark_thresholds(cfg):
            cur = dict(getattr(cfg, "scheduled_maintenance", {}) or {})
            if str(cur.get("id") or "") != str(scheduled.get("id") or ""):
                return None
            cur["notified_thresholds"] = updated_notified
            announced_raw = cur.get("announced_thresholds")
            announced = [int(x) for x in announced_raw if isinstance(x, int)] if isinstance(announced_raw, list) else []
            cur["announced_thresholds"] = sorted(set(announced) | set(due))
            cur.pop("notified_before", None)
            cur["updated_at"] = now.isoformat()
            cfg.scheduled_maintenance = cur
            _enqueue_if_present(cfg, event)
            return cur

        updated_schedule = await update_important_data(_mark_thresholds)
        if not isinstance(updated_schedule, dict):
            return
        scheduled = updated_schedule

    if now >= start_at and not bool(scheduled.get("notified_start", False)):
        active = get_active_maintenance()
        if active:
            logger.info(
                "Scheduled maintenance %s deferred: another maintenance %s is active",
                scheduled.get("id"),
                active.get("id"),
            )
            return

        notice = _maint_scheduled_start_notice(scheduled)
        author_id = scheduled.get("author_id")
        start_event, _users_count, _admins_count = _make_maint_notice_event(
            author_id=author_id if isinstance(author_id, int) else None,
            text=notice,
            kind="maintenance_schedule_started",
        )

        def _activate_scheduled(cfg):
            cur = dict(getattr(cfg, "scheduled_maintenance", {}) or {})
            if str(cur.get("id") or "") != str(scheduled.get("id") or ""):
                return None
            existing = getattr(cfg, "maintenance", {}) or {}
            if isinstance(existing, dict) and existing.get("active"):
                return None
            cfg.maintenance = _scheduled_to_active_record(cur)
            cfg.scheduled_maintenance = {}
            _enqueue_if_present(cfg, start_event)
            return dict(cfg.maintenance)

        activated = await update_important_data(_activate_scheduled)
        if not activated:
            return
        logger.info(
            "Scheduled maintenance activated id=%s start=%s end=%s",
            scheduled.get("id"),
            scheduled.get("scheduled_start"),
            scheduled.get("scheduled_end"),
        )
