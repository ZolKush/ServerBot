"""Validation and staging for administrative subscription inputs."""

from __future__ import annotations

from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode

from ...bot.ui import html_escape
from ...messaging.message_cleanup import record_navigation_result
from ...storage import authorized_users_snapshot, get_user_meta_copy
from ...users.staff import is_billing_exempt_meta
from ..connections import has_connection
from . import state
from .eligibility import is_eligible_paid_subscriber, is_paid_subscriber, parse_id_list


def _confirmation_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Подтвердить", callback_data="product:confirm:apply")],
            [InlineKeyboardButton("❌ Отмена", callback_data="product:cancel")],
        ]
    )


async def handle_mass_reminder_input(update: Update, message: Any, data: dict[str, Any], text: str) -> int:
    targets: list[int] = []
    snapshot = authorized_users_snapshot()
    lowered = text.lower()
    if lowered == "все":
        targets = [int(meta.get("user_id", key)) for key, meta in snapshot.items() if is_eligible_paid_subscriber(meta)]
    elif lowered.startswith("до "):
        cutoff = state.parse_input_datetime(text[3:].strip())
        if cutoff is None:
            await message.reply_text("Некорректная дата. Используйте ДД.ММ.ГГГГ ЧЧ:ММ.")
            return state.PRODUCT_INPUT
        for key, meta in snapshot.items():
            end = state.parse_datetime(meta.get("subscription_end_at"))
            if is_eligible_paid_subscriber(meta) and end and end <= cutoff:
                targets.append(int(meta.get("user_id", key)))
    else:
        parsed_ids = parse_id_list(text)
        if parsed_ids is None:
            await message.reply_text("Введите «все», условие с датой или Telegram ID через запятую.")
            return state.PRODUCT_INPUT
        targets = sorted(parsed_ids)
    if not targets:
        await message.reply_text("Подходящих получателей нет. Повторите ввод.")
        return state.PRODUCT_INPUT
    data[state.CTX_PENDING] = {"kind": "mass_reminder", "target_ids": sorted(set(targets))}
    result = await message.reply_text(
        f"Будет подготовлено напоминаний: <b>{len(set(targets))}</b>. Подтвердите отправку.",
        parse_mode=ParseMode.HTML,
        reply_markup=_confirmation_markup(),
    )
    await record_navigation_result(update, result)
    return state.PRODUCT_CONFIRM


async def handle_mass_date_input(update: Update, message: Any, data: dict[str, Any], text: str) -> int:
    date_part, separator, ids_part = text.partition("|")
    target = state.parse_input_datetime(date_part.strip())
    if target is None:
        await message.reply_text("Некорректная дата. Используйте ДД.ММ.ГГГГ ЧЧ:ММ.")
        return state.PRODUCT_INPUT
    selected_ids = parse_id_list(ids_part.strip()) if separator else None
    if separator and selected_ids is None:
        await message.reply_text("После | укажите корректные Telegram ID через запятую.")
        return state.PRODUCT_INPUT
    snapshot = authorized_users_snapshot()
    candidates = [
        int(meta.get("user_id", key))
        for key, meta in snapshot.items()
        if is_eligible_paid_subscriber(meta) and (selected_ids is None or int(meta.get("user_id", key)) in selected_ids)
    ]
    skipped = (len(selected_ids) - len(candidates)) if selected_ids is not None else 0
    if not candidates:
        await message.reply_text("Нет оплаченных подписчиков, которым можно назначить эту дату.")
        return state.PRODUCT_INPUT
    data[state.CTX_PENDING] = {
        "kind": "mass_date",
        "target_ids": sorted(set(candidates)),
        "target_end_at": target.isoformat(),
        "skipped": max(0, skipped),
    }
    result = await message.reply_text(
        "📅 <b>Проверка массового изменения</b>\n\n"
        f"• Новая дата: <code>{html_escape(state.datetime_text(target.isoformat()))}</code>\n"
        f"• Будет изменено: <b>{len(set(candidates))}</b>\n"
        f"• Пропущено: <b>{max(0, skipped)}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=_confirmation_markup(),
    )
    await record_navigation_result(update, result)
    return state.PRODUCT_CONFIRM


async def handle_user_date_input(
    update: Update,
    message: Any,
    data: dict[str, Any],
    action: str,
    text: str,
) -> int:
    target = state.parse_input_datetime(text)
    if target is None:
        await message.reply_text("Некорректная дата. Используйте ДД.ММ.ГГГГ ЧЧ:ММ.")
        return state.PRODUCT_INPUT
    if action == "manualpay" and target <= state.now():
        await message.reply_text("Для этого действия дата должна находиться в будущем.")
        return state.PRODUCT_INPUT
    pending: dict[str, Any] = {"kind": action, "target_end_at": target.isoformat()}
    target_user_id = data.get(state.CTX_TARGET_UID)
    if isinstance(target_user_id, int):
        pending["target_uid"] = target_user_id
    user = get_user_meta_copy(int(target_user_id or 0))
    if action == "user_end" and (not user or not is_paid_subscriber(user)):
        await message.reply_text(
            "⛔ Невозможно изменить дату окончания\n\n"
            "Оплата пользователя не подтверждена. Сначала руководитель "
            "сервиса должен подтвердить оплату."
        )
        return state.PRODUCT_INPUT
    if action == "manualpay":
        if not user:
            await message.reply_text("Пользователь не найден.")
            return state.PRODUCT_INPUT
        if is_billing_exempt_meta(user):
            await message.reply_text("У руководителя сервиса бессрочный оплаченный доступ.")
            return state.PRODUCT_INPUT
        if not has_connection(user):
            await message.reply_text("Сначала назначьте пользователю персональную ссылку подключения.")
            return state.PRODUCT_INPUT
    data[state.CTX_PENDING] = pending
    labels = {
        "user_end": "Дата окончания пользователя",
        "manualpay": "Ручное подтверждение оплаты",
    }
    result = await message.reply_text(
        f"<b>{html_escape(labels[action])}</b>\n\n"
        f"Новое значение: <code>{html_escape(state.datetime_text(target.isoformat()))}</code>\n\n"
        "Подтвердите изменение.",
        parse_mode=ParseMode.HTML,
        reply_markup=_confirmation_markup(),
    )
    await record_navigation_result(update, result)
    return state.PRODUCT_CONFIRM


__all__ = [
    "handle_mass_date_input",
    "handle_mass_reminder_input",
    "handle_user_date_input",
]
