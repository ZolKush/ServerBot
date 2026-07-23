from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from ..staff import (
    is_admin_meta,
    is_billing_exempt_meta,
    staff_internal_identity,
    staff_public_signature,
    staff_title_label,
)
from ..storage import authorized_users_snapshot, get_user_audit_entries
from .common import clip_html, display_name_from_meta, format_dt_human, get_user_meta, html_escape
from .subscription import CONNECTION_URL_KEY
from .ui import pager_row

USERS_PAGE_SIZE = 30  # кнопок на страницу: лимит Telegram — 100 кнопок на клавиатуру

USER_FILTER_ALL = "all"
USER_FILTER_ACTIVE = "active"
USER_FILTER_DISABLED = "disabled"
USER_FILTER_UNPAID = "unpaid"
USER_FILTER_ADMINS = "admins"
USER_FILTER_BLOCKED = "blocked"
USER_FILTERS = (
    USER_FILTER_ALL,
    USER_FILTER_ACTIVE,
    USER_FILTER_DISABLED,
    USER_FILTER_UNPAID,
    USER_FILTER_ADMINS,
    USER_FILTER_BLOCKED,
)


def _field(value: object, *, limit: int = 160) -> str:
    return clip_html(str(value if value not in (None, "") else "-"), limit=limit)


def users_filter_label(filter_key: str) -> str:
    labels = {
        USER_FILTER_ALL: "Все",
        USER_FILTER_ACTIVE: "Активные",
        USER_FILTER_DISABLED: "Отключенные",
        USER_FILTER_UNPAID: "Неоплаченные",
        USER_FILTER_ADMINS: "Админы",
        USER_FILTER_BLOCKED: "Заблокированные",
    }
    return labels.get(filter_key, labels[USER_FILTER_ALL])


def users_list_title(filter_key: str) -> str:
    return (
        "👥 <b>Пользователи</b>\n\n"
        f"Фильтр: <b>{html_escape(users_filter_label(filter_key))}</b>\n"
        "Выберите пользователя:"
    )


def _filter_buttons(active_filter: str) -> list[list[InlineKeyboardButton]]:
    def _btn(label: str, key: str) -> InlineKeyboardButton:
        suffix = " ✅" if active_filter == key else ""
        return InlineKeyboardButton(f"{label}{suffix}", callback_data=f"users:filter:{key}")

    return [
        [_btn("Все", USER_FILTER_ALL), _btn("Активные", USER_FILTER_ACTIVE)],
        [_btn("Откл.", USER_FILTER_DISABLED), _btn("Неопл.", USER_FILTER_UNPAID)],
        [_btn("Админы", USER_FILTER_ADMINS), _btn("Заблок.", USER_FILTER_BLOCKED)],
    ]


def _passes_filter(meta: dict[str, Any], filter_key: str) -> bool:
    role = str(meta.get("role", "user"))
    tier = str(meta.get("service_tier") or "basic")
    enabled = bool(meta.get("enabled", True))
    is_paid = bool(meta.get("is_paid", False))
    state = str(meta.get("access_state") or ("approved" if enabled else "blocked"))
    if filter_key == USER_FILTER_BLOCKED:
        return state == "blocked"
    if state == "blocked":
        return False
    if filter_key == USER_FILTER_ACTIVE:
        return state == "approved" and enabled
    if filter_key == USER_FILTER_DISABLED:
        return state != "approved" or not enabled
    if filter_key == USER_FILTER_UNPAID:
        return not is_billing_exempt_meta(meta) and tier != "unlimited_trial" and not is_paid
    if filter_key == USER_FILTER_ADMINS:
        return role == "admin"
    return True


def users_list_kb(active_filter: str = USER_FILTER_ALL, page: int = 0) -> InlineKeyboardMarkup:
    if active_filter not in USER_FILTERS:
        active_filter = USER_FILTER_ALL

    buttons: list[list[InlineKeyboardButton]] = []
    buttons.extend(_filter_buttons(active_filter))
    buttons.append([InlineKeyboardButton("📣 Рассылка всем", callback_data="users:all")])

    items: list[tuple[str, bool, bool, str, str, int, str]] = []
    for k, meta in authorized_users_snapshot().items():
        try:
            uid = int(meta.get("user_id", k))
        except (TypeError, ValueError, OverflowError):
            continue
        if not _passes_filter(meta, active_filter):
            continue
        name = display_name_from_meta(meta)
        role = str(meta.get("role", "user"))
        tier = str(meta.get("service_tier") or "basic")
        enabled = bool(meta.get("enabled", True))
        is_paid = bool(meta.get("is_paid", False))
        items.append((role, enabled, is_paid, tier, name.lower(), uid, name))

    items.sort(key=lambda x: (0 if x[0] == "admin" else 1, x[4], x[5]))

    total_pages = max(1, (len(items) + USERS_PAGE_SIZE - 1) // USERS_PAGE_SIZE)
    page = max(0, min(int(page), total_pages - 1))
    page_items = items[page * USERS_PAGE_SIZE : (page + 1) * USERS_PAGE_SIZE]

    row: list[InlineKeyboardButton] = []
    for role, enabled, is_paid, tier, _, uid, name in page_items:
        prefix = ""
        if not enabled:
            prefix += "⛔ "
        if role == "admin":
            prefix += "👑⭐ " if is_paid else "👑 "
        elif tier == "unlimited_trial":
            prefix += "♾️ "
        elif is_paid:
            prefix += "⭐ "
        label = f"{prefix}{name}"
        row.append(InlineKeyboardButton(label[:50], callback_data=f"users:user:{uid}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    if total_pages > 1:
        buttons.append(pager_row("users:page:", page, total_pages))
    buttons.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(buttons)


def users_all_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✉️ Подготовить рассылку", callback_data="users:allmsg")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="users:back")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


def users_all_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Отправить всем", callback_data="users:allsend")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="users:all")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


def user_card_kb(uid: int) -> InlineKeyboardMarkup:
    meta = get_user_meta(uid) or {}
    state = str(meta.get("access_state") or ("approved" if meta.get("enabled", True) else "blocked"))
    enabled = state == "approved"
    role = meta.get("role", "user")

    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton("✉️ Написать сообщение", callback_data=f"users:msg:{uid}")],
        [InlineKeyboardButton("🏷 Изменить ник", callback_data=f"users:nick:{uid}")],
        [
            InlineKeyboardButton("💾 Назначить ссылку", callback_data=f"users:subassign:{uid}"),
            InlineKeyboardButton("📤 Отправить ссылку", callback_data=f"users:subsend:{uid}"),
        ],
        [InlineKeyboardButton("⚙️ Управление доступом", callback_data=f"product:manage:{uid}")],
    ]

    if role != "admin":
        if enabled:
            rows.append([InlineKeyboardButton("🚫 Забанить", callback_data=f"users:access:block:{uid}")])
        elif state == "blocked":
            rows.append([InlineKeyboardButton("✅ Разбанить", callback_data=f"users:access:approve:{uid}")])
        else:
            rows.append(
                [
                    InlineKeyboardButton("✅ Одобрить доступ", callback_data=f"users:access:approve:{uid}"),
                    InlineKeyboardButton("🚫 Забанить", callback_data=f"users:access:block:{uid}"),
                ]
            )

    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="users:back")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def confirm_toggle_kb(uid: int, access_state: str) -> InlineKeyboardMarkup:
    action = (
        "Забанить" if access_state == "approved" else ("Разбанить" if access_state == "blocked" else "Одобрить доступ")
    )
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"✅ Подтвердить: {action}", callback_data=f"users:toggleapply:{uid}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"users:user:{uid}")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


def confirm_access_kb(uid: int, *, desired_state: str, current_state: str) -> InlineKeyboardMarkup:
    if desired_state == "blocked":
        action = "Забанить"
        callback_action = "block"
    else:
        action = "Разбанить" if current_state == "blocked" else "Одобрить доступ"
        callback_action = "approve"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"✅ Подтвердить: {action}",
                    callback_data=f"users:accessapply:{callback_action}:{uid}",
                )
            ],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"users:user:{uid}")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


def format_user_card(meta: dict[str, Any]) -> str:
    uid = meta.get("user_id", "-")
    role = meta.get("role", "user")
    nick = meta.get("nickname") or "-"
    uname = meta.get("username")
    nm = " ".join([x for x in [meta.get("first_name"), meta.get("last_name")] if x]) or "-"
    auth_at = meta.get("auth_at") or "-"
    state = str(meta.get("access_state") or ("approved" if meta.get("enabled", True) else "blocked"))
    state_labels = {
        "approved": "доступ одобрен",
        "pending": "ожидает одобрения",
        "blocked": "заблокирован",
        "logged_out": "вышел",
        "rejected": "заявка отклонена",
    }
    status = state_labels.get(state, "статус неизвестен")
    status_dot = "🟢" if state == "approved" else ("🟡" if state == "pending" else "🔴")
    has_connection = bool(str(meta.get(CONNECTION_URL_KEY, "") or "").strip())
    subscription_updated_at = meta.get("subscription_updated_at") or "-"
    auth_at_human = format_dt_human(auth_at)
    subscription_updated_at_human = format_dt_human(subscription_updated_at)
    name = display_name_from_meta(meta)
    tier_label = {
        "basic": "Базовый доступ",
        "subscriber": "Подписчик",
        "unlimited_trial": "Безлимитный тестовый доступ",
    }.get(str(meta.get("service_tier") or "basic"), "Неизвестно")
    role_label = "Администратор" if role == "admin" else "Пользователь"
    billing_exempt = is_billing_exempt_meta(meta)
    if billing_exempt:
        tier_label = "Бессрочный оплаченный доступ — руководитель сервиса"
    payment_label = (
        "♾️ бессрочно — руководитель сервиса"
        if billing_exempt
        else ("⭐ оплачена" if bool(meta.get("is_paid", False)) else "не оплачена")
    )
    paid_at_label = "не применяется" if billing_exempt else format_dt_human(meta.get("paid_at"))
    end_label = "бессрочно" if billing_exempt else format_dt_human(meta.get("subscription_end_at"))
    auto_reminder_type = {
        "3d": "за 3 дня",
        "1d": "за 1 день",
        "15m": "за 15 минут",
    }.get(str(meta.get("last_auto_payment_reminder_type") or ""), "-")
    lines = [
        f"👤 <b>{_field(name, limit=240)}</b> · {status_dot} {html_escape(status)}\n"
        f"ID: <code>{_field(uid, limit=32)}</code>\n"
        "\n"
        "🔐 <b>Доступ</b>\n"
        f"• Статус: <b>{html_escape(status)}</b>\n"
        f"• Тип: <b>{html_escape(role_label)}</b>\n"
        f"• Уровень: <b>{html_escape(tier_label)}</b>\n"
        f"• Оплата: <b>{payment_label}</b>\n"
        f"• Оплачено: <code>{_field(paid_at_label, limit=120)}</code>\n"
        f"• Окончание: <code>{_field(end_label, limit=120)}</code>\n"
        f"• Авторизация: <code>{_field(auth_at_human, limit=120)}</code>\n"
        "\n"
        "🔗 <b>Подключение</b>\n"
        f"• Персональная ссылка: <b>{'назначена' if has_connection else 'не назначена'}</b>\n"
        f"• Обновлён: <code>{_field(subscription_updated_at_human, limit=120)}</code>\n"
        f"• Тест выдавался: <b>{'да' if meta.get('trial_issued_at') else 'нет'}</b>\n"
        "\n"
        "🔔 <b>Напоминания</b>\n"
        f"• Автоматическое: <code>{_field(format_dt_human(meta.get('last_auto_payment_reminder_at')), limit=120)}</code>"
        f" ({_field(auto_reminder_type, limit=40)})\n"
        f"• Ручное: <code>{_field(format_dt_human(meta.get('last_manual_payment_reminder_at')), limit=120)}</code>\n"
        f"• Отправил: <b>{_field(meta.get('last_manual_payment_reminder_by_name'), limit=180)}</b>\n"
        "\n"
        "📇 <b>Профиль</b>\n"
        f"• Ник: <b>{_field(nick, limit=220)}</b>\n"
        f"• Резервная почта: <code>{_field(meta.get('contact_email'), limit=254)}</code>\n"
        f"• Username: <b>{_field(('@' + uname) if uname else '-', limit=100)}</b>\n"
        f"• Имя: <b>{_field(nm, limit=260)}</b>"
    ]
    if is_admin_meta(meta):
        lines.extend(
            [
                "",
                "🪪 <b>Сотрудник</b>",
                f"• Должность: <b>{html_escape(staff_title_label(meta))}</b>",
                f"• Публичная подпись: <b>{html_escape(staff_public_signature(meta))}</b>",
                f"• Внутренняя личность: <code>{_field(staff_internal_identity(meta), limit=420)}</code>",
            ]
        )
    audit = get_user_audit_entries(int(uid), limit=3) if str(uid).isdigit() else []
    if audit:
        lines.extend(["", "🧾 <b>Последние изменения</b>"])
        for item in audit:
            lines.append(
                f"• <code>{_field(format_dt_human(item.get('ts')), limit=120)}</code> "
                f"{_field(item.get('action'), limit=100)} — "
                f"{_field(item.get('actor_public') or 'Система', limit=180)} "
                f"(<code>{_field(item.get('actor_internal'), limit=260)}</code>)"
            )
    return "\n".join(lines)
