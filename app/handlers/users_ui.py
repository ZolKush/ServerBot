from typing import Any, Dict, List, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from ..storage import authorized_users_snapshot
from .common import breadcrumbs, display_name_from_meta, get_user_meta, html_escape

USER_FILTER_ALL = "all"
USER_FILTER_ACTIVE = "active"
USER_FILTER_DISABLED = "disabled"
USER_FILTER_UNPAID = "unpaid"
USER_FILTER_ADMINS = "admins"
USER_FILTERS = (USER_FILTER_ALL, USER_FILTER_ACTIVE, USER_FILTER_DISABLED, USER_FILTER_UNPAID, USER_FILTER_ADMINS)


def users_filter_label(filter_key: str) -> str:
    labels = {
        USER_FILTER_ALL: "Все",
        USER_FILTER_ACTIVE: "Активные",
        USER_FILTER_DISABLED: "Отключенные",
        USER_FILTER_UNPAID: "Неоплаченные",
        USER_FILTER_ADMINS: "Админы",
    }
    return labels.get(filter_key, labels[USER_FILTER_ALL])


def users_list_title(filter_key: str) -> str:
    return (
        f"<b>{html_escape(breadcrumbs('Админ-панель', 'Пользователи'))}</b>\n\n"
        f"Фильтр: <b>{html_escape(users_filter_label(filter_key))}</b>\n"
        "Выберите пользователя:"
    )


def _filter_buttons(active_filter: str) -> List[List[InlineKeyboardButton]]:
    def _btn(label: str, key: str) -> InlineKeyboardButton:
        suffix = " ✅" if active_filter == key else ""
        return InlineKeyboardButton(f"{label}{suffix}", callback_data=f"users:filter:{key}")

    return [
        [_btn("Все", USER_FILTER_ALL), _btn("Активные", USER_FILTER_ACTIVE)],
        [_btn("Откл.", USER_FILTER_DISABLED), _btn("Неопл.", USER_FILTER_UNPAID)],
        [_btn("Админы", USER_FILTER_ADMINS)],
    ]


def _passes_filter(meta: Dict[str, Any], filter_key: str) -> bool:
    role = str(meta.get("role", "user"))
    enabled = bool(meta.get("enabled", True))
    is_paid = bool(meta.get("is_paid", False))
    if filter_key == USER_FILTER_ACTIVE:
        return enabled
    if filter_key == USER_FILTER_DISABLED:
        return not enabled
    if filter_key == USER_FILTER_UNPAID:
        return not is_paid
    if filter_key == USER_FILTER_ADMINS:
        return role == "admin"
    return True


def users_list_kb(active_filter: str = USER_FILTER_ALL) -> InlineKeyboardMarkup:
    if active_filter not in USER_FILTERS:
        active_filter = USER_FILTER_ALL

    buttons: List[List[InlineKeyboardButton]] = []
    buttons.extend(_filter_buttons(active_filter))
    buttons.append([InlineKeyboardButton("📣 Рассылка всем", callback_data="users:all")])

    items: List[Tuple[str, bool, bool, str, int, str]] = []
    for k, meta in authorized_users_snapshot().items():
        try:
            uid = int(meta.get("user_id", k))
        except Exception:
            continue
        if not _passes_filter(meta, active_filter):
            continue
        name = display_name_from_meta(meta)
        role = str(meta.get("role", "user"))
        enabled = bool(meta.get("enabled", True))
        is_paid = bool(meta.get("is_paid", False))
        items.append((role, enabled, is_paid, name.lower(), uid, name))

    items.sort(key=lambda x: (0 if x[0] == "user" else 1, x[3], x[4]))

    row: List[InlineKeyboardButton] = []
    for role, enabled, is_paid, _, uid, name in items:
        prefix = ""
        if not enabled:
            prefix += "⛔ "
        if role == "admin":
            prefix += "👑⭐ " if is_paid else "👑 "
        elif is_paid:
            prefix += "⭐ "
        label = f"{prefix}{name}"
        row.append(InlineKeyboardButton(label[:50], callback_data=f"users:user:{uid}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    if not items:
        buttons.append([InlineKeyboardButton("∅ Нет пользователей", callback_data="users:noop")])
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
    enabled = bool(meta.get("enabled", True))
    role = meta.get("role", "user")

    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton("✉️ Написать сообщение", callback_data=f"users:msg:{uid}")],
        [InlineKeyboardButton("🏷 Изменить ник", callback_data=f"users:nick:{uid}")],
        [InlineKeyboardButton("⭐ Переключить оплату", callback_data=f"users:paid:{uid}")],
        [
            InlineKeyboardButton("💾 Назначить подписку", callback_data=f"users:subassign:{uid}"),
            InlineKeyboardButton("📤 Отправить подписку", callback_data=f"users:subsend:{uid}"),
        ],
        [InlineKeyboardButton("🪄 Обновить меню", callback_data=f"users:refresh:{uid}")],
    ]

    if role != "admin":
        rows.append([InlineKeyboardButton("🚫 Забанить" if enabled else "✅ Разбанить", callback_data=f"users:toggle:{uid}")])

    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="users:back")])
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def confirm_toggle_kb(uid: int, enabled_now: bool) -> InlineKeyboardMarkup:
    action = "Разбанить" if not enabled_now else "Забанить"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"✅ Подтвердить: {action}", callback_data=f"users:toggleapply:{uid}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"users:user:{uid}")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


def confirm_paid_kb(uid: int, is_paid_now: bool) -> InlineKeyboardMarkup:
    action = "Снять оплату" if is_paid_now else "Отметить оплачено"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"✅ Подтвердить: {action}", callback_data=f"users:paidapply:{uid}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"users:user:{uid}")],
            [InlineKeyboardButton("🏠 Меню", callback_data="menu:home")],
        ]
    )


def format_user_card(meta: Dict[str, Any]) -> str:
    uid = meta.get("user_id", "-")
    role = meta.get("role", "user")
    nick = meta.get("nickname") or "-"
    uname = meta.get("username")
    nm = " ".join([x for x in [meta.get("first_name"), meta.get("last_name")] if x]) or "-"
    auth_at = meta.get("auth_at") or "-"
    status = "активен" if meta.get("enabled", True) else "отключен"
    has_subscription = bool(str(meta.get("subscription_text", "") or "").strip())
    subscription_updated_at = meta.get("subscription_updated_at") or "-"
    return (
        f"<b>{html_escape(breadcrumbs('Админ-панель', 'Пользователи', str(uid)))}</b>\n\n"
        "Карточка пользователя\n"
        f"• ID: <code>{html_escape(str(uid))}</code>\n"
        f"• Роль: <b>{html_escape(str(role))}</b>\n"
        f"• Статус: <b>{html_escape(status)}</b>\n"
        f"• Оплата: <b>{'оплачена' if bool(meta.get('is_paid', False)) else 'не оплачена'}</b>\n"
        f"• Конфиг: <b>{'назначен' if has_subscription else 'не назначен'}</b>\n"
        f"• Обновлён: <code>{html_escape(str(subscription_updated_at))}</code>\n"
        f"• Ник: <b>{html_escape(str(nick))}</b>\n"
        f"• Username: <b>{html_escape(('@' + uname) if uname else '-')}</b>\n"
        f"• Имя: <b>{html_escape(str(nm))}</b>\n"
        f"• Авторизация: <code>{html_escape(str(auth_at))}</code>"
    )
