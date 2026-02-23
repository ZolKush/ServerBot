from typing import Any, Dict, List, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from ..storage import authorized_users_snapshot
from .common import display_name_from_meta, get_user_meta, html_escape


def users_list_kb() -> InlineKeyboardMarkup:
    buttons: List[List[InlineKeyboardButton]] = []
    buttons.append([InlineKeyboardButton("📣 Все пользователи", callback_data="users:all")])

    items: List[Tuple[str, bool, bool, str, int, str]] = []
    for k, meta in authorized_users_snapshot().items():
        try:
            uid = int(meta.get("user_id", k))
        except Exception:
            continue
        name = display_name_from_meta(meta)
        role = meta.get("role", "user")
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
    buttons.append([InlineKeyboardButton("⬅️ В меню", callback_data="users:main")])
    return InlineKeyboardMarkup(buttons)


def users_all_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✉️ Сообщение всем", callback_data="users:allmsg")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="users:back")],
        ]
    )


def user_card_kb(uid: int) -> InlineKeyboardMarkup:
    meta = get_user_meta(uid) or {}
    enabled = bool(meta.get("enabled", True))
    role = meta.get("role", "user")

    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton("✉️ Написать сообщение", callback_data=f"users:msg:{uid}")],
        [InlineKeyboardButton("🏷 Добавить/изменить ник", callback_data=f"users:nick:{uid}")],
        [InlineKeyboardButton("⭐ Переключить оплату", callback_data=f"users:paid:{uid}")],
        [InlineKeyboardButton("📦 Отправить конфигурацию", callback_data=f"users:cfg:{uid}")],
    ]

    if role != "admin":
        rows.append([InlineKeyboardButton("🚫 Забанить" if enabled else "✅ Разбанить", callback_data=f"users:toggle:{uid}")])

    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="users:back")])
    return InlineKeyboardMarkup(rows)


def format_user_card(meta: Dict[str, Any]) -> str:
    uid = meta.get("user_id", "-")
    role = meta.get("role", "user")
    nick = meta.get("nickname") or "-"
    uname = meta.get("username")
    nm = " ".join([x for x in [meta.get("first_name"), meta.get("last_name")] if x]) or "-"
    auth_at = meta.get("auth_at") or "-"
    status = "enabled" if meta.get("enabled", True) else "disabled"
    return (
        "<b>Пользователь</b>\n"
        f"• ID: <code>{html_escape(str(uid))}</code>\n"
        f"• Роль: <b>{html_escape(str(role))}</b>\n"
        f"• Статус: <b>{html_escape(status)}</b>\n"
        f"• Подписка: <b>{'оплачена' if bool(meta.get('is_paid', False)) else 'не оплачена'}</b>\n"
        f"• Ник: <b>{html_escape(str(nick))}</b>\n"
        f"• Username: <b>{html_escape(('@' + uname) if uname else '-')}</b>\n"
        f"• Имя: <b>{html_escape(str(nm))}</b>\n"
        f"• Авторизация: <code>{html_escape(str(auth_at))}</code>"
    )
