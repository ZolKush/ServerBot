from __future__ import annotations

from app.administration.views import administration_markup, service_settings_markup
from app.bot.help import render_help_message
from app.bot.menu import main_menu_inline_kb_for_meta
from app.storage import UserData
from app.users.profile_handlers import personal_profile_text
from app.users.staff import (
    STAFF_TITLE_LEAD,
    STAFF_TITLE_MAINTAINER,
    STAFF_TITLE_SUPPORT,
    can_confirm_payments_meta,
    can_edit_help_meta,
    can_manage_subscription_dates_meta,
    can_send_payment_reminders_meta,
    is_billing_exempt_meta,
)
from app.users.validation import normalize_email
from app.users.views import (
    USER_FILTER_ADMINS,
    USER_FILTER_ALL,
    USER_FILTER_BLOCKED,
    USER_FILTER_DISABLED,
    USER_FILTER_UNPAID,
    _passes_filter,
)


def _admin(uid: int, *, title: str = STAFF_TITLE_SUPPORT, owner: bool = False) -> dict:
    return UserData._normalize_user(
        {
            "user_id": uid,
            "role": "admin",
            "access_state": "approved",
            "admin_level": "owner" if owner else "admin",
            "staff_title": title,
            "first_name": f"Admin {uid}",
            "service_tier": "subscriber",
            "is_paid": True,
        }
    )


def _callbacks(markup) -> set[str]:
    return {
        str(button.callback_data)
        for row in markup.inline_keyboard
        for button in row
        if button.callback_data is not None
    }


def test_staff_permission_matrix_matches_administration_policy() -> None:
    support = _admin(1)
    engineer = _admin(2, title=STAFF_TITLE_MAINTAINER)
    lead = _admin(3, title=STAFF_TITLE_LEAD)
    owner = _admin(4, owner=True)

    assert not can_edit_help_meta(support)
    assert can_edit_help_meta(engineer)
    assert can_edit_help_meta(lead)
    assert can_edit_help_meta(owner)
    assert not can_manage_subscription_dates_meta(engineer)
    assert can_manage_subscription_dates_meta(lead)
    assert can_manage_subscription_dates_meta(owner)
    assert not can_send_payment_reminders_meta(engineer)
    assert can_send_payment_reminders_meta(lead)
    assert can_send_payment_reminders_meta(owner)
    assert not can_confirm_payments_meta(lead)
    assert can_confirm_payments_meta(owner)
    assert not is_billing_exempt_meta(lead)
    assert is_billing_exempt_meta(owner)


def test_administration_buttons_follow_staff_permissions() -> None:
    support = _callbacks(administration_markup(_admin(1)))
    engineer = _callbacks(administration_markup(_admin(2, title=STAFF_TITLE_MAINTAINER)))
    lead = _callbacks(administration_markup(_admin(3, title=STAFF_TITLE_LEAD)))
    owner = _callbacks(service_settings_markup(_admin(4, owner=True)))

    assert "administration:settings" not in support
    assert "administration:settings" in engineer
    assert "product:input:massdate" not in engineer
    assert {"product:input:massdate", "product:input:massremind"}.issubset(lead)
    assert {
        "administration:input:help",
        "administration:input:support_email",
        "administration:input:payment_bank",
        "administration:input:period_next",
    }.issubset(owner)


def test_main_menu_uses_administration_section_name() -> None:
    labels = [button.text for row in main_menu_inline_kb_for_meta(_admin(1)).inline_keyboard for button in row]

    assert "⚙️ Администрирование" in labels
    assert "👤 Профиль сотрудника" not in labels


def test_help_is_editable_plain_text_with_global_support_contact() -> None:
    rendered = render_help_message(
        {
            "help_text": "Шаг <проверки> & обращение",
            "support_email": "support@example.com",
        }
    )

    assert "Шаг &lt;проверки&gt; &amp; обращение" in rendered
    assert "support@example.com" in rendered
    assert "Резервный канал связи с администрацией" in rendered


def test_backup_email_validation_normalization_and_profile_display() -> None:
    assert normalize_email(" User.Name@EXAMPLE.COM ") == "User.Name@example.com"
    assert normalize_email("missing-domain@example") is None
    assert normalize_email("two@@example.com") is None
    assert normalize_email("two..dots@example.com") is None
    assert normalize_email("bad,local@example.com") is None
    meta = UserData._normalize_user(
        {
            "user_id": 10,
            "role": "user",
            "access_state": "approved",
            "contact_email": " User.Name@EXAMPLE.COM ",
        }
    )

    assert meta["contact_email"] == "User.Name@example.com"
    assert "User.Name@example.com" in personal_profile_text(meta)


def test_blocked_users_are_isolated_and_unpaid_staff_remain_visible() -> None:
    blocked = UserData._normalize_user({"user_id": 10, "role": "user", "access_state": "blocked", "enabled": False})
    unpaid_staff = _admin(20)
    unpaid_staff = UserData._normalize_user({**unpaid_staff, "service_tier": "basic", "is_paid": False})
    owner = _admin(30, owner=True)

    assert _passes_filter(blocked, USER_FILTER_BLOCKED)
    assert not _passes_filter(blocked, USER_FILTER_ALL)
    assert not _passes_filter(blocked, USER_FILTER_DISABLED)
    assert not _passes_filter(blocked, USER_FILTER_ADMINS)
    assert _passes_filter(unpaid_staff, USER_FILTER_UNPAID)
    assert not _passes_filter(owner, USER_FILTER_UNPAID)
