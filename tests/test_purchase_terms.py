from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.ext import ConversationHandler

from app import storage
from app.bot.text_limits import utf16_length
from app.config import TZ
from app.messaging.review_sync import record_review_delivery, review_completion
from app.subscriptions.requests import state
from app.subscriptions.requests.confirmation import product_confirm_cb
from app.subscriptions.requests.customer import purchase_create_cb, purchase_show_cb
from app.subscriptions.requests.flow_cleanup import product_cancel
from app.subscriptions.requests.input_processing import product_text_input
from app.subscriptions.requests.operations import create_request
from app.subscriptions.requests.payment_reports import payment_reported_cb, renewal_reported_cb
from app.subscriptions.requests.reminders import manual_reminder_text
from app.subscriptions.requests.review_handlers import product_request_action_cb
from app.subscriptions.requests.review_operations import send_requisites
from app.subscriptions.requests.terms import parse_terms, terms_version, update_request_terms
from app.subscriptions.requests.terms_flow import terms_input_start_cb
from app.subscriptions.requests.views import payment_message, render_payment_template, request_markup
from tests.product_support import _admin, _callback_names, _callback_update, _user


@pytest.fixture
def frozen_now(monkeypatch: pytest.MonkeyPatch) -> datetime:
    now = datetime(2026, 7, 31, 12, 0, tzinfo=TZ)
    monkeypatch.setattr(state, "now", lambda: now)
    return now


def _config(now: datetime) -> storage.UserData:
    return storage.UserData(
        authorized_users={
            "1": _admin(1, admin_level="owner"),
            "2": _admin(2),
            "3": _admin(3),
            "42": _user(42, connection_url="https://connect.test/purchase-terms"),
        },
        product_settings={
            "standard_price_rub": 600,
            "payment_message": "Переведите {amount} ₽ за {months} мес. Доступ до {access_until}. Банк: пример.",
            "current_period_end": (now + timedelta(days=90)).isoformat(),
        },
    )


def _request(cfg: storage.UserData, now: datetime) -> dict:
    return create_request(cfg, kind="purchase", user_id=42, target_end_at=(now + timedelta(days=90)).isoformat())


@pytest.mark.parametrize(
    "text",
    [
        "",
        "500",
        "500 | 0",
        "500 | 37",
        "-1 | 1",
        "1.5 | 1",
        "1000001 | 1",
        "500 | 1.5",
        "500 | 1 | 01.01.2020 12:00",
        "500 | 1 | invalid",
        "500 | 1 |",
        "500 | 1 | 31.02.2027 12:00",
    ],
)
def test_terms_input_rejects_invalid_amount_duration_or_deadline(text: str, frozen_now: datetime) -> None:
    assert parse_terms(text) is None


def test_calendar_months_and_explicit_deadline(frozen_now: datetime) -> None:
    assert parse_terms("125 | 2") == {
        "amount_rub": 125,
        "period_months": 2,
        "target_end_at": datetime(2026, 9, 30, 12, 0, tzinfo=TZ).isoformat(),
    }
    assert parse_terms("125 | 1 | 15.09.2026 23:59") == {
        "amount_rub": 125,
        "period_months": 1,
        "target_end_at": datetime(2026, 9, 15, 23, 59, tzinfo=TZ).isoformat(),
    }


@pytest.mark.asyncio
async def test_purchase_terms_survive_payment_but_next_period_uses_current_standard_price(
    isolated_storage: None,
    frozen_now: datetime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def seed(cfg: storage.UserData) -> None:
        initial = _config(frozen_now)
        cfg.authorized_users = initial.authorized_users
        cfg.product_settings = initial.product_settings

    await storage.update_user_data(seed)
    customer, customer_context = _callback_update(42, "subscription:buy")
    await purchase_show_cb(customer, customer_context)
    assert "600 ₽" in customer.callback_query.edit_message_text.call_args.args[0]
    await purchase_create_cb(customer, customer_context)
    request_id = next(iter(storage.service_requests_snapshot()))
    notification = next(event for _, event in storage.outbox_snapshot() if event["kind"] == "purchase_request")
    assert any(
        button["callback_data"] == f"product:input:terms:{request_id}"
        for row in notification["payload"]["reply_markup"]
        for button in row
    )

    admin, context = _callback_update(2, f"product:input:terms:{request_id}")
    assert await terms_input_start_cb(admin, context) == state.PRODUCT_INPUT
    text_update, _ = _callback_update(2)
    text_update.callback_query = None
    text_update.effective_message.text = "125 | 1 | 31.08.2026 23:59"
    assert await product_text_input(text_update, context) == state.PRODUCT_CONFIRM
    confirm, _ = _callback_update(2, "product:confirm:apply")
    assert await product_confirm_cb(confirm, context) == ConversationHandler.END
    request = storage.service_requests_snapshot()[request_id]
    assert (request["amount_rub"], request["period_months"], request["custom_terms"]) == (125, 1, True)
    assert "Индивидуальные условия" in confirm.callback_query.edit_message_text.call_args.args[0]
    assert storage.product_settings_snapshot()["standard_price_rub"] == 600
    assert storage.audit_log_snapshot()[-1]["action"] == "purchase_terms_changed"

    send, send_context = _callback_update(2, f"product:req:requisites:{request_id}")
    await product_request_action_cb(send, send_context)
    requisites = next(event for _, event in storage.outbox_snapshot() if event["kind"] == "payment_requisites")
    assert "125 ₽" in requisites["payload"]["text"]
    assert "31.08.2026 23:59" in requisites["payload"]["text"]
    assert "600 ₽" not in requisites["payload"]["text"]
    customer.callback_query.data = f"subscription:paid:{request_id}"
    await payment_reported_cb(customer, customer_context)
    owner, owner_context = _callback_update(1, f"product:req:confirm:{request_id}")
    await product_request_action_cb(owner, owner_context)
    current = storage.get_user_meta_copy(42)
    assert current["subscription_end_at"] == request["target_end_at"]
    assert current["is_paid"]
    assert "amount_rub" not in current
    assert "period_months" not in current

    await storage.update_user_data(lambda cfg: cfg.product_settings.update(standard_price_rub=750))
    monkeypatch.setattr(state, "now", lambda: state.parse_datetime(request["target_end_at"]) - timedelta(days=1))
    reminder = manual_reminder_text(current, storage.product_settings_snapshot(), _admin(2))
    assert "750 ₽" in reminder and "125 ₽" not in reminder
    assert "3 мес." in reminder
    await renewal_reported_cb(customer, customer_context)
    renewal = next(item for item in storage.service_requests_snapshot().values() if item["kind"] == "renewal")
    assert renewal["amount_rub"] == 750
    assert renewal["period_months"] == 3
    assert not renewal.get("custom_terms")


@pytest.mark.parametrize("intervention", ["send", "edit", "blocked", "demoted", "expired"])
def test_stale_or_unauthorized_edits_cannot_change_terms(intervention: str, frozen_now: datetime) -> None:
    cfg = _config(frozen_now)
    request = _request(cfg, frozen_now)
    expected = terms_version(request)
    terms = parse_terms("125 | 1")
    actor = deepcopy(cfg.authorized_users["2"])
    if intervention == "send":
        assert send_requisites(cfg, request_id=request["id"], actor=actor)[0] == "sent"
    elif intervention == "edit":
        cfg.service_requests[str(request["id"])]["amount_rub"] = 300
    elif intervention == "blocked":
        cfg.authorized_users["2"].update(access_state="blocked", enabled=False)
    elif intervention == "demoted":
        cfg.authorized_users["2"]["role"] = "user"
    else:
        terms["target_end_at"] = frozen_now.isoformat()
    before = deepcopy(cfg.service_requests)
    outcome = update_request_terms(cfg, request_id=request["id"], actor=actor, expected=expected, terms=terms)
    assert (
        outcome
        == {"send": "stale", "edit": "changed", "blocked": "forbidden", "demoted": "forbidden", "expired": "invalid"}[
            intervention
        ]
    )
    assert cfg.service_requests == before


@pytest.mark.asyncio
async def test_expired_custom_deadline_requires_reediting_instead_of_falling_back(
    isolated_storage: None,
    frozen_now: datetime,
) -> None:
    def seed(cfg: storage.UserData) -> int:
        initial = _config(frozen_now)
        cfg.authorized_users = initial.authorized_users
        cfg.product_settings = initial.product_settings
        request = _request(cfg, frozen_now)
        request.update(custom_terms=True, amount_rub=125, period_months=1, target_end_at=frozen_now.isoformat())
        return request["id"]

    request_id = await storage.update_user_data(seed)
    update, context = _callback_update(2, f"product:req:requisites:{request_id}")
    await product_request_action_cb(update, context)
    assert "дата уже истекла" in update.callback_query.edit_message_text.call_args.args[0]
    assert not storage.outbox_snapshot()
    assert storage.service_requests_snapshot()[str(request_id)]["target_end_at"] == frozen_now.isoformat()


@pytest.mark.asyncio
async def test_cancel_does_not_save_staged_terms(isolated_storage: None, frozen_now: datetime) -> None:
    def seed(cfg: storage.UserData) -> int:
        initial = _config(frozen_now)
        cfg.authorized_users = initial.authorized_users
        cfg.product_settings = initial.product_settings
        return _request(cfg, frozen_now)["id"]

    request_id = await storage.update_user_data(seed)
    before = storage.service_requests_snapshot()
    update, context = _callback_update(2, f"product:input:terms:{request_id}")
    await terms_input_start_cb(update, context)
    text_update, _ = _callback_update(2)
    text_update.callback_query = None
    text_update.effective_message.text = "125 | 1"
    await product_text_input(text_update, context)
    update.callback_query.data = "product:cancel"
    assert await product_cancel(update, context) == ConversationHandler.END
    assert storage.service_requests_snapshot() == before
    assert context.user_data == {}


def test_prices_without_placeholders_and_long_templates_are_bounded(frozen_now: datetime) -> None:
    settings = {"standard_price_rub": 750, "payment_message": "Банк: пример. " + "😀" * 3500}
    text = render_payment_template(settings, access_until=frozen_now + timedelta(days=90))
    assert "Стоимость: 750 ₽" in text
    assert utf16_length(text) <= 3500
    cfg = _config(frozen_now)
    request = _request(cfg, frozen_now)
    request.update(custom_terms=True, amount_rub=125, period_months=1)
    custom = payment_message(settings, request)
    assert "Индивидуальные условия" in custom and "125 ₽" in custom
    assert utf16_length(custom) <= 3500


def test_standard_price_change_does_not_reprice_existing_request(frozen_now: datetime) -> None:
    cfg = _config(frozen_now)
    request = _request(cfg, frozen_now)
    cfg.product_settings["standard_price_rub"] = 750
    assert send_requisites(cfg, request_id=request["id"], actor=cfg.authorized_users["2"])[0] == "sent"
    assert "600 ₽" in next(iter(cfg.outbox.values()))["payload"]["text"]
    assert f"product:input:terms:{request['id']}" not in _callback_names(
        request_markup(cfg.service_requests[str(request["id"])], cfg.authorized_users["2"])
    )


@pytest.mark.asyncio
async def test_terms_prompts_stay_bounded_and_completed_card_is_synchronized(
    isolated_storage: None,
    frozen_now: datetime,
) -> None:
    def seed(cfg: storage.UserData) -> int:
        initial = _config(frozen_now)
        cfg.authorized_users = initial.authorized_users
        cfg.authorized_users["42"]["nickname"] = "😀" * 160
        cfg.product_settings = initial.product_settings
        request = _request(cfg, frozen_now)
        request["comment"] = "😀<>&" * 600
        return request["id"]

    request_id = await storage.update_user_data(seed)
    request = storage.service_requests_snapshot()[str(request_id)]
    bot = SimpleNamespace(edit_message_text=AsyncMock())
    original = SimpleNamespace(chat_id=2, message_id=10)
    completion = review_completion(scope="service", target_id=request_id, generation=request["created_at"])
    await record_review_delivery(bot, completion, 2, original)
    await record_review_delivery(bot, completion, 3, SimpleNamespace(chat_id=3, message_id=20))
    update, context = _callback_update(2, f"product:input:terms:{request_id}")
    context.bot = bot
    update.callback_query.message.chat_id = 2
    update.callback_query.message.message_id = 10
    await terms_input_start_cb(update, context)
    assert utf16_length(update.callback_query.edit_message_text.call_args.args[0]) <= 4096
    assert "2" not in storage.service_requests_snapshot()[str(request_id)]["review_messages"]
    text_update, _ = _callback_update(2)
    text_update.callback_query = None
    text_update.effective_message.text = "125 | 1"
    await product_text_input(text_update, context)
    update.callback_query.data = "product:confirm:apply"
    update.callback_query.message.message_id = 11
    await product_confirm_cb(update, context)
    assert utf16_length(update.callback_query.edit_message_text.call_args.args[0]) <= 4096
    refs = storage.service_requests_snapshot()[str(request_id)]["review_messages"]
    assert refs["2"][0]["message_id"] == 11
    assert refs["3"][0]["message_id"] == 20
    assert any(
        call.kwargs["chat_id"] == 3 and "125 ₽" in call.kwargs["text"] for call in bot.edit_message_text.call_args_list
    )
