from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
from telegram.ext import ConversationHandler

from app import storage
from app.administration import flow_handlers, operations, state, views
from app.persistence import SplitJsonBackend
from app.persistence.errors import MigrationError
from app.persistence.migration import load_v4_source, transform_v4
from app.persistence.normalization import normalize_product_settings
from app.subscriptions.policy import MAX_PRICE_RUB, PLAN_TOTAL_RUB, parse_price, standard_price
from tests.persistence.fixtures import write_v4_source
from tests.product_support import _admin, _callback_names, _callback_update


@pytest.mark.parametrize("value", [None, True, 0, -1, 1.5, "12.50", "-3", "1e3", "", "9" * 100, MAX_PRICE_RUB + 1])
def test_price_rejects_invalid_or_fractional_amounts(value: object) -> None:
    assert parse_price(value) is None
    assert standard_price({"standard_price_rub": value}) == PLAN_TOTAL_RUB


def test_standard_price_defaults_for_existing_settings_and_accepts_whole_rubles() -> None:
    assert standard_price({}) == PLAN_TOTAL_RUB
    assert normalize_product_settings()["standard_price_rub"] == PLAN_TOTAL_RUB
    assert parse_price(" 450 ") == 450
    assert standard_price({"standard_price_rub": 450}) == 450


def test_standard_price_settings_button_is_owner_only() -> None:
    owner = _admin(1, admin_level="owner")
    callback = "administration:input:standard_price"
    assert callback in _callback_names(views.service_settings_markup(owner))
    assert callback not in _callback_names(views.service_settings_markup(_admin(2)))
    assert "450 ₽ / 3 месяца" in views.service_settings_text({"standard_price_rub": 450}, owner)


@pytest.mark.asyncio
async def test_owner_confirms_price_and_persists_billing_audit(isolated_storage: None) -> None:
    await storage.upsert_user_meta(1, _admin(1, admin_level="owner"))
    update, context = _callback_update(1, "administration:input:standard_price")
    assert await flow_handlers.administration_input_start_cb(update, context) == state.ADMINISTRATION_INPUT
    update.callback_query = None
    update.effective_message.text = "450"

    assert await flow_handlers.administration_text_input(update, context) == state.ADMINISTRATION_CONFIRM
    assert standard_price(storage.product_settings_snapshot()) == PLAN_TOTAL_RUB
    assert "450 ₽" in update.effective_message.reply_text.call_args.args[0]

    confirm, _ = _callback_update(1, "administration:confirm")
    assert await flow_handlers.administration_confirm_cb(confirm, context) == ConversationHandler.END
    assert storage.product_settings_snapshot()["standard_price_rub"] == 450
    assert storage.audit_log_snapshot()[-1]["action"] == "standard_price_changed"
    assert storage.audit_log_snapshot()[-1]["details"] == {"old": PLAN_TOTAL_RUB, "new": 450}
    persisted = SplitJsonBackend(storage.storage_data_dir()).snapshot()
    assert persisted.data("subscriptions.billing_settings")["standard_price_rub"] == 450
    assert "standard_price_rub" not in persisted.data("subscriptions.accounts").get("1", {})


@pytest.mark.asyncio
async def test_price_input_retries_invalid_amount_and_cancel_preserves_price(isolated_storage: None) -> None:
    await storage.upsert_user_meta(1, _admin(1, admin_level="owner"))
    update, context = _callback_update(1, "administration:input:standard_price")
    await flow_handlers.administration_input_start_cb(update, context)
    update.callback_query = None
    update.effective_message.text = "450.50"
    assert await flow_handlers.administration_text_input(update, context) == state.ADMINISTRATION_INPUT
    assert state.pending_change(context) is None
    update.effective_message.text = "450"
    assert await flow_handlers.administration_text_input(update, context) == state.ADMINISTRATION_CONFIRM
    cancel, _ = _callback_update(1, "administration:cancel")
    assert await flow_handlers.administration_cancel(cancel, context) == ConversationHandler.END
    assert standard_price(storage.product_settings_snapshot()) == PLAN_TOTAL_RUB


@pytest.mark.asyncio
async def test_staff_cannot_open_price_editor_or_confirm_forged_pending_change(isolated_storage: None) -> None:
    await storage.upsert_user_meta(2, _admin(2))
    update, context = _callback_update(2, "administration:input:standard_price")
    assert await flow_handlers.administration_input_start_cb(update, context) == ConversationHandler.END
    assert "руководителю" in update.callback_query.answer.call_args.args[0]

    state.set_pending_change(context, {"kind": "standard_price", "value": 450})
    update.callback_query.data = "administration:confirm"
    assert await flow_handlers.administration_confirm_cb(update, context) == ConversationHandler.END
    assert standard_price(storage.product_settings_snapshot()) == PLAN_TOTAL_RUB


@pytest.mark.asyncio
async def test_price_change_rechecks_owner_role_inside_transaction(isolated_storage: None) -> None:
    actor = _admin(1, admin_level="owner")
    await storage.upsert_user_meta(1, _admin(1))
    with pytest.raises(ValueError, match="owner_required"):
        await operations.change_standard_price(actor=actor, value=450)
    assert standard_price(storage.product_settings_snapshot()) == PLAN_TOTAL_RUB


def test_legacy_migration_accepts_settings_without_standard_price(tmp_path: Path) -> None:
    root = tmp_path / "legacy"
    write_v4_source(root)
    transformed = transform_v4(load_v4_source(root))
    assert transformed.stores["subscriptions.billing_settings"]["standard_price_rub"] == PLAN_TOTAL_RUB


@pytest.mark.parametrize("value", [True, 0, "450", 450.5, MAX_PRICE_RUB + 1])
def test_legacy_migration_rejects_noncanonical_price(tmp_path: Path, value: object) -> None:
    root = tmp_path / "legacy"
    write_v4_source(root)
    source = load_v4_source(root)
    user_data = {
        **source.user_data,
        "product_settings": {**source.user_data["product_settings"], "standard_price_rub": value},
    }
    with pytest.raises(MigrationError, match="standard_price_rub"):
        transform_v4(replace(source, user_data=user_data))
