from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.constants import ChatType

from app.access import commands as access_commands
from app.administration import flow_handlers as administration_flow
from app.maintenance import flow as maintenance_flow
from app.messaging.message_cleanup import MessageTracker
from app.monitoring.fail2ban import handlers as fail2ban_handlers
from app.monitoring.status import handlers as status_handlers
from app.subscriptions import connections
from app.subscriptions.requests import admin_input
from app.tickets import dashboard_handlers, user_handlers
from app.users.admin import broadcast_handlers, list_handlers


@pytest.fixture
def panel_environment():
    tracker = MessageTracker(enabled=True, retention=timedelta(hours=24))
    tracker.bind({})
    panel = SimpleNamespace(
        message_id=501,
        date=datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc),
        chat=SimpleNamespace(id=42, type=ChatType.PRIVATE),
    )
    bot = SimpleNamespace(message_tracker=tracker)
    incoming = SimpleNamespace(reply_text=AsyncMock(return_value=panel))
    update = SimpleNamespace(
        callback_query=None,
        effective_message=incoming,
        effective_user=SimpleNamespace(id=42),
        get_bot=lambda: bot,
    )
    context = SimpleNamespace(user_data={}, bot=bot)
    return tracker, panel, update, context


async def _assert_panel_tracked(tracker: MessageTracker) -> None:
    assert await tracker.snapshot() == {42: [501]}


@pytest.mark.asyncio
async def test_direct_help_panel_is_registered(monkeypatch, panel_environment) -> None:
    tracker, _panel, update, context = panel_environment
    monkeypatch.setattr(access_commands, "get_user_meta", lambda _user_id: None)
    monkeypatch.setattr(access_commands, "product_settings_snapshot", lambda: {})
    monkeypatch.setattr(access_commands, "render_help_message", lambda _settings: "help")
    monkeypatch.setattr(access_commands, "main_menu_inline_kb", lambda _update: None)

    await access_commands.cmd_help.__wrapped__(update, context)

    await _assert_panel_tracked(tracker)


@pytest.mark.asyncio
async def test_direct_health_panel_is_registered(monkeypatch, panel_environment) -> None:
    tracker, _panel, update, context = panel_environment
    monkeypatch.setattr(status_handlers, "SERVERS", [])

    await status_handlers.cmd_health.__wrapped__(update, context)

    await _assert_panel_tracked(tracker)


@pytest.mark.asyncio
async def test_direct_subscription_panel_is_registered(monkeypatch, panel_environment) -> None:
    tracker, _panel, update, context = panel_environment
    monkeypatch.setattr(connections, "get_user_id", lambda _update: 42)
    monkeypatch.setattr(connections, "get_user_meta_copy", lambda _user_id: {"service_tier": "basic"})
    monkeypatch.setattr(connections, "_dashboard_text", lambda _meta: "subscription")
    monkeypatch.setattr(connections, "_dashboard_markup", lambda _meta: None)

    await connections.subscription_show.__wrapped__(update, context)

    await _assert_panel_tracked(tracker)


@pytest.mark.asyncio
async def test_direct_users_panel_is_registered(monkeypatch, panel_environment) -> None:
    tracker, _panel, update, context = panel_environment
    monkeypatch.setattr(list_handlers, "get_users_filter", lambda _context: "all")
    monkeypatch.setattr(list_handlers, "users_list_title", lambda _active_filter: "users")
    monkeypatch.setattr(list_handlers, "users_list_kb", lambda _active_filter: None)

    await list_handlers.users_entry.__wrapped__(update, context)

    await _assert_panel_tracked(tracker)


@pytest.mark.asyncio
async def test_direct_fail2ban_panel_is_registered(monkeypatch, panel_environment) -> None:
    tracker, _panel, update, context = panel_environment
    monkeypatch.setattr(fail2ban_handlers, "first_server_key", lambda: "local")
    monkeypatch.setattr(
        fail2ban_handlers,
        "build_fail2ban_menu_text",
        AsyncMock(return_value="fail2ban"),
    )
    monkeypatch.setattr(fail2ban_handlers, "menu_keyboard", lambda _server_key: None)

    await fail2ban_handlers.fail2ban_menu.__wrapped__(update, context)

    await _assert_panel_tracked(tracker)


@pytest.mark.asyncio
async def test_direct_maintenance_panel_is_registered(monkeypatch, panel_environment) -> None:
    tracker, _panel, update, context = panel_environment
    monkeypatch.setattr(maintenance_flow, "get_active_maintenance", lambda: None)
    monkeypatch.setattr(maintenance_flow, "get_scheduled_maintenance", lambda: None)
    monkeypatch.setattr(maintenance_flow, "maintenance_menu_text", lambda _scheduled: "maintenance")
    monkeypatch.setattr(maintenance_flow, "maint_mode_kb", lambda: None)

    await maintenance_flow.maint_start.__wrapped__(update, context)

    await _assert_panel_tracked(tracker)


@pytest.mark.asyncio
async def test_direct_user_ticket_panel_is_registered(monkeypatch, panel_environment) -> None:
    tracker, _panel, update, context = panel_environment
    monkeypatch.setattr(user_handlers, "get_user_id", lambda _update: 42)
    monkeypatch.setattr(user_handlers, "is_admin", lambda _update: False)
    monkeypatch.setattr(user_handlers, "get_user_open_tickets", lambda _user_id: [])
    monkeypatch.setattr(user_handlers, "product_settings_snapshot", lambda: {})
    monkeypatch.setattr(user_handlers, "render_support_contact", lambda _settings: "")
    monkeypatch.setattr(user_handlers, "ticket_input_kb", lambda: None)

    await user_handlers.ticket_start.__wrapped__(update, context)

    await _assert_panel_tracked(tracker)


@pytest.mark.asyncio
async def test_direct_admin_ticket_dashboard_is_registered(monkeypatch, panel_environment) -> None:
    tracker, panel, update, context = panel_environment
    monkeypatch.setattr(dashboard_handlers, "get_user_id", lambda _update: 42)
    monkeypatch.setattr(dashboard_handlers, "get_all_tickets_snapshot", lambda: {})
    monkeypatch.setattr(
        dashboard_handlers,
        "safe_edit_or_reply",
        AsyncMock(return_value=panel),
    )

    await dashboard_handlers._show_ticket_dashboard(update, context)

    await _assert_panel_tracked(tracker)


@pytest.mark.asyncio
async def test_broadcast_confirmation_created_after_text_input_is_registered(
    monkeypatch,
    panel_environment,
) -> None:
    tracker, _panel, update, context = panel_environment
    update.effective_message.text = "Служебное объявление"
    monkeypatch.setattr(broadcast_handlers, "_broadcast_recipients", lambda _update, _audience: [7])

    await broadcast_handlers.users_all_msg_text.__wrapped__(update, context)

    await _assert_panel_tracked(tracker)


@pytest.mark.asyncio
async def test_administration_confirmation_created_after_text_input_is_registered(
    monkeypatch,
    panel_environment,
) -> None:
    tracker, _panel, update, context = panel_environment
    update.effective_message.text = "Новая инструкция"
    monkeypatch.setattr(
        administration_flow,
        "actor_meta",
        lambda _update: {"user_id": 42, "role": "admin", "admin_level": "owner"},
    )
    monkeypatch.setattr(administration_flow, "flow_action", lambda _context: "help")

    await administration_flow.administration_text_input.__wrapped__(update, context)

    await _assert_panel_tracked(tracker)


@pytest.mark.asyncio
async def test_product_confirmation_created_after_text_input_is_registered(panel_environment) -> None:
    tracker, _panel, update, _context = panel_environment

    await admin_input.handle_mass_reminder_input(update, update.effective_message, {}, "7")

    await _assert_panel_tracked(tracker)
