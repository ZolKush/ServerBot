from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from telegram import CallbackQuery, Chat, Message, Update, User
from telegram.ext import ApplicationHandlerStop, ConversationHandler, TypeHandler
from telegram.ext._utils.trackingdict import TrackingDict

from app.main import NavigableConversationHandler, blocked_user_guard, build_app
from app.users.states import ADMIN_USER_MSG_TEXT


def _callback_update(uid: int, data: str) -> Update:
    user = User(id=uid, first_name="Admin", is_bot=False)
    chat = Chat(id=uid, type="private")
    message = Message(
        message_id=10,
        date=datetime.now(timezone.utc),
        chat=chat,
        from_user=user,
        text="menu",
    )
    query = CallbackQuery(
        id="query-id",
        from_user=user,
        chat_instance="chat-instance",
        message=message,
        data=data,
    )
    return Update(update_id=1, callback_query=query)


def _navigation_preprocessor(application):
    return next(handler for handler in application.handlers[-1] if isinstance(handler, TypeHandler))


def _conversations(application) -> list[NavigableConversationHandler]:
    return [handler for handler in application.handlers[0] if isinstance(handler, NavigableConversationHandler)]


@pytest.mark.asyncio
async def test_users_button_reenters_after_unfinished_users_flow() -> None:
    application = build_app()
    update = _callback_update(101, "menu:users")
    users_flow = next(handler for handler in _conversations(application) if handler.name == "users_flow")
    key = (101, 101)
    tracking = TrackingDict()
    tracking.update_no_track({key: ADMIN_USER_MSG_TEXT})
    users_flow._conversations = tracking

    # This is the production failure: without the preprocessor the entry point
    # is ignored while a persisted text-input state is still active.
    assert users_flow.check_update(update) is None

    context = SimpleNamespace(user_data={"selected_uid": 999, "users_all_broadcast_text": "draft"})
    await _navigation_preprocessor(application).callback(update, context)

    assert key not in users_flow._conversations
    assert users_flow.check_update(update) is not None
    assert context.user_data == {}
    assert tracking.pop_accessed_write_items() == [(key, TrackingDict.DELETED)]


@pytest.mark.asyncio
async def test_navigation_ends_every_hidden_conversation_for_user() -> None:
    application = build_app()
    # product:manage is a regular global callback, not a menu callback and not
    # an entry point of any conversation. It must still cancel stale text input.
    update = _callback_update(202, "product:manage:303")
    key = (202, 202)
    tracked: list[TrackingDict] = []

    for conversation in _conversations(application):
        mapping = TrackingDict()
        mapping.update_no_track({key: next(iter(conversation.states))})
        conversation._conversations = mapping
        tracked.append(mapping)

    context = SimpleNamespace(
        user_data={
            "ticket_subject": "draft",
            "maint_scope": "all",
            "product_target_uid": 303,
            "selected_uid": 404,
            "users_filter": "active",
        }
    )
    await _navigation_preprocessor(application).callback(update, context)

    assert all(key not in conversation._conversations for conversation in _conversations(application))
    assert context.user_data == {"users_filter": "active"}
    assert all(mapping.pop_accessed_write_items() == [(key, TrackingDict.DELETED)] for mapping in tracked)


def test_unhandled_callback_is_last_callback_fallback() -> None:
    application = build_app()
    handlers = application.handlers[0]
    catch_all_index = next(
        index
        for index, handler in enumerate(handlers)
        if handler.__class__.__name__ == "CallbackQueryHandler" and handler.pattern is None
    )
    assert all(not isinstance(handler, ConversationHandler) for handler in handlers[catch_all_index + 1 :])


@pytest.mark.asyncio
async def test_blocked_user_guard_is_completely_silent(monkeypatch: pytest.MonkeyPatch) -> None:
    update = _callback_update(303, "menu:home")
    monkeypatch.setattr("app.bot.errors.get_user_meta_copy", lambda uid: {"user_id": uid, "access_state": "blocked"})

    with pytest.raises(ApplicationHandlerStop):
        await blocked_user_guard(update, SimpleNamespace())


def test_blocked_guard_runs_before_tracking_and_all_regular_handlers() -> None:
    application = build_app()

    assert -100 in application.handlers
    assert any(isinstance(handler, TypeHandler) for handler in application.handlers[-100])
