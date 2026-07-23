from app.tickets.history import MAX_TICKET_MESSAGES_STORED, _append_ticket_message
from app.tickets.operations import _build_ticket_record
from app.tickets.views import _format_ticket_for_admin, _format_ticket_for_user, _ticket_admin_kb


def test_ticket_history_retains_initial_message_when_trimmed() -> None:
    ticket = _build_ticket_record(
        1,
        user_id=42,
        user_name="user",
        user_username=None,
        subject="subject",
        urgency="p3",
        text="initial report",
    )
    for index in range(MAX_TICKET_MESSAGES_STORED + 10):
        ticket = _append_ticket_message(
            ticket,
            sender_role="admin",
            sender_id=1,
            sender_name="admin",
            text=f"reply {index}",
            kind="reply",
        )

    assert len(ticket["messages"]) == MAX_TICKET_MESSAGES_STORED
    assert ticket["messages"][0]["kind"] == "initial"
    assert ticket["messages"][-1]["text"] == f"reply {MAX_TICKET_MESSAGES_STORED + 9}"


def test_malformed_legacy_ticket_does_not_break_admin_view() -> None:
    ticket = {
        "id": "bad",
        "user_id": "bad",
        "assignee_id": "bad",
        "subject": "&" * 5000,
        "messages": [{"sender_name": "x" * 5000, "text": "&" * 5000}],
    }

    text = _format_ticket_for_admin(ticket, 1)
    keyboard = _ticket_admin_kb(ticket, 1)

    assert len(text) < 4096
    assert keyboard.inline_keyboard


def test_legacy_staff_identity_is_hidden_from_user_ticket_history() -> None:
    ticket = _build_ticket_record(
        1,
        user_id=42,
        user_name="user",
        user_username=None,
        subject="subject",
        urgency="p3",
        text="initial report",
    )
    ticket["assignee_id"] = 100
    ticket["assignee_name"] = "Real Legacy Admin"
    ticket["messages"].append(
        {
            "ts": ticket["created_at"],
            "sender_role": "admin",
            "sender_id": 100,
            "sender_name": "Real Legacy Admin",
            "text": "legacy reply",
            "kind": "reply",
        }
    )

    text = _format_ticket_for_user(ticket)

    assert "Real Legacy Admin" not in text
    assert "Техническая поддержка" in text
