"""Stable Telegram conversation states and callback route patterns."""

TICKET_SUBJECT, TICKET_URGENCY, TICKET_TEXT, TICKET_CONFIRM, TICKET_USER_REPLY_TEXT, TICKET_ADMIN_REPLY_TEXT = range(6)

MAX_TICKET_SUBJECT_LEN = 160
MAX_TICKET_TEXT_LEN = 3200
ACTIVE_PAGE_SIZE = 12
ARCHIVE_PAGE_SIZE = 10

TICKET_LIST_PATTERN = r"^ticket:list(?::\d+)?$"
TICKET_OPEN_PATTERN = r"^ticket:open:\d+$"
TICKET_ARCHIVE_PATTERN = r"^ticket:archive$"
TICKET_ARCHIVE_PAGE_PATTERN = r"^ticket:archive_page:\d+$"
TICKET_TRANSFER_INIT_PATTERN = r"^ticket:transfer_init:\d+$"
TICKET_TRANSFER_TO_PATTERN = r"^ticket:transfer_to:\d+:\d+$"
TICKET_ADMIN_REPLY_PATTERN = r"^ticket:adminreply:\d+$"
TICKET_USER_REPLY_PATTERN = r"^ticket:userreply:\d+$"
TICKET_URGENCY_PATTERN = r"^ticket:(p1|p2|p3)$"
TICKET_CONFIRM_PATTERN = r"^ticket:(send|edit_subj|edit_text|cancel)$"
TICKET_TAKE_PATTERN = r"^ticket:take:\d+$"
TICKET_CLOSE_PATTERN = r"^ticket:close:\d+$"

__all__ = [
    "ACTIVE_PAGE_SIZE",
    "ARCHIVE_PAGE_SIZE",
    "MAX_TICKET_SUBJECT_LEN",
    "MAX_TICKET_TEXT_LEN",
    "TICKET_ADMIN_REPLY_TEXT",
    "TICKET_CONFIRM",
    "TICKET_SUBJECT",
    "TICKET_TEXT",
    "TICKET_URGENCY",
    "TICKET_USER_REPLY_TEXT",
]
