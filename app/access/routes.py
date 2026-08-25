"""Stable route-facing exports for the access feature."""

from .commands import cmd_auth, cmd_help, cmd_logout, cmd_owner, cmd_start
from .request_handlers import access_request_cb, access_review_cb
from .security import auth_prune_task

ACCESS_REQUEST_PATTERN = r"^access:request$"
ACCESS_REVIEW_PATTERN = r"^access:(approve|reject|block):\d+$"
AUTH_COMMANDS = ("auth", "login")

__all__ = [
    "ACCESS_REQUEST_PATTERN",
    "ACCESS_REVIEW_PATTERN",
    "AUTH_COMMANDS",
    "access_request_cb",
    "access_review_cb",
    "auth_prune_task",
    "cmd_auth",
    "cmd_help",
    "cmd_logout",
    "cmd_owner",
    "cmd_start",
]
