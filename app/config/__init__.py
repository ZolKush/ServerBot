"""Stable public configuration API.

The package intentionally contains no logging setup side effect. Process
entrypoints configure logging explicitly after validated settings are loaded.
"""

# ruff: noqa: F401, F403

from ..constants import (
    MENU_ADMINISTRATION,
    MENU_MAINT,
    MENU_REQUESTS,
    MENU_STATUS,
    MENU_SUBSCRIPTION,
    MENU_TICKET,
    MENU_USERS,
)
from ..runtime.logging import configure_logging, logger
from .runtime import *
from .runtime import __all__ as _runtime_exports

__all__ = [
    *_runtime_exports,
    "MENU_ADMINISTRATION",
    "MENU_MAINT",
    "MENU_REQUESTS",
    "MENU_STATUS",
    "MENU_SUBSCRIPTION",
    "MENU_TICKET",
    "MENU_USERS",
    "configure_logging",
    "logger",
]
