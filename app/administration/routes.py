"""Stable route-facing exports for administration."""

from .flow_handlers import (
    administration_cancel,
    administration_confirm_cb,
    administration_input_start_cb,
    administration_text_input,
)
from .profile_handlers import (
    administration_show_cb,
    administration_signature_mode_cb,
    administration_staff_title_apply_cb,
    administration_staff_title_menu_cb,
)
from .settings_handlers import (
    administration_help_reset_cb,
    administration_service_settings_cb,
)
from .state import ADMINISTRATION_CONFIRM, ADMINISTRATION_INPUT

ADMINISTRATION_INPUT_PATTERN = (
    r"^(administration:input:(alias|help|support_email|payment_bank|payment_recipient|"
    r"payment_phone|period_current|period_next)|staff:alias|"
    r"product:input:setting_(bank|recipient|phone|current|next))$"
)
ADMINISTRATION_SHOW_PATTERN = r"^(administration:show|staff:profile)$"
ADMINISTRATION_SIGNATURE_PATTERN = r"^(administration:signature|staff:mode):(title|title_alias)$"
ADMINISTRATION_SETTINGS_PATTERN = r"^(administration:settings|product:owner)$"

__all__ = [
    "ADMINISTRATION_CONFIRM",
    "ADMINISTRATION_INPUT",
    "ADMINISTRATION_INPUT_PATTERN",
    "ADMINISTRATION_SETTINGS_PATTERN",
    "ADMINISTRATION_SHOW_PATTERN",
    "ADMINISTRATION_SIGNATURE_PATTERN",
    "administration_cancel",
    "administration_confirm_cb",
    "administration_help_reset_cb",
    "administration_input_start_cb",
    "administration_service_settings_cb",
    "administration_show_cb",
    "administration_signature_mode_cb",
    "administration_staff_title_apply_cb",
    "administration_staff_title_menu_cb",
    "administration_text_input",
]
