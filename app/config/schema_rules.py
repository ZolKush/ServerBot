from __future__ import annotations

from .schema_fields import SettingsFields


def validate_settings_consistency(settings: SettingsFields) -> None:
    """Validate relationships between environment-backed values."""
    if settings.SUBPROC_SHORT_TIMEOUT > settings.SUBPROC_MEDIUM_TIMEOUT:
        raise ValueError("SUBPROC_SHORT_TIMEOUT must be <= SUBPROC_MEDIUM_TIMEOUT")


__all__ = ["validate_settings_consistency"]
