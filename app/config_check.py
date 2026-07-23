"""Production configuration validation CLI."""

from .config.checks import (
    _check_json_object,
    _check_private_data_permissions,
    _readable_file,
    _writable_parent,
    main,
    validate_configuration,
)

__all__ = [
    "_check_json_object",
    "_check_private_data_permissions",
    "_readable_file",
    "_writable_parent",
    "main",
    "validate_configuration",
]


if __name__ == "__main__":
    raise SystemExit(main())
