from __future__ import annotations

from typing import Any

STAFF_TITLE_SUPPORT = "support_specialist"
STAFF_TITLE_MAINTAINER = "maintenance_engineer"
STAFF_TITLE_LEAD = "lead_maintenance_engineer"
STAFF_TITLE_OWNER = "service_manager"

STAFF_TITLES = (
    STAFF_TITLE_SUPPORT,
    STAFF_TITLE_MAINTAINER,
    STAFF_TITLE_LEAD,
    STAFF_TITLE_OWNER,
)
REGULAR_STAFF_TITLES = (
    STAFF_TITLE_SUPPORT,
    STAFF_TITLE_MAINTAINER,
    STAFF_TITLE_LEAD,
)
STAFF_TITLE_LABELS = {
    STAFF_TITLE_SUPPORT: "Специалист поддержки",
    STAFF_TITLE_MAINTAINER: "Инженер сопровождения",
    STAFF_TITLE_LEAD: "Ведущий инженер сопровождения",
    STAFF_TITLE_OWNER: "Руководитель сервиса",
}

STAFF_DISPLAY_TITLE = "title"
STAFF_DISPLAY_TITLE_ALIAS = "title_alias"
STAFF_DISPLAY_MODES = (STAFF_DISPLAY_TITLE, STAFF_DISPLAY_TITLE_ALIAS)


def is_admin_meta(meta: dict[str, Any] | None) -> bool:
    return bool(meta and meta.get("role") == "admin")


def is_owner_meta(meta: dict[str, Any] | None) -> bool:
    return bool(meta and meta.get("role") == "admin" and meta.get("admin_level") == "owner")


def normalize_staff_title(value: object, *, owner: bool = False) -> str:
    if owner:
        return STAFF_TITLE_OWNER
    title = str(value or "").strip()
    return title if title in REGULAR_STAFF_TITLES else STAFF_TITLE_SUPPORT


def normalize_staff_alias(value: object) -> str | None:
    alias = " ".join(str(value or "").strip().split())
    if not alias:
        return None
    return alias[:32]


def normalize_staff_display_mode(value: object) -> str:
    mode = str(value or "").strip()
    return mode if mode in STAFF_DISPLAY_MODES else STAFF_DISPLAY_TITLE


def staff_title_code(meta: dict[str, Any] | None) -> str:
    if not meta or meta.get("role") != "admin":
        return STAFF_TITLE_SUPPORT
    return normalize_staff_title(meta.get("staff_title"), owner=is_owner_meta(meta))


def staff_title_label(meta: dict[str, Any] | None) -> str:
    return STAFF_TITLE_LABELS[staff_title_code(meta)]


def staff_public_signature(meta: dict[str, Any] | None, *, allow_alias: bool = True) -> str:
    title = staff_title_label(meta)
    if (
        not allow_alias
        or normalize_staff_display_mode((meta or {}).get("staff_display_mode")) != STAFF_DISPLAY_TITLE_ALIAS
    ):
        return title
    alias = normalize_staff_alias((meta or {}).get("staff_alias"))
    return f"{title} «{alias}»" if alias else title


def staff_internal_name(meta: dict[str, Any] | None) -> str:
    if not meta:
        return "неизвестный сотрудник"
    real_name = " ".join(
        str(part).strip()[:256] for part in (meta.get("first_name"), meta.get("last_name")) if str(part or "").strip()
    )[:520]
    username = str(meta.get("username") or "").strip().lstrip("@")[:64]
    if real_name and username:
        return f"{real_name} (@{username})"
    if real_name:
        return real_name
    if username:
        return f"@{username}"
    return str(meta.get("user_id") or "неизвестный сотрудник")


def staff_internal_identity(meta: dict[str, Any] | None) -> str:
    uid = (meta or {}).get("user_id")
    uid_part = f", ID {uid}" if uid not in (None, "") else ""
    return f"{staff_public_signature(meta)} — {staff_internal_name(meta)}{uid_part}"


def is_lead_or_owner_meta(meta: dict[str, Any] | None) -> bool:
    return bool(is_owner_meta(meta) or (is_admin_meta(meta) and staff_title_code(meta) == STAFF_TITLE_LEAD))


__all__ = [
    "REGULAR_STAFF_TITLES",
    "STAFF_DISPLAY_MODES",
    "STAFF_DISPLAY_TITLE",
    "STAFF_DISPLAY_TITLE_ALIAS",
    "STAFF_TITLE_LABELS",
    "STAFF_TITLE_LEAD",
    "STAFF_TITLE_MAINTAINER",
    "STAFF_TITLE_OWNER",
    "STAFF_TITLE_SUPPORT",
    "STAFF_TITLES",
    "is_admin_meta",
    "is_lead_or_owner_meta",
    "is_owner_meta",
    "normalize_staff_alias",
    "normalize_staff_display_mode",
    "normalize_staff_title",
    "staff_internal_identity",
    "staff_internal_name",
    "staff_public_signature",
    "staff_title_code",
    "staff_title_label",
]
