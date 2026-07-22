from __future__ import annotations

import re

_LOCAL_ATOM = r"[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+"
_LOCAL_RE = re.compile(rf"^{_LOCAL_ATOM}(?:\.{_LOCAL_ATOM})*$")
_DOMAIN_LABEL_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$", re.IGNORECASE)


def normalize_email(value: object) -> str | None:
    """Return a conservative normalized Internet email address or ``None``."""

    text = str(value or "").strip()
    if not text:
        return None
    if len(text) > 254 or text.count("@") != 1:
        return None
    local, domain = text.rsplit("@", 1)
    if not local or len(local) > 64 or not _LOCAL_RE.fullmatch(local):
        return None
    try:
        ascii_domain = domain.rstrip(".").encode("idna").decode("ascii").lower()
    except UnicodeError:
        return None
    if not ascii_domain or len(ascii_domain) > 253 or "." not in ascii_domain:
        return None
    labels = ascii_domain.split(".")
    if any(not _DOMAIN_LABEL_RE.fullmatch(label) for label in labels):
        return None
    if len(labels[-1]) < 2:
        return None
    return f"{local}@{ascii_domain}"


__all__ = ["normalize_email"]
