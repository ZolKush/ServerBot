from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from app import storage
from app.config import TZ
from app.subscriptions.requests import operations, state
from tests.product_support import _admin, _user


def test_trial_requires_a_new_externally_limited_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    issued_at = datetime(2026, 8, 11, 12, 0, tzinfo=TZ)
    monkeypatch.setattr(state, "now", lambda: issued_at)
    previous_url = "https://connect.test/permanent"
    cfg = storage.UserData(
        authorized_users={
            "1": _admin(1),
            "42": _user(42, connection_url=previous_url),
        }
    )
    request = operations.create_request(cfg, kind="trial", user_id=42)
    request["trial_duration_hours"] = 24

    with pytest.raises(ValueError, match="connection_not_fresh"):
        operations.finalize_trial(cfg, request, cfg.authorized_users["1"], previous_url)

    updated = operations.finalize_trial(
        cfg,
        request,
        cfg.authorized_users["1"],
        "https://connect.test/trial-24h",
    )

    assert updated["connection_url"] == "https://connect.test/trial-24h"
    assert updated["trial_end_at"] == (issued_at + timedelta(hours=24)).isoformat()
