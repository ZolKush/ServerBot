from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

import pytest
from dotenv import dotenv_values

from tools.migrate_runtime_env import MigrationError, build_migration, migrate_files

PUBLIC_TEMPLATE = """# Canonical public settings.
LOG_LEVEL=INFO
DATA_DIR=/default/data
SERVER_INVENTORY_FILE=/etc/maintbot/servers.toml
NAVIGATION_CLEANUP_ENABLED=true
NAVIGATION_RETENTION_HOURS=24
NAVIGATION_CLEANUP_INTERVAL_SEC=1800
REMNAWAVE_METRICS_URL=
"""

SECRETS_TEMPLATE = """# Canonical secrets.
BOT_TOKEN=
ADMIN_PASSWORD=
OWNER_PASSWORD=
REMNAWAVE_METRICS_USER=
REMNAWAVE_METRICS_PASS=
"""


def _write_sources(
    tmp_path: Path,
    *,
    public: str,
    secrets: str,
) -> tuple[Path, Path, Path, Path]:
    public_path = tmp_path / "legacy.env"
    secrets_path = tmp_path / "legacy.secrets"
    public_template = tmp_path / "public.example"
    secrets_template = tmp_path / "secrets.example"
    public_path.write_text(public, encoding="utf-8")
    secrets_path.write_text(secrets, encoding="utf-8")
    public_template.write_text(PUBLIC_TEMPLATE, encoding="utf-8")
    secrets_template.write_text(SECRETS_TEMPLATE, encoding="utf-8")
    return public_path, secrets_path, public_template, secrets_template


def _parsed(text: str) -> dict[str, str]:
    return {
        str(key): str(value)
        for key, value in dotenv_values(stream=StringIO(text), interpolate=False).items()
        if value is not None
    }


def _build(
    public_path: Path,
    secrets_path: Path,
    public_template: Path,
    secrets_template: Path,
):
    return build_migration(
        public_env=public_path,
        secrets_env=secrets_path,
        public_template=public_template,
        secrets_template=secrets_template,
        server_inventory_file="/opt/maintbot/app/servers.toml",
    )


def test_runtime_env_migration_round_trips_special_characters_and_moves_credentials(tmp_path: Path) -> None:
    public_value = "https://metrics.example/path?q=${UNEXPANDED} # fragment ' single \\ slash\nnext line"
    secret_value = "user #1 '$' \\ account\nsecond line"
    public_path, secrets_path, public_template, secrets_template = _write_sources(
        tmp_path,
        public=(
            f"REMNAWAVE_METRICS_URL={json.dumps(public_value, ensure_ascii=False)}\n"
            f"REMNAWAVE_METRICS_USER={json.dumps(secret_value, ensure_ascii=False)}\n"
        ),
        secrets="BOT_TOKEN='token'\nADMIN_PASSWORD='admin'\nOWNER_PASSWORD='owner'\n",
    )

    migrated = _build(public_path, secrets_path, public_template, secrets_template)
    public_values = _parsed(migrated.public_text)
    secret_values = _parsed(migrated.secrets_text)

    assert public_values["REMNAWAVE_METRICS_URL"] == public_value
    assert secret_values["REMNAWAVE_METRICS_USER"] == secret_value
    assert "REMNAWAVE_METRICS_USER" not in public_values
    assert set(public_values) == set(_parsed(PUBLIC_TEMPLATE))
    assert set(secret_values) == set(_parsed(SECRETS_TEMPLATE))


def test_runtime_env_migration_rejects_conflicting_public_and_secret_credentials(tmp_path: Path) -> None:
    public_path, secrets_path, public_template, secrets_template = _write_sources(
        tmp_path,
        public="REMNAWAVE_METRICS_USER='public-user'\n",
        secrets="REMNAWAVE_METRICS_USER='secret-user'\n",
    )

    with pytest.raises(MigrationError, match="conflicting values for REMNAWAVE_METRICS_USER"):
        _build(public_path, secrets_path, public_template, secrets_template)


def test_runtime_env_migration_rejects_unknown_key_without_exposing_value(tmp_path: Path) -> None:
    marker = "THIS_VALUE_MUST_NOT_APPEAR"
    public_path, secrets_path, public_template, secrets_template = _write_sources(
        tmp_path,
        public=f"UNEXPECTED_SETTING={marker}\n",
        secrets="",
    )

    with pytest.raises(MigrationError) as captured:
        _build(public_path, secrets_path, public_template, secrets_template)

    error = str(captured.value)
    assert "UNEXPECTED_SETTING" in error
    assert marker not in error


def test_runtime_env_migration_never_overwrites_either_output(tmp_path: Path) -> None:
    public_path, secrets_path, public_template, secrets_template = _write_sources(
        tmp_path,
        public="LOG_LEVEL=DEBUG\n",
        secrets="BOT_TOKEN=token\n",
    )
    output_public = tmp_path / "result.env"
    output_secrets = tmp_path / "result.secrets"
    output_public.write_text("keep-existing", encoding="utf-8")

    with pytest.raises(MigrationError, match="refusing to overwrite"):
        migrate_files(
            public_env=public_path,
            secrets_env=secrets_path,
            public_template=public_template,
            secrets_template=secrets_template,
            output_public=output_public,
            output_secrets=output_secrets,
            server_inventory_file="/opt/maintbot/app/servers.toml",
        )

    assert output_public.read_text(encoding="utf-8") == "keep-existing"
    assert not output_secrets.exists()


def test_runtime_env_migration_derives_data_dir_and_drops_legacy_layout_keys(tmp_path: Path) -> None:
    public_path, secrets_path, public_template, secrets_template = _write_sources(
        tmp_path,
        public="""USER_DATA_PATH=/opt/maintbot/data/user_data.json
IMPORTANT_DATA_PATH=/opt/maintbot/data/important_data.json
BOT_MODE=mixed
LOCAL_SERVER_CODE=main
REMOTE_SERVER_SSH_TARGETS=maintbot@example.test:22
MESSAGE_RETENTION_HOURS=12
""",
        secrets="",
    )

    migrated = _build(public_path, secrets_path, public_template, secrets_template)
    values = _parsed(migrated.public_text)

    assert values["DATA_DIR"] == "/opt/maintbot/data"
    assert values["SERVER_INVENTORY_FILE"] == "/opt/maintbot/app/servers.toml"
    assert values["NAVIGATION_RETENTION_HOURS"] == "12"
    assert "USER_DATA_PATH" not in values
    assert "IMPORTANT_DATA_PATH" not in values
    assert "BOT_MODE" not in values
    assert "LOCAL_SERVER_CODE" not in values
    assert "REMOTE_SERVER_SSH_TARGETS" not in values
