from __future__ import annotations

import os
import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "deploy/compose.remote.yaml"


def _shell() -> str:
    shell = shutil.which("sh")
    if shell:
        return shell
    git = shutil.which("git")
    if git:
        candidate = Path(git).resolve().parent.parent / "bin/sh.exe"
        if candidate.is_file():
            return str(candidate)
    pytest.skip("POSIX shell unavailable")


def _command() -> str:
    # Extract only the YAML literal block; no additional YAML dependency is
    # needed to exercise the deployed POSIX script itself.
    document = COMPOSE.read_text(encoding="utf-8")
    block = document.split("    command:\n      - |\n", 1)[1].split("    restart:", 1)[0]
    return textwrap.dedent(block).replace("$$", "$")


def _run(tmp_path: Path, *, files: tuple[str, ...] = (), failed_module: str = "", pickle_path: str = ""):
    data = tmp_path / "data"
    data.mkdir()
    for name in files:
        (data / name).touch()
    calls = tmp_path / "calls.log"
    executable = tmp_path / "python"
    executable.write_text(
        '#!/bin/sh\nprintf "%s\\n" "$*" >> "$CALL_LOG"\nif [ "$2" = "$FAIL_MODULE" ]; then exit 7; fi\n',
        encoding="utf-8",
        newline="\n",
    )
    executable.chmod(0o755)
    environment = os.environ.copy()
    environment.update(
        PATH=f"{tmp_path.as_posix()}{os.pathsep}{environment.get('PATH', '')}",
        CALL_LOG=calls.as_posix(),
        FAIL_MODULE=failed_module,
        MAINTBOT_PTB_PICKLE=pickle_path,
    )
    script = _command().replace("/var/lib/maintbot", data.as_posix())
    result = subprocess.run(
        [_shell(), "-ec", script],
        env=environment,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    return result, calls.read_text(encoding="utf-8").splitlines() if calls.exists() else []


@pytest.mark.parametrize("legacy", ["storage_layout.json", "user_data.json", "important_data.json"])
def test_remote_container_migrates_legacy_before_bootstrap_and_launcher(tmp_path, legacy):
    result, calls = _run(tmp_path, files=(legacy,))
    assert result.returncode == 0, result.stderr
    assert [line.split()[1] for line in calls] == [
        "app.persistence.migrate_sqlite",
        "app.persistence.bootstrap",
        "app.config_check",
        "app.launcher",
    ]


def test_remote_container_never_reimports_legacy_when_database_exists(tmp_path):
    result, calls = _run(tmp_path, files=("maintbot.sqlite3", "storage_layout.json"))
    assert result.returncode == 0, result.stderr
    assert not any("migrate_sqlite" in line for line in calls)
    assert calls[-1] == "-m app.launcher"


@pytest.mark.parametrize("module", ["app.persistence.migrate_sqlite", "app.persistence.bootstrap", "app.config_check"])
def test_remote_container_stops_on_failed_preparation(tmp_path, module):
    result, calls = _run(tmp_path, files=("storage_layout.json",), failed_module=module)
    assert result.returncode == 7
    assert "-m app.launcher" not in calls


@pytest.mark.parametrize("selected", ["custom.pickle", "/external/trusted.pickle"])
def test_remote_container_passes_selected_pickle_to_first_migration(tmp_path, selected):
    result, calls = _run(tmp_path, files=("storage_layout.json", "telegram-migration.pickle"), pickle_path=selected)
    assert result.returncode == 0, result.stderr
    expected = selected if selected.startswith("/") else f"{(tmp_path / 'data').as_posix()}/{selected}"
    assert calls[0].endswith(f"--ptb-pickle {expected}")


def test_remote_container_uses_prepared_pickle_without_replacing_originals(tmp_path):
    result, calls = _run(tmp_path, files=("storage_layout.json", "telegram-migration.pickle"))
    assert result.returncode == 0, result.stderr
    assert calls[0].endswith(f"--ptb-pickle {(tmp_path / 'data/telegram-migration.pickle').as_posix()}")


def test_empty_remote_storage_runs_strict_bootstrap(tmp_path):
    result, calls = _run(tmp_path)
    assert result.returncode == 0, result.stderr
    assert not any("migrate_sqlite" in line for line in calls)
    assert calls[0].startswith("-m app.persistence.bootstrap ")
