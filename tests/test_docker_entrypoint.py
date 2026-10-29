from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
ENTRYPOINT = ROOT / "deploy/docker-entrypoint.sh"


def _shell() -> str:
    shell = shutil.which("sh")
    if shell:
        return shell
    if os.name == "nt":
        git = shutil.which("git")
        if git:
            git_shell = Path(git).resolve().parent.parent / "bin/sh.exe"
            if git_shell.is_file():
                return str(git_shell)
    pytest.skip("POSIX shell unavailable")


def _run(tmp_path: Path, arguments: list[str], *, fail_module: str = ""):
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
        {
            "PATH": f"{tmp_path.as_posix()}{os.pathsep}{environment.get('PATH', '')}",
            "CALL_LOG": calls.as_posix(),
            "FAIL_MODULE": fail_module,
        }
    )
    result = subprocess.run(
        [_shell(), str(ENTRYPOINT), *arguments],
        env=environment,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    return result, calls.read_text(encoding="utf-8").splitlines() if calls.exists() else []


def test_default_container_start_prepares_storage_and_checks_before_launcher(tmp_path):
    result, calls = _run(tmp_path, ["python", "-m", "app.launcher"])

    assert result.returncode == 0, result.stderr
    assert calls == [
        "-m app.persistence.startup --data-dir /var/lib/maintbot",
        "-m app.config_check",
        "-m app.launcher",
    ]


@pytest.mark.parametrize("module", ["app.persistence.startup", "app.config_check"])
def test_container_refuses_launch_after_failed_storage_preparation_or_preflight(tmp_path, module):
    result, calls = _run(tmp_path, ["python", "-m", "app.launcher"], fail_module=module)

    assert result.returncode == 7
    assert "-m app.launcher" not in calls


def test_explicit_migration_bypasses_bootstrap_and_normal_launch(tmp_path):
    arguments = ["python", "-m", "app.persistence.migrate_sqlite", "--data-dir", "/var/lib/maintbot", "--dry-run"]

    result, calls = _run(tmp_path, arguments)

    assert result.returncode == 0, result.stderr
    assert calls == [" ".join(arguments[1:])]


def test_container_script_has_posix_line_endings():
    assert b"\r\n" not in ENTRYPOINT.read_bytes()
