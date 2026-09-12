from __future__ import annotations

import os
import stat
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def helper():
    path = Path(__file__).resolve().parents[1] / "deploy" / "maintbot-helper"
    loader = SourceFileLoader("maintbot_helper", str(path))
    spec = spec_from_loader(loader.name, loader)
    assert spec is not None
    module = module_from_spec(spec)
    loader.exec_module(module)
    return module


def test_helper_has_isolated_python_shebang():
    path = Path(__file__).resolve().parents[1] / "deploy" / "maintbot-helper"
    assert path.read_text(encoding="utf-8").splitlines()[0] == "#!/usr/bin/python3 -I"


def test_helper_does_not_resolve_executables_from_untrusted_path(helper, monkeypatch, tmp_path):
    # Even a PATH entry named "docker" must not extend the root helper's allowlist.
    folder = tmp_path / "untrusted-path"
    folder.mkdir()
    name = "docker.exe" if os.name == "nt" else "docker"
    binary = folder / name
    binary.write_bytes(b"not executed")
    binary.chmod(0o700)
    monkeypatch.setenv("PATH", str(folder))
    with pytest.raises(SystemExit) as error:
        helper.executable(str(tmp_path / name))
    assert error.value.code == 69


def test_log_is_opened_with_nonblocking_and_nofollow_flags(helper, monkeypatch, tmp_path):
    path = tmp_path / "fail2ban.log"
    path.write_bytes(b"safe data")
    flags_seen = []
    original = helper.os.open

    def opened(path, flags):
        flags_seen.append(flags)
        return original(path, flags)

    monkeypatch.setattr(helper.os, "open", opened)
    with helper.open_log(path) as stream:
        assert stream.read() == b"safe data"
    for flag in ("O_NOFOLLOW", "O_NONBLOCK", "O_CLOEXEC"):
        value = getattr(os, flag, 0)
        assert flags_seen[0] & value == value


def test_nonregular_log_is_rejected_and_descriptor_closed(helper, monkeypatch, tmp_path):
    path = tmp_path / "log"
    path.write_bytes(b"test")
    opened = []
    original = helper.os.open

    def track(path, flags):
        descriptor = original(path, flags)
        opened.append(descriptor)
        return descriptor

    monkeypatch.setattr(helper.os, "open", track)
    monkeypatch.setattr(helper.os, "fstat", lambda fd: SimpleNamespace(st_mode=stat.S_IFIFO))
    with pytest.raises(SystemExit) as error:
        helper.open_log(path)
    assert error.value.code == 77
    with pytest.raises(OSError):
        os.read(opened[0], 1)


@pytest.mark.skipif(not hasattr(os, "O_NOFOLLOW"), reason="Linux deployment flags")
def test_replacement_symlink_is_not_followed(helper, tmp_path):
    target = tmp_path / "secret"
    target.write_bytes(b"private")
    link = tmp_path / "log"
    link.symlink_to(target)
    with pytest.raises(OSError):
        helper.open_log(link)


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="Linux deployment FIFO")
def test_fifo_is_rejected_without_waiting_for_a_writer(helper, tmp_path):
    fifo = tmp_path / "log"
    os.mkfifo(fifo)
    with pytest.raises(SystemExit) as error:
        helper.open_log(fifo)
    assert error.value.code == 77


def test_docker_inspection_is_read_only_and_excludes_environment(helper):
    command = helper.docker_inspect_argv("/usr/bin/docker", "api", allowed={"api"})
    assert command[:4] == ["/usr/bin/docker", "inspect", "--type", "container"]
    assert "Config.Env" not in command[-2]
    with pytest.raises(SystemExit):
        helper.docker_inspect_argv("/usr/bin/docker", "other", allowed={"api"})
