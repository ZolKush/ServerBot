"""Remote monitoring adapters executed over SSH."""

from .docker import (
    remote_docker_containers,
    remote_docker_inspect_summary,
    remote_docker_logs_tail,
)
from .fail2ban import (
    remote_fail2ban_events,
    remote_fail2ban_identity,
    remote_fail2ban_stat,
    remote_read_text_range,
    remote_tail_text_file,
)
from .status import RemoteStatusBundle, remote_status_bundle
from .transport import ssh_run_exec, ssh_run_shell

__all__ = [
    "RemoteStatusBundle",
    "remote_docker_containers",
    "remote_docker_inspect_summary",
    "remote_docker_logs_tail",
    "remote_fail2ban_events",
    "remote_fail2ban_identity",
    "remote_fail2ban_stat",
    "remote_read_text_range",
    "remote_status_bundle",
    "remote_tail_text_file",
    "ssh_run_exec",
    "ssh_run_shell",
]
