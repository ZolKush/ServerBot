from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class DockerContainerView:
    name: str
    is_up: bool
    status_text: str
    restarts: str


@dataclass(frozen=True)
class StatusSnapshot:
    title: str
    server_label: str
    server_flag: str
    now_text: str
    uptime_text: str
    memory_raw: str
    disk_raw: str
    ufw_state: str
    ufw_allow: List[str] = field(default_factory=list)
    ufw_deny: List[str] = field(default_factory=list)
    ufw_reject: List[str] = field(default_factory=list)
    containers: List[DockerContainerView] = field(default_factory=list)
    admin_mode: bool = False
