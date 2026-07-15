from dataclasses import dataclass, field


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
    dns_ok_domains: int = 0
    dns_total_domains: int = 0
    dns_bad_domains: int = 0
    dns_unknown_domains: int = 0
    dns_error_details: list[str] = field(default_factory=list)
    ufw_allow: list[str] = field(default_factory=list)
    ufw_deny: list[str] = field(default_factory=list)
    ufw_reject: list[str] = field(default_factory=list)
    containers: list[DockerContainerView] = field(default_factory=list)
    admin_mode: bool = False
    # mixed-mode (RemnaWave metrics) extras
    source_mode: str = "ssh"  # "ssh" | "mixed"
    node_online: bool | None = None  # None — статус ноды не определён через метрики
    online_users: int | None = None
    last_seen_text: str = ""
    metrics_error: str = ""
    disk_updated_at_text: str = ""
    ufw_updated_at_text: str = ""
    show_containers_block: bool = True
