"""Characterization tests for MaintBot's externally visible PTB routing contract."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from telegram.ext import CallbackQueryHandler, CommandHandler, ConversationHandler, MessageHandler, TypeHandler

import app.main as main_module

DEFAULT_ROUTES = (
    "command:start",
    "command:menu",
    "command:help",
    "command:auth",
    "command:login",
    "command:logout",
    "command:owner",
    r"callback:^access:request$",
    r"callback:^access:(approve|reject|block):\d+$",
    "command:health",
    "command:subscription",
    "conversation:administration_flow",
    r"callback:^(administration:show|staff:profile)$",
    r"callback:^(administration:signature|staff:mode):(title|title_alias)$",
    r"callback:^(administration:settings|product:owner)$",
    r"callback:^administration:help:reset$",
    r"callback:^(administration:title|product:titlemenu):\d+$",
    r"callback:^(administration:title|product:title):\d+:[a-z_]+$",
    "conversation:profile_flow",
    r"callback:^(profile:show|product:profile)$",
    r"callback:^profile:email:clear$",
    "conversation:product_flow",
    r"callback:^subscription:buy$",
    r"callback:^subscription:buyconfirm$",
    r"callback:^subscription:paid:\d+$",
    r"callback:^subscription:renew$",
    r"callback:^subscription:connection$",
    r"callback:^product:requests$",
    r"callback:^product:req:view:\d+$",
    r"callback:^product:manage:\d+$",
    r"callback:^product:tier:\d+:(basic|unlimited_trial)$",
    r"callback:^product:remind:\d+$",
    "conversation:maint_flow",
    r"callback:^maint:endconfirm:[0-9a-f]+$",
    r"callback:^maint:cancelend:[0-9a-f]+$",
    r"callback:^maint:end:[0-9a-f]+$",
    r"callback:^maint:schedcancelconfirm:[0-9a-f]+$",
    r"callback:^maint:schedcancelback:[0-9a-f]+$",
    r"callback:^maint:schedcancel:[0-9a-f]+$",
    "conversation:ticket_flow",
    r"callback:^ticket:take:\d+$",
    r"callback:^ticket:close:\d+$",
    "conversation:users_flow",
    "command:cancel",
    r"callback:^menu:home$",
    r"callback:^menu:help$",
    r"callback:^menu:status$",
    r"callback:^menu:subscription$",
    r"callback:^status:pick$",
    r"callback:^status:show:[a-z0-9_-]{1,12}$",
    r"callback:^status:ufw:[a-z0-9_-]{1,12}$",
    r"callback:^status:dnsrefresh:[a-z0-9_-]{1,12}$",
    r"callback:^status:tlsrefresh:[a-z0-9_-]{1,12}$",
    r"callback:^dns:back:[a-z0-9_-]{1,12}$",
    r"callback:^docker:list:[a-z0-9_-]{1,12}$",
    r"callback:^docker:back:[a-z0-9_-]{1,12}$",
    r"callback:^docker:show:[a-z0-9_-]{1,12}:[a-zA-Z0-9_.\-]{1,64}$",
    r"callback:^docker:inspect:[a-z0-9_-]{1,12}:[a-zA-Z0-9_.\-]{1,64}$",
    r"callback:^docker:logs:[a-z0-9_-]{1,12}:[a-zA-Z0-9_.\-]{1,64}:\d{1,4}$",
    "command:fail2ban",
    r"callback:^f2b:menu:[a-z0-9_-]{1,12}$",
    r"callback:^f2b:tail:[a-z0-9_-]{1,12}:\d{1,5}$",
    r"callback:^f2b:digest:[a-z0-9_-]{1,12}$",
    r"callback:^f2b:back:[a-z0-9_-]{1,12}$",
    "callback:*",
    "message",
)

NON_BLOCKING_ROUTES = (
    "command:health",
    r"callback:^menu:status$",
    r"callback:^status:pick$",
    r"callback:^status:show:[a-z0-9_-]{1,12}$",
    r"callback:^status:ufw:[a-z0-9_-]{1,12}$",
    r"callback:^status:dnsrefresh:[a-z0-9_-]{1,12}$",
    r"callback:^status:tlsrefresh:[a-z0-9_-]{1,12}$",
    r"callback:^dns:back:[a-z0-9_-]{1,12}$",
    r"callback:^docker:list:[a-z0-9_-]{1,12}$",
    r"callback:^docker:back:[a-z0-9_-]{1,12}$",
    r"callback:^docker:show:[a-z0-9_-]{1,12}:[a-zA-Z0-9_.\-]{1,64}$",
    r"callback:^docker:inspect:[a-z0-9_-]{1,12}:[a-zA-Z0-9_.\-]{1,64}$",
    r"callback:^docker:logs:[a-z0-9_-]{1,12}:[a-zA-Z0-9_.\-]{1,64}:\d{1,4}$",
    "command:fail2ban",
    r"callback:^f2b:menu:[a-z0-9_-]{1,12}$",
    r"callback:^f2b:tail:[a-z0-9_-]{1,12}:\d{1,5}$",
    r"callback:^f2b:digest:[a-z0-9_-]{1,12}$",
    r"callback:^f2b:back:[a-z0-9_-]{1,12}$",
)

CONVERSATION_ROUTES = {
    "administration_flow": {
        "states": (81, 82),
        "entry_points": (
            r"callback:^(administration:input:(alias|help|support_email|payment_bank|payment_recipient|"
            r"payment_phone|period_current|period_next)|staff:alias|"
            r"product:input:setting_(bank|recipient|phone|current|next))$",
        ),
        "state_routes": {
            81: ("message",),
            82: (r"callback:^administration:confirm$",),
        },
        "fallbacks": (
            "command:cancel",
            r"callback:^administration:cancel$",
            r"callback:^menu:home$",
        ),
    },
    "profile_flow": {
        "states": (91,),
        "entry_points": (r"callback:^profile:email:edit$",),
        "state_routes": {91: ("message",)},
        "fallbacks": (
            "command:cancel",
            r"callback:^profile:show$",
            r"callback:^menu:home$",
        ),
    },
    "product_flow": {
        "states": (0, 1),
        "entry_points": (
            r"callback:^subscription:trial$",
            r"callback:^product:req:(approve|reject|requisites|confirm|notfound):\d+$",
            r"callback:^product:input:(massdate|massremind|user_end:\d+|manualpay:\d+)$",
        ),
        "state_routes": {
            0: ("message",),
            1: (r"callback:^product:confirm:apply$",),
        },
        "fallbacks": (
            "command:cancel",
            r"callback:^(product:cancel|menu:home)$",
        ),
    },
    "maint_flow": {
        "states": (0, 1, 2, 3, 4, 5, 6),
        "entry_points": (
            "command:maint",
            r"callback:^menu:maint$",
            r"callback:^maint:extend:[0-9a-f]+$",
        ),
        "state_routes": {
            0: (r"callback:^maint:mode:(announce|schedule)$",),
            1: (r"callback:^maint:scope:[a-z0-9_-]{1,12}$",),
            2: (r"callback:^maint:urgency:(urgent|planned)$",),
            3: ("message",),
            4: ("message",),
            5: ("message",),
            6: (
                r"callback:^maint:cal:nav:\d{4}-\d{2}$",
                r"callback:^maint:cal:day:\d{4}-\d{2}-\d{2}$",
                r"callback:^maint:cal:noop$",
            ),
        },
        "fallbacks": (
            "command:cancel",
            r"callback:^menu:home$",
        ),
    },
    "ticket_flow": {
        "states": (0, 1, 2, 3, 4, 5),
        "entry_points": (
            "command:ticket",
            r"callback:^menu:ticket$",
            r"callback:^ticket:adminreply:\d+$",
            r"callback:^ticket:userreply:\d+$",
            r"callback:^ticket:list(?::\d+)?$",
            r"callback:^ticket:open:\d+$",
            r"callback:^ticket:archive$",
            r"callback:^ticket:archive_page:\d+$",
            r"callback:^ticket:transfer_init:\d+$",
            r"callback:^ticket:transfer_to:\d+:\d+$",
        ),
        "state_routes": {
            0: ("message",),
            1: (r"callback:^ticket:(p1|p2|p3)$",),
            2: ("message",),
            3: (r"callback:^ticket:(send|edit_subj|edit_text|cancel)$",),
            4: ("message",),
            5: ("message",),
        },
        "fallbacks": (
            "command:cancel",
            r"callback:^menu:home$",
            r"callback:^ticket:list(?::\d+)?$",
            r"callback:^ticket:open:\d+$",
            r"callback:^ticket:archive$",
            r"callback:^ticket:archive_page:\d+$",
            r"callback:^ticket:transfer_init:\d+$",
            r"callback:^ticket:transfer_to:\d+:\d+$",
        ),
    },
    "users_flow": {
        "states": (0, 1, 2, 3, 4, 5, 6, 7),
        "entry_points": (
            "command:users",
            r"callback:^menu:users$",
            r"callback:^users:user:\d+$",
        ),
        "state_routes": {
            0: (
                r"callback:^users:(all|main|back|filter:(all|active|disabled|unpaid|admins|blocked)|"
                r"user:\d+|page:\d+)$",
            ),
            1: (r"callback:^users:(allmsg|back)$",),
            2: ("message",),
            3: (r"callback:^users:(allsend|all|back)$",),
            4: (
                r"callback:^users:(msg:\d+|nick:\d+|cfg:\d+|subassign:\d+|subsend:\d+|"
                r"toggle:\d+|toggleapply:\d+|access:(approve|block):\d+|"
                r"accessapply:(approve|block):\d+|back)$",
                r"callback:^users:user:\d+$",
            ),
            5: ("message",),
            6: ("message",),
            7: ("message", r"callback:^users:user:\d+$"),
        },
        "fallbacks": (
            "command:cancel",
            r"callback:^menu:home$",
        ),
    },
}

DEFAULT_JOB_NAMES = (
    "fail2ban_digest",
    "dns_daily_refresh",
    "dns_refresh_startup",
    "maint_active_reminder",
    "maint_schedule_tick",
    "auth_prune",
    "outbox_delivery",
    "ticket_orphan_release",
    "subscription_lifecycle",
    "docker_status_refresh",
    "tls_certificate_check",
    "message_cleanup",
)


def _pattern(handler: CallbackQueryHandler) -> str:
    if handler.pattern is None:
        return "*"
    return str(getattr(handler.pattern, "pattern", handler.pattern))


def _route_key(handler: object) -> str:
    if isinstance(handler, CommandHandler):
        return "command:" + ",".join(sorted(handler.commands))
    if isinstance(handler, CallbackQueryHandler):
        return "callback:" + _pattern(handler)
    if isinstance(handler, ConversationHandler):
        return f"conversation:{handler.name}"
    if isinstance(handler, MessageHandler):
        return "message"
    if isinstance(handler, TypeHandler):
        return "type"
    return type(handler).__name__


def _effective_block(handler: Any) -> bool:
    value = handler.block
    return bool(value if isinstance(value, bool) else value.value)


def _cron_fields(trigger: CronTrigger) -> dict[str, str]:
    return {field.name: str(field) for field in trigger.fields}


def test_default_handler_groups_and_route_order_are_stable() -> None:
    application = main_module.build_app()

    assert tuple(sorted(application.handlers)) == (-100, -2, -1, 0)
    assert all(len(application.handlers[group]) == 1 for group in (-100, -2, -1))
    assert all(isinstance(application.handlers[group][0], TypeHandler) for group in (-100, -2, -1))

    routes = tuple(_route_key(handler) for handler in application.handlers[0])
    assert routes == DEFAULT_ROUTES
    assert (
        tuple(_route_key(handler) for handler in application.handlers[0] if not _effective_block(handler))
        == NON_BLOCKING_ROUTES
    )


def test_persistent_conversation_contract_is_stable() -> None:
    application = main_module.build_app()
    conversations = {
        handler.name: handler for handler in application.handlers[0] if isinstance(handler, ConversationHandler)
    }

    assert tuple(conversations) == tuple(CONVERSATION_ROUTES)
    for name, expected in CONVERSATION_ROUTES.items():
        conversation = conversations[name]
        assert conversation.persistent is True
        assert (conversation.per_chat, conversation.per_user, conversation.per_message) == (True, True, False)
        assert tuple(conversation.states) == expected["states"]
        assert tuple(_route_key(handler) for handler in conversation.entry_points) == expected["entry_points"]
        assert {
            state: tuple(_route_key(handler) for handler in handlers) for state, handlers in conversation.states.items()
        } == expected["state_routes"]
        assert tuple(_route_key(handler) for handler in conversation.fallbacks) == expected["fallbacks"]


def test_default_job_names_and_schedules_are_stable() -> None:
    before = datetime.now(timezone.utc)
    application = main_module.build_app()
    after = datetime.now(timezone.utc)
    assert application.job_queue is not None
    jobs = {job.name: job for job in application.job_queue.jobs()}

    assert tuple(jobs) == DEFAULT_JOB_NAMES

    intervals = {
        "maint_active_reminder": 1800,
        "maint_schedule_tick": 60,
        "auth_prune": 300,
        "outbox_delivery": 10,
        "ticket_orphan_release": 60,
        "subscription_lifecycle": 60,
        "docker_status_refresh": 21600,
        "tls_certificate_check": 21600,
        "message_cleanup": 1800,
    }
    for name, seconds in intervals.items():
        trigger = jobs[name].job.trigger
        assert isinstance(trigger, IntervalTrigger)
        assert trigger.interval.total_seconds() == seconds

    daily = {
        "fail2ban_digest": ("12", "0"),
        "dns_daily_refresh": ("3", "5"),
    }
    for name, (hour, minute) in daily.items():
        trigger = jobs[name].job.trigger
        assert isinstance(trigger, CronTrigger)
        fields = _cron_fields(trigger)
        assert (fields["hour"], fields["minute"], fields["second"]) == (hour, minute, "0")
        assert fields["day_of_week"] == "sun,mon,tue,wed,thu,fri,sat"
        assert str(trigger.timezone) == "Europe/Moscow"

    startup = jobs["dns_refresh_startup"].job.trigger
    assert isinstance(startup, DateTrigger)
    assert before.timestamp() + 4 <= startup.run_date.timestamp() <= after.timestamp() + 6


def test_mixed_mode_adds_only_ssh_routes_and_node_status_jobs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main_module, "BOT_MODE", "mixed")
    before = datetime.now(timezone.utc)
    application = main_module.build_app()
    after = datetime.now(timezone.utc)

    routes = tuple(_route_key(handler) for handler in application.handlers[0])
    expected_ssh_routes = (
        r"callback:^status:sshrefresh:confirm:[a-z0-9_-]{1,12}$",
        r"callback:^status:sshrefresh:[a-z0-9_-]{1,12}$",
        r"callback:^status:sshdiag:confirm:[a-z0-9_-]{1,12}$",
        r"callback:^status:sshdiag:[a-z0-9_-]{1,12}$",
    )
    assert tuple(route for route in routes if "status:ssh" in route) == expected_ssh_routes
    assert len(routes) == len(DEFAULT_ROUTES) + len(expected_ssh_routes)

    assert application.job_queue is not None
    jobs = {job.name: job for job in application.job_queue.jobs()}
    assert tuple(jobs) == (
        "fail2ban_digest",
        "dns_daily_refresh",
        "dns_refresh_startup",
        "daily_node_status_refresh",
        "daily_node_status_startup",
        *DEFAULT_JOB_NAMES[3:],
    )

    daily = jobs["daily_node_status_refresh"].job.trigger
    assert isinstance(daily, CronTrigger)
    fields = _cron_fields(daily)
    assert (fields["hour"], fields["minute"], fields["second"]) == ("12", "0", "0")
    assert str(daily.timezone) == "Europe/Moscow"

    startup = jobs["daily_node_status_startup"].job.trigger
    assert isinstance(startup, DateTrigger)
    assert before.timestamp() + 9 <= startup.run_date.timestamp() <= after.timestamp() + 11
