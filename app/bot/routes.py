"""PTB route composition in the externally visible registration order."""

from __future__ import annotations

from telegram.ext import Application, CallbackQueryHandler, CommandHandler

from ..access.commands import cmd_auth, cmd_help, cmd_logout, cmd_owner, cmd_start
from ..access.request_handlers import access_request_cb, access_review_cb
from ..administration.profile_handlers import (
    administration_show_cb,
    administration_signature_mode_cb,
    administration_staff_title_apply_cb,
    administration_staff_title_menu_cb,
)
from ..administration.settings_handlers import (
    administration_help_reset_cb,
    administration_service_settings_cb,
)
from ..maintenance.lifecycle import (
    maint_cancel_end_cb,
    maint_end_cb,
    maint_end_confirm_cb,
)
from ..maintenance.scheduling import (
    maint_sched_cancel_back_cb,
    maint_sched_cancel_cb,
    maint_sched_cancel_confirm_cb,
)
from ..monitoring.docker.handlers import (
    docker_back_to_status,
    docker_inspect,
    docker_list_menu,
    docker_logs,
    docker_show,
)
from ..monitoring.fail2ban.handlers import (
    f2b_back_cb,
    f2b_digest_cb,
    f2b_menu_cb,
    f2b_tail_cb,
    fail2ban_menu,
)
from ..monitoring.status.handlers import (
    cmd_health,
    dns_back_cb,
    status_dns_refresh_cb,
    status_pick_cb,
    status_show_cb,
    status_tls_refresh_cb,
    status_ufw_cb,
)
from ..monitoring.status.ssh import (
    status_ssh_diag_cb,
    status_ssh_diag_confirm_cb,
    status_ssh_refresh_cb,
    status_ssh_refresh_confirm_cb,
)
from ..subscriptions.connections import connection_show_cb, subscription_show
from ..subscriptions.requests.admin_listing import (
    product_request_view_cb,
    product_requests_cb,
)
from ..subscriptions.requests.customer import purchase_create_cb, purchase_show_cb
from ..subscriptions.requests.management import (
    product_manage_user_cb,
    product_tier_cb,
)
from ..subscriptions.requests.payment_reports import (
    payment_reported_cb,
    renewal_reported_cb,
)
from ..subscriptions.requests.reminders import product_manual_reminder_cb
from ..tickets.admin_handlers import ticket_close_cb, ticket_take_cb
from ..users.profile_handlers import personal_profile_cb, profile_email_clear_cb
from .conversations import NavigableConversationHandler
from .flow_routes import (
    build_administration_flow,
    build_maintenance_flow,
    build_product_flow,
    build_profile_flow,
    build_ticket_flow,
    build_users_flow,
)
from .menu import menu_home_cb
from .navigation import cancel


def _register_access_routes(application: Application) -> None:
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("menu", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("auth", cmd_auth))
    application.add_handler(CommandHandler("login", cmd_auth))
    application.add_handler(CommandHandler("logout", cmd_logout))
    application.add_handler(CommandHandler("owner", cmd_owner))
    application.add_handler(CallbackQueryHandler(access_request_cb, pattern=r"^access:request$"))
    application.add_handler(CallbackQueryHandler(access_review_cb, pattern=r"^access:(approve|reject|block):\d+$"))
    application.add_handler(CommandHandler("health", cmd_health, block=False))
    application.add_handler(CommandHandler("subscription", subscription_show))


def _register_administration_routes(
    application: Application,
    conversations: list[NavigableConversationHandler],
) -> None:
    flow = build_administration_flow()
    conversations.append(flow)
    application.add_handler(flow)
    application.add_handler(
        CallbackQueryHandler(administration_show_cb, pattern=r"^(administration:show|staff:profile)$")
    )
    application.add_handler(
        CallbackQueryHandler(
            administration_signature_mode_cb,
            pattern=r"^(administration:signature|staff:mode):(title|title_alias)$",
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            administration_service_settings_cb,
            pattern=r"^(administration:settings|product:owner)$",
        )
    )
    application.add_handler(CallbackQueryHandler(administration_help_reset_cb, pattern=r"^administration:help:reset$"))
    application.add_handler(
        CallbackQueryHandler(
            administration_staff_title_menu_cb,
            pattern=r"^(administration:title|product:titlemenu):\d+$",
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            administration_staff_title_apply_cb,
            pattern=r"^(administration:title|product:title):\d+:[a-z_]+$",
        )
    )


def _register_profile_routes(
    application: Application,
    conversations: list[NavigableConversationHandler],
) -> None:
    flow = build_profile_flow()
    conversations.append(flow)
    application.add_handler(flow)
    application.add_handler(CallbackQueryHandler(personal_profile_cb, pattern=r"^(profile:show|product:profile)$"))
    application.add_handler(CallbackQueryHandler(profile_email_clear_cb, pattern=r"^profile:email:clear$"))


def _register_product_routes(
    application: Application,
    conversations: list[NavigableConversationHandler],
) -> None:
    flow = build_product_flow()
    conversations.append(flow)
    application.add_handler(flow)
    application.add_handler(CallbackQueryHandler(purchase_show_cb, pattern=r"^subscription:buy$"))
    application.add_handler(CallbackQueryHandler(purchase_create_cb, pattern=r"^subscription:buyconfirm$"))
    application.add_handler(CallbackQueryHandler(payment_reported_cb, pattern=r"^subscription:paid:\d+$"))
    application.add_handler(CallbackQueryHandler(renewal_reported_cb, pattern=r"^subscription:renew$"))
    application.add_handler(CallbackQueryHandler(connection_show_cb, pattern=r"^subscription:connection$"))
    application.add_handler(CallbackQueryHandler(product_requests_cb, pattern=r"^product:requests$"))
    application.add_handler(CallbackQueryHandler(product_request_view_cb, pattern=r"^product:req:view:\d+$"))
    application.add_handler(CallbackQueryHandler(product_manage_user_cb, pattern=r"^product:manage:\d+$"))
    application.add_handler(
        CallbackQueryHandler(product_tier_cb, pattern=r"^product:tier:\d+:(basic|unlimited_trial)$")
    )
    application.add_handler(CallbackQueryHandler(product_manual_reminder_cb, pattern=r"^product:remind:\d+$"))


def _register_maintenance_routes(
    application: Application,
    conversations: list[NavigableConversationHandler],
    *,
    server_key_pattern: str,
) -> None:
    flow = build_maintenance_flow(server_key_pattern)
    conversations.append(flow)
    application.add_handler(flow)
    application.add_handler(CallbackQueryHandler(maint_end_confirm_cb, pattern=r"^maint:endconfirm:[0-9a-f]+$"))
    application.add_handler(CallbackQueryHandler(maint_cancel_end_cb, pattern=r"^maint:cancelend:[0-9a-f]+$"))
    application.add_handler(CallbackQueryHandler(maint_end_cb, pattern=r"^maint:end:[0-9a-f]+$"))
    application.add_handler(
        CallbackQueryHandler(
            maint_sched_cancel_confirm_cb,
            pattern=r"^maint:schedcancelconfirm:[0-9a-f]+$",
        )
    )
    application.add_handler(
        CallbackQueryHandler(maint_sched_cancel_back_cb, pattern=r"^maint:schedcancelback:[0-9a-f]+$")
    )
    application.add_handler(CallbackQueryHandler(maint_sched_cancel_cb, pattern=r"^maint:schedcancel:[0-9a-f]+$"))


def _register_ticket_routes(
    application: Application,
    conversations: list[NavigableConversationHandler],
) -> None:
    flow = build_ticket_flow()
    conversations.append(flow)
    application.add_handler(flow)
    application.add_handler(CallbackQueryHandler(ticket_take_cb, pattern=r"^ticket:take:\d+$"))
    application.add_handler(CallbackQueryHandler(ticket_close_cb, pattern=r"^ticket:close:\d+$"))


def _register_users_routes(
    application: Application,
    conversations: list[NavigableConversationHandler],
) -> None:
    flow = build_users_flow()
    conversations.append(flow)
    application.add_handler(flow)


def _register_menu_routes(application: Application) -> None:
    application.add_handler(CommandHandler("cancel", cancel))
    application.add_handler(CallbackQueryHandler(menu_home_cb, pattern=r"^menu:home$"))
    application.add_handler(CallbackQueryHandler(cmd_help, pattern=r"^menu:help$"))
    application.add_handler(CallbackQueryHandler(cmd_health, pattern=r"^menu:status$", block=False))
    application.add_handler(CallbackQueryHandler(subscription_show, pattern=r"^menu:subscription$"))


def _register_status_routes(
    application: Application,
    *,
    bot_mode: str,
    server_key_pattern: str,
) -> None:
    application.add_handler(CallbackQueryHandler(status_pick_cb, pattern=r"^status:pick$", block=False))
    application.add_handler(
        CallbackQueryHandler(
            status_show_cb,
            pattern=rf"^status:show:{server_key_pattern}$",
            block=False,
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            status_ufw_cb,
            pattern=rf"^status:ufw:{server_key_pattern}$",
            block=False,
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            status_dns_refresh_cb,
            pattern=rf"^status:dnsrefresh:{server_key_pattern}$",
            block=False,
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            status_tls_refresh_cb,
            pattern=rf"^status:tlsrefresh:{server_key_pattern}$",
            block=False,
        )
    )
    if bot_mode == "mixed":
        application.add_handler(
            CallbackQueryHandler(
                status_ssh_refresh_confirm_cb,
                pattern=rf"^status:sshrefresh:confirm:{server_key_pattern}$",
                block=False,
            )
        )
        application.add_handler(
            CallbackQueryHandler(
                status_ssh_refresh_cb,
                pattern=rf"^status:sshrefresh:{server_key_pattern}$",
                block=False,
            )
        )
        application.add_handler(
            CallbackQueryHandler(
                status_ssh_diag_confirm_cb,
                pattern=rf"^status:sshdiag:confirm:{server_key_pattern}$",
                block=False,
            )
        )
        application.add_handler(
            CallbackQueryHandler(
                status_ssh_diag_cb,
                pattern=rf"^status:sshdiag:{server_key_pattern}$",
                block=False,
            )
        )

    application.add_handler(
        CallbackQueryHandler(
            dns_back_cb,
            pattern=rf"^dns:back:{server_key_pattern}$",
            block=False,
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            docker_list_menu,
            pattern=rf"^docker:list:{server_key_pattern}$",
            block=False,
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            docker_back_to_status,
            pattern=rf"^docker:back:{server_key_pattern}$",
            block=False,
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            docker_show,
            pattern=rf"^docker:show:{server_key_pattern}:[a-zA-Z0-9_.\-]{{1,64}}$",
            block=False,
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            docker_inspect,
            pattern=rf"^docker:inspect:{server_key_pattern}:[a-zA-Z0-9_.\-]{{1,64}}$",
            block=False,
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            docker_logs,
            pattern=rf"^docker:logs:{server_key_pattern}:[a-zA-Z0-9_.\-]{{1,64}}:\d{{1,4}}$",
            block=False,
        )
    )


def _register_fail2ban_routes(application: Application, *, server_key_pattern: str) -> None:
    application.add_handler(CommandHandler("fail2ban", fail2ban_menu, block=False))
    application.add_handler(
        CallbackQueryHandler(
            f2b_menu_cb,
            pattern=rf"^f2b:menu:{server_key_pattern}$",
            block=False,
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            f2b_tail_cb,
            pattern=rf"^f2b:tail:{server_key_pattern}:\d{{1,5}}$",
            block=False,
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            f2b_digest_cb,
            pattern=rf"^f2b:digest:{server_key_pattern}$",
            block=False,
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            f2b_back_cb,
            pattern=rf"^f2b:back:{server_key_pattern}$",
            block=False,
        )
    )


def register_routes(
    application: Application,
    *,
    bot_mode: str,
    server_key_pattern: str,
) -> list[NavigableConversationHandler]:
    """Register group-zero feature routes and return persistent conversations."""
    conversations: list[NavigableConversationHandler] = []
    _register_access_routes(application)
    _register_administration_routes(application, conversations)
    _register_profile_routes(application, conversations)
    _register_product_routes(application, conversations)
    _register_maintenance_routes(
        application,
        conversations,
        server_key_pattern=server_key_pattern,
    )
    _register_ticket_routes(application, conversations)
    _register_users_routes(application, conversations)
    _register_menu_routes(application)
    _register_status_routes(
        application,
        bot_mode=bot_mode,
        server_key_pattern=server_key_pattern,
    )
    _register_fail2ban_routes(application, server_key_pattern=server_key_pattern)
    return conversations
