import re
import sys
import warnings
from datetime import datetime, time as dtime
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from telegram.warnings import PTBUserWarning
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    filters,
)

from app.config import (
    ADMIN_PASSWORD,
    AUTH_PASSWORD,
    BOT_TOKEN,
    DNS_DAILY_REFRESH_AT,
    DNS_STARTUP_REFRESH_DELAY_SEC,
    FAIL2BAN_DAILY_AT,
    MAINT_RESTART_NOTIFY_DELAY_SEC,
    TZ,
    logger,
)
from app.handlers.auth import cmd_auth, cmd_help, cmd_logout, cmd_start
from app.handlers.common import cancel, cancel_to_menu_cb, menu_home_cb
from app.handlers.docker import docker_back_to_status, docker_inspect, docker_list_menu, docker_logs, docker_show
from app.handlers.fail2ban import (
    f2b_back_cb,
    f2b_digest_cb,
    f2b_menu_cb,
    f2b_tail_cb,
    fail2ban_daily_digest,
    fail2ban_menu,
)
from app.handlers.maint import (
    STATE_MAINT_SCOPE,
    STATE_MAINT_DURATION,
    STATE_MAINT_EXTEND,
    STATE_MAINT_URGENCY,
    maint_cancel_end_cb,
    maint_duration,
    maint_end_cb,
    maint_end_confirm_cb,
    maint_extend_cb,
    maint_extend_duration,
    maint_restart_notify,
    maint_scope,
    maint_start,
    maint_urgency,
)
from app.handlers.subscription import subscription_show
from app.handlers.status import (
    cmd_health,
    dns_daily_refresh,
    dns_back_cb,
    status_dns_refresh_cb,
    status_pick_cb,
    status_show_cb,
    status_ufw_cb,
)
from app.handlers.tickets import (
    TICKET_CONFIRM,
    TICKET_SUBJECT,
    TICKET_TEXT,
    TICKET_URGENCY,
    ticket_confirm,
    ticket_start,
    ticket_subject,
    ticket_text,
    ticket_urgency,
)
from app.handlers.users import (
    ADMIN_ALL_MENU,
    ADMIN_ALL_MSG_TEXT,
    ADMIN_ALL_MSG_CONFIRM,
    ADMIN_PICK,
    ADMIN_USER_CFG_TEXT,
    ADMIN_USER_MENU,
    ADMIN_USER_MSG_TEXT,
    ADMIN_USER_NICK_TEXT,
    users_all_menu,
    users_all_msg_confirm,
    users_all_msg_text,
    users_entry,
    users_pick,
    users_user_cfg_text,
    users_user_menu,
    users_user_msg_text,
    users_user_nick_text,
)

PRIVATE_TEXT = filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND

warnings.filterwarnings(
    "ignore",
    message=r"If 'per_message=False', 'CallbackQueryHandler' will not be tracked for every message\..*",
    category=PTBUserWarning,
)


def _parse_schedule_hhmm(raw: str, *, field_name: str, fallback: str) -> tuple[int, int]:
    try:
        t = datetime.strptime(raw, "%H:%M").time()
        return t.hour, t.minute
    except Exception:
        logger.warning("Invalid %s=%s, fallback to %s", field_name, raw, fallback)
        t = datetime.strptime(fallback, "%H:%M").time()
        return t.hour, t.minute


async def on_error(update: object, context) -> None:
    try:
        cb_data = getattr(getattr(update, "callback_query", None), "data", None)
        user_id = getattr(getattr(update, "effective_user", None), "id", None)
        chat_id = getattr(getattr(update, "effective_chat", None), "id", None)
    except Exception:
        cb_data = user_id = chat_id = None
    logger.exception(
        "Unhandled exception in handler: %s (user_id=%s chat_id=%s cb=%s)",
        context.error,
        user_id,
        chat_id,
        cb_data,
    )


def build_app() -> Application:
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN в app/env.secrets, app/.env или переменных окружения")
    if not AUTH_PASSWORD and not ADMIN_PASSWORD:
        logger.warning("Не заданы AUTH_PASSWORD и ADMIN_PASSWORD: авторизация невозможна.")

    app: Application = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("auth", cmd_auth))
    app.add_handler(CommandHandler("logout", cmd_logout))
    app.add_handler(CommandHandler("health", cmd_health))
    app.add_handler(CommandHandler("subscription", subscription_show))

    maint_conv = ConversationHandler(
        entry_points=[
            CommandHandler("maint", maint_start),
            CallbackQueryHandler(maint_start, pattern=r"^menu:maint$"),
            CallbackQueryHandler(maint_extend_cb, pattern=r"^maint:extend:[0-9a-f]+$"),
        ],
        states={
            STATE_MAINT_SCOPE: [CallbackQueryHandler(maint_scope, pattern=r"^maint:scope:[a-z0-9_-]{1,12}$")],
            STATE_MAINT_URGENCY: [CallbackQueryHandler(maint_urgency, pattern=r"^maint:urgency:(urgent|planned)$")],
            STATE_MAINT_DURATION: [MessageHandler(PRIVATE_TEXT, maint_duration)],
            STATE_MAINT_EXTEND: [MessageHandler(PRIVATE_TEXT, maint_extend_duration)],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(cancel_to_menu_cb, pattern=r"^menu:home$"),
        ],
        name="maint_flow",
        persistent=False,
    )
    app.add_handler(maint_conv)
    app.add_handler(CallbackQueryHandler(maint_end_confirm_cb, pattern=r"^maint:endconfirm:[0-9a-f]+$"))
    app.add_handler(CallbackQueryHandler(maint_cancel_end_cb, pattern=r"^maint:cancelend:[0-9a-f]+$"))
    app.add_handler(CallbackQueryHandler(maint_end_cb, pattern=r"^maint:end:[0-9a-f]+$"))

    ticket_conv = ConversationHandler(
        entry_points=[
            CommandHandler("ticket", ticket_start),
            CallbackQueryHandler(ticket_start, pattern=r"^menu:ticket$"),
        ],
        states={
            TICKET_SUBJECT: [MessageHandler(PRIVATE_TEXT, ticket_subject)],
            TICKET_URGENCY: [CallbackQueryHandler(ticket_urgency, pattern=r"^ticket:(p1|p2|p3)$")],
            TICKET_TEXT: [MessageHandler(PRIVATE_TEXT, ticket_text)],
            TICKET_CONFIRM: [CallbackQueryHandler(ticket_confirm, pattern=r"^ticket:(send|edit_subj|edit_text|cancel)$")],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(cancel_to_menu_cb, pattern=r"^menu:home$"),
        ],
        name="ticket_flow",
        persistent=False,
    )
    app.add_handler(ticket_conv)

    users_conv = ConversationHandler(
        entry_points=[
            CommandHandler("users", users_entry),
            CallbackQueryHandler(users_entry, pattern=r"^menu:users$"),
        ],
        states={
            ADMIN_PICK: [
                CallbackQueryHandler(users_pick, pattern=r"^users:(all|main|back|noop|filter:(all|active|disabled|unpaid|admins)|user:\d+)$"),
            ],
            ADMIN_ALL_MENU: [
                CallbackQueryHandler(users_all_menu, pattern=r"^users:(allmsg|back)$"),
            ],
            ADMIN_ALL_MSG_TEXT: [
                MessageHandler(PRIVATE_TEXT, users_all_msg_text),
            ],
            ADMIN_ALL_MSG_CONFIRM: [
                CallbackQueryHandler(users_all_msg_confirm, pattern=r"^users:(allsend|all|back)$"),
            ],
            ADMIN_USER_MENU: [
                CallbackQueryHandler(
                    users_user_menu,
                    pattern=r"^users:(msg:\d+|nick:\d+|cfg:\d+|subassign:\d+|subsend:\d+|refresh:\d+|toggle:\d+|toggleapply:\d+|paid:\d+|paidapply:\d+|back)$",
                ),
                CallbackQueryHandler(users_pick, pattern=r"^users:user:\d+$"),
            ],
            ADMIN_USER_MSG_TEXT: [
                MessageHandler(PRIVATE_TEXT, users_user_msg_text),
            ],
            ADMIN_USER_NICK_TEXT: [
                MessageHandler(PRIVATE_TEXT, users_user_nick_text),
            ],
            ADMIN_USER_CFG_TEXT: [
                MessageHandler(PRIVATE_TEXT, users_user_cfg_text),
                CallbackQueryHandler(users_pick, pattern=r"^users:user:\d+$"),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(cancel_to_menu_cb, pattern=r"^menu:home$"),
        ],
        name="users_flow",
        persistent=False,
    )
    app.add_handler(users_conv)
    app.add_handler(CallbackQueryHandler(menu_home_cb, pattern=r"^menu:home$"))
    app.add_handler(CallbackQueryHandler(cmd_help, pattern=r"^menu:help$"))
    app.add_handler(CallbackQueryHandler(cmd_health, pattern=r"^menu:status$"))
    app.add_handler(CallbackQueryHandler(subscription_show, pattern=r"^menu:subscription$"))

    app.add_handler(CallbackQueryHandler(status_pick_cb, pattern=r"^status:pick$"))
    app.add_handler(CallbackQueryHandler(status_show_cb, pattern=r"^status:show:[a-z0-9_-]{1,12}$"))
    app.add_handler(CallbackQueryHandler(status_ufw_cb, pattern=r"^status:ufw:[a-z0-9_-]{1,12}$"))
    app.add_handler(CallbackQueryHandler(status_dns_refresh_cb, pattern=r"^status:dnsrefresh:[a-z0-9_-]{1,12}$"))
    app.add_handler(CallbackQueryHandler(dns_back_cb, pattern=r"^dns:back:[a-z0-9_-]{1,12}$"))
    app.add_handler(CallbackQueryHandler(docker_list_menu, pattern=r"^docker:list:[a-z0-9_-]{1,12}$"))
    app.add_handler(CallbackQueryHandler(docker_back_to_status, pattern=r"^docker:back:[a-z0-9_-]{1,12}$"))
    app.add_handler(
        CallbackQueryHandler(docker_show, pattern=r"^docker:show:[a-z0-9_-]{1,12}:[a-zA-Z0-9_.\-]{1,64}$")
    )
    app.add_handler(
        CallbackQueryHandler(docker_inspect, pattern=r"^docker:inspect:[a-z0-9_-]{1,12}:[a-zA-Z0-9_.\-]{1,64}$")
    )
    app.add_handler(
        CallbackQueryHandler(docker_logs, pattern=r"^docker:logs:[a-z0-9_-]{1,12}:[a-zA-Z0-9_.\-]{1,64}:\d{1,3}$")
    )

    app.add_handler(CommandHandler("fail2ban", fail2ban_menu))
    app.add_handler(CallbackQueryHandler(f2b_menu_cb, pattern=r"^f2b:menu:[a-z0-9_-]{1,12}$"))
    app.add_handler(CallbackQueryHandler(f2b_tail_cb, pattern=r"^f2b:tail:[a-z0-9_-]{1,12}:\d{1,5}$"))
    app.add_handler(CallbackQueryHandler(f2b_digest_cb, pattern=r"^f2b:digest:[a-z0-9_-]{1,12}$"))
    app.add_handler(CallbackQueryHandler(f2b_back_cb, pattern=r"^f2b:back:[a-z0-9_-]{1,12}$"))

    if app.job_queue:
        hh, mm = _parse_schedule_hhmm(FAIL2BAN_DAILY_AT, field_name="FAIL2BAN_DAILY_AT", fallback="12:00")
        dns_hh, dns_mm = _parse_schedule_hhmm(DNS_DAILY_REFRESH_AT, field_name="DNS_DAILY_REFRESH_AT", fallback="03:05")
        app.job_queue.run_daily(
            fail2ban_daily_digest,
            time=dtime(hour=hh, minute=mm, tzinfo=TZ),
            name="fail2ban_digest",
        )
        app.job_queue.run_daily(
            dns_daily_refresh,
            time=dtime(hour=dns_hh, minute=dns_mm, tzinfo=TZ),
            name="dns_daily_refresh",
        )
        app.job_queue.run_once(dns_daily_refresh, when=DNS_STARTUP_REFRESH_DELAY_SEC, name="dns_refresh_startup")
        app.job_queue.run_once(
            maint_restart_notify,
            when=MAINT_RESTART_NOTIFY_DELAY_SEC,
            name="maint_restart_notify",
        )
    else:
        logger.warning("JobQueue недоступен: для ежедневной выжимки установите python-telegram-bot[job-queue].")

    app.add_error_handler(on_error)
    return app


def main() -> None:
    app = build_app()
    logger.info("Bot started")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
