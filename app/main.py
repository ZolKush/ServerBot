import re
import sys
from datetime import datetime, time as dtime
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parent.parent))

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
    FAIL2BAN_DAILY_AT,
    MENU_MAINT,
    MENU_STATUS,
    MENU_TICKET,
    MENU_USERS,
    TZ,
    logger,
)
from app.handlers.auth import cmd_auth, cmd_help, cmd_logout, cmd_start
from app.handlers.common import cancel
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
    STATE_MAINT_DURATION,
    STATE_MAINT_EXTEND,
    STATE_MAINT_URGENCY,
    maint_duration,
    maint_end_cb,
    maint_extend_cb,
    maint_extend_duration,
    maint_restart_notify,
    maint_start,
    maint_urgency,
)
from app.handlers.status import cmd_health, dns_back_cb, dns_check_cb
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
    ADMIN_PICK,
    ADMIN_USER_CFG_TEXT,
    ADMIN_USER_MENU,
    ADMIN_USER_MSG_TEXT,
    ADMIN_USER_NICK_TEXT,
    users_all_menu,
    users_all_msg_text,
    users_entry,
    users_pick,
    users_user_cfg_text,
    users_user_menu,
    users_user_msg_text,
    users_user_nick_text,
)

PRIVATE_TEXT = filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND


async def on_error(update: object, context) -> None:
    logger.exception("Unhandled exception in handler: %s", context.error)


def build_app() -> Application:
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN в .env")
    if not AUTH_PASSWORD and not ADMIN_PASSWORD:
        logger.warning("Не заданы AUTH_PASSWORD и ADMIN_PASSWORD: авторизация невозможна.")

    app: Application = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("auth", cmd_auth))
    app.add_handler(CommandHandler("logout", cmd_logout))
    app.add_handler(CommandHandler("health", cmd_health))

    maint_conv = ConversationHandler(
        entry_points=[
            CommandHandler("maint", maint_start),
            MessageHandler(filters.ChatType.PRIVATE & filters.Regex(rf"^{re.escape(MENU_MAINT)}$"), maint_start),
            CallbackQueryHandler(maint_extend_cb, pattern=r"^maint:extend:\d+$"),
        ],
        states={
            STATE_MAINT_URGENCY: [CallbackQueryHandler(maint_urgency, pattern=r"^maint:(urgent|planned)$")],
            STATE_MAINT_DURATION: [MessageHandler(PRIVATE_TEXT, maint_duration)],
            STATE_MAINT_EXTEND: [MessageHandler(PRIVATE_TEXT, maint_extend_duration)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="maint_flow",
        persistent=False,
    )
    app.add_handler(maint_conv)
    app.add_handler(CallbackQueryHandler(maint_end_cb, pattern=r"^maint:end:\d+$"))

    ticket_conv = ConversationHandler(
        entry_points=[
            CommandHandler("ticket", ticket_start),
            MessageHandler(filters.ChatType.PRIVATE & filters.Regex(rf"^{re.escape(MENU_TICKET)}$"), ticket_start),
        ],
        states={
            TICKET_SUBJECT: [MessageHandler(PRIVATE_TEXT, ticket_subject)],
            TICKET_URGENCY: [CallbackQueryHandler(ticket_urgency, pattern=r"^ticket:(p1|p2|p3)$")],
            TICKET_TEXT: [MessageHandler(PRIVATE_TEXT, ticket_text)],
            TICKET_CONFIRM: [CallbackQueryHandler(ticket_confirm, pattern=r"^ticket:send$")],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="ticket_flow",
        persistent=False,
    )
    app.add_handler(ticket_conv)

    users_conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.ChatType.PRIVATE & filters.Regex(rf"^{re.escape(MENU_USERS)}$"), users_entry),
            CommandHandler("users", users_entry),
        ],
        states={
            ADMIN_PICK: [
                CallbackQueryHandler(users_pick, pattern=r"^users:(all|main|user:\d+|back)$"),
            ],
            ADMIN_ALL_MENU: [
                CallbackQueryHandler(users_all_menu, pattern=r"^users:(allmsg|back)$"),
            ],
            ADMIN_ALL_MSG_TEXT: [
                MessageHandler(PRIVATE_TEXT, users_all_msg_text),
            ],
            ADMIN_USER_MENU: [
                CallbackQueryHandler(
                    users_user_menu,
                    pattern=r"^users:(msg:\d+|nick:\d+|cfg:\d+|toggle:\d+|paid:\d+|back)$",
                ),
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
        fallbacks=[CommandHandler("cancel", cancel)],
        name="users_flow",
        persistent=False,
    )
    app.add_handler(users_conv)

    app.add_handler(CallbackQueryHandler(dns_check_cb, pattern=r"^dns:check$"))
    app.add_handler(CallbackQueryHandler(dns_back_cb, pattern=r"^dns:back$"))
    app.add_handler(CallbackQueryHandler(docker_list_menu, pattern=r"^docker:list$"))
    app.add_handler(CallbackQueryHandler(docker_back_to_status, pattern=r"^docker:back$"))
    app.add_handler(CallbackQueryHandler(docker_show, pattern=r"^docker:show:[a-zA-Z0-9_.\-]{1,64}$"))
    app.add_handler(CallbackQueryHandler(docker_inspect, pattern=r"^docker:inspect:[a-zA-Z0-9_.\-]{1,64}$"))
    app.add_handler(CallbackQueryHandler(docker_logs, pattern=r"^docker:logs:[a-zA-Z0-9_.\-]{1,64}:\d{1,3}$"))

    app.add_handler(CommandHandler("fail2ban", fail2ban_menu))
    app.add_handler(CallbackQueryHandler(f2b_menu_cb, pattern=r"^f2b:menu$"))
    app.add_handler(CallbackQueryHandler(f2b_tail_cb, pattern=r"^f2b:tail:\d{1,5}$"))
    app.add_handler(CallbackQueryHandler(f2b_digest_cb, pattern=r"^f2b:digest$"))
    app.add_handler(CallbackQueryHandler(f2b_back_cb, pattern=r"^f2b:back$"))

    app.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & filters.Regex(rf"^{re.escape(MENU_STATUS)}$"),
            cmd_health,
        )
    )

    if app.job_queue:
        try:
            t = datetime.strptime(FAIL2BAN_DAILY_AT, "%H:%M").time()
            hh, mm = t.hour, t.minute
        except Exception:
            logger.warning("Invalid FAIL2BAN_DAILY_AT=%s, fallback to 12:00", FAIL2BAN_DAILY_AT)
            hh, mm = 12, 0
        app.job_queue.run_daily(
            fail2ban_daily_digest,
            time=dtime(hour=hh, minute=mm, tzinfo=TZ),
            name="fail2ban_digest",
        )
        app.job_queue.run_once(maint_restart_notify, when=2, name="maint_restart_notify")
    else:
        logger.warning("JobQueue недоступен: для ежедневной выжимки установите python-telegram-bot[job-queue].")

    app.add_error_handler(on_error)
    return app


def main() -> None:
    app = build_app()
    logger.info("Bot started")
    app.run_polling()


if __name__ == "__main__":
    main()
