import contextlib
import sys
import time
import warnings
from datetime import datetime
from datetime import time as dtime
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    _PROJECT_ROOT = Path(__file__).resolve().parent.parent
    if str(_PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(_PROJECT_ROOT))

from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    PicklePersistence,
    filters,
)
from telegram.warnings import PTBUserWarning

from app.config import (
    AUTH_PRUNE_INTERVAL_SEC,
    BOT_MODE,
    BOT_TOKEN,
    DAILY_NODE_STATUS_REFRESH_AT,
    DNS_DAILY_REFRESH_AT,
    DNS_STARTUP_REFRESH_DELAY_SEC,
    ERROR_NOTIFY_INTERVAL_SEC,
    FAIL2BAN_DAILY_AT,
    INSTANCE_LOCK_PATH,
    MAINT_RESTART_NOTIFY_DELAY_SEC,
    MAINT_RESTART_REMINDER_INTERVAL_SEC,
    OUTBOX_PROCESS_INTERVAL_SEC,
    PTB_PERSISTENCE_PATH,
    SERVER_KEY_PATTERN,
    TZ,
    logger,
)
from app.handlers.auth import (
    access_request_cb,
    access_review_cb,
    auth_prune_task,
    cmd_auth,
    cmd_help,
    cmd_logout,
    cmd_start,
)
from app.handlers.common import (
    authorized_ids,
    cancel,
    cancel_to_menu_cb,
    clip_html_message,
    html_escape,
    is_authorized,
    is_enabled,
    menu_home_cb,
    reply_need_auth,
)
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
    STATE_MAINT_MODE,
    STATE_MAINT_SCHEDULE_DATE,
    STATE_MAINT_SCHEDULE_RANGE,
    STATE_MAINT_SCOPE,
    STATE_MAINT_URGENCY,
    maint_cal_day,
    maint_cal_nav,
    maint_cal_noop,
    maint_cancel_end_cb,
    maint_duration,
    maint_end_cb,
    maint_end_confirm_cb,
    maint_extend_cb,
    maint_extend_duration,
    maint_mode,
    maint_restart_notify,
    maint_sched_cancel_back_cb,
    maint_sched_cancel_cb,
    maint_sched_cancel_confirm_cb,
    maint_schedule_range,
    maint_schedule_tick,
    maint_scope,
    maint_start,
    maint_urgency,
)
from app.handlers.status import (
    cmd_health,
    daily_node_status_refresh,
    dns_back_cb,
    dns_daily_refresh,
    status_dns_refresh_cb,
    status_pick_cb,
    status_show_cb,
    status_ssh_diag_cb,
    status_ssh_diag_confirm_cb,
    status_ssh_refresh_cb,
    status_ssh_refresh_confirm_cb,
    status_ufw_cb,
)
from app.handlers.subscription import subscription_show
from app.handlers.tickets import (
    TICKET_ADMIN_REPLY_TEXT,
    TICKET_CONFIRM,
    TICKET_SUBJECT,
    TICKET_TEXT,
    TICKET_URGENCY,
    TICKET_USER_REPLY_TEXT,
    release_orphaned_tickets,
    ticket_admin_reply_start,
    ticket_admin_reply_text,
    ticket_archive_cb,
    ticket_archive_page_cb,
    ticket_close_cb,
    ticket_confirm,
    ticket_list_cb,
    ticket_open_cb,
    ticket_start,
    ticket_subject,
    ticket_take_cb,
    ticket_text,
    ticket_transfer_init_cb,
    ticket_transfer_to_cb,
    ticket_urgency,
    ticket_user_reply_start,
    ticket_user_reply_text,
)
from app.handlers.users import (
    ADMIN_ALL_MENU,
    ADMIN_ALL_MSG_CONFIRM,
    ADMIN_ALL_MSG_TEXT,
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
from app.services.outbox import process_outbox_job
from app.services.remnawave_metrics import close_metrics_client
from app.single_instance import ALREADY_RUNNING_EXIT_CODE, InstanceAlreadyRunning, SingleInstanceLock

PRIVATE_TEXT = filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND
PRIVATE_TICKET_INPUT = (
    filters.ChatType.PRIVATE & ~filters.COMMAND & (filters.TEXT | filters.PHOTO | filters.Document.ALL)
)


def _conversation_handler(**kwargs: Any) -> ConversationHandler:
    # These flows deliberately mix callback and text steps and are tracked per
    # user/chat. per_message=True is therefore not applicable. Suppress only
    # PTB's informational warning emitted by the constructor.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r"If 'per_message=False', 'CallbackQueryHandler' will not be tracked for every message\..*",
            category=PTBUserWarning,
        )
        return ConversationHandler(**kwargs)


def _parse_schedule_hhmm(raw: str, *, field_name: str, fallback: str) -> tuple[int, int]:
    try:
        t = datetime.strptime(raw, "%H:%M").time()
        return t.hour, t.minute
    except Exception:
        logger.warning("Invalid %s=%s, fallback to %s", field_name, raw, fallback)
        t = datetime.strptime(fallback, "%H:%M").time()
        return t.hour, t.minute


_LAST_ERROR_NOTIFY_AT = 0.0


async def on_error(update: object, context) -> None:
    try:
        cb_data = getattr(getattr(update, "callback_query", None), "data", None)
        user_id = getattr(getattr(update, "effective_user", None), "id", None)
        chat_id = getattr(getattr(update, "effective_chat", None), "id", None)
    except Exception:
        cb_data = user_id = chat_id = None
    error = context.error
    exc_info = None
    if isinstance(error, BaseException):
        exc_info = (type(error), error, error.__traceback__)
    logger.error(
        "Unhandled exception in handler: %s (user_id=%s chat_id=%s cb=%s)",
        error,
        user_id,
        chat_id,
        cb_data,
        exc_info=exc_info,
    )

    global _LAST_ERROR_NOTIFY_AT
    now = time.monotonic()
    if now - _LAST_ERROR_NOTIFY_AT < ERROR_NOTIFY_INTERVAL_SEC:
        return
    try:
        admins = authorized_ids(role_filter="admin")
        if not admins:
            return
        err_text = clip_html_message(
            f"⚠️ <b>Необработанная ошибка в боте</b>\n<code>{html_escape(str(error))[:500]}</code>"
        )
        delivered = False
        for aid in admins:
            try:
                await context.bot.send_message(chat_id=aid, text=err_text, parse_mode=ParseMode.HTML)
                delivered = True
            except Exception:
                logger.warning("Не удалось отправить уведомление об ошибке администратору %s", aid)
        if delivered:
            _LAST_ERROR_NOTIFY_AT = now
    except Exception:
        logger.exception("Не удалось уведомить админов об ошибке")


async def _post_shutdown(_app: Application) -> None:
    await close_metrics_client()


async def fallback_text(update, context) -> None:
    msg = update.effective_message
    if not msg:
        return
    if not is_authorized(update):
        await reply_need_auth(update)
        return
    if not is_enabled(update):
        return
    await msg.reply_text("Не понимаю команду. Используйте /menu для меню или /help для подсказок.")


def build_app() -> Application:
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN в app/env.secrets, app/.env или переменных окружения")

    persistence_path = Path(PTB_PERSISTENCE_PATH)
    persistence_dir = persistence_path.parent
    persistence_dir.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):
        persistence_dir.chmod(0o700)
    if persistence_path.exists():
        if not persistence_path.is_file():
            raise RuntimeError(f"PTB_PERSISTENCE_PATH не является файлом: {persistence_path}")
        with contextlib.suppress(OSError):
            persistence_path.chmod(0o600)
    persistence = PicklePersistence(filepath=str(persistence_path))
    app: Application = (
        ApplicationBuilder().token(BOT_TOKEN).persistence(persistence).post_shutdown(_post_shutdown).build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("auth", cmd_auth))
    app.add_handler(CommandHandler("login", cmd_auth))
    app.add_handler(CommandHandler("logout", cmd_logout))
    app.add_handler(CallbackQueryHandler(access_request_cb, pattern=r"^access:request$"))
    app.add_handler(CallbackQueryHandler(access_review_cb, pattern=r"^access:(approve|reject|block):\d+$"))
    app.add_handler(CommandHandler("health", cmd_health, block=False))
    app.add_handler(CommandHandler("subscription", subscription_show))

    maint_conv = _conversation_handler(
        entry_points=[
            CommandHandler("maint", maint_start),
            CallbackQueryHandler(maint_start, pattern=r"^menu:maint$"),
            CallbackQueryHandler(maint_extend_cb, pattern=r"^maint:extend:[0-9a-f]+$"),
        ],
        states={
            STATE_MAINT_MODE: [CallbackQueryHandler(maint_mode, pattern=r"^maint:mode:(announce|schedule)$")],
            STATE_MAINT_SCOPE: [CallbackQueryHandler(maint_scope, pattern=rf"^maint:scope:{SERVER_KEY_PATTERN}$")],
            STATE_MAINT_URGENCY: [CallbackQueryHandler(maint_urgency, pattern=r"^maint:urgency:(urgent|planned)$")],
            STATE_MAINT_DURATION: [MessageHandler(PRIVATE_TEXT, maint_duration)],
            STATE_MAINT_EXTEND: [MessageHandler(PRIVATE_TEXT, maint_extend_duration)],
            STATE_MAINT_SCHEDULE_RANGE: [MessageHandler(PRIVATE_TEXT, maint_schedule_range)],
            STATE_MAINT_SCHEDULE_DATE: [
                CallbackQueryHandler(maint_cal_nav, pattern=r"^maint:cal:nav:\d{4}-\d{2}$"),
                CallbackQueryHandler(maint_cal_day, pattern=r"^maint:cal:day:\d{4}-\d{2}-\d{2}$"),
                CallbackQueryHandler(maint_cal_noop, pattern=r"^maint:cal:noop$"),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(cancel_to_menu_cb, pattern=r"^menu:home$"),
        ],
        name="maint_flow",
        persistent=True,
    )
    app.add_handler(maint_conv)
    app.add_handler(CallbackQueryHandler(maint_end_confirm_cb, pattern=r"^maint:endconfirm:[0-9a-f]+$"))
    app.add_handler(CallbackQueryHandler(maint_cancel_end_cb, pattern=r"^maint:cancelend:[0-9a-f]+$"))
    app.add_handler(CallbackQueryHandler(maint_end_cb, pattern=r"^maint:end:[0-9a-f]+$"))
    app.add_handler(
        CallbackQueryHandler(maint_sched_cancel_confirm_cb, pattern=r"^maint:schedcancelconfirm:[0-9a-f]+$")
    )
    app.add_handler(CallbackQueryHandler(maint_sched_cancel_back_cb, pattern=r"^maint:schedcancelback:[0-9a-f]+$"))
    app.add_handler(CallbackQueryHandler(maint_sched_cancel_cb, pattern=r"^maint:schedcancel:[0-9a-f]+$"))

    _ticket_persistent_eps = [
        CallbackQueryHandler(ticket_list_cb, pattern=r"^ticket:list(?::\d+)?$"),
        CallbackQueryHandler(ticket_open_cb, pattern=r"^ticket:open:\d+$"),
        CallbackQueryHandler(ticket_archive_cb, pattern=r"^ticket:archive$"),
        CallbackQueryHandler(ticket_archive_page_cb, pattern=r"^ticket:archive_page:\d+$"),
        CallbackQueryHandler(ticket_transfer_init_cb, pattern=r"^ticket:transfer_init:\d+$"),
        CallbackQueryHandler(ticket_transfer_to_cb, pattern=r"^ticket:transfer_to:\d+:\d+$"),
    ]
    ticket_conv = _conversation_handler(
        entry_points=[
            CommandHandler("ticket", ticket_start),
            CallbackQueryHandler(ticket_start, pattern=r"^menu:ticket$"),
            CallbackQueryHandler(ticket_admin_reply_start, pattern=r"^ticket:adminreply:\d+$"),
            CallbackQueryHandler(ticket_user_reply_start, pattern=r"^ticket:userreply:\d+$"),
            *_ticket_persistent_eps,
        ],
        states={
            TICKET_SUBJECT: [MessageHandler(PRIVATE_TEXT, ticket_subject)],
            TICKET_URGENCY: [CallbackQueryHandler(ticket_urgency, pattern=r"^ticket:(p1|p2|p3)$")],
            TICKET_TEXT: [MessageHandler(PRIVATE_TICKET_INPUT, ticket_text)],
            TICKET_CONFIRM: [
                CallbackQueryHandler(ticket_confirm, pattern=r"^ticket:(send|edit_subj|edit_text|cancel)$")
            ],
            TICKET_USER_REPLY_TEXT: [MessageHandler(PRIVATE_TICKET_INPUT, ticket_user_reply_text)],
            TICKET_ADMIN_REPLY_TEXT: [MessageHandler(PRIVATE_TICKET_INPUT, ticket_admin_reply_text)],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(cancel_to_menu_cb, pattern=r"^menu:home$"),
            *_ticket_persistent_eps,
        ],
        name="ticket_flow",
        persistent=True,
    )
    app.add_handler(ticket_conv)
    app.add_handler(CallbackQueryHandler(ticket_take_cb, pattern=r"^ticket:take:\d+$"))
    app.add_handler(CallbackQueryHandler(ticket_close_cb, pattern=r"^ticket:close:\d+$"))

    users_conv = _conversation_handler(
        entry_points=[
            CommandHandler("users", users_entry),
            CallbackQueryHandler(users_entry, pattern=r"^menu:users$"),
        ],
        states={
            ADMIN_PICK: [
                CallbackQueryHandler(
                    users_pick,
                    pattern=r"^users:(all|main|back|filter:(all|active|disabled|unpaid|admins)|user:\d+|page:\d+)$",
                ),
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
                    pattern=r"^users:(msg:\d+|nick:\d+|cfg:\d+|subassign:\d+|subsend:\d+|toggle:\d+|toggleapply:\d+|paid:\d+|paidapply:\d+|back)$",
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
        persistent=True,
    )
    app.add_handler(users_conv)
    app.add_handler(CallbackQueryHandler(menu_home_cb, pattern=r"^menu:home$"))
    app.add_handler(CallbackQueryHandler(cmd_help, pattern=r"^menu:help$"))
    app.add_handler(CallbackQueryHandler(cmd_health, pattern=r"^menu:status$", block=False))
    app.add_handler(CallbackQueryHandler(subscription_show, pattern=r"^menu:subscription$"))

    app.add_handler(CallbackQueryHandler(status_pick_cb, pattern=r"^status:pick$", block=False))
    app.add_handler(CallbackQueryHandler(status_show_cb, pattern=rf"^status:show:{SERVER_KEY_PATTERN}$", block=False))
    app.add_handler(CallbackQueryHandler(status_ufw_cb, pattern=rf"^status:ufw:{SERVER_KEY_PATTERN}$", block=False))
    app.add_handler(
        CallbackQueryHandler(status_dns_refresh_cb, pattern=rf"^status:dnsrefresh:{SERVER_KEY_PATTERN}$", block=False)
    )
    if BOT_MODE == "mixed":
        app.add_handler(
            CallbackQueryHandler(
                status_ssh_refresh_confirm_cb,
                pattern=rf"^status:sshrefresh:confirm:{SERVER_KEY_PATTERN}$",
                block=False,
            )
        )
        app.add_handler(
            CallbackQueryHandler(
                status_ssh_refresh_cb,
                pattern=rf"^status:sshrefresh:{SERVER_KEY_PATTERN}$",
                block=False,
            )
        )
        app.add_handler(
            CallbackQueryHandler(
                status_ssh_diag_confirm_cb,
                pattern=rf"^status:sshdiag:confirm:{SERVER_KEY_PATTERN}$",
                block=False,
            )
        )
        app.add_handler(
            CallbackQueryHandler(
                status_ssh_diag_cb,
                pattern=rf"^status:sshdiag:{SERVER_KEY_PATTERN}$",
                block=False,
            )
        )
    app.add_handler(CallbackQueryHandler(dns_back_cb, pattern=rf"^dns:back:{SERVER_KEY_PATTERN}$", block=False))
    app.add_handler(CallbackQueryHandler(docker_list_menu, pattern=rf"^docker:list:{SERVER_KEY_PATTERN}$", block=False))
    app.add_handler(
        CallbackQueryHandler(docker_back_to_status, pattern=rf"^docker:back:{SERVER_KEY_PATTERN}$", block=False)
    )
    app.add_handler(
        CallbackQueryHandler(
            docker_show, pattern=rf"^docker:show:{SERVER_KEY_PATTERN}:[a-zA-Z0-9_.\-]{{1,64}}$", block=False
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            docker_inspect, pattern=rf"^docker:inspect:{SERVER_KEY_PATTERN}:[a-zA-Z0-9_.\-]{{1,64}}$", block=False
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            docker_logs, pattern=rf"^docker:logs:{SERVER_KEY_PATTERN}:[a-zA-Z0-9_.\-]{{1,64}}:\d{{1,4}}$", block=False
        )
    )

    app.add_handler(CommandHandler("fail2ban", fail2ban_menu, block=False))
    app.add_handler(CallbackQueryHandler(f2b_menu_cb, pattern=rf"^f2b:menu:{SERVER_KEY_PATTERN}$", block=False))
    app.add_handler(
        CallbackQueryHandler(f2b_tail_cb, pattern=rf"^f2b:tail:{SERVER_KEY_PATTERN}:\d{{1,5}}$", block=False)
    )
    app.add_handler(CallbackQueryHandler(f2b_digest_cb, pattern=rf"^f2b:digest:{SERVER_KEY_PATTERN}$", block=False))
    app.add_handler(CallbackQueryHandler(f2b_back_cb, pattern=rf"^f2b:back:{SERVER_KEY_PATTERN}$", block=False))

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
        if BOT_MODE == "mixed":
            dns_hh2, dns_mm2 = _parse_schedule_hhmm(
                DAILY_NODE_STATUS_REFRESH_AT,
                field_name="DAILY_NODE_STATUS_REFRESH_AT",
                fallback="12:00",
            )
            app.job_queue.run_daily(
                daily_node_status_refresh,
                time=dtime(hour=dns_hh2, minute=dns_mm2, tzinfo=TZ),
                name="daily_node_status_refresh",
            )
            app.job_queue.run_once(
                daily_node_status_refresh,
                when=DNS_STARTUP_REFRESH_DELAY_SEC + 5,
                name="daily_node_status_startup",
            )
        app.job_queue.run_repeating(
            maint_restart_notify,
            interval=MAINT_RESTART_REMINDER_INTERVAL_SEC,
            first=MAINT_RESTART_NOTIFY_DELAY_SEC,
            name="maint_active_reminder",
        )
        app.job_queue.run_repeating(
            maint_schedule_tick,
            interval=60,
            first=10,
            name="maint_schedule_tick",
        )
        app.job_queue.run_repeating(
            auth_prune_task,
            interval=AUTH_PRUNE_INTERVAL_SEC,
            first=AUTH_PRUNE_INTERVAL_SEC,
            name="auth_prune",
        )
        app.job_queue.run_repeating(
            process_outbox_job,
            interval=OUTBOX_PROCESS_INTERVAL_SEC,
            first=1,
            name="outbox_delivery",
        )
        app.job_queue.run_repeating(
            release_orphaned_tickets,
            interval=60,
            first=3,
            name="ticket_orphan_release",
        )
    else:
        logger.warning("JobQueue недоступен: для ежедневной выжимки установите python-telegram-bot[job-queue].")

    # Fallback должен жить в группе 0 ПОСЛЕ всех разговоров: если активный
    # ConversationHandler уже обработал сообщение, первый сработавший обработчик
    # группы останавливает остальные в этой же группе, и fallback молчит.
    # В отдельной группе (group=10) он срабатывал бы всегда — даже на ввод
    # текста тикета или времени техработ.
    app.add_handler(MessageHandler(PRIVATE_TEXT, fallback_text))
    app.add_error_handler(on_error)
    return app


def run_application(*, instance_lock: SingleInstanceLock | None = None) -> None:
    lock = instance_lock or SingleInstanceLock(INSTANCE_LOCK_PATH)
    owns_lock = instance_lock is None
    if owns_lock:
        try:
            lock.acquire()
        except InstanceAlreadyRunning as exc:
            logger.warning("MaintBot не запущен: %s", exc)
            raise SystemExit(ALREADY_RUNNING_EXIT_CODE) from exc
    try:
        app = build_app()
        logger.info("Bot started", extra={"action": "startup"})
        app.run_polling(drop_pending_updates=False)
    finally:
        if owns_lock:
            lock.release()


def main() -> None:
    run_application()


if __name__ == "__main__":
    main()
