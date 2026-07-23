"""Telegram application assembly and process-level execution."""

from __future__ import annotations

import contextlib
from datetime import timedelta
from pathlib import Path

from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    MessageHandler,
    PicklePersistence,
    TypeHandler,
)

from ..config import (
    BOT_MODE,
    BOT_TOKEN,
    INSTANCE_LOCK_PATH,
    MESSAGE_CLEANUP_ENABLED,
    MESSAGE_RETENTION_HOURS,
    PTB_PERSISTENCE_PATH,
    SERVER_KEY_PATTERN,
    logger,
)
from ..messaging.message_cleanup import MessageCleanupStats, MessageTracker, TrackingExtBot
from ..monitoring.remnawave.client import close_metrics_client
from ..runtime.lock import ALREADY_RUNNING_EXIT_CODE, InstanceAlreadyRunning, SingleInstanceLock
from ..subscriptions.requests.flow_cleanup import abandon_product_flow
from .conversations import reset_navigation_state
from .errors import blocked_user_guard, fallback_text, on_error, unhandled_callback
from .flow_routes import PRIVATE_TEXT
from .jobs import register_jobs
from .routes import register_routes


def _build_persistence(path: str) -> PicklePersistence:
    persistence_path = Path(path)
    persistence_dir = persistence_path.parent
    persistence_dir.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):
        persistence_dir.chmod(0o700)
    if persistence_path.exists():
        if not persistence_path.is_file():
            raise RuntimeError(f"PTB_PERSISTENCE_PATH не является файлом: {persistence_path}")
        with contextlib.suppress(OSError):
            persistence_path.chmod(0o600)
    return PicklePersistence(filepath=str(persistence_path))


def _log_message_cleanup(stats: MessageCleanupStats, *, reason: str) -> None:
    if reason == "startup" or stats.deleted or stats.expired or stats.failed:
        logger.info(
            "Message cleanup (%s): chats=%s candidates=%s deleted=%s expired=%s failed=%s",
            reason,
            stats.tracked_chats,
            stats.candidates,
            stats.deleted,
            stats.expired,
            stats.failed,
            extra={"action": "message_cleanup"},
        )
    if stats.expired:
        logger.warning(
            "Message cleanup skipped %s messages older than Telegram's 48-hour deletion limit",
            stats.expired,
            extra={"action": "message_cleanup_expired"},
        )


async def _post_shutdown(_application: Application) -> None:
    await close_metrics_client()


def build_application(*, bot_mode: str = BOT_MODE) -> Application:
    """Build the complete PTB application without starting network polling."""
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN в app/env.secrets, app/.env или переменных окружения")

    message_tracker = MessageTracker(
        enabled=MESSAGE_CLEANUP_ENABLED,
        retention=timedelta(hours=MESSAGE_RETENTION_HOURS),
    )
    bot = TrackingExtBot(BOT_TOKEN, message_tracker=message_tracker)

    async def post_init(application: Application) -> None:
        message_tracker.bind(application.bot_data)
        if not MESSAGE_CLEANUP_ENABLED:
            return
        try:
            stats = await message_tracker.cleanup(application.bot)
        except Exception:
            logger.exception(
                "Message cleanup startup check failed",
                extra={"action": "message_cleanup_failed"},
            )
        else:
            _log_message_cleanup(stats, reason="startup")

    async def message_cleanup_job(context) -> None:
        try:
            stats = await message_tracker.cleanup(context.bot)
        except Exception:
            logger.exception(
                "Scheduled message cleanup failed",
                extra={"action": "message_cleanup_failed"},
            )
        else:
            _log_message_cleanup(stats, reason="scheduled")

    application: Application = (
        ApplicationBuilder()
        .bot(bot)
        .persistence(_build_persistence(PTB_PERSISTENCE_PATH))
        .post_init(post_init)
        .post_shutdown(_post_shutdown)
        .build()
    )

    conversations = register_routes(
        application,
        bot_mode=bot_mode,
        server_key_pattern=SERVER_KEY_PATTERN,
    )
    register_jobs(
        application,
        bot_mode=bot_mode,
        message_cleanup_job=message_cleanup_job,
    )

    navigation_callbacks = [handler for handler in application.handlers[0] if isinstance(handler, CallbackQueryHandler)]

    async def message_tracking_preprocessor(update: Update, _context) -> None:
        await message_tracker.observe_update(update)

    async def navigation_preprocessor(update: Update, context) -> None:
        await reset_navigation_state(
            update,
            context,
            conversations,
            navigation_callbacks,
            abandon_flow=abandon_product_flow,
        )

    # Blocked accounts are stopped before tracking, navigation and every command.
    application.add_handler(TypeHandler(Update, blocked_user_guard), group=-100)

    # Tracking and stale-flow cleanup run before the regular group-zero handlers.
    application.add_handler(TypeHandler(Update, message_tracking_preprocessor), group=-2)
    application.add_handler(TypeHandler(Update, navigation_preprocessor), group=-1)

    # A catch-all callback acknowledges obsolete buttons; the text fallback stays
    # last in group zero so active conversations retain exclusive message handling.
    application.add_handler(CallbackQueryHandler(unhandled_callback))
    application.add_handler(MessageHandler(PRIVATE_TEXT, fallback_text))
    application.add_error_handler(on_error)
    return application


def run_application(*, instance_lock: SingleInstanceLock | None = None, bot_mode: str = BOT_MODE) -> None:
    """Build and poll while respecting the process-wide single-instance lock."""
    lock = instance_lock or SingleInstanceLock(INSTANCE_LOCK_PATH)
    owns_lock = instance_lock is None
    if owns_lock:
        try:
            lock.acquire()
        except InstanceAlreadyRunning as exc:
            logger.warning("MaintBot не запущен: %s", exc)
            raise SystemExit(ALREADY_RUNNING_EXIT_CODE) from exc
    try:
        application = build_application(bot_mode=bot_mode)
        logger.info("Bot started", extra={"action": "startup"})
        application.run_polling(drop_pending_updates=False)
    finally:
        if owns_lock:
            lock.release()
