from __future__ import annotations

import re
from datetime import datetime
from typing import cast

from telegram import Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from ..bot.guards import (
    authorized_ids,
    display_name,
    require_admin,
    require_private,
)
from ..config import TZ, logger
from ..messaging.outbox import message_payload
from ..messaging.review_sync import review_completion, sync_access_review_messages
from ..storage import make_outbox_event
from .operations import AccessReviewAction, review_access, submit_access_request
from .views import (
    ACCESS_NOTIFICATION_TEXTS,
    ACCESS_RESULT_TEXTS,
    ACCESS_REVIEW_RESULT_TEXTS,
    access_request_card,
    access_request_markup,
    access_request_markup_descriptor,
)


@require_private
async def access_request_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    message = update.effective_message
    if query:
        await query.answer()
    user = update.effective_user
    if not user or not message:
        return

    requested_at = datetime.now(TZ)
    admin_ids = authorized_ids(role_filter="admin")
    notification_event = None
    if admin_ids:
        pending_meta: dict[str, object] = {
            "user_id": user.id,
            "username": user.username,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "access_state": "pending",
        }
        notification_event = make_outbox_event(
            kind="access_request",
            recipient_ids=admin_ids,
            payload=message_payload(
                access_request_card(pending_meta),
                reply_markup=access_request_markup_descriptor(user.id),
            ),
            completion=review_completion(
                scope="access",
                target_id=user.id,
                generation=requested_at.isoformat(),
            ),
        )

    result = await submit_access_request(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
        requested_at=requested_at,
        notification_event=notification_event,
    )
    suffix = (
        "\n\nСейчас нет активного администратора; заявка сохранена." if result == "created" and not admin_ids else ""
    )
    text = ACCESS_RESULT_TEXTS[result] + suffix
    if query:
        await query.edit_message_text(text)
    else:
        await message.reply_text(text)


@require_admin
async def access_review_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    actor = update.effective_user
    if not query or not actor:
        return
    match = re.fullmatch(r"access:(approve|reject|block):(\d+)", query.data or "")
    if not match:
        return
    action_text, user_id_text = match.groups()
    action = cast(AccessReviewAction, action_text)
    target_user_id = int(user_id_text)
    if target_user_id == actor.id:
        await query.answer("Нельзя изменить собственный доступ этой кнопкой.", show_alert=True)
        return

    reviewed_at = datetime.now(TZ)
    actor_name = display_name(update)
    desired = {
        "approve": "approved",
        "reject": "rejected",
        "block": "blocked",
    }[action]
    notification_event = make_outbox_event(
        kind=f"access_{desired}",
        recipient_ids=[target_user_id],
        payload=message_payload(
            ACCESS_NOTIFICATION_TEXTS[desired],
            parse_mode=None,
        ),
        allow_blocked_delivery=desired == "blocked",
    )
    outcome, _meta = await review_access(
        actor_id=actor.id,
        actor_name=actor_name,
        target_user_id=target_user_id,
        action=action,
        reviewed_at=reviewed_at,
        notification_event=notification_event,
    )
    if outcome == "updated":
        logger.info(
            "Access request %s target_uid=%s by admin=%s",
            action,
            target_user_id,
            actor.id,
            extra={"user_id": actor.id, "action": f"access_{action}"},
        )
        await query.answer("Решение сохранено.")
        if getattr(context, "bot", None) is not None:
            await sync_access_review_messages(context.bot, target_user_id)
        if isinstance(_meta, dict):
            try:
                await query.edit_message_text(
                    access_request_card(_meta),
                    parse_mode=ParseMode.HTML,
                    reply_markup=access_request_markup(_meta),
                )
            except BadRequest as exc:
                if "message is not modified" not in str(exc).lower():
                    logger.warning(
                        "Could not refresh clicked access card target_uid=%s: %s",
                        target_user_id,
                        exc,
                        extra={"user_id": actor.id, "action": "access_card_refresh"},
                    )
        return
    await query.answer(
        ACCESS_REVIEW_RESULT_TEXTS.get(outcome, "Заявка уже обработана."),
        show_alert=True,
    )
    if isinstance(_meta, dict):
        if getattr(context, "bot", None) is not None:
            await sync_access_review_messages(context.bot, target_user_id)
        try:
            await query.edit_message_text(
                access_request_card(_meta),
                parse_mode=ParseMode.HTML,
                reply_markup=access_request_markup(_meta),
            )
        except BadRequest as exc:
            if "message is not modified" not in str(exc).lower():
                logger.warning(
                    "Could not refresh stale access card target_uid=%s: %s",
                    target_user_id,
                    exc,
                    extra={"user_id": actor.id, "action": "access_card_refresh"},
                )


__all__ = [
    "access_request_cb",
    "access_review_cb",
]
