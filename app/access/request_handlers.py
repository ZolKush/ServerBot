from __future__ import annotations

import re
from datetime import datetime
from typing import cast

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ..bot.guards import (
    authorized_ids,
    display_name,
    require_admin,
    require_private,
    staff_title,
)
from ..bot.ui import html_escape
from ..config import TZ, logger
from ..messaging.outbox import message_payload
from ..storage import make_outbox_event
from .operations import AccessReviewAction, review_access, submit_access_request
from .views import (
    ACCESS_DECISION_LABELS,
    ACCESS_NOTIFICATION_TEXTS,
    ACCESS_RESULT_TEXTS,
    ACCESS_REVIEW_RESULT_TEXTS,
    access_request_markup_descriptor,
    post_rejection_markup,
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
    display = (
        f"@{user.username}" if user.username else " ".join(part for part in (user.first_name, user.last_name) if part)
    )
    display = display or str(user.id)
    notification_event = None
    if admin_ids:
        notification_event = make_outbox_event(
            kind="access_request",
            recipient_ids=admin_ids,
            payload=message_payload(
                "🔐 <b>Новая заявка на доступ</b>\n\n"
                f"Пользователь: <b>{html_escape(display)}</b>\n"
                f"ID: <code>{user.id}</code>",
                reply_markup=access_request_markup_descriptor(user.id),
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
    actor_public = staff_title(update)
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
        original_text = str(getattr(query.message, "text_html", "") or "Заявка")
        await query.edit_message_text(
            original_text + f"\n\n<b>Решение:</b> {ACCESS_DECISION_LABELS[action]} · {html_escape(actor_public)}",
            parse_mode=ParseMode.HTML,
            reply_markup=post_rejection_markup(target_user_id) if action == "reject" else None,
        )
        return
    await query.answer(
        ACCESS_REVIEW_RESULT_TEXTS.get(outcome, "Заявка уже обработана."),
        show_alert=True,
    )


__all__ = [
    "access_request_cb",
    "access_review_cb",
]
