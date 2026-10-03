"""Administrative request list and request-card handlers."""

from __future__ import annotations

import re

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from ...bot.guards import require_admin
from ...bot.ui import clip_text
from ...config import logger
from ...messaging.review_sync import record_review_delivery, review_completion
from ...storage import get_user_meta_copy, service_requests_snapshot
from . import state
from .views import request_card, request_markup, request_status_label, user_nickname

REQUESTS_PAGE_SIZE = 10


@require_admin
async def product_requests_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    match = re.fullmatch(r"product:requests(?::(active|archive):(\d+))?", query.data or "")
    if not match:
        return
    section = match.group(1) or "active"
    archived = section == "archive"
    statuses = state.ARCHIVED_REQUEST_STATUSES if archived else state.ACTIVE_REQUEST_STATUSES
    await query.answer()
    requests = [request for request in service_requests_snapshot().values() if request.get("status") in statuses]
    requests.sort(
        key=lambda item: (
            str(item.get("created_at") or ""),
            int(item.get("id", 0) or 0),
        ),
        reverse=True,
    )
    page_count = max(1, (len(requests) + REQUESTS_PAGE_SIZE - 1) // REQUESTS_PAGE_SIZE)
    page = min(int(match.group(2) or 0), page_count - 1)
    title = "🗃 <b>Архив заявок</b>" if archived else "📥 <b>Заявки</b>"
    count_label = "Завершённых заявок" if archived else "Активных заявок"
    lines = [title, "", f"{count_label}: <b>{len(requests)}</b>"]
    if requests:
        lines.append(f"Страница {page + 1} из {page_count}")
    rows: list[list[InlineKeyboardButton]] = []
    for request in requests[page * REQUESTS_PAGE_SIZE : (page + 1) * REQUESTS_PAGE_SIZE]:
        request_id = int(request.get("id", 0) or 0)
        user_id = int(request.get("user_id", 0) or 0)
        meta = get_user_meta_copy(user_id) or {}
        icon = {
            "trial": "🧪",
            "purchase": "💳",
            "renewal": "🔄",
        }.get(str(request.get("kind")), "📄")
        label = f"{icon} #{request_id} {user_nickname(meta)} · {request_status_label(request.get('status'))}"
        rows.append(
            [
                InlineKeyboardButton(
                    clip_text(label, limit=60),
                    callback_data=f"product:req:view:{request_id}" + (f":archive:{page}" if archived else ""),
                )
            ]
        )
    if not requests:
        lines.extend(["", "Архив заявок пуст." if archived else "Новых заявок нет."])
    navigation = []
    if page > 0:
        navigation.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"product:requests:{section}:{page - 1}"))
    if page + 1 < page_count:
        navigation.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"product:requests:{section}:{page + 1}"))
    if navigation:
        rows.append(navigation)
    rows.append(
        [
            InlineKeyboardButton(
                "📥 Активные заявки" if archived else "🗃 Архив заявок",
                callback_data="product:requests" if archived else "product:requests:archive:0",
            )
        ]
    )
    rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:home")])
    await query.edit_message_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


@require_admin
async def product_request_view_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    if not query:
        return
    match = re.fullmatch(r"product:req:view:(\d+)(?::archive:(\d+))?", query.data or "")
    if not match:
        return
    request = service_requests_snapshot().get(match.group(1))
    if not isinstance(request, dict):
        await query.answer("Заявка не найдена.", show_alert=True)
        return
    archived = request.get("status") in state.ARCHIVED_REQUEST_STATUSES
    if match.group(2) is not None and not archived:
        await query.answer("Заявка не находится в архиве.", show_alert=True)
        return
    await query.answer()
    meta = get_user_meta_copy(int(request.get("user_id", 0) or 0)) or {}
    actor = state.actor_meta(update) or {}
    await query.edit_message_text(
        request_card(request, meta),
        parse_mode=ParseMode.HTML,
        reply_markup=request_markup(
            request,
            actor,
            archive_page=int(match.group(2) or 0),
        ),
    )
    if archived:
        return
    user = update.effective_user
    message = query.message
    generation = str(request.get("created_at") or "")
    if user and message and generation:
        try:
            await record_review_delivery(
                context.bot,
                review_completion(
                    scope="service",
                    target_id=int(request.get("id", 0) or 0),
                    generation=generation,
                ),
                user.id,
                message,
            )
        except Exception:
            logger.exception(
                "Could not register manually opened service request card request_id=%s",
                request.get("id"),
            )


__all__ = ["product_request_view_cb", "product_requests_cb"]
