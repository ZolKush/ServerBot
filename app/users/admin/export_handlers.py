"""Owner-only handlers for exporting client records."""

from __future__ import annotations

import asyncio
from datetime import datetime
from io import BytesIO

from telegram import InputFile, Update
from telegram.ext import ContextTypes

from ...bot.guards import get_user_id, require_owner
from ...config import TZ
from ...storage import (
    UserData,
    append_audit_entry,
    authorized_users_snapshot,
    get_user_meta_copy,
    service_requests_snapshot,
    update_user_data,
)
from .client_export import build_client_workbook, client_export_filename


@require_owner
async def export_clients_xlsx_cb(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    query = update.callback_query
    chat = update.effective_chat
    actor_id = get_user_id(update)
    actor = get_user_meta_copy(actor_id or 0)
    if query is None or chat is None or actor is None:
        return
    await query.answer("Формирую таблицу…")

    generated_at = datetime.now(TZ)
    users = authorized_users_snapshot()
    requests = service_requests_snapshot()
    content, client_count, request_count = await asyncio.to_thread(
        build_client_workbook,
        users,
        requests,
        generated_at=generated_at,
    )
    filename = client_export_filename(generated_at)
    await context.bot.send_document(
        chat_id=chat.id,
        document=InputFile(BytesIO(content), filename=filename),
        caption=(
            f"Выгрузка клиентов: {client_count}. "
            f"Связанных заявок: {request_count}. "
            f"Сформировано {generated_at:%d.%m.%Y %H:%M}."
        ),
    )

    def _audit(data: UserData) -> None:
        append_audit_entry(
            data,
            action="clients_exported",
            actor_meta=actor,
            details={
                "client_count": client_count,
                "request_count": request_count,
                "filename": filename,
            },
        )

    await update_user_data(_audit)


__all__ = ["export_clients_xlsx_cb"]
