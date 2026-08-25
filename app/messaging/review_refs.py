"""Persistence helpers for Telegram review-card message references."""

from __future__ import annotations

import copy
from typing import Any

from ..storage import UserData, update_user_data

REVIEW_COMPLETION_TYPE = "review_card"
ReviewDelivery = tuple[str, int, int, int, bool]


def review_completion(*, scope: str, target_id: int, generation: str) -> dict[str, Any]:
    if scope not in {"access", "service"}:
        raise ValueError(f"unsupported review-card scope: {scope}")
    return {
        "type": REVIEW_COMPLETION_TYPE,
        "scope": scope,
        "target_id": int(target_id),
        "generation": str(generation),
    }


def _message_coordinates(message: Any, recipient_id: int) -> tuple[int, int] | None:
    try:
        message_id = int(getattr(message, "message_id", 0) or 0)
        chat_id = int(getattr(message, "chat_id", 0) or 0)
        if not chat_id:
            chat_id = int(getattr(getattr(message, "chat", None), "id", 0) or recipient_id)
    except (TypeError, ValueError, OverflowError):
        return None
    return (chat_id, message_id) if chat_id and message_id > 0 else None


def _append_reference(
    refs: dict[str, Any],
    *,
    admin_id: int,
    chat_id: int,
    message_id: int,
    generation: str,
) -> None:
    admin_refs = refs.get(str(admin_id))
    if not isinstance(admin_refs, list):
        admin_refs = [admin_refs] if isinstance(admin_refs, dict) else []
    admin_refs = [
        ref
        for ref in admin_refs
        if not (
            isinstance(ref, dict)
            and int(ref.get("chat_id", 0) or 0) == chat_id
            and int(ref.get("message_id", 0) or 0) == message_id
        )
    ]
    refs[str(admin_id)] = [
        *admin_refs,
        {
            "chat_id": chat_id,
            "message_id": message_id,
            "generation": generation,
        },
    ][-20:]


async def register_review_reference(
    completion: object,
    recipient_id: int,
    message: Any,
) -> ReviewDelivery | None:
    """Persist one delivered card and return the validated delivery descriptor."""

    if not isinstance(completion, dict) or completion.get("type") != REVIEW_COMPLETION_TYPE:
        return None
    scope = str(completion.get("scope") or "")
    generation = str(completion.get("generation") or "")
    try:
        target_id = int(completion.get("target_id", 0) or 0)
    except (TypeError, ValueError, OverflowError):
        return None
    coordinates = _message_coordinates(message, recipient_id)
    if scope not in {"access", "service"} or target_id <= 0 or not generation or not coordinates:
        return None
    chat_id, message_id = coordinates

    def _record(data: UserData) -> bool:
        if scope == "access":
            current = data.authorized_users.get(str(target_id))
            if not isinstance(current, dict) or str(current.get("access_requested_at") or "") != generation:
                return False
            updated = dict(current)
            raw_refs = updated.get("review_messages")
            refs: dict[str, Any] = copy.deepcopy(raw_refs) if isinstance(raw_refs, dict) else {}
            _append_reference(
                refs,
                admin_id=recipient_id,
                chat_id=chat_id,
                message_id=message_id,
                generation=generation,
            )
            updated["review_messages"] = refs
            data.authorized_users[str(target_id)] = UserData._normalize_user(updated)
            return True

        request = data.service_requests.get(str(target_id))
        if not isinstance(request, dict) or str(request.get("created_at") or "") != generation:
            return False
        updated_request = dict(request)
        raw_refs = updated_request.get("review_messages")
        refs = copy.deepcopy(raw_refs) if isinstance(raw_refs, dict) else {}
        _append_reference(
            refs,
            admin_id=recipient_id,
            chat_id=chat_id,
            message_id=message_id,
            generation=generation,
        )
        updated_request["review_messages"] = refs
        data.service_requests[str(target_id)] = updated_request
        return True

    registered = await update_user_data(_record)
    return scope, target_id, chat_id, message_id, registered


def _without_reference(raw_refs: object, *, chat_id: int, message_id: int) -> tuple[list[Any], bool]:
    refs = raw_refs if isinstance(raw_refs, list) else [raw_refs]
    remaining = [
        ref
        for ref in refs
        if not (
            isinstance(ref, dict)
            and int(ref.get("chat_id", 0) or 0) == chat_id
            and int(ref.get("message_id", 0) or 0) == message_id
        )
    ]
    return remaining, len(remaining) != len(refs)


async def remove_review_reference(
    *,
    scope: str,
    target_id: int,
    admin_id: int,
    chat_id: int,
    message_id: int,
) -> None:
    """Forget a Telegram card only when the stored coordinates still match."""

    def _remove(data: UserData) -> None:
        if scope == "access":
            current = data.authorized_users.get(str(target_id))
            if not isinstance(current, dict):
                return
            updated = dict(current)
            raw_refs = updated.get("review_messages")
            refs: dict[str, Any] = dict(raw_refs) if isinstance(raw_refs, dict) else {}
            remaining, changed = _without_reference(
                refs.get(str(admin_id)),
                chat_id=chat_id,
                message_id=message_id,
            )
            if not changed:
                return
            if remaining:
                refs[str(admin_id)] = remaining
            else:
                refs.pop(str(admin_id), None)
            updated["review_messages"] = refs
            data.authorized_users[str(target_id)] = UserData._normalize_user(updated)
            return

        request = data.service_requests.get(str(target_id))
        if not isinstance(request, dict):
            return
        updated_request = dict(request)
        raw_refs = updated_request.get("review_messages")
        refs = dict(raw_refs) if isinstance(raw_refs, dict) else {}
        remaining, changed = _without_reference(
            refs.get(str(admin_id)),
            chat_id=chat_id,
            message_id=message_id,
        )
        if not changed:
            return
        if remaining:
            refs[str(admin_id)] = remaining
        else:
            refs.pop(str(admin_id), None)
        updated_request["review_messages"] = refs
        data.service_requests[str(target_id)] = updated_request

    await update_user_data(_remove)


async def remove_review_references_for_message(
    *,
    admin_id: int,
    chat_id: int,
    message_id: int,
) -> int:
    """Forget every review reference when its Telegram message becomes another screen."""

    if admin_id <= 0 or not chat_id or message_id <= 0:
        return 0

    def _remove(data: UserData) -> int:
        removed = 0
        admin_key = str(admin_id)
        for user_key, current in list(data.authorized_users.items()):
            if not isinstance(current, dict):
                continue
            raw_refs = current.get("review_messages")
            refs: dict[str, Any] = copy.deepcopy(raw_refs) if isinstance(raw_refs, dict) else {}
            remaining, changed = _without_reference(
                refs.get(admin_key),
                chat_id=chat_id,
                message_id=message_id,
            )
            if not changed:
                continue
            removed += 1
            if remaining:
                refs[admin_key] = remaining
            else:
                refs.pop(admin_key, None)
            data.authorized_users[user_key] = UserData._normalize_user({**current, "review_messages": refs})

        for request_key, request in list(data.service_requests.items()):
            if not isinstance(request, dict):
                continue
            raw_refs = request.get("review_messages")
            refs = copy.deepcopy(raw_refs) if isinstance(raw_refs, dict) else {}
            remaining, changed = _without_reference(
                refs.get(admin_key),
                chat_id=chat_id,
                message_id=message_id,
            )
            if not changed:
                continue
            removed += 1
            if remaining:
                refs[admin_key] = remaining
            else:
                refs.pop(admin_key, None)
            data.service_requests[request_key] = {**request, "review_messages": refs}
        return removed

    return await update_user_data(_remove)


__all__ = [
    "REVIEW_COMPLETION_TYPE",
    "register_review_reference",
    "remove_review_reference",
    "remove_review_references_for_message",
    "review_completion",
]
