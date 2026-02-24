"""Routes for persisted UI conversation history."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from fastapi import APIRouter

from backend.conversations import MAX_CONVERSATION_MESSAGES
from backend.models.api import (
    ConversationDeleteResponse,
    ConversationSnapshotPayload,
    ConversationUpsertRequest,
)

logger = logging.getLogger(__name__)
router = APIRouter()


def _trim_messages(messages: list[dict] | None) -> list[dict]:
    return list(messages or [])[-MAX_CONVERSATION_MESSAGES:]


def _get_conversation_store():
    from backend.api.main import app_state

    return app_state.get("conversation_store")


@router.get("/conversations", response_model=list[ConversationSnapshotPayload])
async def list_conversations(limit: int = 20) -> list[ConversationSnapshotPayload]:
    """List recently saved UI conversation snapshots."""
    store = _get_conversation_store()
    if store is None:
        return []
    rows = await store.list_conversations(limit=limit)
    return [ConversationSnapshotPayload(**row) for row in rows]


@router.put(
    "/conversations/{frontend_session_id}",
    response_model=ConversationSnapshotPayload,
)
async def upsert_conversation(
    frontend_session_id: str,
    payload: ConversationUpsertRequest,
) -> ConversationSnapshotPayload:
    """Create/update one UI conversation snapshot keyed by frontend session id."""
    store = _get_conversation_store()
    if store is None:
        now = datetime.now(UTC)
        logger.info(
            "conversation_snapshot_logs_only",
            extra={
                "frontend_session_id": frontend_session_id,
                "title": payload.title,
                "target_database_id": payload.target_database_id,
                "message_count": len(payload.messages or []),
            },
        )
        return ConversationSnapshotPayload(
            frontend_session_id=frontend_session_id,
            title=payload.title,
            target_database_id=payload.target_database_id,
            conversation_id=payload.conversation_id,
            session_summary=payload.session_summary,
            session_state=payload.session_state or {},
            messages=_trim_messages(payload.messages),
            created_at=now,
            updated_at=payload.updated_at or now,
        )

    saved = await store.upsert_conversation(
        frontend_session_id=frontend_session_id,
        title=payload.title,
        target_database_id=payload.target_database_id,
        conversation_id=payload.conversation_id,
        session_summary=payload.session_summary,
        session_state=payload.session_state or {},
        messages=_trim_messages(payload.messages),
        updated_at=payload.updated_at,
    )
    return ConversationSnapshotPayload(**saved)


@router.delete(
    "/conversations/{frontend_session_id}",
    response_model=ConversationDeleteResponse,
)
async def delete_conversation(frontend_session_id: str) -> ConversationDeleteResponse:
    """Delete one saved conversation snapshot by frontend session id."""
    store = _get_conversation_store()
    if store is None:
        return ConversationDeleteResponse(ok=True, deleted=False)
    deleted = await store.delete_conversation(frontend_session_id)
    return ConversationDeleteResponse(ok=True, deleted=deleted)
