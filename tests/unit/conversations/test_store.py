"""Unit tests for conversation persistence store behavior."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from backend.conversations.store import ConversationStore


@pytest.mark.asyncio
async def test_upsert_uses_monotonic_updated_at_guard() -> None:
    """Stale in-flight upserts should not overwrite newer snapshots."""
    store = ConversationStore(database_url="postgresql://example")
    stale_existing_row = {
        "frontend_session_id": "session-1",
        "title": "Newest snapshot",
        "target_database_id": "db_live",
        "conversation_id": "conv_live",
        "session_summary": "latest",
        "session_state": {"step": "latest"},
        "messages": [{"role": "user", "content": "latest message"}],
        "created_at": datetime(2026, 2, 20, 10, 0, tzinfo=UTC),
        "updated_at": datetime(2026, 2, 20, 10, 5, tzinfo=UTC),
    }
    pool = AsyncMock()
    pool.fetchrow = AsyncMock(side_effect=[None, stale_existing_row])
    store._pool = pool

    result = await store.upsert_conversation(
        frontend_session_id="session-1",
        title="Older snapshot",
        target_database_id="db_old",
        conversation_id="conv_old",
        session_summary="older",
        session_state={"step": "older"},
        messages=[{"role": "user", "content": "older message"}],
        updated_at=datetime(2026, 2, 20, 10, 1, tzinfo=UTC),
    )

    assert result["title"] == "Newest snapshot"
    assert pool.fetchrow.await_count == 2
    first_sql = pool.fetchrow.await_args_list[0].args[0]
    assert "WHERE EXCLUDED.updated_at > ui_conversations.updated_at" in first_sql
    second_sql = pool.fetchrow.await_args_list[1].args[0]
    assert "FROM ui_conversations" in second_sql
    assert "WHERE frontend_session_id = $1" in second_sql


@pytest.mark.asyncio
async def test_upsert_raises_when_no_row_available() -> None:
    """Store should raise when both upsert and fallback read produce no row."""
    store = ConversationStore(database_url="postgresql://example")
    pool = AsyncMock()
    pool.fetchrow = AsyncMock(side_effect=[None, None])
    store._pool = pool

    with pytest.raises(RuntimeError, match="Failed to persist conversation snapshot"):
        await store.upsert_conversation(
            frontend_session_id="session-missing",
            title="Snapshot",
            target_database_id=None,
            conversation_id=None,
            session_summary=None,
            session_state={},
            messages=[],
            updated_at=datetime(2026, 2, 20, 10, 1, tzinfo=UTC),
        )
