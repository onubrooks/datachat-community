"""Storage utilities for persisted UI conversations."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import asyncpg

from backend.config import get_settings

MAX_CONVERSATION_MESSAGES = 50

_CREATE_CONVERSATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS ui_conversations (
    frontend_session_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    target_database_id TEXT,
    conversation_id TEXT,
    session_summary TEXT,
    session_state JSONB NOT NULL,
    messages JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
"""

_CREATE_CONVERSATIONS_UPDATED_INDEX = """
CREATE INDEX IF NOT EXISTS ui_conversations_updated_at_idx
ON ui_conversations (updated_at DESC);
"""


class ConversationStore:
    """Persist UI conversation snapshots in the system database."""

    def __init__(self, database_url: str | None = None) -> None:
        settings = get_settings()
        self._database_url = database_url or (
            str(settings.system_database.url) if settings.system_database.url else None
        )
        self._pool: asyncpg.Pool | None = None

    async def initialize(self) -> None:
        if self._pool is None:
            if not self._database_url:
                raise ValueError("SYSTEM_DATABASE_URL must be set for conversation storage.")
            dsn = self._normalize_postgres_url(self._database_url)
            self._pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)
        await self._pool.execute(_CREATE_CONVERSATIONS_TABLE)
        await self._pool.execute(_CREATE_CONVERSATIONS_UPDATED_INDEX)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def list_conversations(self, *, limit: int = 20) -> list[dict[str, Any]]:
        self._ensure_pool()
        bounded_limit = max(1, min(limit, 200))
        rows = await self._pool.fetch(
            """
            SELECT
                frontend_session_id,
                title,
                target_database_id,
                conversation_id,
                session_summary,
                session_state,
                messages,
                created_at,
                updated_at
            FROM ui_conversations
            ORDER BY updated_at DESC
            LIMIT $1
            """,
            bounded_limit,
        )
        return [self._row_to_payload(row) for row in rows]

    async def upsert_conversation(
        self,
        *,
        frontend_session_id: str,
        title: str,
        target_database_id: str | None,
        conversation_id: str | None,
        session_summary: str | None,
        session_state: dict[str, Any] | None,
        messages: list[dict[str, Any]],
        updated_at: datetime | None = None,
    ) -> dict[str, Any]:
        self._ensure_pool()
        now = datetime.now(UTC)
        resolved_updated_at = updated_at or now
        trimmed_messages = (messages or [])[-MAX_CONVERSATION_MESSAGES:]
        row = await self._pool.fetchrow(
            """
            INSERT INTO ui_conversations (
                frontend_session_id,
                title,
                target_database_id,
                conversation_id,
                session_summary,
                session_state,
                messages,
                created_at,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9
            )
            ON CONFLICT (frontend_session_id) DO UPDATE SET
                title = EXCLUDED.title,
                target_database_id = EXCLUDED.target_database_id,
                conversation_id = EXCLUDED.conversation_id,
                session_summary = EXCLUDED.session_summary,
                session_state = EXCLUDED.session_state,
                messages = EXCLUDED.messages,
                updated_at = EXCLUDED.updated_at
            WHERE EXCLUDED.updated_at > ui_conversations.updated_at
            RETURNING
                frontend_session_id,
                title,
                target_database_id,
                conversation_id,
                session_summary,
                session_state,
                messages,
                created_at,
                updated_at
            """,
            frontend_session_id,
            title,
            target_database_id,
            conversation_id,
            session_summary,
            json.dumps(session_state or {}),
            json.dumps(trimmed_messages),
            now,
            resolved_updated_at,
        )
        if row is None:
            row = await self._pool.fetchrow(
                """
                SELECT
                    frontend_session_id,
                    title,
                    target_database_id,
                    conversation_id,
                    session_summary,
                    session_state,
                    messages,
                    created_at,
                    updated_at
                FROM ui_conversations
                WHERE frontend_session_id = $1
                """,
                frontend_session_id,
            )
        if row is None:
            raise RuntimeError("Failed to persist conversation snapshot")
        return self._row_to_payload(row)

    async def delete_conversation(self, frontend_session_id: str) -> bool:
        self._ensure_pool()
        result = await self._pool.execute(
            "DELETE FROM ui_conversations WHERE frontend_session_id = $1",
            frontend_session_id,
        )
        try:
            deleted_count = int(str(result).split()[-1])
        except (ValueError, IndexError):
            deleted_count = 0
        return deleted_count > 0

    def _ensure_pool(self) -> None:
        if self._pool is None:
            raise RuntimeError("ConversationStore not initialized")

    @staticmethod
    def _normalize_postgres_url(url: str) -> str:
        if url.startswith("postgresql+asyncpg://"):
            return "postgresql://" + url[len("postgresql+asyncpg://") :]
        return url

    @staticmethod
    def _decode_json_field(value: Any) -> Any:
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return None
        return value

    @classmethod
    def _row_to_payload(cls, row: asyncpg.Record) -> dict[str, Any]:
        return {
            "frontend_session_id": str(row["frontend_session_id"]),
            "title": str(row["title"]),
            "target_database_id": row["target_database_id"],
            "conversation_id": row["conversation_id"],
            "session_summary": row["session_summary"],
            "session_state": cls._decode_json_field(row["session_state"]) or {},
            "messages": cls._decode_json_field(row["messages"]) or [],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
