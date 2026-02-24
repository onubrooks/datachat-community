"""Storage utilities for UI feedback and issue reports."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import asyncpg

from backend.config import get_settings

_CREATE_FEEDBACK_TABLE = """
CREATE TABLE IF NOT EXISTS ui_feedback (
    feedback_id UUID PRIMARY KEY,
    category TEXT NOT NULL,
    sentiment TEXT,
    message TEXT,
    context JSONB NOT NULL,
    conversation_id TEXT,
    message_id TEXT,
    target_database_id TEXT,
    answer_source TEXT,
    answer_confidence DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL
);
"""

_CREATE_FEEDBACK_CREATED_INDEX = """
CREATE INDEX IF NOT EXISTS ui_feedback_created_at_idx
ON ui_feedback (created_at DESC);
"""


class FeedbackStore:
    """Persist UI feedback entries in the system database."""

    def __init__(self, database_url: str | None = None) -> None:
        settings = get_settings()
        self._database_url = database_url or (
            str(settings.system_database.url) if settings.system_database.url else None
        )
        self._pool: asyncpg.Pool | None = None

    async def initialize(self) -> None:
        if self._pool is None:
            if not self._database_url:
                raise ValueError("SYSTEM_DATABASE_URL must be set for feedback storage.")
            dsn = self._normalize_postgres_url(self._database_url)
            self._pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)
        await self._pool.execute(_CREATE_FEEDBACK_TABLE)
        await self._pool.execute(_CREATE_FEEDBACK_CREATED_INDEX)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def create_feedback(
        self,
        *,
        category: str,
        sentiment: str | None,
        message: str | None,
        context: dict,
        conversation_id: str | None,
        message_id: str | None,
        target_database_id: str | None,
        answer_source: str | None,
        answer_confidence: float | None,
    ) -> tuple[UUID, datetime]:
        self._ensure_pool()
        feedback_id = uuid4()
        created_at = datetime.now(UTC)
        await self._pool.execute(
            """
            INSERT INTO ui_feedback (
                feedback_id,
                category,
                sentiment,
                message,
                context,
                conversation_id,
                message_id,
                target_database_id,
                answer_source,
                answer_confidence,
                created_at
            ) VALUES (
                $1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10, $11
            )
            """,
            feedback_id,
            category,
            sentiment,
            message,
            json.dumps(context or {}),
            conversation_id,
            message_id,
            target_database_id,
            answer_source,
            answer_confidence,
            created_at,
        )
        return feedback_id, created_at

    async def get_summary(self, *, days: int = 30) -> dict:
        self._ensure_pool()
        cutoff = datetime.now(UTC) - timedelta(days=max(days, 1))
        rows = await self._pool.fetch(
            """
            SELECT category, sentiment, COUNT(*) AS count
            FROM ui_feedback
            WHERE created_at >= $1
            GROUP BY category, sentiment
            ORDER BY category, sentiment
            """,
            cutoff,
        )
        return {
            "window_days": max(days, 1),
            "totals": [
                {
                    "category": row["category"],
                    "sentiment": row["sentiment"],
                    "count": int(row["count"]),
                }
                for row in rows
            ],
        }

    def _ensure_pool(self) -> None:
        if self._pool is None:
            raise RuntimeError("FeedbackStore not initialized")

    @staticmethod
    def _normalize_postgres_url(url: str) -> str:
        if url.startswith("postgresql+asyncpg://"):
            return "postgresql://" + url[len("postgresql+asyncpg://") :]
        return url
