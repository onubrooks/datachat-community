"""Feedback routes for answer quality and issue reporting."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from uuid import uuid4

from fastapi import APIRouter

from backend.models.api import (
    FeedbackSubmitRequest,
    FeedbackSubmitResponse,
    FeedbackSummaryResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter()


def _get_feedback_store():
    from backend.api.main import app_state

    return app_state.get("feedback_store")


@router.post("/feedback", response_model=FeedbackSubmitResponse)
async def submit_feedback(payload: FeedbackSubmitRequest) -> FeedbackSubmitResponse:
    """Persist structured feedback for later triage and model/retrieval tuning."""
    feedback_store = _get_feedback_store()
    context = {
        "query": payload.query,
        "answer": payload.answer,
        "sql": payload.sql,
        "sources": payload.sources or [],
        "metadata": payload.metadata or {},
    }

    if feedback_store is None:
        fallback_id = str(uuid4())
        logger.info(
            "feedback_submission_logs_only",
            extra={
                "feedback_id": fallback_id,
                "category": payload.category,
                "sentiment": payload.sentiment,
                "conversation_id": payload.conversation_id,
                "message_id": payload.message_id,
                "target_database_id": payload.target_database_id,
                "answer_source": payload.answer_source,
                "answer_confidence": payload.answer_confidence,
                "feedback_message": payload.message,
                "context": context,
            },
        )
        return FeedbackSubmitResponse(
            ok=True,
            feedback_id=fallback_id,
            saved_to="logs_only",
            created_at=datetime.now(UTC).isoformat(),
        )

    feedback_id, created_at = await feedback_store.create_feedback(
        category=payload.category,
        sentiment=payload.sentiment,
        message=payload.message,
        context=context,
        conversation_id=payload.conversation_id,
        message_id=payload.message_id,
        target_database_id=payload.target_database_id,
        answer_source=payload.answer_source,
        answer_confidence=payload.answer_confidence,
    )
    return FeedbackSubmitResponse(
        ok=True,
        feedback_id=str(feedback_id),
        saved_to="system_database",
        created_at=created_at.isoformat(),
    )


@router.get("/feedback/summary", response_model=FeedbackSummaryResponse)
async def get_feedback_summary(days: int = 30) -> FeedbackSummaryResponse:
    """Return aggregate feedback counts for an ops window."""
    feedback_store = _get_feedback_store()
    if feedback_store is None:
        return FeedbackSummaryResponse(window_days=max(days, 1), totals=[])
    summary = await feedback_store.get_summary(days=days)
    return FeedbackSummaryResponse(**summary)
