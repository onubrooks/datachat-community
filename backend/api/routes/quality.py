"""Quality finding summary routes backed by persisted AI runs."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

router = APIRouter()


class QualitySeverityBreakdownResponse(BaseModel):
    severity: str
    count: int


class QualityCategoryBreakdownResponse(BaseModel):
    category: str
    count: int


class QualityCodeBreakdownResponse(BaseModel):
    code: str
    severity: str
    category: str
    count: int


class RecentQualityFindingResponse(BaseModel):
    finding_id: str
    run_id: str
    finding_type: str
    severity: str
    category: str
    code: str
    message: str
    entity_type: str | None = None
    entity_id: str | None = None
    details: dict = Field(default_factory=dict)
    created_at: datetime | None = None
    route: str = "unknown"
    query: str | None = None


class QualitySummaryResponse(BaseModel):
    window_hours: int
    total_findings: int
    runs_with_findings: int
    severity_breakdown: list[QualitySeverityBreakdownResponse] = Field(default_factory=list)
    category_breakdown: list[QualityCategoryBreakdownResponse] = Field(default_factory=list)
    code_breakdown: list[QualityCodeBreakdownResponse] = Field(default_factory=list)
    recent_findings: list[RecentQualityFindingResponse] = Field(default_factory=list)


def _get_run_store():
    from backend.api.main import app_state

    store = app_state.get("run_store")
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Run store unavailable",
        )
    return store


@router.get("/quality/summary", response_model=QualitySummaryResponse)
async def quality_summary(window_hours: int = 24) -> QualitySummaryResponse:
    store = _get_run_store()
    payload = await store.summarize_quality(window_hours=window_hours)
    return QualitySummaryResponse(**payload)
