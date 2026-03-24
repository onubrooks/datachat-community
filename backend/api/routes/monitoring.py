"""Monitoring rollup routes based on persisted AI runs."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

router = APIRouter()


class RouteBreakdownResponse(BaseModel):
    route: str
    count: int
    success_rate: float
    failed: int


class FailureBreakdownResponse(BaseModel):
    failure_class: str
    count: int


class RecentFailureResponse(BaseModel):
    run_id: str
    route: str
    failure_class: str | None = None
    query: str
    created_at: datetime


class MonitoringSummaryResponse(BaseModel):
    window_hours: int
    total_runs: int
    completed_runs: int
    failed_runs: int
    success_rate: float
    p50_latency_ms: float | None = None
    p95_latency_ms: float | None = None
    clarification_rate: float
    retrieval_miss_rate: float
    route_breakdown: list[RouteBreakdownResponse] = Field(default_factory=list)
    failure_breakdown: list[FailureBreakdownResponse] = Field(default_factory=list)
    recent_failures: list[RecentFailureResponse] = Field(default_factory=list)


def _get_run_store():
    from backend.api.main import app_state

    store = app_state.get("run_store")
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Run store unavailable",
        )
    return store


@router.get("/monitoring/summary", response_model=MonitoringSummaryResponse)
async def monitoring_summary(window_hours: int = 24) -> MonitoringSummaryResponse:
    store = _get_run_store()
    payload = await store.summarize_runs(window_hours=window_hours)
    return MonitoringSummaryResponse(**payload)
