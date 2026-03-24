"""Run history inspection routes."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

router = APIRouter()


class RunStepResponse(BaseModel):
    step_id: str
    step_order: int
    step_name: str
    status: str
    latency_ms: float | None = None
    summary: dict = Field(default_factory=dict)
    created_at: datetime | None = None


class RunSummaryResponse(BaseModel):
    run_id: str
    run_type: str
    status: str
    route: str | None = None
    connection_id: str | None = None
    conversation_id: str | None = None
    correlation_id: str | None = None
    failure_class: str | None = None
    confidence: float | None = None
    warning_count: int = 0
    error_count: int = 0
    latency_ms: float | None = None
    summary: dict = Field(default_factory=dict)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class QualityFindingResponse(BaseModel):
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


class RunDetailResponse(RunSummaryResponse):
    output: dict = Field(default_factory=dict)
    steps: list[RunStepResponse] = Field(default_factory=list)
    quality_findings: list[QualityFindingResponse] = Field(default_factory=list)


class RunListResponse(BaseModel):
    runs: list[RunSummaryResponse] = Field(default_factory=list)


def _get_run_store():
    from backend.api.main import app_state

    store = app_state.get("run_store")
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Run store unavailable",
        )
    return store


@router.get("/runs", response_model=RunListResponse)
async def list_runs(limit: int = 50) -> RunListResponse:
    store = _get_run_store()
    runs = await store.list_runs(limit=limit)
    return RunListResponse(runs=runs)


@router.get("/runs/{run_id}", response_model=RunDetailResponse)
async def get_run(run_id: UUID) -> RunDetailResponse:
    store = _get_run_store()
    payload = await store.get_run(run_id)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Run not found",
        )
    return RunDetailResponse(**payload)
