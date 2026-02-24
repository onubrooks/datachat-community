"""DataPoint CRUD and sync routes."""

from __future__ import annotations

import inspect
import json
import logging
from pathlib import Path
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, TypeAdapter

from backend.knowledge.conflicts import ConflictMode
from backend.knowledge.contracts import ContractIssue, validate_datapoint_contract
from backend.knowledge.lifecycle import apply_lifecycle_metadata
from backend.models.datapoint import DataPoint
from backend.sync.orchestrator import save_datapoint_to_disk

router = APIRouter()
logger = logging.getLogger(__name__)

DATA_ROOT = Path("datapoints")
DATA_DIR = DATA_ROOT / "managed"
SOURCE_PRIORITY = {
    "user": 4,
    "managed": 3,
    "custom": 2,
    "unknown": 2,
    "demo": 1,
    "example": 1,
}
datapoint_adapter = TypeAdapter(DataPoint)


class SyncStatusResponse(BaseModel):
    status: str
    job_id: str | None
    sync_type: str | None
    started_at: str | None
    finished_at: str | None
    total_datapoints: int
    processed_datapoints: int
    error: str | None


class SyncTriggerResponse(BaseModel):
    job_id: UUID


class SyncTriggerRequest(BaseModel):
    scope: Literal["auto", "global", "database"] = "auto"
    connection_id: str | None = None
    conflict_mode: ConflictMode = "error"


class DataPointSummary(BaseModel):
    datapoint_id: str
    type: str
    name: str | None
    source_tier: str | None = None
    source_path: str | None = None
    lifecycle_version: str | None = None
    lifecycle_reviewer: str | None = None
    lifecycle_changed_by: str | None = None
    lifecycle_changed_reason: str | None = None
    lifecycle_changed_at: str | None = None


class DataPointListResponse(BaseModel):
    datapoints: list[DataPointSummary]


def _get_vector_store():
    from backend.api.main import app_state

    vector_store = app_state.get("vector_store")
    if vector_store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Vector store unavailable",
        )
    return vector_store


def _get_orchestrator():
    from backend.api.main import app_state

    orchestrator = app_state.get("sync_orchestrator")
    if orchestrator is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Sync orchestrator unavailable",
        )
    return orchestrator


def _file_path(datapoint_id: str) -> Path:
    return DATA_DIR / f"{datapoint_id}.json"


async def _resolve_maybe_awaitable(value):
    """Return awaited value when a callable returns a coroutine (tests/mocks)."""
    if inspect.isawaitable(value):
        return await value
    return value


def _read_existing_datapoint(path: Path) -> DataPoint | None:
    """Load existing DataPoint payload for lifecycle version bumps.

    Returns None when the file cannot be parsed/validated so PUT can still
    replace corrupted payloads.
    """
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return datapoint_adapter.validate_python(payload)
    except Exception as exc:  # pragma: no cover - defensive recovery path
        logger.warning(
            "Failed to load existing datapoint for lifecycle metadata; "
            "continuing update without previous lifecycle context",
            extra={"path": str(path), "error": str(exc)},
        )
        return None


def _issue_to_dict(issue: ContractIssue) -> dict[str, str]:
    return {
        "code": issue.code,
        "message": issue.message,
        "severity": issue.severity,
        "field": issue.field or "",
    }


def _validate_datapoint_contract_or_400(datapoint: DataPoint) -> None:
    report = validate_datapoint_contract(datapoint, strict=True)
    if report.is_valid:
        return
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail={
            "message": "DataPoint contract validation failed.",
            "datapoint_id": report.datapoint_id,
            "contract_errors": [_issue_to_dict(issue) for issue in report.errors],
        },
    )


@router.post("/datapoints", status_code=status.HTTP_201_CREATED)
async def create_datapoint(payload: dict) -> dict:
    datapoint = datapoint_adapter.validate_python(payload)
    apply_lifecycle_metadata(datapoint, action="create", changed_by="api")
    _validate_datapoint_contract_or_400(datapoint)
    path = _file_path(datapoint.datapoint_id)
    if path.exists():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Datapoint already exists",
        )
    save_datapoint_to_disk(datapoint.model_dump(mode="json", by_alias=True), path)

    orchestrator = _get_orchestrator()
    orchestrator.enqueue_sync_incremental(
        [datapoint.datapoint_id],
        conflict_mode="prefer_latest",
    )

    return datapoint.model_dump(mode="json", by_alias=True)


@router.put("/datapoints/{datapoint_id}")
async def update_datapoint(datapoint_id: str, payload: dict) -> dict:
    datapoint = datapoint_adapter.validate_python(payload)
    if datapoint.datapoint_id != datapoint_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Datapoint ID mismatch",
        )
    path = _file_path(datapoint_id)
    if not path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Datapoint not found",
        )
    previous = _read_existing_datapoint(path)
    apply_lifecycle_metadata(
        datapoint,
        action="update",
        changed_by="api",
        previous_datapoint=previous,
    )
    _validate_datapoint_contract_or_400(datapoint)
    save_datapoint_to_disk(datapoint.model_dump(mode="json", by_alias=True), path)

    orchestrator = _get_orchestrator()
    orchestrator.enqueue_sync_incremental(
        [datapoint.datapoint_id],
        conflict_mode="prefer_latest",
    )

    return datapoint.model_dump(mode="json", by_alias=True)


@router.delete("/datapoints/{datapoint_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_datapoint(datapoint_id: str) -> None:
    path = _file_path(datapoint_id)
    if not path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Datapoint not found",
        )
    path.unlink(missing_ok=True)

    orchestrator = _get_orchestrator()
    orchestrator.enqueue_sync_incremental([datapoint_id])


@router.post("/sync", response_model=SyncTriggerResponse)
async def trigger_sync(payload: SyncTriggerRequest | None = None) -> SyncTriggerResponse:
    orchestrator = _get_orchestrator()
    scope = payload.scope if payload else "auto"
    connection_id = payload.connection_id if payload else None
    conflict_mode: ConflictMode = payload.conflict_mode if payload else "error"
    if scope == "database" and not connection_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="connection_id is required when scope=database",
        )
    if scope in {"auto", "global"} and connection_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="connection_id is only allowed when scope=database",
        )
    job_id = await _resolve_maybe_awaitable(
        orchestrator.enqueue_sync_all(
            scope=scope,
            connection_id=connection_id,
            conflict_mode=conflict_mode,
        )
    )
    return SyncTriggerResponse(job_id=job_id)


@router.get("/datapoints", response_model=DataPointListResponse)
async def list_datapoints() -> DataPointListResponse:
    vector_store = _get_vector_store()
    try:
        items = await vector_store.list_datapoints(limit=10000)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list datapoints: {exc}",
        ) from exc

    deduped: dict[str, DataPointSummary] = {}
    for item in items:
        datapoint_id = str(item.get("datapoint_id", ""))
        if not datapoint_id:
            continue
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        source_tier_raw = metadata.get("source_tier")
        source_tier = str(source_tier_raw) if source_tier_raw is not None else "unknown"
        source_path_raw = metadata.get("source_path")
        source_path = str(source_path_raw) if source_path_raw else None
        lifecycle_version_raw = metadata.get("lifecycle_version")
        lifecycle_reviewer_raw = metadata.get("lifecycle_reviewer")
        lifecycle_changed_by_raw = metadata.get("lifecycle_changed_by")
        lifecycle_changed_reason_raw = metadata.get("lifecycle_changed_reason")
        lifecycle_changed_at_raw = metadata.get("lifecycle_changed_at")
        summary = DataPointSummary(
            datapoint_id=datapoint_id,
            type=str(metadata.get("type", "Unknown")),
            name=str(metadata["name"]) if metadata.get("name") is not None else None,
            source_tier=source_tier,
            source_path=source_path,
            lifecycle_version=(
                str(lifecycle_version_raw) if lifecycle_version_raw is not None else None
            ),
            lifecycle_reviewer=(
                str(lifecycle_reviewer_raw) if lifecycle_reviewer_raw is not None else None
            ),
            lifecycle_changed_by=(
                str(lifecycle_changed_by_raw) if lifecycle_changed_by_raw is not None else None
            ),
            lifecycle_changed_reason=(
                str(lifecycle_changed_reason_raw)
                if lifecycle_changed_reason_raw is not None
                else None
            ),
            lifecycle_changed_at=(
                str(lifecycle_changed_at_raw) if lifecycle_changed_at_raw is not None else None
            ),
        )
        existing = deduped.get(datapoint_id)
        if existing is None:
            deduped[datapoint_id] = summary
            continue
        existing_priority = SOURCE_PRIORITY.get(existing.source_tier or "unknown", 0)
        candidate_priority = SOURCE_PRIORITY.get(summary.source_tier or "unknown", 0)
        if candidate_priority > existing_priority:
            deduped[datapoint_id] = summary

    datapoints = sorted(
        deduped.values(),
        key=lambda item: (
            -(SOURCE_PRIORITY.get(item.source_tier or "unknown", 0)),
            item.type,
            item.name or item.datapoint_id,
        ),
    )
    return DataPointListResponse(datapoints=datapoints)


@router.get("/datapoints/{datapoint_id}")
async def get_datapoint(datapoint_id: str) -> dict:
    """Fetch a managed datapoint JSON document by ID."""
    path = _file_path(datapoint_id)
    if not path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Datapoint not found",
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        datapoint = datapoint_adapter.validate_python(payload)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to load datapoint {datapoint_id}: {exc}",
        ) from exc
    return datapoint.model_dump(mode="json", by_alias=True)


@router.get("/sync/status", response_model=SyncStatusResponse)
async def get_sync_status() -> SyncStatusResponse:
    orchestrator = _get_orchestrator()
    status_payload = orchestrator.get_status()
    for key in ("started_at", "finished_at"):
        value = status_payload.get(key)
        if value is not None and hasattr(value, "isoformat"):
            status_payload[key] = value.isoformat()
    return SyncStatusResponse(**status_payload)
