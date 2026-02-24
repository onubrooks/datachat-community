"""Profiling and DataPoint generation routes."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field, TypeAdapter, ValidationError

from backend.database.manager import DatabaseConnectionManager
from backend.knowledge.contracts import ContractIssue, validate_datapoint_contract
from backend.profiling.generator import DataPointGenerator
from backend.profiling.models import GenerationProgress, PendingDataPoint, ProfilingProgress
from backend.profiling.profiler import SchemaProfiler
from backend.profiling.store import ProfilingStore

router = APIRouter()


class ProfilingRequest(BaseModel):
    sample_size: int = Field(default=100, gt=0, le=1000)
    max_tables: int | None = Field(default=50, ge=1, le=500)
    max_columns_per_table: int = Field(default=100, ge=1, le=500)
    query_timeout_seconds: int = Field(default=5, ge=1, le=60)
    per_table_timeout_seconds: int = Field(default=20, ge=1, le=300)
    total_timeout_seconds: int = Field(default=180, ge=10, le=1800)
    fail_fast: bool = False
    tables: list[str] | None = None


class ProfilingJobResponse(BaseModel):
    job_id: UUID
    connection_id: UUID
    status: str
    progress: ProfilingProgress | None = None
    error: str | None = None
    profile_id: UUID | None = None


class GenerateDataPointsRequest(BaseModel):
    profile_id: UUID
    tables: list[str] | None = None
    depth: str = Field(default="metrics_basic")
    batch_size: int = Field(default=10, ge=1, le=50)
    max_tables: int | None = Field(default=None, ge=1, le=500)
    max_metrics_per_table: int = Field(default=3, ge=1, le=10)
    replace_existing: bool = Field(default=True)


class GenerationJobResponse(BaseModel):
    job_id: UUID
    profile_id: UUID
    status: str
    progress: GenerationProgress | None = None
    error: str | None = None


class PendingDataPointResponse(BaseModel):
    pending_id: UUID
    profile_id: UUID
    datapoint: dict
    confidence: float
    status: str
    review_note: str | None = None


class PendingDataPointListResponse(BaseModel):
    pending: list[PendingDataPointResponse]


class ProfileTablesResponse(BaseModel):
    profile_id: UUID
    tables: list[str]


class ReviewNoteRequest(BaseModel):
    review_note: str | None = None
    datapoint: dict | None = None


DATA_DIR = Path("datapoints") / "managed"


def _datapoint_path(datapoint_id: str) -> Path:
    return DATA_DIR / f"{datapoint_id}.json"


def _normalize_table_name(name: str) -> str:
    return name.strip().lower()


def _extract_table_keys(datapoint) -> list[str]:
    if datapoint.type == "Schema":
        return [_normalize_table_name(datapoint.table_name)]
    if datapoint.type == "Business" and datapoint.related_tables:
        return [_normalize_table_name(table) for table in datapoint.related_tables]
    return []


def _remove_existing_datapoints_for_table(
    table_key: str, exclude_ids: set[str]
) -> list[str]:
    if not DATA_DIR.exists():
        return []
    removed_ids: list[str] = []
    for path in DATA_DIR.glob("*.json"):
        try:
            with path.open() as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue
        datapoint_id = str(payload.get("datapoint_id", path.stem))
        if datapoint_id in exclude_ids:
            continue
        datapoint_type = payload.get("type")
        if datapoint_type == "Schema":
            table_name = payload.get("table_name")
            if table_name and _normalize_table_name(table_name) == table_key:
                path.unlink(missing_ok=True)
                removed_ids.append(datapoint_id)
        elif datapoint_type == "Business":
            related_tables = payload.get("related_tables") or []
            if any(_normalize_table_name(table) == table_key for table in related_tables):
                path.unlink(missing_ok=True)
                removed_ids.append(datapoint_id)
    return removed_ids


def _get_store() -> ProfilingStore:
    from backend.api.main import app_state

    store = app_state.get("profiling_store")
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Profiling store unavailable",
        )
    return store


def _get_manager() -> DatabaseConnectionManager:
    from backend.api.main import app_state

    manager = app_state.get("database_manager")
    if manager is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database registry unavailable",
        )
    return manager


def _get_vector_store():
    from backend.api.main import app_state

    store = app_state.get("vector_store")
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Vector store unavailable",
        )
    return store


def _get_knowledge_graph():
    from backend.api.main import app_state

    graph = app_state.get("knowledge_graph")
    if graph is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Knowledge graph unavailable",
        )
    return graph


def _to_pending_response(pending: PendingDataPoint) -> PendingDataPointResponse:
    return PendingDataPointResponse(
        pending_id=pending.pending_id,
        profile_id=pending.profile_id,
        datapoint=pending.datapoint,
        confidence=pending.confidence,
        status=pending.status,
        review_note=pending.review_note,
    )


def _issue_to_dict(issue: ContractIssue) -> dict[str, str]:
    return {
        "code": issue.code,
        "message": issue.message,
        "severity": issue.severity,
        "field": issue.field or "",
    }


def _validate_datapoint_contract_or_400(datapoint) -> None:
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


def _attach_connection_metadata(datapoint, connection_id: UUID) -> None:
    metadata = datapoint.metadata if isinstance(datapoint.metadata, dict) else {}
    metadata["connection_id"] = str(connection_id)
    datapoint.metadata = metadata


def _select_generation_tables(
    profile_tables: list,
    requested: list[str] | None,
    max_tables: int | None,
) -> list[str]:
    selection = profile_tables
    if requested:
        requested_set = {name.lower() for name in requested}
        selection = [table for table in profile_tables if table.name.lower() in requested_set]
    selection = sorted(selection, key=lambda item: item.row_count or 0, reverse=True)
    if max_tables is not None:
        selection = selection[:max_tables]
    return [table.name for table in selection]


@router.post(
    "/databases/{connection_id}/profile",
    response_model=ProfilingJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def start_profiling_job(
    connection_id: UUID, payload: ProfilingRequest
) -> ProfilingJobResponse:
    store = _get_store()
    manager = _get_manager()

    job = await store.create_job(connection_id)

    async def run_job() -> None:
        profiler = SchemaProfiler(manager)

        async def progress_callback(
            total: int,
            completed: int,
            failed: int = 0,
            skipped: int = 0,
        ) -> None:
            await store.update_job(
                job.job_id,
                progress=ProfilingProgress(
                    total_tables=total,
                    tables_completed=completed,
                    tables_failed=failed,
                    tables_skipped=skipped,
                ),
            )

        try:
            await store.update_job(job.job_id, status="running")
            profile = await profiler.profile_database(
                str(connection_id),
                sample_size=payload.sample_size,
                tables=payload.tables,
                progress_callback=progress_callback,
                max_tables=payload.max_tables,
                max_columns_per_table=payload.max_columns_per_table,
                query_timeout_seconds=payload.query_timeout_seconds,
                per_table_timeout_seconds=payload.per_table_timeout_seconds,
                total_timeout_seconds=payload.total_timeout_seconds,
                fail_fast=payload.fail_fast,
            )
            await store.save_profile(profile)
            await store.update_job(
                job.job_id,
                status="completed",
                profile_id=profile.profile_id,
                progress=ProfilingProgress(
                    total_tables=profile.total_tables_discovered,
                    tables_completed=profile.tables_profiled,
                    tables_failed=profile.tables_failed,
                    tables_skipped=profile.tables_skipped,
                ),
            )
        except Exception as exc:
            await store.update_job(job.job_id, status="failed", error=str(exc))

    asyncio.create_task(run_job())

    return ProfilingJobResponse(
        job_id=job.job_id,
        connection_id=job.connection_id,
        status=job.status,
        progress=job.progress,
        error=job.error,
        profile_id=job.profile_id,
    )


@router.get("/profiling/jobs/{job_id}", response_model=ProfilingJobResponse)
async def get_profiling_job(job_id: UUID) -> ProfilingJobResponse:
    store = _get_store()
    try:
        job = await store.get_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return ProfilingJobResponse(
        job_id=job.job_id,
        connection_id=job.connection_id,
        status=job.status,
        progress=job.progress,
        error=job.error,
        profile_id=job.profile_id,
    )


@router.get(
    "/profiling/jobs/connection/{connection_id}/latest",
    response_model=ProfilingJobResponse | None,
)
async def get_latest_profiling_job(connection_id: UUID) -> ProfilingJobResponse | None:
    store = _get_store()
    job = await store.get_latest_job_for_connection(connection_id)
    if job is None:
        return None
    return ProfilingJobResponse(
        job_id=job.job_id,
        connection_id=job.connection_id,
        status=job.status,
        progress=job.progress,
        error=job.error,
        profile_id=job.profile_id,
    )


@router.post("/datapoints/generate", response_model=GenerationJobResponse)
async def generate_datapoints(payload: GenerateDataPointsRequest) -> GenerationJobResponse:
    store = _get_store()
    profile = await store.get_profile(payload.profile_id)

    job = await store.create_generation_job(profile.profile_id)

    async def run_generation() -> None:
        generator = DataPointGenerator()
        try:
            selected_tables = _select_generation_tables(
                profile.tables, payload.tables, payload.max_tables
            )
            if payload.depth == "schema_only":
                total_tables = len(selected_tables)
            else:
                eligible_tables = [
                    table
                    for table in profile.tables
                    if table.name in selected_tables
                    and any(
                        token in col.data_type.lower()
                        for col in table.columns
                        for token in ["int", "numeric", "decimal", "float"]
                    )
                ]
                total_tables = len(eligible_tables)
            await store.update_generation_job(
                job.job_id,
                status="running",
                progress=GenerationProgress(
                    total_tables=total_tables,
                    tables_completed=0,
                    batch_size=payload.batch_size,
                ),
            )
            async def progress_callback(total: int, completed: int) -> None:
                await store.update_generation_job(
                    job.job_id,
                    progress=GenerationProgress(
                        total_tables=total,
                        tables_completed=completed,
                        batch_size=payload.batch_size,
                    ),
                )
            generated = await generator.generate_from_profile(
                profile,
                tables=selected_tables,
                depth=payload.depth,
                batch_size=payload.batch_size,
                max_tables=payload.max_tables,
                max_metrics_per_table=payload.max_metrics_per_table,
                progress_callback=progress_callback,
            )

            if payload.replace_existing:
                await store.delete_pending_for_profile(profile.profile_id)

            pending_items = [
                PendingDataPoint(
                    profile_id=profile.profile_id,
                    datapoint=item.datapoint,
                    confidence=item.confidence,
                )
                for item in generated.schema_datapoints + generated.business_datapoints
            ]

            await store.add_pending_datapoints(profile.profile_id, pending_items)
            await store.update_generation_job(
                job.job_id,
                status="completed",
                progress=GenerationProgress(
                    total_tables=total_tables,
                    tables_completed=total_tables,
                    batch_size=payload.batch_size,
                ),
            )
        except Exception as exc:
            await store.update_generation_job(
                job.job_id, status="failed", error=str(exc)
            )

    asyncio.create_task(run_generation())

    return GenerationJobResponse(
        job_id=job.job_id,
        profile_id=job.profile_id,
        status=job.status,
        progress=job.progress,
        error=job.error,
    )


@router.get("/datapoints/generate/jobs/{job_id}", response_model=GenerationJobResponse)
async def get_generation_job(job_id: UUID) -> GenerationJobResponse:
    store = _get_store()
    try:
        job = await store.get_generation_job(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return GenerationJobResponse(
        job_id=job.job_id,
        profile_id=job.profile_id,
        status=job.status,
        progress=job.progress,
        error=job.error,
    )


@router.get(
    "/datapoints/generate/profiles/{profile_id}",
    response_model=GenerationJobResponse | None,
)
async def get_latest_generation_job(profile_id: UUID) -> GenerationJobResponse | None:
    store = _get_store()
    job = await store.get_latest_generation_job(profile_id)
    if job is None:
        return None
    return GenerationJobResponse(
        job_id=job.job_id,
        profile_id=job.profile_id,
        status=job.status,
        progress=job.progress,
        error=job.error,
    )


@router.get("/profiling/profiles/{profile_id}/tables", response_model=ProfileTablesResponse)
async def list_profile_tables(profile_id: UUID) -> ProfileTablesResponse:
    store = _get_store()
    profile = await store.get_profile(profile_id)
    return ProfileTablesResponse(
        profile_id=profile.profile_id,
        tables=[
            table.name
            for table in sorted(
                profile.tables, key=lambda item: item.row_count or 0, reverse=True
            )
        ],
    )


@router.get("/datapoints/pending", response_model=PendingDataPointListResponse)
async def list_pending_datapoints(
    status_filter: str = "pending",
    connection_id: UUID | None = None,
) -> PendingDataPointListResponse:
    store = _get_store()
    normalized_status = status_filter.strip().lower() if status_filter else ""
    allowed_statuses = {"pending", "approved", "rejected", "all"}
    if normalized_status not in allowed_statuses:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid status_filter. Use pending, approved, rejected, or all.",
        )
    pending_status = None if normalized_status == "all" else normalized_status
    pending = await store.list_pending(status=pending_status, connection_id=connection_id)
    return PendingDataPointListResponse(
        pending=[_to_pending_response(item) for item in pending]
    )


@router.post("/datapoints/pending/{pending_id}/approve", response_model=PendingDataPointResponse)
async def approve_datapoint(
    pending_id: UUID, payload: ReviewNoteRequest | None = None
) -> PendingDataPointResponse:
    store = _get_store()
    vector_store = _get_vector_store()
    graph = _get_knowledge_graph()

    pending_items = await store.list_pending(status="pending")
    pending_item = next((item for item in pending_items if item.pending_id == pending_id), None)
    if pending_item is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pending DataPoint not found: {pending_id}",
        )

    candidate_payload = (
        payload.datapoint
        if payload is not None and payload.datapoint is not None
        else pending_item.datapoint
    )
    from backend.models.datapoint import DataPoint

    try:
        datapoint = TypeAdapter(DataPoint).validate_python(candidate_payload)
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid DataPoint payload: {exc.errors()[0]['msg']}",
        ) from exc
    _validate_datapoint_contract_or_400(datapoint)
    profile = await store.get_profile(pending_item.profile_id)
    _attach_connection_metadata(datapoint, profile.connection_id)

    pending = await store.update_pending_status(
        pending_id,
        status="approved",
        review_note=payload.review_note if payload else None,
        datapoint=datapoint.model_dump(mode="json", by_alias=True),
    )

    from backend.sync.orchestrator import save_datapoint_to_disk

    table_keys = _extract_table_keys(datapoint)
    if table_keys:
        removed_ids: set[str] = set()
        for table_key in sorted(set(table_keys)):
            removed_ids.update(
                _remove_existing_datapoints_for_table(
                    table_key, exclude_ids={datapoint.datapoint_id}
                )
            )
        if removed_ids:
            await vector_store.delete(sorted(removed_ids))
            for removed_id in sorted(removed_ids):
                graph.remove_datapoint(removed_id)
    save_datapoint_to_disk(
        datapoint.model_dump(mode="json", by_alias=True),
        _datapoint_path(datapoint.datapoint_id),
    )
    await vector_store.add_datapoints([datapoint])
    graph.add_datapoint(datapoint)

    return _to_pending_response(pending)


@router.post("/datapoints/pending/{pending_id}/reject", response_model=PendingDataPointResponse)
async def reject_datapoint(
    pending_id: UUID, payload: ReviewNoteRequest | None = None
) -> PendingDataPointResponse:
    store = _get_store()
    pending = await store.update_pending_status(
        pending_id, status="rejected", review_note=payload.review_note if payload else None
    )
    return _to_pending_response(pending)


@router.post("/datapoints/pending/bulk-approve", response_model=PendingDataPointListResponse)
async def bulk_approve_datapoints(
    connection_id: UUID | None = None,
) -> PendingDataPointListResponse:
    store = _get_store()
    vector_store = _get_vector_store()
    graph = _get_knowledge_graph()

    from backend.models.datapoint import DataPoint
    from backend.sync.orchestrator import save_datapoint_to_disk

    pending_items = await store.list_pending(
        status="pending", connection_id=connection_id
    )
    datapoints_by_pending_id: dict[UUID, DataPoint] = {}
    profile_connection_ids: dict[UUID, UUID] = {}
    for item in pending_items:
        try:
            datapoint = TypeAdapter(DataPoint).validate_python(item.datapoint)
        except ValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"Invalid DataPoint payload in pending item {item.pending_id}: "
                    f"{exc.errors()[0]['msg']}"
                ),
            ) from exc
        _validate_datapoint_contract_or_400(datapoint)
        if item.profile_id not in profile_connection_ids:
            profile = await store.get_profile(item.profile_id)
            profile_connection_ids[item.profile_id] = profile.connection_id
        _attach_connection_metadata(datapoint, profile_connection_ids[item.profile_id])
        datapoints_by_pending_id[item.pending_id] = datapoint

    approved = await store.bulk_update_pending(
        status="approved",
        connection_id=connection_id,
        pending_ids=list(datapoints_by_pending_id.keys()),
    )
    datapoints: list[DataPoint] = []
    for item in approved:
        datapoint = datapoints_by_pending_id.get(item.pending_id)
        if datapoint is None:
            try:
                datapoint = TypeAdapter(DataPoint).validate_python(item.datapoint)
            except ValidationError as exc:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        "Invalid DataPoint payload in approved item "
                        f"{item.pending_id}: {exc.errors()[0]['msg']}"
                    ),
                ) from exc
            _validate_datapoint_contract_or_400(datapoint)
            profile = await store.get_profile(item.profile_id)
            _attach_connection_metadata(datapoint, profile.connection_id)
        datapoints.append(datapoint)
    if datapoints:
        exclude_ids = {datapoint.datapoint_id for datapoint in datapoints}
        removed_ids: set[str] = set()
        table_keys: set[str] = set()
        for datapoint in datapoints:
            table_keys.update(_extract_table_keys(datapoint))
        for table_key in sorted(table_keys):
            removed_ids.update(
                _remove_existing_datapoints_for_table(table_key, exclude_ids=exclude_ids)
            )
        if removed_ids:
            await vector_store.delete(sorted(removed_ids))
            for removed_id in sorted(removed_ids):
                graph.remove_datapoint(removed_id)
        for datapoint in datapoints:
            save_datapoint_to_disk(
                datapoint.model_dump(mode="json", by_alias=True),
                _datapoint_path(datapoint.datapoint_id),
            )
        await vector_store.add_datapoints(datapoints)
        for datapoint in datapoints:
            graph.add_datapoint(datapoint)

    return PendingDataPointListResponse(
        pending=[_to_pending_response(item) for item in approved]
    )
