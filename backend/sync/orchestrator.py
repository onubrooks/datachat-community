"""Sync orchestrator for DataPoints."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from backend.knowledge.conflicts import (
    ConflictMode,
    DataPointConflict,
    resolve_datapoint_conflicts,
)
from backend.knowledge.datapoints import DataPointLoader
from backend.models.datapoint import DataPoint

logger = logging.getLogger(__name__)


@dataclass
class SyncJob:
    """Status for a sync job."""

    job_id: UUID
    status: str
    sync_type: str
    started_at: datetime
    finished_at: datetime | None = None
    total_datapoints: int = 0
    processed_datapoints: int = 0
    error: str | None = None


@dataclass
class IncrementalLoadResult:
    """Incremental load outputs for safe update semantics."""

    datapoints: list[DataPoint]
    ids_to_delete: list[str]
    errors: list[str]


class SyncOrchestrator:
    """Coordinate syncing DataPoints into vector store and knowledge graph."""

    def __init__(
        self,
        vector_store,
        knowledge_graph,
        datapoints_dir: str | Path = "datapoints",
        loader: DataPointLoader | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._vector_store = vector_store
        self._knowledge_graph = knowledge_graph
        self._datapoints_dir = Path(datapoints_dir)
        self._loader = loader or DataPointLoader(strict_contracts=True)
        self._loop = loop
        self._current_job: SyncJob | None = None
        self._lock = asyncio.Lock()

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def get_status(self) -> dict:
        if not self._current_job:
            return {
                "status": "idle",
                "job_id": None,
                "sync_type": None,
                "started_at": None,
                "finished_at": None,
                "total_datapoints": 0,
                "processed_datapoints": 0,
                "error": None,
            }
        payload = asdict(self._current_job)
        payload["job_id"] = str(payload["job_id"])
        return payload

    def enqueue_sync_all(
        self,
        *,
        scope: str = "auto",
        connection_id: str | None = None,
        conflict_mode: ConflictMode = "error",
    ) -> UUID:
        job_id = uuid4()
        self._schedule_job(
            job_id,
            "full",
            None,
            scope=scope,
            connection_id=connection_id,
            conflict_mode=conflict_mode,
        )
        return job_id

    def enqueue_sync_incremental(
        self, datapoint_ids: Iterable[str], *, conflict_mode: ConflictMode = "error"
    ) -> UUID:
        job_id = uuid4()
        self._schedule_job(
            job_id,
            "incremental",
            list(datapoint_ids),
            conflict_mode=conflict_mode,
        )
        return job_id

    async def sync_all(
        self,
        *,
        scope: str = "auto",
        connection_id: str | None = None,
        conflict_mode: ConflictMode = "error",
    ) -> SyncJob:
        job = self._start_job(uuid4(), "full")
        await self._run_sync_all(
            job,
            scope=scope,
            connection_id=connection_id,
            conflict_mode=conflict_mode,
        )
        return job

    async def sync_incremental(
        self, datapoint_ids: Iterable[str], *, conflict_mode: ConflictMode = "error"
    ) -> SyncJob:
        job = self._start_job(uuid4(), "incremental")
        await self._run_sync_incremental(
            job,
            list(datapoint_ids),
            conflict_mode=conflict_mode,
        )
        return job

    def _schedule_job(
        self,
        job_id: UUID,
        sync_type: str,
        datapoint_ids: list[str] | None,
        *,
        scope: str = "auto",
        connection_id: str | None = None,
        conflict_mode: ConflictMode = "error",
    ) -> None:
        if not self._loop:
            raise RuntimeError("Sync orchestrator requires an event loop")
        if sync_type == "full":
            coro = self._run_sync_all(
                self._start_job(job_id, sync_type),
                scope=scope,
                connection_id=connection_id,
                conflict_mode=conflict_mode,
            )
        else:
            coro = self._run_sync_incremental(
                self._start_job(job_id, sync_type),
                datapoint_ids or [],
                conflict_mode=conflict_mode,
            )
        asyncio.run_coroutine_threadsafe(coro, self._loop)

    def _start_job(self, job_id: UUID, sync_type: str) -> SyncJob:
        job = SyncJob(
            job_id=job_id,
            status="running",
            sync_type=sync_type,
            started_at=datetime.now(UTC),
        )
        self._current_job = job
        return job

    async def _run_sync_all(
        self,
        job: SyncJob,
        *,
        scope: str = "auto",
        connection_id: str | None = None,
        conflict_mode: ConflictMode = "error",
    ) -> None:
        async with self._lock:
            try:
                datapoints = self._load_all_datapoints(
                    scope=scope,
                    connection_id=connection_id,
                )
                resolution = resolve_datapoint_conflicts(datapoints, mode=conflict_mode)
                self._log_conflict_decisions(resolution.conflicts)
                datapoints = resolution.datapoints
                job.total_datapoints = len(datapoints)

                await self._vector_store.clear()
                self._knowledge_graph.clear()

                if datapoints:
                    await self._vector_store.add_datapoints(datapoints)
                    for datapoint in datapoints:
                        self._knowledge_graph.add_datapoint(datapoint)
                        job.processed_datapoints += 1

                job.status = "completed"
            except Exception as exc:
                job.status = "failed"
                job.error = str(exc)
            finally:
                job.finished_at = datetime.now(UTC)

    async def _run_sync_incremental(
        self,
        job: SyncJob,
        datapoint_ids: list[str],
        *,
        conflict_mode: ConflictMode = "error",
    ) -> None:
        async with self._lock:
            try:
                job.total_datapoints = len(datapoint_ids)
                load_result = self._load_datapoints_by_id(datapoint_ids)
                resolution = resolve_datapoint_conflicts(
                    load_result.datapoints,
                    mode=conflict_mode,
                )
                self._log_conflict_decisions(resolution.conflicts)
                load_result.datapoints = resolution.datapoints

                if load_result.ids_to_delete:
                    await self._vector_store.delete(load_result.ids_to_delete)
                    for datapoint_id in load_result.ids_to_delete:
                        if hasattr(self._knowledge_graph, "remove_datapoint"):
                            self._knowledge_graph.remove_datapoint(datapoint_id)

                if load_result.datapoints:
                    await self._vector_store.add_datapoints(load_result.datapoints)
                    for datapoint in load_result.datapoints:
                        self._knowledge_graph.add_datapoint(datapoint)
                        job.processed_datapoints += 1

                if load_result.errors:
                    job.status = "failed"
                    sample = "; ".join(load_result.errors[:5])
                    suffix = (
                        ""
                        if len(load_result.errors) <= 5
                        else f"; ... +{len(load_result.errors) - 5} more"
                    )
                    job.error = (
                        "Incremental sync completed with contract/validation failures: "
                        f"{sample}{suffix}"
                    )
                else:
                    job.status = "completed"
            except Exception as exc:
                job.status = "failed"
                job.error = str(exc)
            finally:
                job.finished_at = datetime.now(UTC)

    def _load_all_datapoints(
        self,
        *,
        scope: str = "auto",
        connection_id: str | None = None,
    ) -> list[DataPoint]:
        datapoint_files = self._collect_datapoint_files()
        datapoints: list[DataPoint] = []
        load_errors: list[str] = []
        for file_path in datapoint_files:
            try:
                datapoint = self._loader.load_file(file_path)
                self._apply_scope(datapoint, scope=scope, connection_id=connection_id)
                datapoints.append(datapoint)
            except Exception as exc:
                load_errors.append(f"{file_path}: {exc}")
        if load_errors:
            sample = "; ".join(load_errors[:5])
            suffix = "" if len(load_errors) <= 5 else f"; ... +{len(load_errors) - 5} more"
            raise RuntimeError(
                "DataPoint sync aborted: "
                f"{len(load_errors)} file(s) failed validation or loading. "
                f"{sample}{suffix}"
            )
        return datapoints

    def _load_datapoints_by_id(self, datapoint_ids: Iterable[str]) -> IncrementalLoadResult:
        requested_ids: list[str] = []
        seen: set[str] = set()
        for item in datapoint_ids:
            item_id = str(item).strip()
            if not item_id or item_id in seen:
                continue
            seen.add(item_id)
            requested_ids.append(item_id)

        if not requested_ids:
            return IncrementalLoadResult(datapoints=[], ids_to_delete=[], errors=[])

        id_set = set(requested_ids)
        datapoint_files = self._collect_datapoint_files()
        files_by_stem = {path.stem: path for path in datapoint_files if path.stem in id_set}
        datapoints: list[DataPoint] = []
        ids_to_delete: list[str] = []
        load_errors: list[str] = []

        for requested_id in requested_ids:
            file_path = files_by_stem.get(requested_id)
            if file_path is None:
                # Missing file is treated as a delete signal.
                ids_to_delete.append(requested_id)
                continue
            try:
                datapoint = self._loader.load_file(file_path)
            except Exception as exc:
                load_errors.append(f"{file_path}: {exc}")
                continue
            if datapoint.datapoint_id != requested_id:
                load_errors.append(
                    f"{file_path}: datapoint_id mismatch (expected {requested_id}, "
                    f"found {datapoint.datapoint_id})"
                )
                continue
            datapoints.append(datapoint)
            ids_to_delete.append(requested_id)

        return IncrementalLoadResult(
            datapoints=datapoints,
            ids_to_delete=ids_to_delete,
            errors=load_errors,
        )

    @staticmethod
    def _log_conflict_decisions(conflicts: list[DataPointConflict]) -> None:
        for conflict in conflicts:
            if not conflict.resolved_datapoint_id:
                continue
            logger.info(
                "datapoint_conflict_resolved",
                extra={
                    "conflict_key": conflict.key,
                    "mode": conflict.mode,
                    "winner_datapoint_id": conflict.resolved_datapoint_id,
                    "candidate_datapoint_ids": conflict.datapoint_ids,
                    "candidate_source_tiers": conflict.source_tiers,
                },
            )

    def _collect_datapoint_files(self) -> list[Path]:
        if not self._datapoints_dir.exists():
            return []
        files = []
        for path in self._datapoints_dir.rglob("*.json"):
            if "schemas" in path.parts:
                continue
            files.append(path)
        return files

    @staticmethod
    def _apply_scope(
        datapoint: DataPoint,
        *,
        scope: str = "auto",
        connection_id: str | None = None,
    ) -> None:
        if scope not in {"auto", "global", "database"}:
            return

        metadata = datapoint.metadata if isinstance(datapoint.metadata, dict) else {}
        if scope == "global":
            metadata["scope"] = "global"
            metadata.pop("connection_id", None)
        elif scope == "database" and connection_id:
            metadata["scope"] = "database"
            metadata["connection_id"] = str(connection_id)
        datapoint.metadata = metadata


def save_datapoint_to_disk(datapoint: dict, filepath: Path) -> None:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as handle:
        json.dump(datapoint, handle, indent=2)
