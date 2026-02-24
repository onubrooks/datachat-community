"""Storage utilities for profiling jobs and pending DataPoints."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import UUID

import asyncpg

from backend.config import get_settings
from backend.profiling.models import (
    DatabaseProfile,
    GenerationJob,
    GenerationProgress,
    PendingDataPoint,
    ProfilingJob,
    ProfilingProgress,
)

_CREATE_JOBS_TABLE = """
CREATE TABLE IF NOT EXISTS profiling_jobs (
    job_id UUID PRIMARY KEY,
    connection_id UUID NOT NULL,
    status TEXT NOT NULL,
    progress JSONB,
    error TEXT,
    profile_id UUID,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
"""

_CREATE_PROFILES_TABLE = """
CREATE TABLE IF NOT EXISTS profiling_profiles (
    profile_id UUID PRIMARY KEY,
    connection_id UUID NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
"""

_CREATE_PENDING_TABLE = """
CREATE TABLE IF NOT EXISTS pending_datapoints (
    pending_id UUID PRIMARY KEY,
    profile_id UUID NOT NULL,
    datapoint JSONB NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT
);
"""

_CREATE_GENERATION_JOBS_TABLE = """
CREATE TABLE IF NOT EXISTS datapoint_generation_jobs (
    job_id UUID PRIMARY KEY,
    profile_id UUID NOT NULL,
    status TEXT NOT NULL,
    progress JSONB,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
"""


class ProfilingStore:
    """Persist profiling jobs, profiles, and pending DataPoints."""

    def __init__(self, database_url: str | None = None) -> None:
        settings = get_settings()
        self._database_url = database_url or (
            str(settings.system_database.url) if settings.system_database.url else None
        )
        self._pool: asyncpg.Pool | None = None

    async def initialize(self) -> None:
        if self._pool is None:
            if not self._database_url:
                raise ValueError("SYSTEM_DATABASE_URL must be set for the profiling store.")
            dsn = self._normalize_postgres_url(self._database_url)
            self._pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)
        await self._pool.execute(_CREATE_JOBS_TABLE)
        await self._pool.execute(_CREATE_PROFILES_TABLE)
        await self._pool.execute(_CREATE_PENDING_TABLE)
        await self._pool.execute(_CREATE_GENERATION_JOBS_TABLE)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def create_job(self, connection_id: UUID) -> ProfilingJob:
        self._ensure_pool()
        job = ProfilingJob(connection_id=connection_id)
        await self._pool.execute(
            """
            INSERT INTO profiling_jobs (
                job_id, connection_id, status, progress, error, profile_id, created_at, updated_at
            ) VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8)
            """,
            job.job_id,
            job.connection_id,
            job.status,
            json.dumps(job.progress.model_dump()) if job.progress else None,
            job.error,
            job.profile_id,
            job.created_at,
            job.updated_at,
        )
        return job

    async def update_job(
        self,
        job_id: UUID,
        status: str | None = None,
        progress: ProfilingProgress | None = None,
        error: str | None = None,
        profile_id: UUID | None = None,
    ) -> ProfilingJob:
        self._ensure_pool()
        updated_at = datetime.now(UTC)
        await self._pool.execute(
            """
            UPDATE profiling_jobs
            SET status = COALESCE($2, status),
                progress = COALESCE($3::jsonb, progress),
                error = COALESCE($4, error),
                profile_id = COALESCE($5, profile_id),
                updated_at = $6
            WHERE job_id = $1
            """,
            job_id,
            status,
            json.dumps(progress.model_dump()) if progress else None,
            error,
            profile_id,
            updated_at,
        )
        return await self.get_job(job_id)

    async def create_generation_job(self, profile_id: UUID) -> GenerationJob:
        self._ensure_pool()
        job = GenerationJob(profile_id=profile_id)
        await self._pool.execute(
            """
            INSERT INTO datapoint_generation_jobs (
                job_id, profile_id, status, progress, error, created_at, updated_at
            ) VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)
            """,
            job.job_id,
            job.profile_id,
            job.status,
            json.dumps(job.progress.model_dump()) if job.progress else None,
            job.error,
            job.created_at,
            job.updated_at,
        )
        return job

    async def get_generation_job(self, job_id: UUID) -> GenerationJob:
        self._ensure_pool()
        row = await self._pool.fetchrow(
            "SELECT * FROM datapoint_generation_jobs WHERE job_id = $1",
            job_id,
        )
        if not row:
            raise KeyError(f"Generation job {job_id} not found")
        progress_payload = row["progress"]
        if isinstance(progress_payload, str):
            progress_payload = json.loads(progress_payload)
        progress = GenerationProgress(**progress_payload) if progress_payload else None
        return GenerationJob(
            job_id=row["job_id"],
            profile_id=row["profile_id"],
            status=row["status"],
            progress=progress,
            error=row["error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def get_latest_generation_job(self, profile_id: UUID) -> GenerationJob | None:
        self._ensure_pool()
        row = await self._pool.fetchrow(
            """
            SELECT * FROM datapoint_generation_jobs
            WHERE profile_id = $1
            ORDER BY created_at DESC
            LIMIT 1
            """,
            profile_id,
        )
        if not row:
            return None
        progress_payload = row["progress"]
        if isinstance(progress_payload, str):
            progress_payload = json.loads(progress_payload)
        progress = GenerationProgress(**progress_payload) if progress_payload else None
        return GenerationJob(
            job_id=row["job_id"],
            profile_id=row["profile_id"],
            status=row["status"],
            progress=progress,
            error=row["error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def update_generation_job(
        self,
        job_id: UUID,
        status: str | None = None,
        progress: GenerationProgress | None = None,
        error: str | None = None,
    ) -> GenerationJob:
        self._ensure_pool()
        updated_at = datetime.now(UTC)
        await self._pool.execute(
            """
            UPDATE datapoint_generation_jobs
            SET status = COALESCE($2, status),
                progress = COALESCE($3::jsonb, progress),
                error = COALESCE($4, error),
                updated_at = $5
            WHERE job_id = $1
            """,
            job_id,
            status,
            json.dumps(progress.model_dump()) if progress else None,
            error,
            updated_at,
        )
        return await self.get_generation_job(job_id)

    async def get_job(self, job_id: UUID) -> ProfilingJob:
        self._ensure_pool()
        row = await self._pool.fetchrow(
            """
            SELECT job_id, connection_id, status, progress, error, profile_id, created_at, updated_at
            FROM profiling_jobs
            WHERE job_id = $1
            """,
            job_id,
        )
        if row is None:
            raise KeyError(f"Profiling job not found: {job_id}")
        progress = None
        if row["progress"]:
            progress_data = row["progress"]
            if isinstance(progress_data, str):
                progress_data = json.loads(progress_data)
            progress = ProfilingProgress(**progress_data)
        return ProfilingJob(
            job_id=row["job_id"],
            connection_id=row["connection_id"],
            status=row["status"],
            progress=progress,
            error=row["error"],
            profile_id=row["profile_id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def get_latest_job_for_connection(
        self, connection_id: UUID
    ) -> ProfilingJob | None:
        self._ensure_pool()
        row = await self._pool.fetchrow(
            """
            SELECT job_id, connection_id, status, progress, error, profile_id, created_at, updated_at
            FROM profiling_jobs
            WHERE connection_id = $1
            ORDER BY created_at DESC
            LIMIT 1
            """,
            connection_id,
        )
        if row is None:
            return None
        progress = None
        if row["progress"]:
            progress_data = row["progress"]
            if isinstance(progress_data, str):
                progress_data = json.loads(progress_data)
            progress = ProfilingProgress(**progress_data)
        return ProfilingJob(
            job_id=row["job_id"],
            connection_id=row["connection_id"],
            status=row["status"],
            progress=progress,
            error=row["error"],
            profile_id=row["profile_id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def save_profile(self, profile: DatabaseProfile) -> DatabaseProfile:
        self._ensure_pool()
        await self._pool.execute(
            """
            INSERT INTO profiling_profiles (profile_id, connection_id, payload, created_at)
            VALUES ($1, $2, $3::jsonb, $4)
            """,
            profile.profile_id,
            profile.connection_id,
            json.dumps(profile.model_dump(mode="json", by_alias=True)),
            profile.created_at,
        )
        return profile

    async def get_profile(self, profile_id: UUID) -> DatabaseProfile:
        self._ensure_pool()
        row = await self._pool.fetchrow(
            """
            SELECT payload
            FROM profiling_profiles
            WHERE profile_id = $1
            """,
            profile_id,
        )
        if row is None:
            raise KeyError(f"Profile not found: {profile_id}")
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return DatabaseProfile.model_validate(payload)

    async def add_pending_datapoints(
        self, profile_id: UUID, datapoints: list[PendingDataPoint]
    ) -> list[PendingDataPoint]:
        self._ensure_pool()
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for item in datapoints:
                    await conn.execute(
                        """
                        INSERT INTO pending_datapoints (
                            pending_id, profile_id, datapoint, confidence, status, created_at
                        ) VALUES ($1, $2, $3::jsonb, $4, $5, $6)
                        """,
                        item.pending_id,
                        profile_id,
                        json.dumps(item.datapoint),
                        item.confidence,
                        item.status,
                        item.created_at,
                    )
        return datapoints

    async def list_pending(
        self,
        status: str | None = None,
        connection_id: UUID | None = None,
    ) -> list[PendingDataPoint]:
        self._ensure_pool()
        rows = await self._pool.fetch(
            """
            SELECT p.pending_id, p.profile_id, p.datapoint, p.confidence, p.status,
                   p.created_at, p.reviewed_at, p.review_note
            FROM pending_datapoints p
            JOIN profiling_profiles pr ON pr.profile_id = p.profile_id
            WHERE ($1::text IS NULL OR p.status = $1)
              AND ($2::uuid IS NULL OR pr.connection_id = $2)
            ORDER BY p.created_at DESC
            """,
            status,
            connection_id,
        )
        return [self._row_to_pending(row) for row in rows]

    async def update_pending_status(
        self,
        pending_id: UUID,
        status: str,
        review_note: str | None = None,
        datapoint: dict | None = None,
    ) -> PendingDataPoint:
        self._ensure_pool()
        reviewed_at = datetime.now(UTC)
        datapoint_payload = json.dumps(datapoint) if datapoint else None
        row = await self._pool.fetchrow(
            """
            UPDATE pending_datapoints
            SET status = $2, reviewed_at = $3, review_note = $4,
                datapoint = COALESCE($5::jsonb, datapoint)
            WHERE pending_id = $1
            RETURNING pending_id, profile_id, datapoint, confidence, status,
                      created_at, reviewed_at, review_note
            """,
            pending_id,
            status,
            reviewed_at,
            review_note,
            datapoint_payload,
        )
        if row is None:
            raise KeyError(f"Pending DataPoint not found: {pending_id}")
        return self._row_to_pending(row)

    async def bulk_update_pending(
        self,
        status: str,
        connection_id: UUID | None = None,
        pending_ids: list[UUID] | None = None,
    ) -> list[PendingDataPoint]:
        self._ensure_pool()
        reviewed_at = datetime.now(UTC)
        id_filter = [pending_id for pending_id in (pending_ids or []) if pending_id]
        if pending_ids is not None and not id_filter:
            return []
        if connection_id is None:
            if id_filter:
                rows = await self._pool.fetch(
                    """
                    UPDATE pending_datapoints
                    SET status = $1, reviewed_at = $2
                    WHERE status = 'pending'
                      AND pending_id = ANY($3::uuid[])
                    RETURNING pending_id, profile_id, datapoint, confidence, status,
                              created_at, reviewed_at, review_note
                    """,
                    status,
                    reviewed_at,
                    id_filter,
                )
            else:
                rows = await self._pool.fetch(
                    """
                    UPDATE pending_datapoints
                    SET status = $1, reviewed_at = $2
                    WHERE status = 'pending'
                    RETURNING pending_id, profile_id, datapoint, confidence, status,
                              created_at, reviewed_at, review_note
                    """,
                    status,
                    reviewed_at,
                )
        else:
            if id_filter:
                rows = await self._pool.fetch(
                    """
                    UPDATE pending_datapoints AS p
                    SET status = $1, reviewed_at = $2
                    FROM profiling_profiles AS pr
                    WHERE p.profile_id = pr.profile_id
                      AND p.status = 'pending'
                      AND pr.connection_id = $3
                      AND p.pending_id = ANY($4::uuid[])
                    RETURNING p.pending_id, p.profile_id, p.datapoint, p.confidence, p.status,
                              p.created_at, p.reviewed_at, p.review_note
                    """,
                    status,
                    reviewed_at,
                    connection_id,
                    id_filter,
                )
            else:
                rows = await self._pool.fetch(
                    """
                    UPDATE pending_datapoints AS p
                    SET status = $1, reviewed_at = $2
                    FROM profiling_profiles AS pr
                    WHERE p.profile_id = pr.profile_id
                      AND p.status = 'pending'
                      AND pr.connection_id = $3
                    RETURNING p.pending_id, p.profile_id, p.datapoint, p.confidence, p.status,
                              p.created_at, p.reviewed_at, p.review_note
                    """,
                    status,
                    reviewed_at,
                    connection_id,
                )
        return [self._row_to_pending(row) for row in rows]

    async def delete_pending_for_profile(self, profile_id: UUID) -> None:
        """Remove pending datapoints for a profile (used before regeneration)."""
        self._ensure_pool()
        await self._pool.execute(
            """
            DELETE FROM pending_datapoints
            WHERE profile_id = $1 AND status = 'pending'
            """,
            profile_id,
        )

    @staticmethod
    def _row_to_pending(row: asyncpg.Record) -> PendingDataPoint:
        payload = row["datapoint"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return PendingDataPoint(
            pending_id=row["pending_id"],
            profile_id=row["profile_id"],
            datapoint=payload,
            confidence=row["confidence"],
            status=row["status"],
            created_at=row["created_at"],
            reviewed_at=row["reviewed_at"],
            review_note=row["review_note"],
        )

    def _ensure_pool(self) -> None:
        if self._pool is None:
            raise RuntimeError("ProfilingStore is not initialized")

    @staticmethod
    def _normalize_postgres_url(database_url: str) -> str:
        if database_url.startswith("postgresql+asyncpg://"):
            return database_url.replace("postgresql+asyncpg://", "postgresql://", 1)
        return database_url
