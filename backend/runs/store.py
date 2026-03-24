"""Persistence for AI workflow runs and ordered steps."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from statistics import median
from typing import Any
from uuid import UUID

import asyncpg

from backend.config import get_settings

_CREATE_RUNS_TABLE = """
CREATE TABLE IF NOT EXISTS ai_runs (
    run_id UUID PRIMARY KEY,
    run_type TEXT NOT NULL,
    status TEXT NOT NULL,
    route TEXT,
    connection_id TEXT,
    conversation_id TEXT,
    correlation_id TEXT,
    failure_class TEXT,
    confidence DOUBLE PRECISION,
    warning_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    latency_ms DOUBLE PRECISION,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
"""

_CREATE_RUNS_CREATED_INDEX = """
CREATE INDEX IF NOT EXISTS ai_runs_created_at_idx
ON ai_runs (created_at DESC);
"""

_CREATE_RUN_STEPS_TABLE = """
CREATE TABLE IF NOT EXISTS ai_run_steps (
    step_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES ai_runs(run_id) ON DELETE CASCADE,
    step_order INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    status TEXT NOT NULL,
    latency_ms DOUBLE PRECISION,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL
);
"""

_CREATE_RUN_STEPS_RUN_ORDER_INDEX = """
CREATE INDEX IF NOT EXISTS ai_run_steps_run_id_step_order_idx
ON ai_run_steps (run_id, step_order ASC);
"""


class RunStore:
    """Persist completed or failed AI workflow runs in the system database."""

    def __init__(self, database_url: str | None = None) -> None:
        settings = get_settings()
        self._database_url = database_url or (
            str(settings.system_database.url) if settings.system_database.url else None
        )
        self._pool: asyncpg.Pool | None = None

    async def initialize(self) -> None:
        if self._pool is None:
            if not self._database_url:
                raise ValueError("SYSTEM_DATABASE_URL must be set for run storage.")
            dsn = self._normalize_postgres_url(self._database_url)
            self._pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)
        await self._pool.execute(_CREATE_RUNS_TABLE)
        await self._pool.execute(_CREATE_RUNS_CREATED_INDEX)
        await self._pool.execute(_CREATE_RUN_STEPS_TABLE)
        await self._pool.execute(_CREATE_RUN_STEPS_RUN_ORDER_INDEX)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def save_run(
        self,
        *,
        run_id: UUID,
        run_type: str,
        status: str,
        route: str | None,
        connection_id: str | None,
        conversation_id: str | None,
        correlation_id: str | None,
        failure_class: str | None,
        confidence: float | None,
        warning_count: int,
        error_count: int,
        latency_ms: float | None,
        summary: dict[str, Any] | None,
        output: dict[str, Any] | None,
        started_at: datetime,
        completed_at: datetime | None,
        steps: list[dict[str, Any]],
    ) -> None:
        self._ensure_pool()
        now = datetime.now(UTC)
        created_at = started_at or now
        await self._pool.execute(
            """
            INSERT INTO ai_runs (
                run_id,
                run_type,
                status,
                route,
                connection_id,
                conversation_id,
                correlation_id,
                failure_class,
                confidence,
                warning_count,
                error_count,
                latency_ms,
                summary_json,
                output_json,
                started_at,
                completed_at,
                created_at,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14::jsonb, $15, $16, $17, $18
            )
            ON CONFLICT (run_id) DO UPDATE SET
                run_type = EXCLUDED.run_type,
                status = EXCLUDED.status,
                route = EXCLUDED.route,
                connection_id = EXCLUDED.connection_id,
                conversation_id = EXCLUDED.conversation_id,
                correlation_id = EXCLUDED.correlation_id,
                failure_class = EXCLUDED.failure_class,
                confidence = EXCLUDED.confidence,
                warning_count = EXCLUDED.warning_count,
                error_count = EXCLUDED.error_count,
                latency_ms = EXCLUDED.latency_ms,
                summary_json = EXCLUDED.summary_json,
                output_json = EXCLUDED.output_json,
                started_at = EXCLUDED.started_at,
                completed_at = EXCLUDED.completed_at,
                updated_at = EXCLUDED.updated_at
            """,
            run_id,
            run_type,
            status,
            route,
            connection_id,
            conversation_id,
            correlation_id,
            failure_class,
            confidence,
            warning_count,
            error_count,
            latency_ms,
            json.dumps(summary or {}),
            json.dumps(output or {}),
            started_at,
            completed_at,
            created_at,
            now,
        )

        await self._pool.execute("DELETE FROM ai_run_steps WHERE run_id = $1", run_id)
        if not steps:
            return

        payload = []
        for item in steps:
            payload.append(
                (
                    item["step_id"],
                    run_id,
                    item["step_order"],
                    item["step_name"],
                    item["status"],
                    item.get("latency_ms"),
                    json.dumps(item.get("summary") or {}),
                    item.get("created_at") or now,
                )
            )
        await self._pool.executemany(
            """
            INSERT INTO ai_run_steps (
                step_id,
                run_id,
                step_order,
                step_name,
                status,
                latency_ms,
                summary_json,
                created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
            """,
            payload,
        )

    async def list_runs(self, *, limit: int = 50) -> list[dict[str, Any]]:
        self._ensure_pool()
        bounded_limit = max(1, min(limit, 200))
        rows = await self._pool.fetch(
            """
            SELECT
                run_id,
                run_type,
                status,
                route,
                connection_id,
                conversation_id,
                correlation_id,
                failure_class,
                confidence,
                warning_count,
                error_count,
                latency_ms,
                summary_json,
                started_at,
                completed_at,
                created_at,
                updated_at
            FROM ai_runs
            ORDER BY created_at DESC
            LIMIT $1
            """,
            bounded_limit,
        )
        return [self._row_to_run_summary(row) for row in rows]

    async def get_run(self, run_id: UUID) -> dict[str, Any] | None:
        self._ensure_pool()
        row = await self._pool.fetchrow(
            """
            SELECT
                run_id,
                run_type,
                status,
                route,
                connection_id,
                conversation_id,
                correlation_id,
                failure_class,
                confidence,
                warning_count,
                error_count,
                latency_ms,
                summary_json,
                output_json,
                started_at,
                completed_at,
                created_at,
                updated_at
            FROM ai_runs
            WHERE run_id = $1
            """,
            run_id,
        )
        if row is None:
            return None
        steps = await self._pool.fetch(
            """
            SELECT
                step_id,
                step_order,
                step_name,
                status,
                latency_ms,
                summary_json,
                created_at
            FROM ai_run_steps
            WHERE run_id = $1
            ORDER BY step_order ASC
            """,
            run_id,
        )
        payload = self._row_to_run_detail(row)
        payload["steps"] = [self._row_to_step(item) for item in steps]
        return payload

    async def summarize_runs(self, *, window_hours: int = 24) -> dict[str, Any]:
        self._ensure_pool()
        bounded_window = max(1, min(window_hours, 24 * 30))
        rows = await self._pool.fetch(
            """
            SELECT
                run_id,
                status,
                route,
                failure_class,
                latency_ms,
                summary_json,
                output_json,
                created_at
            FROM ai_runs
            WHERE created_at >= NOW() - ($1::int * INTERVAL '1 hour')
            ORDER BY created_at DESC
            """,
            bounded_window,
        )
        payloads = [
            {
                "run_id": str(row["run_id"]),
                "status": row["status"],
                "route": row["route"] or "unknown",
                "failure_class": row["failure_class"],
                "latency_ms": row["latency_ms"],
                "summary": self._decode_json_field(row["summary_json"]) or {},
                "output": self._decode_json_field(row["output_json"]) or {},
                "created_at": row["created_at"],
            }
            for row in rows
        ]

        total_runs = len(payloads)
        completed_runs = sum(1 for row in payloads if row["status"] == "completed")
        failed_runs = sum(1 for row in payloads if row["status"] == "failed")
        success_rate = (completed_runs / total_runs) if total_runs else 0.0
        latencies = sorted(
            row["latency_ms"] for row in payloads if isinstance(row["latency_ms"], (int, float))
        )
        p50_latency_ms = median(latencies) if latencies else None
        p95_latency_ms = self._percentile(latencies, 0.95)

        clarification_hits = 0
        retrieval_misses = 0
        route_counts: dict[str, dict[str, Any]] = {}
        failure_counts: dict[str, int] = {}
        recent_failures: list[dict[str, Any]] = []

        for row in payloads:
            summary = row["summary"] if isinstance(row["summary"], dict) else {}
            route = str(row["route"] or "unknown")
            if bool(summary.get("clarification_needed")):
                clarification_hits += 1
            if int(summary.get("retrieved_datapoint_count", 0) or 0) == 0:
                retrieval_misses += 1

            route_entry = route_counts.setdefault(
                route,
                {"route": route, "count": 0, "completed": 0, "failed": 0},
            )
            route_entry["count"] += 1
            if row["status"] == "completed":
                route_entry["completed"] += 1
            elif row["status"] == "failed":
                route_entry["failed"] += 1

            failure_class = row["failure_class"]
            if failure_class:
                failure_counts[str(failure_class)] = failure_counts.get(str(failure_class), 0) + 1
            if row["status"] == "failed" and len(recent_failures) < 10:
                recent_failures.append(
                    {
                        "run_id": row["run_id"],
                        "route": route,
                        "failure_class": failure_class,
                        "query": str(summary.get("query") or ""),
                        "created_at": row["created_at"],
                    }
                )

        route_breakdown = [
            {
                "route": entry["route"],
                "count": entry["count"],
                "success_rate": (entry["completed"] / entry["count"]) if entry["count"] else 0.0,
                "failed": entry["failed"],
            }
            for entry in sorted(route_counts.values(), key=lambda item: (-item["count"], item["route"]))
        ]
        failure_breakdown = [
            {"failure_class": key, "count": count}
            for key, count in sorted(failure_counts.items(), key=lambda item: (-item[1], item[0]))
        ]

        return {
            "window_hours": bounded_window,
            "total_runs": total_runs,
            "completed_runs": completed_runs,
            "failed_runs": failed_runs,
            "success_rate": success_rate,
            "p50_latency_ms": p50_latency_ms,
            "p95_latency_ms": p95_latency_ms,
            "clarification_rate": (clarification_hits / total_runs) if total_runs else 0.0,
            "retrieval_miss_rate": (retrieval_misses / total_runs) if total_runs else 0.0,
            "route_breakdown": route_breakdown,
            "failure_breakdown": failure_breakdown,
            "recent_failures": recent_failures,
        }

    def _ensure_pool(self) -> None:
        if self._pool is None:
            raise RuntimeError("RunStore not initialized")

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

    @staticmethod
    def _percentile(values: list[float], percentile: float) -> float | None:
        if not values:
            return None
        if len(values) == 1:
            return float(values[0])
        position = max(0, min(len(values) - 1, int(round((len(values) - 1) * percentile))))
        return float(values[position])

    @classmethod
    def _row_to_run_summary(cls, row: asyncpg.Record) -> dict[str, Any]:
        return {
            "run_id": str(row["run_id"]),
            "run_type": row["run_type"],
            "status": row["status"],
            "route": row["route"],
            "connection_id": row["connection_id"],
            "conversation_id": row["conversation_id"],
            "correlation_id": row["correlation_id"],
            "failure_class": row["failure_class"],
            "confidence": row["confidence"],
            "warning_count": row["warning_count"],
            "error_count": row["error_count"],
            "latency_ms": row["latency_ms"],
            "summary": cls._decode_json_field(row["summary_json"]) or {},
            "started_at": row["started_at"],
            "completed_at": row["completed_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    @classmethod
    def _row_to_run_detail(cls, row: asyncpg.Record) -> dict[str, Any]:
        payload = cls._row_to_run_summary(row)
        payload["output"] = cls._decode_json_field(row["output_json"]) or {}
        return payload

    @classmethod
    def _row_to_step(cls, row: asyncpg.Record) -> dict[str, Any]:
        return {
            "step_id": str(row["step_id"]),
            "step_order": row["step_order"],
            "step_name": row["step_name"],
            "status": row["status"],
            "latency_ms": row["latency_ms"],
            "summary": cls._decode_json_field(row["summary_json"]) or {},
            "created_at": row["created_at"],
        }
