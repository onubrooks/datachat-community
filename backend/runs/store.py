"""Persistence for AI workflow runs and ordered steps."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from statistics import median
from typing import Any
from uuid import UUID, uuid4

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

_CREATE_QUALITY_FINDINGS_TABLE = """
CREATE TABLE IF NOT EXISTS ai_quality_findings (
    finding_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES ai_runs(run_id) ON DELETE CASCADE,
    finding_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    category TEXT NOT NULL,
    code TEXT NOT NULL,
    message TEXT NOT NULL,
    entity_type TEXT,
    entity_id TEXT,
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL
);
"""

_CREATE_QUALITY_FINDINGS_RUN_INDEX = """
CREATE INDEX IF NOT EXISTS ai_quality_findings_run_id_idx
ON ai_quality_findings (run_id, created_at DESC);
"""

_CREATE_QUALITY_FINDINGS_CREATED_INDEX = """
CREATE INDEX IF NOT EXISTS ai_quality_findings_created_at_idx
ON ai_quality_findings (created_at DESC);
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
        await self._pool.execute(_CREATE_QUALITY_FINDINGS_TABLE)
        await self._pool.execute(_CREATE_QUALITY_FINDINGS_RUN_INDEX)
        await self._pool.execute(_CREATE_QUALITY_FINDINGS_CREATED_INDEX)

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
        quality_findings: list[dict[str, Any]] | None = None,
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
        if steps:
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
        await self._pool.execute("DELETE FROM ai_quality_findings WHERE run_id = $1", run_id)
        if not quality_findings:
            return

        finding_payload = []
        for item in quality_findings:
            normalized = self._normalize_quality_finding(item, created_at=now)
            finding_payload.append(
                (
                    normalized["finding_id"],
                    run_id,
                    normalized["finding_type"],
                    normalized["severity"],
                    normalized["category"],
                    normalized["code"],
                    normalized["message"],
                    normalized["entity_type"],
                    normalized["entity_id"],
                    json.dumps(normalized["details"]),
                    normalized["created_at"],
                )
            )
        await self._pool.executemany(
            """
            INSERT INTO ai_quality_findings (
                finding_id,
                run_id,
                finding_type,
                severity,
                category,
                code,
                message,
                entity_type,
                entity_id,
                details_json,
                created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11)
            """,
            finding_payload,
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
        findings = await self._pool.fetch(
            """
            SELECT
                run_id,
                finding_id,
                finding_type,
                severity,
                category,
                code,
                message,
                entity_type,
                entity_id,
                details_json,
                created_at
            FROM ai_quality_findings
            WHERE run_id = $1
            ORDER BY created_at ASC
            """,
            run_id,
        )
        payload = self._row_to_run_detail(row)
        payload["steps"] = [self._row_to_step(item) for item in steps]
        payload["quality_findings"] = [self._row_to_quality_finding(item) for item in findings]
        return payload

    async def compare_runs(
        self, primary_run_id: UUID, comparison_run_id: UUID
    ) -> dict[str, Any] | None:
        primary_run = await self.get_run(primary_run_id)
        comparison_run = await self.get_run(comparison_run_id)
        if primary_run is None or comparison_run is None:
            return None

        primary_sql = self._extract_generated_sql_from_run(primary_run)
        comparison_sql = self._extract_generated_sql_from_run(comparison_run)
        primary_datapoints = self._extract_selected_datapoints_from_run(primary_run)
        comparison_datapoints = self._extract_selected_datapoints_from_run(comparison_run)

        primary_datapoint_map = {
            self._datapoint_identity(item): item for item in primary_datapoints
        }
        comparison_datapoint_map = {
            self._datapoint_identity(item): item for item in comparison_datapoints
        }

        primary_findings = primary_run.get("quality_findings", [])
        comparison_findings = comparison_run.get("quality_findings", [])
        primary_finding_map = {
            self._quality_identity(item): item for item in primary_findings
        }
        comparison_finding_map = {
            self._quality_identity(item): item for item in comparison_findings
        }

        primary_confidence = primary_run.get("confidence")
        comparison_confidence = comparison_run.get("confidence")
        confidence_delta = (
            float(comparison_confidence) - float(primary_confidence)
            if isinstance(primary_confidence, (int, float))
            and isinstance(comparison_confidence, (int, float))
            else None
        )

        primary_latency = primary_run.get("latency_ms")
        comparison_latency = comparison_run.get("latency_ms")
        latency_delta_ms = (
            float(comparison_latency) - float(primary_latency)
            if isinstance(primary_latency, (int, float))
            and isinstance(comparison_latency, (int, float))
            else None
        )

        return {
            "primary_run": primary_run,
            "comparison_run": comparison_run,
            "diff": {
                "sql_changed": (primary_sql or "").strip() != (comparison_sql or "").strip(),
                "primary_sql": primary_sql,
                "comparison_sql": comparison_sql,
                "confidence_delta": confidence_delta,
                "latency_delta_ms": latency_delta_ms,
                "warning_delta": int(comparison_run.get("warning_count", 0))
                - int(primary_run.get("warning_count", 0)),
                "error_delta": int(comparison_run.get("error_count", 0))
                - int(primary_run.get("error_count", 0)),
                "status_changed": primary_run.get("status") != comparison_run.get("status"),
                "failure_class_changed": primary_run.get("failure_class")
                != comparison_run.get("failure_class"),
                "datapoints_added": [
                    item
                    for key, item in comparison_datapoint_map.items()
                    if key not in primary_datapoint_map
                ],
                "datapoints_removed": [
                    item
                    for key, item in primary_datapoint_map.items()
                    if key not in comparison_datapoint_map
                ],
                "quality_findings_added": [
                    item
                    for key, item in comparison_finding_map.items()
                    if key not in primary_finding_map
                ],
                "quality_findings_resolved": [
                    item
                    for key, item in primary_finding_map.items()
                    if key not in comparison_finding_map
                ],
            },
        }

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
        finding_rows = await self._pool.fetch(
            """
            SELECT severity, category, code, COUNT(*) AS finding_count
            FROM ai_quality_findings
            WHERE created_at >= NOW() - ($1::int * INTERVAL '1 hour')
            GROUP BY severity, category, code
            ORDER BY finding_count DESC, code ASC
            LIMIT 12
            """,
            bounded_window,
        )

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
            "quality_breakdown": [
                {
                    "severity": row["severity"],
                    "category": row["category"],
                    "code": row["code"],
                    "count": row["finding_count"],
                }
                for row in finding_rows
            ],
        }

    async def summarize_run_trends(
        self,
        *,
        window_hours: int = 24,
        bucket_hours: int = 1,
    ) -> dict[str, Any]:
        self._ensure_pool()
        bounded_window = max(1, min(window_hours, 24 * 30))
        bounded_bucket = max(1, min(bucket_hours, bounded_window))
        rows = await self._pool.fetch(
            """
            SELECT
                run_id,
                status,
                latency_ms,
                summary_json,
                created_at
            FROM ai_runs
            WHERE created_at >= NOW() - ($1::int * INTERVAL '1 hour')
            ORDER BY created_at ASC
            """,
            bounded_window,
        )

        buckets: dict[datetime, dict[str, Any]] = {}
        for row in rows:
            created_at = row["created_at"]
            bucket_start = created_at.replace(
                minute=0,
                second=0,
                microsecond=0,
                hour=(created_at.hour // bounded_bucket) * bounded_bucket,
            )
            bucket = buckets.setdefault(
                bucket_start,
                {
                    "bucket_start": bucket_start,
                    "total_runs": 0,
                    "failed_runs": 0,
                    "latencies": [],
                    "clarification_runs": 0,
                    "retrieval_miss_runs": 0,
                },
            )
            bucket["total_runs"] += 1
            if row["status"] == "failed":
                bucket["failed_runs"] += 1
            if isinstance(row["latency_ms"], (int, float)):
                bucket["latencies"].append(float(row["latency_ms"]))
            summary = self._decode_json_field(row["summary_json"]) or {}
            if isinstance(summary, dict):
                if bool(summary.get("clarification_needed")):
                    bucket["clarification_runs"] += 1
                if int(summary.get("retrieved_datapoint_count", 0) or 0) == 0:
                    bucket["retrieval_miss_runs"] += 1

        trend = []
        for bucket in sorted(buckets.values(), key=lambda item: item["bucket_start"]):
            total_runs = bucket["total_runs"]
            latencies = sorted(bucket["latencies"])
            trend.append(
                {
                    "bucket_start": bucket["bucket_start"],
                    "total_runs": total_runs,
                    "failed_runs": bucket["failed_runs"],
                    "success_rate": ((total_runs - bucket["failed_runs"]) / total_runs)
                    if total_runs
                    else 0.0,
                    "p50_latency_ms": median(latencies) if latencies else None,
                    "clarification_rate": (bucket["clarification_runs"] / total_runs)
                    if total_runs
                    else 0.0,
                    "retrieval_miss_rate": (bucket["retrieval_miss_runs"] / total_runs)
                    if total_runs
                    else 0.0,
                }
            )

        return {
            "window_hours": bounded_window,
            "bucket_hours": bounded_bucket,
            "trend": trend,
        }

    async def summarize_quality(self, *, window_hours: int = 24) -> dict[str, Any]:
        self._ensure_pool()
        bounded_window = max(1, min(window_hours, 24 * 30))
        finding_rows = await self._pool.fetch(
            """
            SELECT
                q.finding_id,
                q.run_id,
                q.finding_type,
                q.severity,
                q.category,
                q.code,
                q.message,
                q.entity_type,
                q.entity_id,
                q.details_json,
                q.created_at,
                r.route,
                r.summary_json
            FROM ai_quality_findings q
            JOIN ai_runs r ON r.run_id = q.run_id
            WHERE q.created_at >= NOW() - ($1::int * INTERVAL '1 hour')
            ORDER BY q.created_at DESC
            """,
            bounded_window,
        )

        findings = [self._row_to_quality_finding(row) for row in finding_rows]
        total_findings = len(findings)
        runs_with_findings = len({item["run_id"] for item in findings})
        severity_counts: dict[str, int] = {}
        code_counts: dict[str, dict[str, Any]] = {}
        category_counts: dict[str, int] = {}
        recent_findings: list[dict[str, Any]] = []

        for finding, row in zip(findings, finding_rows, strict=False):
            severity = finding["severity"]
            category = finding["category"]
            code = finding["code"]
            severity_counts[severity] = severity_counts.get(severity, 0) + 1
            category_counts[category] = category_counts.get(category, 0) + 1
            code_entry = code_counts.setdefault(
                code,
                {
                    "code": code,
                    "severity": severity,
                    "category": category,
                    "count": 0,
                },
            )
            code_entry["count"] += 1

            if len(recent_findings) < 25:
                summary = self._decode_json_field(row["summary_json"]) or {}
                recent_findings.append(
                    {
                        **finding,
                        "route": row["route"] or "unknown",
                        "query": summary.get("query") if isinstance(summary, dict) else None,
                    }
                )

        return {
            "window_hours": bounded_window,
            "total_findings": total_findings,
            "runs_with_findings": runs_with_findings,
            "severity_breakdown": [
                {"severity": key, "count": count}
                for key, count in sorted(severity_counts.items(), key=lambda item: (-item[1], item[0]))
            ],
            "category_breakdown": [
                {"category": key, "count": count}
                for key, count in sorted(category_counts.items(), key=lambda item: (-item[1], item[0]))
            ],
            "code_breakdown": sorted(
                code_counts.values(),
                key=lambda item: (-int(item["count"]), str(item["code"])),
            ),
            "recent_findings": recent_findings,
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

    @staticmethod
    def _extract_generated_sql_from_run(run: dict[str, Any]) -> str | None:
        steps = run.get("steps", [])
        if not isinstance(steps, list):
            return None
        sql_step = next(
            (step for step in steps if isinstance(step, dict) and step.get("step_name") == "sql"),
            steps[0] if steps else None,
        )
        if not isinstance(sql_step, dict):
            return None
        summary = sql_step.get("summary", {})
        if isinstance(summary, dict):
            outputs = summary.get("outputs")
            if isinstance(outputs, dict):
                generated_sql = outputs.get("generated_sql")
                if isinstance(generated_sql, str) and generated_sql.strip():
                    return generated_sql
            direct = summary.get("generated_sql")
            if isinstance(direct, str) and direct.strip():
                return direct
        return None

    @staticmethod
    def _extract_selected_datapoints_from_run(
        run: dict[str, Any],
    ) -> list[dict[str, Any]]:
        output = run.get("output", {})
        if not isinstance(output, dict):
            return []
        retrieval_trace = output.get("retrieval_trace", {})
        if not isinstance(retrieval_trace, dict):
            return []
        selected = retrieval_trace.get("selected_datapoints", [])
        if not isinstance(selected, list):
            return []
        return [item for item in selected if isinstance(item, dict)]

    @staticmethod
    def _datapoint_identity(item: dict[str, Any]) -> str:
        datapoint_id = item.get("datapoint_id")
        if isinstance(datapoint_id, str) and datapoint_id.strip():
            return datapoint_id
        name = item.get("name")
        if isinstance(name, str) and name.strip():
            return name
        return json.dumps(item, sort_keys=True, default=str)

    @staticmethod
    def _quality_identity(item: dict[str, Any]) -> str:
        return f"{item.get('code')}::{item.get('message')}"

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

    @classmethod
    def _row_to_quality_finding(cls, row: asyncpg.Record) -> dict[str, Any]:
        return {
            "finding_id": str(row["finding_id"]),
            "run_id": str(row["run_id"]),
            "finding_type": row["finding_type"],
            "severity": row["severity"],
            "category": row["category"],
            "code": row["code"],
            "message": row["message"],
            "entity_type": row["entity_type"],
            "entity_id": row["entity_id"],
            "details": cls._decode_json_field(row["details_json"]) or {},
            "created_at": row["created_at"],
        }

    @staticmethod
    def _normalize_quality_finding(
        finding: dict[str, Any],
        *,
        created_at: datetime,
    ) -> dict[str, Any]:
        finding_id = finding.get("finding_id")
        return {
            "finding_id": UUID(str(finding_id)) if finding_id else uuid4(),
            "finding_type": str(finding.get("finding_type") or "advisory"),
            "severity": str(finding.get("severity") or "info"),
            "category": str(finding.get("category") or "general"),
            "code": str(finding.get("code") or "unspecified"),
            "message": str(finding.get("message") or "Quality finding recorded."),
            "entity_type": (
                str(finding["entity_type"]) if finding.get("entity_type") is not None else None
            ),
            "entity_id": str(finding["entity_id"]) if finding.get("entity_id") is not None else None,
            "details": finding.get("details") if isinstance(finding.get("details"), dict) else {},
            "created_at": finding.get("created_at") or created_at,
        }
