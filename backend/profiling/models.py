"""Profiling and DataPoint generation models."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class ProfilingLimits(BaseModel):
    """Safety and budget limits applied during profiling."""

    sample_size: int = Field(default=100, ge=1, le=1000)
    max_tables: int | None = Field(default=50, ge=1, le=500)
    max_columns_per_table: int = Field(default=100, ge=1, le=500)
    query_timeout_seconds: int = Field(default=5, ge=1, le=60)
    per_table_timeout_seconds: int = Field(default=20, ge=1, le=300)
    total_timeout_seconds: int = Field(default=180, ge=10, le=1800)
    fail_fast: bool = False


class ColumnProfile(BaseModel):
    """Profile for a database column."""

    name: str
    data_type: str
    nullable: bool
    default_value: str | None = None
    sample_values: list[str] = Field(default_factory=list)
    null_count: int | None = None
    distinct_count: int | None = None
    min_value: str | None = None
    max_value: str | None = None


class RelationshipProfile(BaseModel):
    """Profiled relationship between tables."""

    source_table: str
    source_column: str
    target_table: str
    target_column: str
    relationship_type: Literal["foreign_key", "heuristic"] = "foreign_key"
    cardinality: Literal["1:1", "1:N", "N:1", "N:N"] = "N:1"


class TableProfile(BaseModel):
    """Profile for a database table."""

    schema_name: str = Field(..., alias="schema")
    name: str
    row_count: int | None
    columns: list[ColumnProfile]
    relationships: list[RelationshipProfile] = Field(default_factory=list)
    sample_size: int
    status: Literal["completed", "partial", "failed"] = "completed"
    error: str | None = None
    warnings: list[str] = Field(default_factory=list)
    profiled_column_count: int | None = None
    sampled_column_count: int | None = None

    model_config = ConfigDict(populate_by_name=True)


class DatabaseProfile(BaseModel):
    """Profile for a database connection."""

    profile_id: UUID = Field(default_factory=uuid4)
    connection_id: UUID
    tables: list[TableProfile]
    profiling_limits: ProfilingLimits = Field(default_factory=ProfilingLimits)
    total_tables_discovered: int = 0
    tables_profiled: int = 0
    tables_failed: int = 0
    tables_skipped: int = 0
    partial_failures: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    stats_cache: dict[str, dict[str, Any]] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ProfilingProgress(BaseModel):
    """Progress tracking for a profiling job."""

    total_tables: int
    tables_completed: int
    tables_failed: int = 0
    tables_skipped: int = 0


class GenerationProgress(BaseModel):
    """Progress tracking for DataPoint generation."""

    total_tables: int
    tables_completed: int
    batch_size: int


class ProfilingJob(BaseModel):
    """Profiling job status."""

    job_id: UUID = Field(default_factory=uuid4)
    connection_id: UUID
    status: Literal["pending", "running", "completed", "failed"] = "pending"
    progress: ProfilingProgress | None = None
    error: str | None = None
    profile_id: UUID | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GenerationJob(BaseModel):
    """DataPoint generation job status."""

    job_id: UUID = Field(default_factory=uuid4)
    profile_id: UUID
    status: Literal["pending", "running", "completed", "failed"] = "pending"
    progress: GenerationProgress | None = None
    error: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class GeneratedDataPoint(BaseModel):
    """Generated DataPoint candidate with confidence score."""

    datapoint: dict
    confidence: float = Field(ge=0.0, le=1.0)
    explanation: str | None = None


class GeneratedDataPoints(BaseModel):
    """Collection of generated DataPoints."""

    profile_id: UUID
    schema_datapoints: list[GeneratedDataPoint]
    business_datapoints: list[GeneratedDataPoint]


class PendingDataPoint(BaseModel):
    """Pending DataPoint awaiting approval."""

    pending_id: UUID = Field(default_factory=uuid4)
    profile_id: UUID
    datapoint: dict
    confidence: float = Field(ge=0.0, le=1.0)
    status: Literal["pending", "approved", "rejected"] = "pending"
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    reviewed_at: datetime | None = None
    review_note: str | None = None
