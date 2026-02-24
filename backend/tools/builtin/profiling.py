"""Built-in profiling tools."""

from __future__ import annotations

from typing import Any

from backend.database.manager import DatabaseConnectionManager
from backend.profiling.generator import DataPointGenerator
from backend.profiling.models import PendingDataPoint
from backend.profiling.profiler import SchemaProfiler
from backend.profiling.store import ProfilingStore
from backend.tools.base import ToolCategory, ToolContext, tool


async def _get_default_connection_id() -> str:
    manager = DatabaseConnectionManager()
    await manager.initialize()
    try:
        connection = await manager.get_default_connection()
        if not connection:
            raise ValueError("No default database connection configured.")
        return str(connection.connection_id)
    finally:
        await manager.close()


@tool(
    name="profile_and_generate_datapoints",
    description="Profile the database and generate pending DataPoints.",
    category=ToolCategory.PROFILING,
    requires_approval=True,
    max_execution_time_seconds=300,
)
async def profile_and_generate_datapoints(
    connection_id: str | None = None,
    depth: str = "metrics_basic",
    batch_size: int = 10,
    max_tables: int | None = None,
    sample_size: int = 100,
    max_columns_per_table: int = 100,
    query_timeout_seconds: int = 5,
    per_table_timeout_seconds: int = 20,
    total_timeout_seconds: int = 180,
    ctx: ToolContext | None = None,
) -> dict[str, Any]:
    if connection_id is None:
        connection_id = await _get_default_connection_id()

    manager = DatabaseConnectionManager()
    await manager.initialize()
    store = ProfilingStore()
    await store.initialize()
    try:
        profiler = SchemaProfiler(manager)
        profile = await profiler.profile_database(
            connection_id=connection_id,
            sample_size=sample_size,
            max_tables=max_tables,
            max_columns_per_table=max_columns_per_table,
            query_timeout_seconds=query_timeout_seconds,
            per_table_timeout_seconds=per_table_timeout_seconds,
            total_timeout_seconds=total_timeout_seconds,
        )
        await store.save_profile(profile)

        generator = DataPointGenerator()
        generated = await generator.generate_from_profile(
            profile,
            depth=depth,
            batch_size=batch_size,
            max_tables=max_tables,
        )
        pending = [
            PendingDataPoint(
                profile_id=profile.profile_id,
                datapoint=item.datapoint,
                confidence=item.confidence,
            )
            for item in generated.schema_datapoints + generated.business_datapoints
        ]
        if pending:
            await store.add_pending_datapoints(profile.profile_id, pending)
        return {
            "profile_id": str(profile.profile_id),
            "pending_count": len(pending),
            "depth": depth,
        }
    finally:
        await store.close()
        await manager.close()
