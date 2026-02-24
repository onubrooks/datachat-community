"""Schema profiling utilities."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from time import monotonic
from urllib.parse import urlparse

import asyncpg

from backend.database.manager import DatabaseConnectionManager
from backend.profiling.cache import write_profile_cache
from backend.profiling.models import (
    ColumnProfile,
    DatabaseProfile,
    ProfilingLimits,
    RelationshipProfile,
    TableProfile,
)
from backend.profiling.query_templates import (
    get_profiling_templates,
    normalize_database_type,
    supported_profile_template_databases,
)


class SchemaProfiler:
    """Profiles database schemas and samples data."""

    def __init__(self, manager: DatabaseConnectionManager) -> None:
        self._manager = manager

    async def profile_database(
        self,
        connection_id: str,
        sample_size: int = 100,
        tables: Sequence[str] | None = None,
        progress_callback: Callable[[int, int], Awaitable[None] | None] | None = None,
        *,
        max_tables: int | None = 50,
        max_columns_per_table: int = 100,
        query_timeout_seconds: int = 5,
        per_table_timeout_seconds: int = 20,
        total_timeout_seconds: int = 180,
        fail_fast: bool = False,
    ) -> DatabaseProfile:
        connection = await self._manager.get_connection(connection_id)
        db_url = connection.database_url.get_secret_value()
        parsed = urlparse(db_url.replace("postgresql+asyncpg://", "postgresql://"))
        db_type = normalize_database_type(connection.database_type or parsed.scheme)
        templates = get_profiling_templates(db_type)
        if templates is None:
            supported = ", ".join(supported_profile_template_databases())
            raise ValueError(
                f"Profiling templates are not configured for database type '{db_type}'. "
                f"Supported template sets: {supported}."
            )
        if db_type != "postgresql":
            raise ValueError(
                "Schema profiling execution currently supports PostgreSQL only. "
                f"Template set for '{db_type}' is present for upcoming connector support."
            )
        if not parsed.hostname:
            raise ValueError("Invalid database URL.")

        limits = ProfilingLimits(
            sample_size=sample_size,
            max_tables=max_tables,
            max_columns_per_table=max_columns_per_table,
            query_timeout_seconds=query_timeout_seconds,
            per_table_timeout_seconds=per_table_timeout_seconds,
            total_timeout_seconds=total_timeout_seconds,
            fail_fast=fail_fast,
        )

        conn = await asyncpg.connect(dsn=db_url.replace("postgresql+asyncpg://", "postgresql://"))
        try:
            await conn.execute(f"SET statement_timeout = {limits.query_timeout_seconds * 1000}")

            table_rows, total_discovered = await self._fetch_tables(
                conn,
                tables,
                templates.list_tables,
                max_tables=limits.max_tables,
                query_timeout_seconds=limits.query_timeout_seconds,
            )
            table_profiles: list[TableProfile] = []
            partial_failures: list[str] = []
            warnings: list[str] = []
            stats_cache: dict[str, dict[str, object]] = {}

            total_tables = len(table_rows)
            completed = 0
            failed = 0
            skipped = max(total_discovered - total_tables, 0)
            started_at = monotonic()

            for row in table_rows:
                schema = row["table_schema"]
                table = row["table_name"]
                table_key = f"{schema}.{table}"

                elapsed = monotonic() - started_at
                if elapsed >= limits.total_timeout_seconds:
                    remaining = total_tables - (completed + failed)
                    skipped += max(remaining, 0)
                    warnings.append(
                        "Profiling stopped early after hitting total timeout "
                        f"({limits.total_timeout_seconds}s)."
                    )
                    break

                try:
                    profile = await asyncio.wait_for(
                        self._profile_table(
                            conn=conn,
                            schema=schema,
                            table=table,
                            sample_size=limits.sample_size,
                            max_columns_per_table=limits.max_columns_per_table,
                            query_timeout_seconds=limits.query_timeout_seconds,
                            templates=templates,
                        ),
                        timeout=limits.per_table_timeout_seconds,
                    )
                    table_profiles.append(profile)
                    if profile.status == "failed":
                        failed += 1
                        partial_failures.append(
                            f"{table_key}: {profile.error or 'profiling failed'}"
                        )
                    else:
                        completed += 1
                        stats_cache[table_key] = {
                            "row_count": profile.row_count,
                            "status": profile.status,
                            "columns": [
                                {
                                    "name": column.name,
                                    "data_type": column.data_type,
                                    "null_count": column.null_count,
                                    "distinct_count": column.distinct_count,
                                    "sample_values": column.sample_values[:3],
                                }
                                for column in profile.columns[:20]
                            ],
                        }
                        if profile.status == "partial":
                            partial_failures.append(
                                f"{table_key}: partial profile ({'; '.join(profile.warnings[:2])})"
                            )
                except TimeoutError:
                    failed += 1
                    timeout_msg = (
                        f"{table_key}: profiling timed out after "
                        f"{limits.per_table_timeout_seconds}s"
                    )
                    partial_failures.append(timeout_msg)
                    table_profiles.append(
                        TableProfile(
                            schema=schema,
                            name=table,
                            row_count=None,
                            columns=[],
                            relationships=[],
                            sample_size=limits.sample_size,
                            status="failed",
                            error=timeout_msg,
                            warnings=[timeout_msg],
                            profiled_column_count=0,
                            sampled_column_count=0,
                        )
                    )
                except Exception as exc:
                    failed += 1
                    error_msg = f"{table_key}: {exc}"
                    partial_failures.append(error_msg)
                    table_profiles.append(
                        TableProfile(
                            schema=schema,
                            name=table,
                            row_count=None,
                            columns=[],
                            relationships=[],
                            sample_size=limits.sample_size,
                            status="failed",
                            error=str(exc),
                            warnings=[str(exc)],
                            profiled_column_count=0,
                            sampled_column_count=0,
                        )
                    )

                if limits.fail_fast and failed:
                    remaining = total_tables - (completed + failed)
                    skipped += max(remaining, 0)
                    warnings.append("Fail-fast mode stopped profiling after the first table failure.")
                    break

                await self._emit_progress(
                    progress_callback,
                    total_tables,
                    completed,
                    failed,
                    skipped,
                )

            profile = DatabaseProfile(
                connection_id=connection.connection_id,
                tables=table_profiles,
                profiling_limits=limits,
                total_tables_discovered=total_discovered,
                tables_profiled=completed,
                tables_failed=failed,
                tables_skipped=skipped,
                partial_failures=partial_failures,
                warnings=warnings,
                stats_cache=stats_cache,
            )
            self._write_profile_cache(profile=profile, database_type=db_type, database_url=db_url)
            return profile
        finally:
            await conn.close()

    async def _profile_table(
        self,
        *,
        conn: asyncpg.Connection,
        schema: str,
        table: str,
        sample_size: int,
        max_columns_per_table: int,
        query_timeout_seconds: int,
        templates,
    ) -> TableProfile:
        columns, table_warnings, was_partial = await self._fetch_columns(
            conn,
            schema,
            table,
            sample_size=sample_size,
            max_columns_per_table=max_columns_per_table,
            query_timeout_seconds=query_timeout_seconds,
            columns_query=templates.list_columns,
            stats_query=templates.column_stats,
        )
        row_count = await self._estimate_row_count(
            conn,
            schema,
            table,
            query_timeout_seconds=query_timeout_seconds,
            row_estimate_query=templates.row_estimate,
        )
        relationships = await self._fetch_relationships(
            conn,
            schema,
            table,
            query_timeout_seconds=query_timeout_seconds,
            relationships_query=templates.relationships,
        )

        status = "partial" if was_partial else "completed"
        return TableProfile(
            schema=schema,
            name=table,
            row_count=row_count,
            columns=columns,
            relationships=relationships,
            sample_size=sample_size,
            status=status,
            warnings=table_warnings,
            profiled_column_count=len(columns),
            sampled_column_count=min(len(columns), max_columns_per_table),
        )

    async def _fetch_tables(
        self,
        conn: asyncpg.Connection,
        tables: Sequence[str] | None,
        base_query: str,
        *,
        max_tables: int | None,
        query_timeout_seconds: int,
    ) -> tuple[list[asyncpg.Record], int]:
        scoped_query = (
            "SELECT table_schema, table_name "
            f"FROM ({base_query}) AS scoped_tables"
        )

        if tables:
            rows = await conn.fetch(
                scoped_query + " WHERE table_name = ANY($1) ORDER BY table_schema, table_name",
                list(tables),
                timeout=query_timeout_seconds,
            )
            return list(rows), len(rows)

        if max_tables is None:
            rows = await conn.fetch(
                scoped_query + " ORDER BY table_schema, table_name",
                timeout=query_timeout_seconds,
            )
            return list(rows), len(rows)

        total_row = await conn.fetchrow(
            "SELECT COUNT(*) AS total FROM (" + scoped_query + ") AS scoped_tables_count",
            timeout=query_timeout_seconds,
        )
        rows = await conn.fetch(
            scoped_query + " ORDER BY table_schema, table_name LIMIT $1",
            max_tables,
            timeout=query_timeout_seconds,
        )
        discovered = int(total_row["total"]) if total_row else len(rows)
        return list(rows), discovered

    async def _fetch_columns(
        self,
        conn: asyncpg.Connection,
        schema: str,
        table: str,
        *,
        sample_size: int,
        max_columns_per_table: int,
        query_timeout_seconds: int,
        columns_query: str,
        stats_query: str | None,
    ) -> tuple[list[ColumnProfile], list[str], bool]:
        column_rows = await conn.fetch(
            columns_query,
            schema,
            table,
            timeout=query_timeout_seconds,
        )
        warnings: list[str] = []
        was_partial = False
        rows = list(column_rows)
        if len(rows) > max_columns_per_table:
            rows = rows[:max_columns_per_table]
            was_partial = True
            warnings.append(
                f"Column profiling capped at {max_columns_per_table} columns for {schema}.{table}."
            )

        columns: list[ColumnProfile] = []
        for row in rows:
            column_name = row["column_name"]
            column_warnings: list[str] = []
            stats = await self._fetch_column_stats(
                conn,
                schema,
                table,
                column_name,
                query_timeout_seconds=query_timeout_seconds,
                stats_query=stats_query,
            )
            if not stats:
                was_partial = True
                column_warnings.append(f"{column_name}: stats unavailable")
            samples = await self._fetch_column_samples(
                conn,
                schema,
                table,
                column_name,
                sample_size=sample_size,
                query_timeout_seconds=query_timeout_seconds,
            )
            if not samples:
                was_partial = True
                column_warnings.append(f"{column_name}: sample values unavailable")
            warnings.extend(column_warnings)
            columns.append(
                ColumnProfile(
                    name=column_name,
                    data_type=row["data_type"],
                    nullable=row["is_nullable"] == "YES",
                    default_value=row["column_default"],
                    sample_values=samples,
                    null_count=stats.get("null_count"),
                    distinct_count=stats.get("distinct_count"),
                    min_value=stats.get("min_value"),
                    max_value=stats.get("max_value"),
                )
            )
        return columns, warnings, was_partial

    async def _fetch_column_stats(
        self,
        conn: asyncpg.Connection,
        schema: str,
        table: str,
        column: str,
        *,
        query_timeout_seconds: int,
        stats_query: str | None,
    ) -> dict[str, str | int | None]:
        if not stats_query:
            return {}
        qualified_table = f"{self._quote_identifier(schema)}.{self._quote_identifier(table)}"
        qualified_column = self._quote_identifier(column)
        query = stats_query.format(column=qualified_column, table=qualified_table)
        try:
            row = await conn.fetchrow(query, timeout=query_timeout_seconds)
        except Exception:
            row = None

        return {
            "null_count": row["null_count"] if row else None,
            "distinct_count": row["distinct_count"] if row else None,
            "min_value": row["min_value"] if row else None,
            "max_value": row["max_value"] if row else None,
        }

    async def _fetch_column_samples(
        self,
        conn: asyncpg.Connection,
        schema: str,
        table: str,
        column: str,
        *,
        sample_size: int,
        query_timeout_seconds: int,
    ) -> list[str]:
        qualified_table = f"{self._quote_identifier(schema)}.{self._quote_identifier(table)}"
        qualified_column = self._quote_identifier(column)
        query = (
            f"SELECT {qualified_column}::text AS value "
            f"FROM {qualified_table} "
            f"WHERE {qualified_column} IS NOT NULL "
            f"LIMIT {sample_size}"
        )
        try:
            rows = await conn.fetch(query, timeout=query_timeout_seconds)
        except Exception:
            return []
        return [row["value"] for row in rows if row["value"] is not None]

    async def _estimate_row_count(
        self,
        conn: asyncpg.Connection,
        schema: str,
        table: str,
        *,
        query_timeout_seconds: int,
        row_estimate_query: str | None,
    ) -> int | None:
        if not row_estimate_query:
            return None
        try:
            row = await conn.fetchrow(
                row_estimate_query,
                schema,
                table,
                timeout=query_timeout_seconds,
            )
        except Exception:
            row = None
        if row is None:
            return None
        return int(row["estimate"])

    async def _fetch_relationships(
        self,
        conn: asyncpg.Connection,
        schema: str,
        table: str,
        *,
        query_timeout_seconds: int,
        relationships_query: str | None,
    ) -> list[RelationshipProfile]:
        if not relationships_query:
            return []
        try:
            rows = await conn.fetch(
                relationships_query,
                schema,
                table,
                timeout=query_timeout_seconds,
            )
        except Exception:
            rows = []

        relationships: list[RelationshipProfile] = []
        for row in rows:
            relationships.append(
                RelationshipProfile(
                    source_table=row["source_table"],
                    source_column=row["source_column"],
                    target_table=row["target_table"],
                    target_column=row["target_column"],
                    relationship_type="foreign_key",
                    cardinality="N:1",
                )
            )
        return relationships

    async def _emit_progress(
        self,
        callback: Callable[[int, int], Awaitable[None] | None] | None,
        total_tables: int,
        tables_completed: int,
        tables_failed: int,
        tables_skipped: int,
    ) -> None:
        if callback is None:
            return
        try:
            result = callback(
                total_tables,
                tables_completed,
                tables_failed,
                tables_skipped,
            )
        except TypeError:
            result = callback(total_tables, tables_completed)
        if asyncio.iscoroutine(result):
            await result

    def _write_profile_cache(
        self,
        *,
        profile: DatabaseProfile,
        database_type: str,
        database_url: str,
    ) -> None:
        payload = {
            "database_type": database_type,
            "connection_id": str(profile.connection_id),
            "created_at": profile.created_at.isoformat(),
            "total_tables_discovered": profile.total_tables_discovered,
            "tables_profiled": profile.tables_profiled,
            "tables_failed": profile.tables_failed,
            "tables_skipped": profile.tables_skipped,
            "partial_failures": profile.partial_failures[:100],
            "limits": profile.profiling_limits.model_dump(),
            "tables": [],
        }
        for table in profile.tables:
            payload["tables"].append(
                {
                    "name": f"{table.schema_name}.{table.name}",
                    "status": table.status,
                    "row_count": table.row_count,
                    "warnings": table.warnings[:10],
                    "columns": [
                        {
                            "name": column.name,
                            "data_type": column.data_type,
                            "null_count": column.null_count,
                            "distinct_count": column.distinct_count,
                            "sample_values": column.sample_values[:3],
                        }
                        for column in table.columns[:20]
                    ],
                }
            )
        write_profile_cache(
            database_type=database_type,
            database_url=database_url,
            payload=payload,
        )

    @staticmethod
    def _quote_identifier(value: str) -> str:
        escaped = value.replace('"', '""')
        return f'"{escaped}"'
