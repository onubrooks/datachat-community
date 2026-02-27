"""Schema profiling utilities."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from time import monotonic
from urllib.parse import urlparse

import asyncpg

from backend.connectors.base import BaseConnector, TableInfo
from backend.connectors.factory import create_connector
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

        if db_type != "postgresql":
            return await self._profile_database_with_connector(
                connection_id=connection.connection_id,
                db_url=db_url,
                db_type=db_type,
                tables=tables,
                progress_callback=progress_callback,
                limits=limits,
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

    async def _profile_database_with_connector(
        self,
        *,
        connection_id,
        db_url: str,
        db_type: str,
        tables: Sequence[str] | None,
        progress_callback: Callable[[int, int], Awaitable[None] | None] | None,
        limits: ProfilingLimits,
    ) -> DatabaseProfile:
        connector = create_connector(
            database_url=db_url,
            database_type=db_type,
            timeout=limits.query_timeout_seconds,
        )
        await connector.connect()
        try:
            discovered_tables = await connector.get_schema()
            discovered_tables = sorted(
                discovered_tables,
                key=lambda table: (table.schema_name.lower(), table.table_name.lower()),
            )

            if tables:
                requested = {name.strip().lower() for name in tables if name.strip()}
                scoped_tables = [
                    table_info
                    for table_info in discovered_tables
                    if self._table_matches_selection(
                        table_info.schema_name,
                        table_info.table_name,
                        requested,
                    )
                ]
            else:
                scoped_tables = discovered_tables

            total_discovered = len(scoped_tables)
            if limits.max_tables is not None:
                table_infos = scoped_tables[: limits.max_tables]
            else:
                table_infos = scoped_tables

            table_profiles: list[TableProfile] = []
            partial_failures: list[str] = []
            warnings: list[str] = []
            stats_cache: dict[str, dict[str, object]] = {}

            total_tables = len(table_infos)
            completed = 0
            failed = 0
            skipped = max(total_discovered - total_tables, 0)
            started_at = monotonic()

            for table_info in table_infos:
                schema = table_info.schema_name
                table = table_info.table_name
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
                        self._profile_table_with_connector(
                            connector=connector,
                            db_type=db_type,
                            table_info=table_info,
                            sample_size=limits.sample_size,
                            max_columns_per_table=limits.max_columns_per_table,
                            query_timeout_seconds=limits.query_timeout_seconds,
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
                connection_id=connection_id,
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
            await connector.close()

    async def _profile_table_with_connector(
        self,
        *,
        connector: BaseConnector,
        db_type: str,
        table_info: TableInfo,
        sample_size: int,
        max_columns_per_table: int,
        query_timeout_seconds: int,
    ) -> TableProfile:
        schema = table_info.schema_name
        table = table_info.table_name
        row_count = table_info.row_count if table_info.row_count is None else max(table_info.row_count, 0)
        relationships: list[RelationshipProfile] = []
        warnings: list[str] = []
        was_partial = False

        columns_meta = list(table_info.columns)
        if len(columns_meta) > max_columns_per_table:
            columns_meta = columns_meta[:max_columns_per_table]
            was_partial = True
            warnings.append(
                f"Column profiling capped at {max_columns_per_table} columns for {schema}.{table}."
            )

        columns: list[ColumnProfile] = []
        for column in columns_meta:
            if (
                column.is_foreign_key
                and column.foreign_table
                and column.foreign_column
            ):
                relationships.append(
                    RelationshipProfile(
                        source_table=table,
                        source_column=column.name,
                        target_table=column.foreign_table,
                        target_column=column.foreign_column,
                        relationship_type="foreign_key",
                        cardinality="N:1",
                    )
                )

            stats = await self._fetch_column_stats_with_connector(
                connector,
                db_type=db_type,
                schema=schema,
                table=table,
                column=column.name,
                query_timeout_seconds=query_timeout_seconds,
            )
            if not stats:
                was_partial = True
                warnings.append(f"{column.name}: stats unavailable")

            samples = await self._fetch_column_samples_with_connector(
                connector,
                db_type=db_type,
                schema=schema,
                table=table,
                column=column.name,
                sample_size=sample_size,
                query_timeout_seconds=query_timeout_seconds,
            )
            if not samples:
                was_partial = True
                warnings.append(f"{column.name}: sample values unavailable")

            columns.append(
                ColumnProfile(
                    name=column.name,
                    data_type=column.data_type,
                    nullable=column.is_nullable,
                    default_value=column.default_value,
                    sample_values=samples,
                    null_count=stats.get("null_count"),
                    distinct_count=stats.get("distinct_count"),
                    min_value=stats.get("min_value"),
                    max_value=stats.get("max_value"),
                )
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
            warnings=warnings,
            profiled_column_count=len(columns),
            sampled_column_count=min(len(columns), max_columns_per_table),
        )

    async def _fetch_column_stats_with_connector(
        self,
        connector: BaseConnector,
        *,
        db_type: str,
        schema: str,
        table: str,
        column: str,
        query_timeout_seconds: int,
    ) -> dict[str, str | int | None]:
        query = self._build_column_stats_query(
            db_type=db_type,
            schema=schema,
            table=table,
            column=column,
        )
        if not query:
            return {}

        try:
            result = await connector.execute(query, timeout=query_timeout_seconds)
        except Exception:
            return {}

        row = result.rows[0] if result.rows else {}
        return {
            "null_count": self._coerce_int(row.get("null_count")),
            "distinct_count": self._coerce_int(row.get("distinct_count")),
            "min_value": self._coerce_str(row.get("min_value")),
            "max_value": self._coerce_str(row.get("max_value")),
        }

    async def _fetch_column_samples_with_connector(
        self,
        connector: BaseConnector,
        *,
        db_type: str,
        schema: str,
        table: str,
        column: str,
        sample_size: int,
        query_timeout_seconds: int,
    ) -> list[str]:
        query = self._build_column_sample_query(
            db_type=db_type,
            schema=schema,
            table=table,
            column=column,
            sample_size=sample_size,
        )
        try:
            result = await connector.execute(query, timeout=query_timeout_seconds)
        except Exception:
            return []

        samples: list[str] = []
        for row in result.rows:
            value = row.get("value")
            if value is not None:
                samples.append(str(value))
        return samples

    def _build_column_sample_query(
        self,
        *,
        db_type: str,
        schema: str,
        table: str,
        column: str,
        sample_size: int,
    ) -> str:
        qualified_table = self._qualified_table_name(schema=schema, table=table, db_type=db_type)
        qualified_column = self._quote_identifier(column, db_type=db_type)
        if db_type == "clickhouse":
            value_expr = f"toString({qualified_column})"
        elif db_type == "mysql":
            value_expr = f"CAST({qualified_column} AS CHAR)"
        else:
            value_expr = f"{qualified_column}::text"
        return (
            f"SELECT {value_expr} AS value "
            f"FROM {qualified_table} "
            f"WHERE {qualified_column} IS NOT NULL "
            f"LIMIT {max(sample_size, 1)}"
        )

    def _build_column_stats_query(
        self,
        *,
        db_type: str,
        schema: str,
        table: str,
        column: str,
    ) -> str | None:
        qualified_table = self._qualified_table_name(schema=schema, table=table, db_type=db_type)
        qualified_column = self._quote_identifier(column, db_type=db_type)
        if db_type == "mysql":
            return (
                "SELECT "
                f"SUM(CASE WHEN {qualified_column} IS NULL THEN 1 ELSE 0 END) AS null_count, "
                f"COUNT(DISTINCT {qualified_column}) AS distinct_count, "
                f"CAST(MIN({qualified_column}) AS CHAR) AS min_value, "
                f"CAST(MAX({qualified_column}) AS CHAR) AS max_value "
                f"FROM {qualified_table}"
            )
        if db_type == "clickhouse":
            return (
                "SELECT "
                f"countIf({qualified_column} IS NULL) AS null_count, "
                f"uniqExact({qualified_column}) AS distinct_count, "
                f"toString(min({qualified_column})) AS min_value, "
                f"toString(max({qualified_column})) AS max_value "
                f"FROM {qualified_table}"
            )
        return None

    def _qualified_table_name(self, *, schema: str, table: str, db_type: str) -> str:
        quoted_schema = self._quote_identifier(schema, db_type=db_type)
        quoted_table = self._quote_identifier(table, db_type=db_type)
        return f"{quoted_schema}.{quoted_table}"

    @staticmethod
    def _coerce_int(value: object) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_str(value: object) -> str | None:
        if value is None:
            return None
        return str(value)

    @staticmethod
    def _table_matches_selection(schema: str, table: str, requested: set[str]) -> bool:
        table_name = table.lower()
        qualified_name = f"{schema}.{table}".lower()
        return table_name in requested or qualified_name in requested

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
        estimate = int(row["estimate"])
        return estimate if estimate >= 0 else None

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
    def _quote_identifier(value: str, *, db_type: str = "postgresql") -> str:
        if db_type in {"mysql", "clickhouse"}:
            escaped = value.replace("`", "``")
            return f"`{escaped}`"
        escaped = value.replace('"', '""')
        return f'"{escaped}"'
