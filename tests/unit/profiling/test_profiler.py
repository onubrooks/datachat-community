"""Unit tests for SchemaProfiler."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from pydantic import SecretStr

from backend.database.manager import DatabaseConnectionManager
from backend.models.database import DatabaseConnection
from backend.profiling.profiler import SchemaProfiler
from backend.profiling.query_templates import supported_profile_template_databases


class TestSchemaProfiler:
    """Test SchemaProfiler behavior with mocked asyncpg."""

    @pytest.fixture
    def connection(self):
        return DatabaseConnection(
            connection_id=uuid4(),
            name="Warehouse",
            database_url=SecretStr("postgresql://user:pass@localhost:5432/warehouse"),
            database_type="postgresql",
            is_active=True,
            is_default=True,
            tags=[],
            description=None,
            datapoint_count=0,
        )

    @pytest.fixture
    def manager(self, connection):
        manager = AsyncMock(spec=DatabaseConnectionManager)
        manager.get_connection = AsyncMock(return_value=connection)
        return manager

    def _build_connection(self):
        conn = AsyncMock()

        async def fetch(query, *args, **kwargs):
            if "information_schema.tables" in query:
                return [
                    {"table_schema": "public", "table_name": "orders"},
                ]
            if "information_schema.columns" in query:
                return [
                    {
                        "column_name": "order_id",
                        "data_type": "integer",
                        "is_nullable": "NO",
                        "column_default": None,
                    },
                    {
                        "column_name": "created_at",
                        "data_type": "timestamp",
                        "is_nullable": "YES",
                        "column_default": None,
                    },
                ]
            if "information_schema.table_constraints" in query:
                return [
                    {
                        "source_table": "orders",
                        "source_column": "customer_id",
                        "target_table": "customers",
                        "target_column": "id",
                    }
                ]
            if "SELECT" in query and "LIMIT" in query:
                return [{"value": "sample"}, {"value": "sample2"}]
            return []

        async def fetchrow(query, *args, **kwargs):
            if "scoped_tables" in query:
                return {"total": 1}
            if "pg_class" in query:
                return {"estimate": 1234}
            if "COUNT(*) FILTER" in query:
                return {
                    "null_count": 1,
                    "distinct_count": 2,
                    "min_value": "1",
                    "max_value": "5",
                }
            return None

        conn.fetch.side_effect = fetch
        conn.fetchrow.side_effect = fetchrow
        return conn

    @pytest.mark.asyncio
    async def test_profiles_postgres_schema(self, manager):
        profiler = SchemaProfiler(manager)
        mock_connection = self._build_connection()

        with patch("asyncpg.connect", new=AsyncMock(return_value=mock_connection)):
            profile = await profiler.profile_database(
                str(uuid4()),
                sample_size=2,
                query_timeout_seconds=2,
            )

        assert profile.tables[0].name == "orders"
        assert profile.tables[0].row_count == 1234
        assert len(profile.tables[0].columns) == 2
        assert profile.tables_profiled == 1
        assert profile.tables_failed == 0

    @pytest.mark.asyncio
    async def test_samples_data_with_correct_size(self, manager):
        profiler = SchemaProfiler(manager)
        mock_connection = self._build_connection()

        with patch("asyncpg.connect", new=AsyncMock(return_value=mock_connection)):
            profile = await profiler.profile_database(str(uuid4()), sample_size=2)

        samples = profile.tables[0].columns[0].sample_values
        assert len(samples) == 2

    @pytest.mark.asyncio
    async def test_discovers_foreign_keys(self, manager):
        profiler = SchemaProfiler(manager)
        mock_connection = self._build_connection()

        with patch("asyncpg.connect", new=AsyncMock(return_value=mock_connection)):
            profile = await profiler.profile_database(str(uuid4()), sample_size=2)

        relationships = profile.tables[0].relationships
        assert relationships
        assert relationships[0].target_table == "customers"

    @pytest.mark.asyncio
    async def test_calculates_statistics(self, manager):
        profiler = SchemaProfiler(manager)
        mock_connection = self._build_connection()

        with patch("asyncpg.connect", new=AsyncMock(return_value=mock_connection)):
            profile = await profiler.profile_database(str(uuid4()), sample_size=2)

        column = profile.tables[0].columns[0]
        assert column.null_count == 1
        assert column.distinct_count == 2
        assert column.min_value == "1"
        assert column.max_value == "5"

    @pytest.mark.asyncio
    async def test_respects_max_tables_limit(self, manager):
        profiler = SchemaProfiler(manager)
        mock_connection = self._build_connection()

        async def fetch_tables(query, *args, **kwargs):
            if "information_schema.tables" in query:
                rows = [
                    {"table_schema": "public", "table_name": "orders"},
                    {"table_schema": "public", "table_name": "customers"},
                ]
                if "LIMIT" in query and args:
                    return rows[: int(args[0])]
                return rows
            return await self._build_connection().fetch(query, *args, **kwargs)

        mock_connection.fetch.side_effect = fetch_tables

        async def fetchrow(query, *args, **kwargs):
            if "scoped_tables" in query:
                return {"total": 2}
            return await self._build_connection().fetchrow(query, *args, **kwargs)

        mock_connection.fetchrow.side_effect = fetchrow

        with patch("asyncpg.connect", new=AsyncMock(return_value=mock_connection)):
            profile = await profiler.profile_database(
                str(uuid4()),
                sample_size=2,
                max_tables=1,
            )

        assert len(profile.tables) == 1
        assert profile.total_tables_discovered == 2
        assert profile.tables_skipped == 1

    @pytest.mark.asyncio
    async def test_continues_on_table_failure_and_records_partial(self, manager):
        profiler = SchemaProfiler(manager)
        mock_connection = self._build_connection()

        async def fetch(query, *args, **kwargs):
            if "information_schema.tables" in query:
                return [
                    {"table_schema": "public", "table_name": "orders"},
                    {"table_schema": "public", "table_name": "broken_table"},
                ]
            if "information_schema.columns" in query:
                schema, table = args[0], args[1]
                if table == "broken_table":
                    raise RuntimeError("catalog read failed")
                if schema == "public" and table == "orders":
                    return [
                        {
                            "column_name": "order_id",
                            "data_type": "integer",
                            "is_nullable": "NO",
                            "column_default": None,
                        }
                    ]
            if "SELECT" in query and "LIMIT" in query:
                return [{"value": "sample"}]
            if "information_schema.table_constraints" in query:
                return []
            return []

        async def fetchrow(query, *args, **kwargs):
            if "scoped_tables" in query:
                return {"total": 2}
            if "pg_class" in query:
                return {"estimate": 11}
            if "COUNT(*) FILTER" in query:
                return {
                    "null_count": 0,
                    "distinct_count": 1,
                    "min_value": "1",
                    "max_value": "1",
                }
            return None

        mock_connection.fetch.side_effect = fetch
        mock_connection.fetchrow.side_effect = fetchrow

        with patch("asyncpg.connect", new=AsyncMock(return_value=mock_connection)):
            profile = await profiler.profile_database(str(uuid4()), sample_size=2)

        assert profile.tables_profiled == 1
        assert profile.tables_failed == 1
        assert any("broken_table" in message for message in profile.partial_failures)
        failed_tables = [table for table in profile.tables if table.status == "failed"]
        assert failed_tables

    @pytest.mark.asyncio
    async def test_fetch_tables_with_explicit_tables_handles_ordered_template(self, manager):
        profiler = SchemaProfiler(manager)
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[{"table_schema": "public", "table_name": "orders"}])

        base_query = (
            "SELECT table_schema, table_name "
            "FROM information_schema.tables "
            "WHERE table_type = 'BASE TABLE' "
            "ORDER BY table_schema, table_name"
        )

        rows, discovered = await profiler._fetch_tables(
            conn,
            tables=["orders"],
            base_query=base_query,
            max_tables=50,
            query_timeout_seconds=2,
        )

        assert rows == [{"table_schema": "public", "table_name": "orders"}]
        assert discovered == 1
        executed_query = conn.fetch.await_args.args[0]
        assert "FROM (" in executed_query
        assert "WHERE table_name = ANY($1)" in executed_query
        assert "ORDER BY table_schema, table_name AND" not in executed_query

    @pytest.mark.asyncio
    async def test_fetch_tables_with_max_tables_handles_ordered_template(self, manager):
        profiler = SchemaProfiler(manager)
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[{"table_schema": "public", "table_name": "orders"}])
        conn.fetchrow = AsyncMock(return_value={"total": 3})

        base_query = (
            "SELECT table_schema, table_name "
            "FROM information_schema.tables "
            "WHERE table_type = 'BASE TABLE' "
            "ORDER BY table_schema, table_name"
        )

        rows, discovered = await profiler._fetch_tables(
            conn,
            tables=None,
            base_query=base_query,
            max_tables=1,
            query_timeout_seconds=2,
        )

        assert rows == [{"table_schema": "public", "table_name": "orders"}]
        assert discovered == 3
        count_query = conn.fetchrow.await_args.args[0]
        select_query = conn.fetch.await_args.args[0]
        assert "FROM (" in count_query
        assert "ORDER BY table_schema, table_name ORDER BY" not in select_query
        assert "LIMIT $1" in select_query


def test_templates_cover_popular_warehouses():
    supported = supported_profile_template_databases()
    for database in ("postgresql", "mysql", "bigquery", "clickhouse", "redshift"):
        assert database in supported
