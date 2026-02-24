"""
PostgreSQL Connector

Async PostgreSQL connector using asyncpg for high-performance database access.

Features:
- Connection pooling with asyncpg
- Schema introspection (tables, columns, constraints)
- Parameterized query execution
- Query timeout support
- Foreign key relationship discovery
- Row count estimation

Usage:
    connector = PostgresConnector(
        host="localhost",
        port=5432,
        database="mydb",
        user="postgres",
        password="secret"
    )

    await connector.connect()

    # Execute query
    result = await connector.execute(
        "SELECT * FROM users WHERE age > $1",
        params=[18]
    )

    # Get schema
    tables = await connector.get_schema(schema_name="public")

    await connector.close()
"""

import logging
import time
from typing import Any

import asyncpg

from backend.connectors.base import (
    BaseConnector,
    ColumnInfo,
    ConnectionError,
    QueryError,
    QueryResult,
    SchemaError,
    TableInfo,
)

logger = logging.getLogger(__name__)


class PostgresConnector(BaseConnector):
    """
    PostgreSQL database connector using asyncpg.

    Provides async interface for PostgreSQL with connection pooling,
    schema introspection, and query execution.
    """

    async def connect(self) -> None:
        """
        Establish connection to PostgreSQL and create connection pool.

        Creates an asyncpg connection pool for efficient connection reuse.

        Raises:
            ConnectionError: If connection fails
        """
        if self._connected and self._pool:
            logger.debug("Already connected, skipping connection")
            return

        try:
            logger.info(f"Connecting to PostgreSQL at {self.host}:{self.port}/{self.database}")

            # Create pool with high command_timeout
            # We'll use per-query timeouts instead of pool-level timeout
            # to allow query-specific timeout overrides
            self._pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                min_size=1,
                max_size=self.pool_size,
                command_timeout=None,  # No pool-level timeout; use per-query instead
                **self.kwargs,
            )

            # Test connection
            async with self._pool.acquire() as conn:
                version = await conn.fetchval("SELECT version()")
                logger.info(f"Connected to PostgreSQL: {version.split(',')[0]}")

            self._connected = True

        except asyncpg.PostgresError as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error during connection: {e}")
            raise ConnectionError(f"Connection error: {e}") from e

    async def execute(
        self,
        query: str,
        params: list[Any] | None = None,
        timeout: int | None = None,
    ) -> QueryResult:
        """
        Execute a SQL query.

        Args:
            query: SQL query (use $1, $2, ... for parameters)
            params: Query parameters
            timeout: Query timeout in seconds (overrides default)

        Returns:
            QueryResult with rows and metadata

        Raises:
            QueryError: If query fails
            ConnectionError: If not connected
        """
        if not self._connected or not self._pool:
            raise ConnectionError("Not connected to database. Call connect() first.")

        start_time = time.perf_counter()
        query_timeout = timeout or self.timeout

        try:
            async with self._pool.acquire() as conn:
                # Set statement timeout (server-side)
                await conn.execute(f"SET statement_timeout = {query_timeout * 1000}")

                # Execute query with client-side timeout
                # This ensures both client and server respect the timeout
                if params:
                    rows = await conn.fetch(query, *params, timeout=query_timeout)
                else:
                    rows = await conn.fetch(query, timeout=query_timeout)

                # Convert to dict format
                result_rows = [dict(row) for row in rows]
                columns = list(rows[0].keys()) if rows else []

                execution_time_ms = (time.perf_counter() - start_time) * 1000

                logger.debug(
                    f"Query executed in {execution_time_ms:.2f}ms, returned {len(result_rows)} rows"
                )

                return QueryResult(
                    rows=result_rows,
                    row_count=len(result_rows),
                    columns=columns,
                    execution_time_ms=execution_time_ms,
                )

        except asyncpg.QueryCanceledError as e:
            logger.error(f"Query timed out after {query_timeout}s: {query[:100]}...")
            raise QueryError(f"Query timeout ({query_timeout}s)") from e
        except asyncpg.PostgresError as e:
            logger.error(f"Query failed: {e}\nQuery: {query[:200]}...")
            raise QueryError(f"Query execution failed: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error during query execution: {e}")
            raise QueryError(f"Query error: {e}") from e

    async def get_schema(self, schema_name: str | None = None) -> list[TableInfo]:
        """
        Introspect PostgreSQL schema.

        Retrieves information about tables, columns, data types, and constraints
        from information_schema and pg_catalog.

        Args:
            schema_name: Specific schema (default: public)

        Returns:
            List of TableInfo objects with complete schema information

        Raises:
            SchemaError: If schema introspection fails
        """
        if not self._connected or not self._pool:
            raise ConnectionError("Not connected to database. Call connect() first.")

        schema_filter = schema_name or "public"

        try:
            async with self._pool.acquire() as conn:
                # Get tables
                tables_query = """
                    SELECT
                        table_schema,
                        table_name,
                        table_type
                    FROM information_schema.tables
                    WHERE table_schema = $1
                    AND table_type IN ('BASE TABLE', 'VIEW')
                    ORDER BY table_name
                """
                tables = await conn.fetch(tables_query, schema_filter)

                table_infos = []

                for table_row in tables:
                    table_schema = table_row["table_schema"]
                    table_name = table_row["table_name"]
                    table_type = table_row["table_type"]

                    # Get columns for this table
                    columns_query = """
                        SELECT
                            column_name,
                            data_type,
                            is_nullable,
                            column_default
                        FROM information_schema.columns
                        WHERE table_schema = $1 AND table_name = $2
                        ORDER BY ordinal_position
                    """
                    columns = await conn.fetch(columns_query, table_schema, table_name)

                    # Get primary keys
                    pk_query = """
                        SELECT a.attname
                        FROM pg_index i
                        JOIN pg_attribute a ON a.attrelid = i.indrelid
                            AND a.attnum = ANY(i.indkey)
                        WHERE i.indrelid = $1::regclass
                        AND i.indisprimary
                    """
                    full_table_name = f"{table_schema}.{table_name}"
                    pk_cols = await conn.fetch(pk_query, full_table_name)
                    pk_columns = {row["attname"] for row in pk_cols}

                    # Get foreign keys
                    fk_query = """
                        SELECT
                            kcu.column_name,
                            ccu.table_name AS foreign_table_name,
                            ccu.column_name AS foreign_column_name
                        FROM information_schema.table_constraints AS tc
                        JOIN information_schema.key_column_usage AS kcu
                            ON tc.constraint_name = kcu.constraint_name
                            AND tc.table_schema = kcu.table_schema
                        JOIN information_schema.constraint_column_usage AS ccu
                            ON ccu.constraint_name = tc.constraint_name
                            AND ccu.table_schema = tc.table_schema
                        WHERE tc.constraint_type = 'FOREIGN KEY'
                        AND tc.table_schema = $1
                        AND tc.table_name = $2
                    """
                    fk_rows = await conn.fetch(fk_query, table_schema, table_name)
                    fk_map = {
                        row["column_name"]: {
                            "foreign_table": row["foreign_table_name"],
                            "foreign_column": row["foreign_column_name"],
                        }
                        for row in fk_rows
                    }

                    # Get row count estimate
                    row_count_query = """
                        SELECT reltuples::bigint AS estimate
                        FROM pg_class
                        WHERE oid = $1::regclass
                    """
                    try:
                        row_count_result = await conn.fetchval(row_count_query, full_table_name)
                        row_count = int(row_count_result) if row_count_result else None
                    except Exception:
                        row_count = None

                    # Build column info list
                    column_infos = []
                    for col in columns:
                        col_name = col["column_name"]
                        is_pk = col_name in pk_columns
                        is_fk = col_name in fk_map

                        column_info = ColumnInfo(
                            name=col_name,
                            data_type=col["data_type"],
                            is_nullable=col["is_nullable"] == "YES",
                            default_value=col["column_default"],
                            is_primary_key=is_pk,
                            is_foreign_key=is_fk,
                            foreign_table=fk_map[col_name]["foreign_table"] if is_fk else None,
                            foreign_column=fk_map[col_name]["foreign_column"] if is_fk else None,
                        )
                        column_infos.append(column_info)

                    # Create TableInfo
                    table_info = TableInfo(
                        schema=table_schema,
                        table_name=table_name,
                        columns=column_infos,
                        row_count=row_count,
                        table_type=table_type,
                    )
                    table_infos.append(table_info)

                logger.info(
                    f"Introspected schema '{schema_filter}': found {len(table_infos)} tables"
                )

                return table_infos

        except asyncpg.PostgresError as e:
            logger.error(f"Schema introspection failed: {e}")
            raise SchemaError(f"Failed to introspect schema: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error during schema introspection: {e}")
            raise SchemaError(f"Schema introspection error: {e}") from e

    async def close(self) -> None:
        """
        Close connection pool and clean up resources.

        Safe to call multiple times.
        """
        if not self._pool:
            logger.debug("No connection pool to close")
            return

        try:
            await self._pool.close()
            self._pool = None
            self._connected = False
            logger.info("PostgreSQL connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
            raise ConnectionError(f"Failed to close connection: {e}") from e
