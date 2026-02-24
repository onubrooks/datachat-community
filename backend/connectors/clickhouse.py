"""
ClickHouse Connector

Async ClickHouse connector using clickhouse-connect for OLAP database access.

Features:
- Async query execution using asyncio.to_thread
- Schema introspection (databases, tables, columns)
- Parameterized query execution
- Query timeout support
- ClickHouse-specific optimizations

Usage:
    connector = ClickHouseConnector(
        host="localhost",
        port=8123,
        database="default",
        user="default",
        password=""
    )

    await connector.connect()

    # Execute query
    result = await connector.execute(
        "SELECT * FROM events WHERE date >= {date:Date}",
        params={"date": "2024-01-01"}
    )

    # Get schema
    tables = await connector.get_schema(schema_name="default")

    await connector.close()
"""

import asyncio
import logging
import time
from typing import Any

try:
    import clickhouse_connect
    from clickhouse_connect.driver import Client
except ImportError:
    clickhouse_connect = None
    Client = None

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


class ClickHouseConnector(BaseConnector):
    """
    ClickHouse database connector using clickhouse-connect.

    Provides async interface for ClickHouse OLAP database with schema
    introspection and query execution.

    Note: Uses clickhouse-connect which is synchronous, wrapped with
    asyncio.to_thread for async compatibility.
    """

    def __init__(
        self,
        host: str,
        port: int = 8123,
        database: str = "default",
        user: str = "default",
        password: str = "",
        pool_size: int = 10,
        timeout: int = 30,
        **kwargs,
    ):
        """
        Initialize ClickHouse connector.

        Args:
            host: ClickHouse host
            port: HTTP port (default: 8123)
            database: Database name (default: 'default')
            user: Username (default: 'default')
            password: Password
            pool_size: Connection pool size
            timeout: Query timeout in seconds
            **kwargs: Additional parameters for clickhouse-connect
        """
        if clickhouse_connect is None:
            raise ImportError(
                "clickhouse-connect is not installed. "
                "Install it with: pip install clickhouse-connect"
            )

        super().__init__(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            pool_size=pool_size,
            timeout=timeout,
            **kwargs,
        )

        self._client: Client | None = None

    async def connect(self) -> None:
        """
        Establish connection to ClickHouse.

        Creates a clickhouse-connect client.

        Raises:
            ConnectionError: If connection fails
        """
        if self._connected and self._client:
            logger.debug("Already connected, skipping connection")
            return

        try:
            logger.info(f"Connecting to ClickHouse at {self.host}:{self.port}/{self.database}")

            # Create client (synchronous operation, wrap in to_thread)
            self._client = await asyncio.to_thread(
                clickhouse_connect.get_client,
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.user,
                password=self.password,
                **self.kwargs,
            )

            # Test connection
            version = await asyncio.to_thread(self._client.command, "SELECT version()")
            logger.info(f"Connected to ClickHouse: version {version}")

            self._connected = True

        except Exception as e:
            logger.error(f"ClickHouse connection failed: {e}")
            raise ConnectionError(f"Failed to connect to ClickHouse: {e}") from e

    async def execute(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> QueryResult:
        """
        Execute a SQL query.

        Args:
            query: SQL query (use {param:Type} for parameters)
            params: Query parameters as dict (ClickHouse style)
            timeout: Query timeout in seconds (overrides default)

        Returns:
            QueryResult with rows and metadata

        Raises:
            QueryError: If query fails
            ConnectionError: If not connected

        Note:
            ClickHouse uses {param:Type} syntax for parameters, not $1, $2.
            Example: "SELECT * FROM table WHERE id = {id:UInt64}"
        """
        if not self._connected or not self._client:
            raise ConnectionError("Not connected to database. Call connect() first.")

        start_time = time.perf_counter()
        query_timeout = timeout or self.timeout

        try:
            # Execute query with parameters
            result = await asyncio.to_thread(
                self._client.query,
                query,
                parameters=params or {},
                settings={"max_execution_time": query_timeout},
            )

            # Convert to list of dicts
            columns = result.column_names
            result_rows = [
                {col: row[i] for i, col in enumerate(columns)} for row in result.result_rows
            ]

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

        except Exception as e:
            logger.error(f"Query failed: {e}\nQuery: {query[:200]}...")
            raise QueryError(f"Query execution failed: {e}") from e

    async def get_schema(self, schema_name: str | None = None) -> list[TableInfo]:
        """
        Introspect ClickHouse schema.

        Retrieves information about tables, columns, and data types
        from system.tables and system.columns.

        Args:
            schema_name: Specific database (default: current database)

        Returns:
            List of TableInfo objects

        Raises:
            SchemaError: If schema introspection fails
        """
        if not self._connected or not self._client:
            raise ConnectionError("Not connected to database. Call connect() first.")

        database_filter = schema_name or self.database

        try:
            # Get tables
            tables_query = """
                SELECT
                    database,
                    name,
                    engine
                FROM system.tables
                WHERE database = {db:String}
                ORDER BY name
            """
            tables_result = await self.execute(tables_query, params={"db": database_filter})

            table_infos = []

            for table_row in tables_result.rows:
                table_db = table_row["database"]
                table_name = table_row["name"]
                engine = table_row["engine"]

                # Get columns for this table
                columns_query = """
                    SELECT
                        name,
                        type,
                        default_kind,
                        default_expression
                    FROM system.columns
                    WHERE database = {db:String}
                    AND table = {table:String}
                    ORDER BY position
                """
                columns_result = await self.execute(
                    columns_query,
                    params={"db": table_db, "table": table_name},
                )

                # Get row count
                count_query = f"SELECT count() FROM {table_db}.{table_name}"
                try:
                    count_result = await self.execute(count_query)
                    row_count = count_result.rows[0].get("count()") if count_result.rows else None
                except Exception:
                    row_count = None

                # Build column info list
                column_infos = []
                for col in columns_result.rows:
                    # ClickHouse doesn't have traditional PKs/FKs, but we can detect certain patterns
                    col_name = col["name"]
                    data_type = col["type"]
                    default_kind = col.get("default_kind", "")
                    default_expr = col.get("default_expression")

                    # Check if nullable (ClickHouse uses Nullable(Type) syntax)
                    is_nullable = "Nullable" in data_type

                    column_info = ColumnInfo(
                        name=col_name,
                        data_type=data_type,
                        is_nullable=is_nullable,
                        default_value=default_expr if default_kind else None,
                        is_primary_key=False,  # ClickHouse doesn't have traditional PKs
                        is_foreign_key=False,  # ClickHouse doesn't have FKs
                    )
                    column_infos.append(column_info)

                # Create TableInfo
                table_info = TableInfo(
                    schema=table_db,
                    table_name=table_name,
                    columns=column_infos,
                    row_count=row_count,
                    table_type=engine,  # Use engine as table_type for ClickHouse
                )
                table_infos.append(table_info)

            logger.info(
                f"Introspected database '{database_filter}': found {len(table_infos)} tables"
            )

            return table_infos

        except Exception as e:
            logger.error(f"Schema introspection failed: {e}")
            raise SchemaError(f"Failed to introspect schema: {e}") from e

    async def close(self) -> None:
        """
        Close ClickHouse client and clean up resources.

        Safe to call multiple times.
        """
        if not self._client:
            logger.debug("No client to close")
            return

        try:
            await asyncio.to_thread(self._client.close)
            self._client = None
            self._connected = False
            logger.info("ClickHouse connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
            raise ConnectionError(f"Failed to close connection: {e}") from e
