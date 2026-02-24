"""
MySQL Connector

Async-compatible MySQL connector using mysql-connector-python.

The underlying driver is synchronous, so query and schema operations are
executed in worker threads via asyncio.to_thread.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
except ImportError:  # pragma: no cover - dependency guard
    mysql = None
    MySQLError = Exception

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


class MySQLConnector(BaseConnector):
    """MySQL database connector using mysql-connector-python."""

    def __init__(
        self,
        host: str,
        port: int = 3306,
        database: str = "",
        user: str = "root",
        password: str = "",
        pool_size: int = 10,
        timeout: int = 30,
        **kwargs,
    ) -> None:
        if mysql is None:
            raise ImportError(
                "mysql-connector-python is not installed. "
                "Install it with: pip install mysql-connector-python"
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

    async def connect(self) -> None:
        """Validate connection credentials."""
        if self._connected:
            return
        try:
            await asyncio.to_thread(self._test_connection_sync)
            self._connected = True
        except MySQLError as exc:
            logger.error(f"MySQL connection failed: {exc}")
            raise ConnectionError(f"Failed to connect to MySQL: {exc}") from exc
        except Exception as exc:
            logger.error(f"MySQL connection failed: {exc}")
            raise ConnectionError(f"Connection error: {exc}") from exc

    async def execute(
        self,
        query: str,
        params: list[Any] | dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> QueryResult:
        """Execute SQL query and return rows."""
        if not self._connected:
            raise ConnectionError("Not connected to database. Call connect() first.")

        start_time = time.perf_counter()
        query_timeout = timeout or self.timeout
        try:
            rows, columns = await asyncio.to_thread(
                self._execute_sync,
                query,
                params,
                query_timeout,
            )
            execution_time_ms = (time.perf_counter() - start_time) * 1000
            return QueryResult(
                rows=rows,
                row_count=len(rows),
                columns=columns,
                execution_time_ms=execution_time_ms,
            )
        except MySQLError as exc:
            logger.error(f"MySQL query failed: {exc}\nQuery: {query[:200]}...")
            raise QueryError(f"Query execution failed: {exc}") from exc
        except Exception as exc:
            logger.error(f"MySQL query failed: {exc}\nQuery: {query[:200]}...")
            raise QueryError(f"Query error: {exc}") from exc

    async def get_schema(self, schema_name: str | None = None) -> list[TableInfo]:
        """Introspect schema via information_schema."""
        if not self._connected:
            raise ConnectionError("Not connected to database. Call connect() first.")
        target_schema = schema_name or self.database
        try:
            return await asyncio.to_thread(self._get_schema_sync, target_schema)
        except MySQLError as exc:
            logger.error(f"MySQL schema introspection failed: {exc}")
            raise SchemaError(f"Failed to introspect schema: {exc}") from exc
        except Exception as exc:
            logger.error(f"MySQL schema introspection failed: {exc}")
            raise SchemaError(f"Schema error: {exc}") from exc

    async def close(self) -> None:
        """Close connector state."""
        self._connected = False

    def _connection_kwargs(self, query_timeout: int | None = None) -> dict[str, Any]:
        kwargs = {
            "host": self.host,
            "port": self.port,
            "database": self.database or None,
            "user": self.user,
            "password": self.password,
            "autocommit": True,
            "connection_timeout": query_timeout or self.timeout,
        }
        kwargs.update(self.kwargs)
        return kwargs

    def _test_connection_sync(self) -> None:
        conn = mysql.connector.connect(**self._connection_kwargs())
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT VERSION()")
            cursor.fetchone()
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            conn.close()

    def _execute_sync(
        self,
        query: str,
        params: list[Any] | dict[str, Any] | None,
        query_timeout: int,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        conn = mysql.connector.connect(**self._connection_kwargs(query_timeout))
        cursor = conn.cursor(dictionary=True)
        try:
            if params is None:
                cursor.execute(query)
            elif isinstance(params, dict):
                cursor.execute(query, params)
            else:
                cursor.execute(query, tuple(params))
            if cursor.with_rows:
                rows = cursor.fetchall()
                columns = list(rows[0].keys()) if rows else [col[0] for col in cursor.description]
                return rows, columns
            return [], []
        finally:
            cursor.close()
            conn.close()

    def _get_schema_sync(self, schema_name: str) -> list[TableInfo]:
        conn = mysql.connector.connect(**self._connection_kwargs())
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                """
                SELECT table_schema, table_name, table_type, table_rows
                FROM information_schema.tables
                WHERE table_schema = %s
                ORDER BY table_name
                """,
                (schema_name,),
            )
            tables = cursor.fetchall()

            table_infos: list[TableInfo] = []
            for table_row in tables:
                table_schema = str(table_row["table_schema"])
                table_name = str(table_row["table_name"])
                table_type = str(table_row["table_type"])
                row_count = (
                    int(table_row["table_rows"]) if table_row.get("table_rows") is not None else None
                )

                cursor.execute(
                    """
                    SELECT
                        c.column_name,
                        c.column_type,
                        c.is_nullable,
                        c.column_default,
                        c.column_key
                    FROM information_schema.columns c
                    WHERE c.table_schema = %s AND c.table_name = %s
                    ORDER BY c.ordinal_position
                    """,
                    (table_schema, table_name),
                )
                columns_rows = cursor.fetchall()

                cursor.execute(
                    """
                    SELECT
                        kcu.column_name,
                        kcu.referenced_table_name AS foreign_table_name,
                        kcu.referenced_column_name AS foreign_column_name
                    FROM information_schema.key_column_usage kcu
                    WHERE kcu.table_schema = %s
                    AND kcu.table_name = %s
                    AND kcu.referenced_table_name IS NOT NULL
                    """,
                    (table_schema, table_name),
                )
                fk_rows = cursor.fetchall()
                fk_map = {
                    str(row["column_name"]): (
                        str(row["foreign_table_name"]),
                        str(row["foreign_column_name"]),
                    )
                    for row in fk_rows
                }

                columns: list[ColumnInfo] = []
                for col_row in columns_rows:
                    col_name = str(col_row["column_name"])
                    fk_target = fk_map.get(col_name)
                    columns.append(
                        ColumnInfo(
                            name=col_name,
                            data_type=str(col_row["column_type"]),
                            is_nullable=str(col_row["is_nullable"]).upper() == "YES",
                            default_value=(
                                str(col_row["column_default"])
                                if col_row["column_default"] is not None
                                else None
                            ),
                            is_primary_key=str(col_row["column_key"]).upper() == "PRI",
                            is_foreign_key=fk_target is not None,
                            foreign_table=fk_target[0] if fk_target else None,
                            foreign_column=fk_target[1] if fk_target else None,
                        )
                    )

                table_infos.append(
                    TableInfo(
                        schema=table_schema,
                        table_name=table_name,
                        columns=columns,
                        row_count=row_count,
                        table_type=table_type,
                    )
                )

            return table_infos
        finally:
            cursor.close()
            conn.close()
