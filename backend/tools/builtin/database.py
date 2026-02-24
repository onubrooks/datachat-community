"""Built-in database tools."""

from __future__ import annotations

import re
from typing import Any

from backend.connectors.base import BaseConnector
from backend.connectors.factory import create_connector
from backend.database.manager import DatabaseConnectionManager
from backend.tools.base import ToolCategory, ToolContext, tool


async def _get_default_connector() -> BaseConnector:
    manager = DatabaseConnectionManager()
    await manager.initialize()
    try:
        connection = await manager.get_default_connection()
        if not connection:
            raise ValueError("No default database connection configured.")
        database_url = connection.database_url.get_secret_value()
        connector = create_connector(
            database_url=database_url,
            database_type=connection.database_type,
        )
        await connector.connect()
        return connector
    finally:
        await manager.close()


def _build_connector_from_context(database_url: str, database_type: str | None) -> BaseConnector:
    return create_connector(
        database_url=database_url,
        database_type=database_type,
    )


async def _get_connector_from_context(ctx: ToolContext | None) -> BaseConnector:
    if ctx:
        database_url = ctx.metadata.get("database_url")
        database_type = ctx.metadata.get("database_type")
        if database_url:
            connector = _build_connector_from_context(database_url, database_type)
            await connector.connect()
            return connector
    return await _get_default_connector()


def _safe_identifier(name: str) -> str:
    value = name.strip().strip('"')
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid identifier: {name}")
    return value


@tool(
    name="list_tables",
    description="List tables available in the target database.",
    category=ToolCategory.DATABASE,
)
async def list_tables(schema: str | None = None, ctx: ToolContext | None = None) -> dict[str, Any]:
    connector = await _get_connector_from_context(ctx)
    try:
        if schema:
            schema = _safe_identifier(schema)
        schema_rows = await connector.get_schema(schema_name=schema)
        return {
            "tables": [
                {
                    "schema": table.schema_name,
                    "table": table.table_name,
                    "row_count": table.row_count,
                    "table_type": table.table_type,
                }
                for table in schema_rows
            ]
        }
    finally:
        await connector.close()


@tool(
    name="list_columns",
    description="List columns for a given table.",
    category=ToolCategory.DATABASE,
)
async def list_columns(
    table: str, schema: str | None = None, ctx: ToolContext | None = None
) -> dict[str, Any]:
    connector = await _get_connector_from_context(ctx)
    try:
        if "." in table and not schema:
            maybe_schema, maybe_table = table.split(".", 1)
            schema = maybe_schema
            table = maybe_table
        table = _safe_identifier(table)
        if schema:
            schema = _safe_identifier(schema)
        schema_rows = await connector.get_schema(schema_name=schema)
        match = None
        for table_info in schema_rows:
            if table_info.table_name == table:
                match = table_info
                break
        if not match:
            raise ValueError(f"Table not found: {table}")
        return {
            "table": f"{match.schema_name}.{match.table_name}",
            "columns": [
                {
                    "name": col.name,
                    "type": col.data_type,
                    "nullable": col.is_nullable,
                }
                for col in match.columns
            ],
        }
    finally:
        await connector.close()


@tool(
    name="get_table_sample",
    description="Fetch a small sample of rows from a table.",
    category=ToolCategory.DATABASE,
)
async def get_table_sample(
    table: str,
    schema: str | None = None,
    limit: int = 5,
    ctx: ToolContext | None = None,
) -> dict[str, Any]:
    connector = await _get_connector_from_context(ctx)
    try:
        if "." in table and not schema:
            maybe_schema, maybe_table = table.split(".", 1)
            schema = maybe_schema
            table = maybe_table
        table = _safe_identifier(table)
        if schema:
            schema = _safe_identifier(schema)
        bounded_limit = max(1, min(limit, 100))
        schema_prefix = f"{schema}." if schema else ""
        query = f"SELECT * FROM {schema_prefix}{table} LIMIT {bounded_limit}"
        result = await connector.execute(query)
        return {
            "columns": result.columns,
            "rows": result.rows,
            "row_count": result.row_count,
            "limit": bounded_limit,
        }
    finally:
        await connector.close()
