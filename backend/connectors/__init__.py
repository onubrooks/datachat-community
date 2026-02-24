"""
Database Connectors Module

Provides async database connectors for various databases.

Available Connectors:
    - BaseConnector: Abstract base class
    - PostgresConnector: PostgreSQL connector (asyncpg)
    - ClickHouseConnector: ClickHouse connector (clickhouse-connect)
    - MySQLConnector: MySQL connector (mysql-connector-python)

Usage:
    from backend.connectors import PostgresConnector

    connector = PostgresConnector(
        host="localhost",
        port=5432,
        database="mydb",
        user="postgres",
        password="secret"
    )

    async with connector:
        result = await connector.execute("SELECT * FROM users")
        tables = await connector.get_schema()
"""

from backend.connectors.base import (
    BaseConnector,
    ColumnInfo,
    ConnectionError,
    ConnectorError,
    QueryError,
    QueryResult,
    SchemaError,
    TableInfo,
)
from backend.connectors.clickhouse import ClickHouseConnector
from backend.connectors.factory import create_connector, infer_database_type, resolve_database_type
from backend.connectors.mysql import MySQLConnector
from backend.connectors.postgres import PostgresConnector

__all__ = [
    "BaseConnector",
    "PostgresConnector",
    "ClickHouseConnector",
    "MySQLConnector",
    "create_connector",
    "infer_database_type",
    "resolve_database_type",
    "ColumnInfo",
    "TableInfo",
    "QueryResult",
    "ConnectorError",
    "ConnectionError",
    "QueryError",
    "SchemaError",
]
