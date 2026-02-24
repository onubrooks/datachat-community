"""Connector factory for supported database URLs."""

from __future__ import annotations

from urllib.parse import urlparse

from backend.connectors.base import BaseConnector
from backend.connectors.clickhouse import ClickHouseConnector
from backend.connectors.mysql import MySQLConnector
from backend.connectors.postgres import PostgresConnector

_POSTGRES_SCHEMES = {"postgres", "postgresql"}
_CLICKHOUSE_SCHEMES = {"clickhouse"}
_MYSQL_SCHEMES = {"mysql"}


def infer_database_type(database_url: str) -> str:
    """Infer logical database type from connection URL scheme."""
    parsed = _parse_url(database_url)
    scheme = parsed.scheme.split("+")[0].lower()
    if scheme in _POSTGRES_SCHEMES:
        return "postgresql"
    if scheme in _CLICKHOUSE_SCHEMES:
        return "clickhouse"
    if scheme in _MYSQL_SCHEMES:
        return "mysql"
    raise ValueError(f"Unsupported database URL scheme: {parsed.scheme}")


def resolve_database_type(database_type: str | None, database_url: str) -> str:
    """Resolve target database type from explicit type or URL."""
    if database_type:
        value = database_type.strip().lower()
        if value in {"postgres", "postgresql"}:
            return "postgresql"
        if value == "clickhouse":
            return "clickhouse"
        if value == "mysql":
            return "mysql"
        raise ValueError(f"Unsupported database type: {database_type}")
    return infer_database_type(database_url)


def create_connector(
    *,
    database_url: str,
    database_type: str | None = None,
    pool_size: int = 10,
    timeout: int = 30,
    **kwargs,
) -> BaseConnector:
    """Create a typed connector instance from URL + optional database_type."""
    parsed = _parse_url(database_url)
    if not parsed.hostname:
        raise ValueError("Invalid database URL: host is required.")

    target_type = resolve_database_type(database_type, database_url)
    db_name = parsed.path.lstrip("/")

    if target_type == "postgresql":
        return PostgresConnector(
            host=parsed.hostname,
            port=parsed.port or 5432,
            database=db_name or "datachat",
            user=parsed.username or "postgres",
            password=parsed.password or "",
            pool_size=pool_size,
            timeout=timeout,
            **kwargs,
        )

    if target_type == "clickhouse":
        return ClickHouseConnector(
            host=parsed.hostname,
            port=parsed.port or 8123,
            database=db_name or "default",
            user=parsed.username or "default",
            password=parsed.password or "",
            pool_size=pool_size,
            timeout=timeout,
            **kwargs,
        )

    if target_type == "mysql":
        return MySQLConnector(
            host=parsed.hostname,
            port=parsed.port or 3306,
            database=db_name or "",
            user=parsed.username or "root",
            password=parsed.password or "",
            pool_size=pool_size,
            timeout=timeout,
            **kwargs,
        )

    raise ValueError(f"Unsupported database type: {target_type}")


def _parse_url(database_url: str):
    normalized = database_url.replace("postgresql+asyncpg://", "postgresql://")
    return urlparse(normalized)
