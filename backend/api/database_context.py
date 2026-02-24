"""Shared database context helpers for API routes."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from pydantic import SecretStr

from backend.config import get_settings
from backend.connectors.factory import infer_database_type
from backend.database.manager import DatabaseConnectionManager
from backend.models.database import DatabaseConnection

ENV_DATABASE_CONNECTION_ID = UUID("00000000-0000-0000-0000-00000000dada")


def _build_environment_connection(*, is_default: bool = True) -> DatabaseConnection | None:
    """Build a virtual connection from DATABASE_URL when configured."""
    settings = get_settings()
    if not settings.database.url:
        return None

    database_url = str(settings.database.url)
    try:
        database_type = infer_database_type(database_url)
    except Exception:
        database_type = settings.database.db_type

    return DatabaseConnection(
        connection_id=ENV_DATABASE_CONNECTION_ID,
        name="Environment Database",
        database_url=SecretStr(database_url),
        database_type=database_type,
        is_active=True,
        is_default=is_default,
        tags=["env"],
        description="Loaded from DATABASE_URL",
        created_at=datetime.now(UTC),
        last_profiled=None,
        datapoint_count=0,
    )


async def list_available_connections(
    manager: DatabaseConnectionManager | None,
) -> list[DatabaseConnection]:
    """Return registry connections plus environment fallback when available."""
    managed_connections: list[DatabaseConnection] = []
    if manager is not None:
        managed_connections = await manager.list_connections()

    env_connection = _build_environment_connection(
        is_default=not any(connection.is_default for connection in managed_connections)
    )
    if env_connection is None:
        return managed_connections

    env_url = env_connection.database_url.get_secret_value()
    for connection in managed_connections:
        if connection.database_url.get_secret_value() == env_url:
            return managed_connections

    return [*managed_connections, env_connection]


async def resolve_database_type_and_url(
    *,
    target_database: str | None,
    manager: DatabaseConnectionManager | None,
) -> tuple[str | None, str | None]:
    """
    Resolve runtime database context.

    Raises:
        KeyError: target_database was provided but not found.
        ValueError: target_database cannot be parsed as UUID and is not a known virtual target.
    """
    env_connection = _build_environment_connection(is_default=True)

    if target_database:
        if manager is not None:
            try:
                connection = await manager.get_connection(target_database)
                return connection.database_type, connection.database_url.get_secret_value()
            except KeyError:
                pass

        if env_connection and target_database == str(env_connection.connection_id):
            return env_connection.database_type, env_connection.database_url.get_secret_value()

        if env_connection is None and manager is None:
            raise KeyError(
                "Database connection not found and DATABASE_URL is not configured."
            )
        raise KeyError(f"Database connection not found: {target_database}")

    if manager is not None:
        connection = await manager.get_default_connection()
        if connection is not None:
            return connection.database_type, connection.database_url.get_secret_value()

    if env_connection is not None:
        return env_connection.database_type, env_connection.database_url.get_secret_value()

    return None, None


def environment_connection_id() -> str:
    """Public helper for exposing the virtual environment connection identifier."""
    return str(ENV_DATABASE_CONNECTION_ID)

