"""Database registry routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from backend.api.database_context import (
    environment_connection_id,
    list_available_connections,
)
from backend.connectors.base import (
    ConnectionError as ConnectorConnectionError,
)
from backend.connectors.base import (
    SchemaError as ConnectorSchemaError,
)
from backend.connectors.factory import create_connector
from backend.database.manager import DatabaseConnectionManager
from backend.models.database import (
    DatabaseConnection,
    DatabaseConnectionCreate,
    DatabaseConnectionUpdate,
    DatabaseConnectionUpdateDefault,
    DatabaseSchemaColumn,
    DatabaseSchemaResponse,
    DatabaseSchemaTable,
)

router = APIRouter()


def _get_manager() -> DatabaseConnectionManager:
    from backend.api.main import app_state

    manager = app_state.get("database_manager")
    if manager is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database registry is unavailable. Ensure DATABASE_CREDENTIALS_KEY is set.",
        )
    return manager


@router.post("/databases", response_model=DatabaseConnection, status_code=status.HTTP_201_CREATED)
async def create_database_connection(
    payload: DatabaseConnectionCreate,
) -> DatabaseConnection:
    """Create a new database connection."""
    manager = _get_manager()
    try:
        return await manager.add_connection(
            name=payload.name,
            database_url=payload.database_url.get_secret_value(),
            database_type=payload.database_type,
            tags=payload.tags,
            description=payload.description,
            is_default=payload.is_default,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ConnectorConnectionError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to connect to database: {exc}",
        ) from exc


@router.get("/databases", response_model=list[DatabaseConnection])
async def list_database_connections() -> list[DatabaseConnection]:
    """List active registry connections plus DATABASE_URL fallback."""
    from backend.api.main import app_state

    manager = app_state.get("database_manager")
    return await list_available_connections(manager)


@router.get("/databases/{connection_id}", response_model=DatabaseConnection)
async def get_database_connection(connection_id: str) -> DatabaseConnection:
    """Retrieve a single connection by ID."""
    from backend.api.main import app_state

    manager = app_state.get("database_manager")
    if connection_id == environment_connection_id():
        connections = await list_available_connections(manager)
        for connection in connections:
            if str(connection.connection_id) == connection_id:
                return connection
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection not found: {connection_id}",
        )

    manager = _get_manager()
    try:
        return await manager.get_connection(connection_id)
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/databases/{connection_id}/schema", response_model=DatabaseSchemaResponse)
async def get_database_schema(connection_id: str) -> DatabaseSchemaResponse:
    """Introspect schema for the selected connection."""
    from backend.api.main import app_state

    manager = app_state.get("database_manager")
    connections = await list_available_connections(manager)
    connection = next(
        (item for item in connections if str(item.connection_id) == connection_id),
        None,
    )
    if connection is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection not found: {connection_id}",
        )

    connector = create_connector(
        database_type=connection.database_type,
        database_url=connection.database_url.get_secret_value(),
    )
    try:
        await connector.connect()
        schema_tables = await connector.get_schema()
    except (ConnectorConnectionError, ConnectorSchemaError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to introspect schema: {exc}",
        ) from exc
    finally:
        await connector.close()

    tables = [
        DatabaseSchemaTable(
            schema_name=table.schema_name,
            table_name=table.table_name,
            row_count=table.row_count,
            table_type=table.table_type,
            columns=[
                DatabaseSchemaColumn(
                    name=column.name,
                    data_type=column.data_type,
                    is_nullable=column.is_nullable,
                    is_primary_key=column.is_primary_key,
                    is_foreign_key=column.is_foreign_key,
                    foreign_table=column.foreign_table,
                    foreign_column=column.foreign_column,
                )
                for column in table.columns
            ],
        )
        for table in schema_tables
    ]
    tables.sort(key=lambda item: (item.schema_name, item.table_name))

    return DatabaseSchemaResponse(
        connection_id=connection.connection_id,
        database_type=connection.database_type,
        tables=tables,
    )


@router.patch("/databases/{connection_id}", response_model=DatabaseConnection)
async def update_database_connection(
    connection_id: str,
    payload: DatabaseConnectionUpdate,
) -> DatabaseConnection:
    """Update an existing connection."""
    if connection_id == environment_connection_id():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Environment Database is derived from DATABASE_URL and cannot be edited here.",
        )
    update_payload = payload.model_dump(exclude_unset=True)
    if not update_payload:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one field must be provided for update.",
        )
    if update_payload.get("name") is None:
        update_payload.pop("name", None)
    if update_payload.get("database_type") is None:
        update_payload.pop("database_type", None)
    if "database_url" in update_payload and update_payload["database_url"] is not None:
        update_payload["database_url"] = update_payload["database_url"].get_secret_value()
    if update_payload.get("database_url") is None:
        update_payload.pop("database_url", None)
    if not update_payload:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one non-empty field must be provided for update.",
        )
    manager = _get_manager()
    try:
        return await manager.update_connection(connection_id, updates=update_payload)
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ConnectorConnectionError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to connect to database: {exc}",
        ) from exc


@router.put("/databases/{connection_id}/default", status_code=status.HTTP_204_NO_CONTENT)
async def set_default_database(
    connection_id: str, payload: DatabaseConnectionUpdateDefault
) -> None:
    """Set the default database connection."""
    if not payload.is_default:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="is_default must be true to set default connection",
        )
    if connection_id == environment_connection_id():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Environment Database is derived from DATABASE_URL and cannot be set "
                "as an explicit registry default."
            ),
        )
    manager = _get_manager()
    try:
        await manager.set_default(connection_id)
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.delete("/databases/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_database_connection(connection_id: str) -> None:
    """Delete a database connection."""
    if connection_id == environment_connection_id():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Environment Database is derived from DATABASE_URL and cannot be deleted here.",
        )
    manager = _get_manager()
    try:
        await manager.remove_connection(connection_id)
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
