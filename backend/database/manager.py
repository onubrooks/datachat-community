"""Database connection registry manager."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import asyncpg
from cryptography.fernet import Fernet, InvalidToken
from pydantic import SecretStr

from backend.config import get_settings
from backend.connectors.factory import create_connector, infer_database_type, resolve_database_type
from backend.models.database import DatabaseConnection

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS database_connections (
    connection_id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    database_url_encrypted TEXT NOT NULL,
    database_type TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    tags TEXT[] NOT NULL DEFAULT '{}',
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    last_profiled TIMESTAMPTZ,
    datapoint_count INTEGER NOT NULL DEFAULT 0
);
"""

_CREATE_DEFAULT_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS database_connections_default_idx
ON database_connections (is_default)
WHERE is_default;
"""


class DatabaseConnectionManager:
    """Manage database connections stored in the system database."""

    def __init__(
        self,
        system_database_url: str | None = None,
        encryption_key: str | bytes | None = None,
        pool: asyncpg.Pool | None = None,
    ) -> None:
        settings = get_settings()
        self._system_database_url = system_database_url or (
            str(settings.system_database.url) if settings.system_database.url else None
        )
        self._pool = pool
        self._encryption_key = encryption_key or settings.database_credentials_key
        self._cipher: Fernet | None = None

    async def initialize(self) -> None:
        """Initialize connection pool and ensure schema exists."""
        self._ensure_cipher()
        if self._pool is None:
            if not self._system_database_url:
                raise ValueError("SYSTEM_DATABASE_URL must be set for the database registry.")
            dsn = self._normalize_postgres_url(self._system_database_url)
            self._pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)
        await self._pool.execute(_CREATE_TABLE_SQL)
        await self._pool.execute(_CREATE_DEFAULT_INDEX_SQL)

    async def close(self) -> None:
        """Close the underlying connection pool."""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def add_connection(
        self,
        name: str,
        database_url: str,
        database_type: str,
        tags: list[str] | None = None,
        description: str | None = None,
        is_default: bool = False,
    ) -> DatabaseConnection:
        """Add a new database connection after validation."""
        self._ensure_pool()
        normalized_database_type = resolve_database_type(database_type, database_url)
        await self._validate_connection(normalized_database_type, database_url)

        connection_id = uuid4()
        created_at = datetime.now(UTC)
        encrypted_url = self._encrypt_url(database_url)
        tags = tags or []

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if is_default:
                    await conn.execute(
                        "UPDATE database_connections SET is_default = FALSE WHERE is_default = TRUE"
                    )

                row = await conn.fetchrow(
                    """
                    INSERT INTO database_connections (
                        connection_id,
                        name,
                        database_url_encrypted,
                        database_type,
                        is_active,
                        is_default,
                        tags,
                        description,
                        created_at,
                        last_profiled,
                        datapoint_count
                    ) VALUES ($1, $2, $3, $4, TRUE, $5, $6, $7, $8, NULL, 0)
                    RETURNING
                        connection_id,
                        name,
                        database_url_encrypted,
                        database_type,
                        is_active,
                        is_default,
                        tags,
                        description,
                        created_at,
                        last_profiled,
                        datapoint_count
                    """,
                    connection_id,
                    name,
                    encrypted_url,
                    normalized_database_type,
                    is_default,
                    tags,
                    description,
                    created_at,
                )

        return self._row_to_connection(row)

    async def list_connections(self) -> list[DatabaseConnection]:
        """List active database connections."""
        self._ensure_pool()
        rows = await self._pool.fetch(
            """
            SELECT
                connection_id,
                name,
                database_url_encrypted,
                database_type,
                is_active,
                is_default,
                tags,
                description,
                created_at,
                last_profiled,
                datapoint_count
            FROM database_connections
            WHERE is_active = TRUE
            ORDER BY created_at DESC
            """
        )
        return [self._row_to_connection(row) for row in rows]

    async def get_connection(self, connection_id: UUID | str) -> DatabaseConnection:
        """Retrieve a single active connection."""
        self._ensure_pool()
        connection_uuid = self._coerce_uuid(connection_id)
        row = await self._pool.fetchrow(
            """
            SELECT
                connection_id,
                name,
                database_url_encrypted,
                database_type,
                is_active,
                is_default,
                tags,
                description,
                created_at,
                last_profiled,
                datapoint_count
            FROM database_connections
            WHERE connection_id = $1 AND is_active = TRUE
            """,
            connection_uuid,
        )
        if row is None:
            raise KeyError(f"Connection not found: {connection_id}")
        return self._row_to_connection(row)

    async def get_default_connection(self) -> DatabaseConnection | None:
        """Return the default connection if set."""
        self._ensure_pool()
        row = await self._pool.fetchrow(
            """
            SELECT
                connection_id,
                name,
                database_url_encrypted,
                database_type,
                is_active,
                is_default,
                tags,
                description,
                created_at,
                last_profiled,
                datapoint_count
            FROM database_connections
            WHERE is_default = TRUE AND is_active = TRUE
            """
        )
        if row is None:
            return None
        return self._row_to_connection(row)

    async def set_default(self, connection_id: UUID | str) -> None:
        """Mark a connection as the default."""
        self._ensure_pool()
        connection_uuid = self._coerce_uuid(connection_id)

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                exists = await conn.fetchval(
                    """
                    SELECT 1
                    FROM database_connections
                    WHERE connection_id = $1 AND is_active = TRUE
                    """,
                    connection_uuid,
                )
                if not exists:
                    raise KeyError(f"Connection not found: {connection_id}")

                await conn.execute(
                    "UPDATE database_connections SET is_default = FALSE WHERE is_default = TRUE"
                )
                await conn.execute(
                    "UPDATE database_connections SET is_default = TRUE WHERE connection_id = $1",
                    connection_uuid,
                )

    async def remove_connection(self, connection_id: UUID | str) -> None:
        """Remove a connection from the registry."""
        self._ensure_pool()
        connection_uuid = self._coerce_uuid(connection_id)
        result = await self._pool.execute(
            "DELETE FROM database_connections WHERE connection_id = $1",
            connection_uuid,
        )
        deleted = int(result.split()[-1]) if result else 0
        if deleted == 0:
            raise KeyError(f"Connection not found: {connection_id}")

    async def update_connection(
        self,
        connection_id: UUID | str,
        *,
        updates: dict[str, str | None],
    ) -> DatabaseConnection:
        """Update editable connection fields."""
        self._ensure_pool()
        connection_uuid = self._coerce_uuid(connection_id)
        existing = await self.get_connection(connection_uuid)

        name = updates.get("name", existing.name)
        url_updated = "database_url" in updates
        type_updated = "database_type" in updates
        description_updated = "description" in updates

        database_url = (
            str(updates.get("database_url"))
            if url_updated
            else existing.database_url.get_secret_value()
        )
        if type_updated:
            database_type = resolve_database_type(
                str(updates.get("database_type")),
                database_url,
            )
        elif url_updated:
            database_type = infer_database_type(database_url)
        else:
            database_type = existing.database_type

        if url_updated or type_updated:
            await self._validate_connection(database_type, database_url)

        description = updates.get("description") if description_updated else existing.description
        encrypted_url = self._encrypt_url(database_url)

        row = await self._pool.fetchrow(
            """
            UPDATE database_connections
            SET
                name = $2,
                database_url_encrypted = $3,
                database_type = $4,
                description = $5
            WHERE connection_id = $1
            RETURNING
                connection_id,
                name,
                database_url_encrypted,
                database_type,
                is_active,
                is_default,
                tags,
                description,
                created_at,
                last_profiled,
                datapoint_count
            """,
            connection_uuid,
            name,
            encrypted_url,
            database_type,
            description,
        )
        if row is None:
            raise KeyError(f"Connection not found: {connection_id}")
        return self._row_to_connection(row)

    def _row_to_connection(self, row: asyncpg.Record) -> DatabaseConnection:
        decrypted_url = self._decrypt_url(row["database_url_encrypted"])
        return DatabaseConnection(
            connection_id=row["connection_id"],
            name=row["name"],
            database_url=SecretStr(decrypted_url),
            database_type=row["database_type"],
            is_active=row["is_active"],
            is_default=row["is_default"],
            tags=list(row["tags"] or []),
            description=row["description"],
            created_at=row["created_at"],
            last_profiled=row["last_profiled"],
            datapoint_count=row["datapoint_count"],
        )

    async def _validate_connection(self, database_type: str, database_url: str) -> None:
        inferred_database_type = infer_database_type(database_url)
        if inferred_database_type != database_type:
            raise ValueError(
                "Database type does not match URL scheme: "
                f"received '{database_type}', inferred '{inferred_database_type}' from "
                f"'{database_url}'."
            )
        connector = create_connector(database_url=database_url, database_type=database_type)

        try:
            await connector.connect()
        finally:
            await connector.close()

    def _encrypt_url(self, database_url: str) -> str:
        cipher = self._ensure_cipher()
        return cipher.encrypt(database_url.encode("utf-8")).decode("utf-8")

    def _decrypt_url(self, encrypted_url: str) -> str:
        cipher = self._ensure_cipher()
        try:
            return cipher.decrypt(encrypted_url.encode("utf-8")).decode("utf-8")
        except InvalidToken as exc:
            raise ValueError("Failed to decrypt database URL.") from exc

    def _ensure_cipher(self) -> Fernet:
        if self._cipher is not None:
            return self._cipher
        if not self._encryption_key:
            raise ValueError(
                "DATABASE_CREDENTIALS_KEY must be set to store encrypted database URLs."
            )
        key = self._encryption_key
        if isinstance(key, str):
            key = key.encode("utf-8")
        try:
            self._cipher = Fernet(key)
        except (ValueError, TypeError) as exc:
            raise ValueError(
                "Invalid DATABASE_CREDENTIALS_KEY. Use a Fernet-compatible base64 key."
            ) from exc
        return self._cipher

    def _ensure_pool(self) -> None:
        if self._pool is None:
            raise RuntimeError("DatabaseConnectionManager is not initialized")

    @staticmethod
    def _coerce_uuid(connection_id: UUID | str) -> UUID:
        if isinstance(connection_id, UUID):
            return connection_id
        try:
            return UUID(str(connection_id))
        except ValueError as exc:
            raise ValueError("Invalid connection ID.") from exc

    @staticmethod
    def _normalize_postgres_url(database_url: str) -> str:
        if database_url.startswith("postgresql+asyncpg://"):
            return database_url.replace("postgresql+asyncpg://", "postgresql://", 1)
        return database_url
