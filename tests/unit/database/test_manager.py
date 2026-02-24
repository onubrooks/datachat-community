"""Unit tests for DatabaseConnectionManager."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from cryptography.fernet import Fernet

from backend.database.manager import DatabaseConnectionManager


class TestDatabaseConnectionManager:
    """Test database connection registry behavior."""

    @pytest.fixture
    def encryption_key(self):
        return Fernet.generate_key()

    @pytest.fixture
    def pool(self):
        pool = AsyncMock()
        pool.fetch = AsyncMock()
        pool.fetchrow = AsyncMock()
        pool.fetchval = AsyncMock()
        pool.execute = AsyncMock(return_value="DELETE 1")
        return pool

    def _build_conn(self, pool: AsyncMock):
        conn = AsyncMock()
        conn_context = AsyncMock()
        conn_context.__aenter__.return_value = conn
        conn_context.__aexit__.return_value = None
        pool.acquire = MagicMock(return_value=conn_context)
        tx_context = AsyncMock()
        tx_context.__aenter__.return_value = None
        tx_context.__aexit__.return_value = None
        conn.transaction = MagicMock(return_value=tx_context)
        return conn

    @pytest.mark.asyncio
    async def test_add_connection_validates_and_returns(self, encryption_key, pool):
        manager = DatabaseConnectionManager(encryption_key=encryption_key, pool=pool)
        manager._validate_connection = AsyncMock()
        conn = self._build_conn(pool)

        database_url = "postgresql://user:pass@localhost:5432/warehouse"
        encrypted_url = manager._encrypt_url(database_url)
        row = {
            "connection_id": uuid4(),
            "name": "Warehouse",
            "database_url_encrypted": encrypted_url,
            "database_type": "postgresql",
            "is_active": True,
            "is_default": True,
            "tags": ["prod"],
            "description": "Primary warehouse",
            "created_at": "2024-01-01T00:00:00Z",
            "last_profiled": None,
            "datapoint_count": 0,
        }
        conn.fetchrow = AsyncMock(return_value=row)

        connection = await manager.add_connection(
            name="Warehouse",
            database_url=database_url,
            database_type="postgresql",
            tags=["prod"],
            description="Primary warehouse",
            is_default=True,
        )

        manager._validate_connection.assert_awaited_once()
        assert connection.name == "Warehouse"
        assert connection.database_url.get_secret_value() == database_url
        assert connection.is_default is True

    @pytest.mark.asyncio
    async def test_list_connections_returns_active(self, encryption_key, pool):
        manager = DatabaseConnectionManager(encryption_key=encryption_key, pool=pool)
        database_url = "postgresql://user:pass@localhost:5432/warehouse"
        encrypted_url = manager._encrypt_url(database_url)
        pool.fetch.return_value = [
            {
                "connection_id": uuid4(),
                "name": "Warehouse",
                "database_url_encrypted": encrypted_url,
                "database_type": "postgresql",
                "is_active": True,
                "is_default": False,
                "tags": [],
                "description": None,
                "created_at": "2024-01-01T00:00:00Z",
                "last_profiled": None,
                "datapoint_count": 0,
            }
        ]

        results = await manager.list_connections()

        assert len(results) == 1
        assert results[0].database_url.get_secret_value() == database_url

    @pytest.mark.asyncio
    async def test_set_default_updates_flags(self, encryption_key, pool):
        manager = DatabaseConnectionManager(encryption_key=encryption_key, pool=pool)
        conn = self._build_conn(pool)
        conn.fetchval = AsyncMock(return_value=1)

        await manager.set_default(uuid4())

        assert conn.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_remove_connection_not_found(self, encryption_key, pool):
        manager = DatabaseConnectionManager(encryption_key=encryption_key, pool=pool)
        pool.execute = AsyncMock(return_value="DELETE 0")

        with pytest.raises(KeyError):
            await manager.remove_connection(uuid4())

    @pytest.mark.asyncio
    async def test_add_connection_accepts_mysql(self, encryption_key, pool):
        manager = DatabaseConnectionManager(encryption_key=encryption_key, pool=pool)
        manager._validate_connection = AsyncMock()
        conn = self._build_conn(pool)

        database_url = "mysql://user:pass@localhost:3306/app"
        encrypted_url = manager._encrypt_url(database_url)
        row = {
            "connection_id": uuid4(),
            "name": "MySQL",
            "database_url_encrypted": encrypted_url,
            "database_type": "mysql",
            "is_active": True,
            "is_default": False,
            "tags": [],
            "description": None,
            "created_at": "2024-01-01T00:00:00Z",
            "last_profiled": None,
            "datapoint_count": 0,
        }
        conn.fetchrow = AsyncMock(return_value=row)

        connection = await manager.add_connection(
            name="MySQL",
            database_url=database_url,
            database_type="mysql",
        )

        assert connection.database_type == "mysql"
        manager._validate_connection.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_validate_connection_rejects_type_url_mismatch(self, encryption_key, pool):
        manager = DatabaseConnectionManager(encryption_key=encryption_key, pool=pool)

        with pytest.raises(ValueError, match="does not match URL scheme"):
            await manager._validate_connection(
                database_type="postgresql",
                database_url="mysql://user:pass@localhost:3306/app",
            )

    @pytest.mark.asyncio
    async def test_update_connection_updates_fields(self, encryption_key, pool):
        manager = DatabaseConnectionManager(encryption_key=encryption_key, pool=pool)
        manager._validate_connection = AsyncMock()

        old_url = "postgresql://user:pass@localhost:5432/warehouse"
        old_row = {
            "connection_id": uuid4(),
            "name": "Warehouse",
            "database_url_encrypted": manager._encrypt_url(old_url),
            "database_type": "postgresql",
            "is_active": True,
            "is_default": False,
            "tags": [],
            "description": "old",
            "created_at": "2024-01-01T00:00:00Z",
            "last_profiled": None,
            "datapoint_count": 0,
        }
        new_url = "mysql://user:pass@localhost:3306/warehouse"
        updated_row = {
            **old_row,
            "name": "Warehouse V2",
            "database_url_encrypted": manager._encrypt_url(new_url),
            "database_type": "mysql",
            "description": "new",
        }
        pool.fetchrow = AsyncMock(side_effect=[old_row, updated_row])

        updated = await manager.update_connection(
            old_row["connection_id"],
            updates={
                "name": "Warehouse V2",
                "database_url": new_url,
                "database_type": "mysql",
                "description": "new",
            },
        )

        assert updated.name == "Warehouse V2"
        assert updated.database_type == "mysql"
        assert updated.database_url.get_secret_value() == new_url
        assert updated.description == "new"
        manager._validate_connection.assert_awaited_once_with("mysql", new_url)
