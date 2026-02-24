"""Unit tests for database registry endpoints."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from backend.api.main import app
from backend.connectors.base import (
    ColumnInfo,
    TableInfo,
)
from backend.connectors.base import (
    ConnectionError as ConnectorConnectionError,
)
from backend.models.database import DatabaseConnection


class TestDatabaseEndpoints:
    """Test CRUD endpoints for database connections."""

    @pytest.fixture
    def client(self):
        return TestClient(app)

    @pytest.fixture
    def sample_connection(self):
        return DatabaseConnection(
            connection_id=uuid4(),
            name="Warehouse",
            database_url=SecretStr("postgresql://user:pass@localhost:5432/warehouse"),
            database_type="postgresql",
            is_active=True,
            is_default=False,
            tags=["prod"],
            description="Primary warehouse",
            datapoint_count=0,
        )

    def test_create_connection(self, client, sample_connection):
        manager = AsyncMock()
        manager.add_connection = AsyncMock(return_value=sample_connection)

        with patch("backend.api.routes.databases._get_manager", return_value=manager):
            response = client.post(
                "/api/v1/databases",
                json={
                    "name": "Warehouse",
                    "database_url": "postgresql://user:pass@localhost:5432/warehouse",
                    "database_type": "postgresql",
                    "tags": ["prod"],
                    "description": "Primary warehouse",
                    "is_default": False,
                },
            )

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "Warehouse"
        manager.add_connection.assert_awaited_once()

    def test_list_connections(self, client, sample_connection):
        with (
            patch(
                "backend.api.main.app_state",
                {"database_manager": None},
            ),
            patch(
                "backend.api.routes.databases.list_available_connections",
                new=AsyncMock(return_value=[sample_connection]),
            ),
        ):
            response = client.get("/api/v1/databases")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["name"] == "Warehouse"

    def test_get_environment_connection(self, client, sample_connection):
        env_connection_id = str(sample_connection.connection_id)
        with (
            patch(
                "backend.api.main.app_state",
                {"database_manager": None},
            ),
            patch(
                "backend.api.routes.databases.environment_connection_id",
                return_value=env_connection_id,
            ),
            patch(
                "backend.api.routes.databases.list_available_connections",
                new=AsyncMock(return_value=[sample_connection]),
            ),
        ):
            response = client.get(f"/api/v1/databases/{env_connection_id}")

        assert response.status_code == 200
        assert response.json()["name"] == "Warehouse"

    def test_get_connection(self, client, sample_connection):
        manager = AsyncMock()
        manager.get_connection = AsyncMock(return_value=sample_connection)

        with patch("backend.api.routes.databases._get_manager", return_value=manager):
            response = client.get(f"/api/v1/databases/{sample_connection.connection_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Warehouse"

    def test_get_connection_not_found(self, client):
        manager = AsyncMock()
        manager.get_connection = AsyncMock(side_effect=KeyError("Not found"))

        with patch("backend.api.routes.databases._get_manager", return_value=manager):
            response = client.get("/api/v1/databases/00000000-0000-0000-0000-000000000000")

        assert response.status_code == 404

    def test_set_default_connection(self, client, sample_connection):
        manager = AsyncMock()
        manager.set_default = AsyncMock(return_value=None)

        with patch("backend.api.routes.databases._get_manager", return_value=manager):
            response = client.put(
                f"/api/v1/databases/{sample_connection.connection_id}/default",
                json={"is_default": True},
            )

        assert response.status_code == 204
        manager.set_default.assert_awaited_once()

    def test_update_connection(self, client, sample_connection):
        manager = AsyncMock()
        manager.update_connection = AsyncMock(return_value=sample_connection)

        with patch("backend.api.routes.databases._get_manager", return_value=manager):
            response = client.patch(
                f"/api/v1/databases/{sample_connection.connection_id}",
                json={
                    "name": "Warehouse",
                    "database_url": "postgresql://user:pass@localhost:5432/warehouse",
                    "database_type": "postgresql",
                    "description": "Updated",
                },
            )

        assert response.status_code == 200
        manager.update_connection.assert_awaited_once()

    def test_update_connection_empty_payload_returns_bad_request(self, client, sample_connection):
        manager = AsyncMock()
        manager.update_connection = AsyncMock(return_value=sample_connection)

        with patch("backend.api.routes.databases._get_manager", return_value=manager):
            response = client.patch(
                f"/api/v1/databases/{sample_connection.connection_id}",
                json={},
            )

        assert response.status_code == 400
        assert "At least one field" in response.json()["detail"]

    def test_update_environment_connection_returns_bad_request(self, client):
        with patch(
            "backend.api.routes.databases.environment_connection_id",
            return_value="00000000-0000-0000-0000-00000000dada",
        ):
            response = client.patch(
                "/api/v1/databases/00000000-0000-0000-0000-00000000dada",
                json={"name": "Nope"},
            )

        assert response.status_code == 400
        assert "Environment Database" in response.json()["detail"]

    def test_set_default_environment_connection_returns_bad_request(self, client):
        with patch(
            "backend.api.routes.databases.environment_connection_id",
            return_value="00000000-0000-0000-0000-00000000dada",
        ):
            response = client.put(
                "/api/v1/databases/00000000-0000-0000-0000-00000000dada/default",
                json={"is_default": True},
            )

        assert response.status_code == 400
        assert "Environment Database" in response.json()["detail"]

    def test_delete_connection(self, client, sample_connection):
        manager = AsyncMock()
        manager.remove_connection = AsyncMock(return_value=None)

        with patch("backend.api.routes.databases._get_manager", return_value=manager):
            response = client.delete(f"/api/v1/databases/{sample_connection.connection_id}")

        assert response.status_code == 204
        manager.remove_connection.assert_awaited_once()

    def test_delete_environment_connection_returns_bad_request(self, client):
        with patch(
            "backend.api.routes.databases.environment_connection_id",
            return_value="00000000-0000-0000-0000-00000000dada",
        ):
            response = client.delete("/api/v1/databases/00000000-0000-0000-0000-00000000dada")

        assert response.status_code == 400
        assert "Environment Database" in response.json()["detail"]

    def test_create_connection_validation_error(self, client):
        manager = AsyncMock()
        manager.add_connection = AsyncMock(side_effect=ValueError("Invalid URL"))

        with patch("backend.api.routes.databases._get_manager", return_value=manager):
            response = client.post(
                "/api/v1/databases",
                json={
                    "name": "Bad",
                    "database_url": "not-a-url",
                    "database_type": "postgresql",
                },
            )

        assert response.status_code == 400

    def test_create_connection_connection_error_returns_bad_request(self, client):
        manager = AsyncMock()
        manager.add_connection = AsyncMock(side_effect=ConnectorConnectionError("dial tcp timeout"))

        with patch("backend.api.routes.databases._get_manager", return_value=manager):
            response = client.post(
                "/api/v1/databases",
                json={
                    "name": "Warehouse",
                    "database_url": "postgresql://user:pass@localhost:5432/warehouse",
                    "database_type": "postgresql",
                },
            )

        assert response.status_code == 400
        assert "Failed to connect to database" in response.json()["detail"]

    def test_get_connection_schema(self, client, sample_connection):
        connector = AsyncMock()
        connector.connect = AsyncMock(return_value=None)
        connector.close = AsyncMock(return_value=None)
        connector.get_schema = AsyncMock(
            return_value=[
                TableInfo(
                    schema="public",
                    table_name="orders",
                    row_count=250,
                    table_type="BASE TABLE",
                    columns=[
                        ColumnInfo(
                            name="order_id",
                            data_type="integer",
                            is_nullable=False,
                            is_primary_key=True,
                        ),
                        ColumnInfo(
                            name="customer_id",
                            data_type="integer",
                            is_nullable=False,
                            is_foreign_key=True,
                            foreign_table="customers",
                            foreign_column="customer_id",
                        ),
                    ],
                )
            ]
        )

        with (
            patch("backend.api.main.app_state", {"database_manager": None}),
            patch(
                "backend.api.routes.databases.list_available_connections",
                new=AsyncMock(return_value=[sample_connection]),
            ),
            patch(
                "backend.api.routes.databases.create_connector",
                return_value=connector,
            ),
        ):
            response = client.get(f"/api/v1/databases/{sample_connection.connection_id}/schema")

        assert response.status_code == 200
        payload = response.json()
        assert payload["connection_id"] == str(sample_connection.connection_id)
        assert len(payload["tables"]) == 1
        assert payload["tables"][0]["table_name"] == "orders"
        assert payload["tables"][0]["columns"][0]["is_primary_key"] is True
