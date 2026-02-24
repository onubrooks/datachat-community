"""Tests for API database context resolution helpers."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.api.database_context import (
    ENV_DATABASE_CONNECTION_ID,
    list_available_connections,
    resolve_database_type_and_url,
)
from backend.config import get_settings


@pytest.mark.asyncio
async def test_resolve_database_context_with_environment_target(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "mysql://root:password@localhost:3306/demo")
    get_settings.cache_clear()

    database_type, database_url = await resolve_database_type_and_url(
        target_database=str(ENV_DATABASE_CONNECTION_ID),
        manager=None,
    )

    assert database_type == "mysql"
    assert database_url == "mysql://root:password@localhost:3306/demo"


@pytest.mark.asyncio
async def test_list_available_connections_appends_environment_connection(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "mysql://root:password@localhost:3306/demo")
    get_settings.cache_clear()

    manager = SimpleNamespace(
        list_connections=lambda: None,
    )

    async def _list_connections():
        return []

    manager.list_connections = _list_connections

    connections = await list_available_connections(manager)

    assert len(connections) == 1
    assert str(connections[0].connection_id) == str(ENV_DATABASE_CONNECTION_ID)
    assert connections[0].database_type == "mysql"
    assert connections[0].is_default is True


@pytest.mark.asyncio
async def test_list_available_connections_skips_duplicate_environment_url(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/app")
    get_settings.cache_clear()

    managed_connection = SimpleNamespace(
        connection_id="11111111-1111-1111-1111-111111111111",
        name="Managed",
        database_url=SimpleNamespace(
            get_secret_value=lambda: "postgresql://user:pass@localhost:5432/app"
        ),
        database_type="postgresql",
        is_active=True,
        is_default=True,
        tags=[],
        description=None,
        created_at=None,
        last_profiled=None,
        datapoint_count=0,
    )

    async def _list_connections():
        return [managed_connection]

    manager = SimpleNamespace(list_connections=_list_connections)
    connections = await list_available_connections(manager)

    assert len(connections) == 1
    assert connections[0].name == "Managed"
