"""Unit tests for MySQLConnector."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from backend.connectors.base import ConnectionError, QueryError, SchemaError
from backend.connectors.mysql import MySQLConnector


def _install_fake_mysql(monkeypatch, connect_impl: Mock) -> None:
    fake_mysql = SimpleNamespace(
        connector=SimpleNamespace(connect=connect_impl),
    )
    monkeypatch.setattr("backend.connectors.mysql.mysql", fake_mysql)


def _build_connection(*, with_rows: bool = True, rows: list[dict] | None = None):
    conn = Mock()
    cursor = Mock()
    conn.cursor.return_value = cursor
    cursor.with_rows = with_rows
    cursor.fetchall.return_value = rows or []
    cursor.description = [("id",), ("name",)]
    return conn, cursor


@pytest.mark.asyncio
async def test_connect_success(monkeypatch):
    conn, cursor = _build_connection()
    connect_impl = Mock(return_value=conn)
    _install_fake_mysql(monkeypatch, connect_impl)

    connector = MySQLConnector(
        host="localhost",
        port=3306,
        database="app",
        user="root",
        password="secret",
    )
    await connector.connect()

    assert connector.is_connected is True
    cursor.execute.assert_called_with("SELECT VERSION()")
    conn.close.assert_called_once()


@pytest.mark.asyncio
async def test_connect_failure_raises_connection_error(monkeypatch):
    connect_impl = Mock(side_effect=Exception("connection refused"))
    _install_fake_mysql(monkeypatch, connect_impl)

    connector = MySQLConnector(
        host="localhost",
        port=3306,
        database="app",
        user="root",
        password="secret",
    )

    with pytest.raises(ConnectionError, match="Failed to connect|Connection error"):
        await connector.connect()


@pytest.mark.asyncio
async def test_execute_query_returns_rows(monkeypatch):
    conn1, _ = _build_connection()
    rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    conn2, cursor2 = _build_connection(rows=rows)
    connect_impl = Mock(side_effect=[conn1, conn2])
    _install_fake_mysql(monkeypatch, connect_impl)

    connector = MySQLConnector(
        host="localhost",
        port=3306,
        database="app",
        user="root",
        password="secret",
    )
    await connector.connect()
    result = await connector.execute("SELECT id, name FROM users")

    assert result.row_count == 2
    assert result.columns == ["id", "name"]
    assert result.rows[0]["id"] == 1
    cursor2.execute.assert_called_with("SELECT id, name FROM users")


@pytest.mark.asyncio
async def test_execute_without_connect_raises(monkeypatch):
    _install_fake_mysql(monkeypatch, Mock())
    connector = MySQLConnector(
        host="localhost",
        port=3306,
        database="app",
        user="root",
        password="secret",
    )
    with pytest.raises(ConnectionError, match="Not connected"):
        await connector.execute("SELECT 1")


@pytest.mark.asyncio
async def test_execute_error_raises_query_error(monkeypatch):
    conn1, _ = _build_connection()
    conn2, cursor2 = _build_connection()
    cursor2.execute.side_effect = Exception("bad query")
    connect_impl = Mock(side_effect=[conn1, conn2])
    _install_fake_mysql(monkeypatch, connect_impl)

    connector = MySQLConnector(
        host="localhost",
        port=3306,
        database="app",
        user="root",
        password="secret",
    )
    await connector.connect()
    with pytest.raises(QueryError, match="Query"):
        await connector.execute("SELECT nope")


@pytest.mark.asyncio
async def test_get_schema_success(monkeypatch):
    conn1, _ = _build_connection()
    conn2, cursor2 = _build_connection()
    cursor2.fetchall.side_effect = [
        [
            {
                "table_schema": "app",
                "table_name": "users",
                "table_type": "BASE TABLE",
                "table_rows": 10,
            }
        ],
        [
            {
                "column_name": "id",
                "column_type": "int",
                "is_nullable": "NO",
                "column_default": None,
                "column_key": "PRI",
            }
        ],
        [],
    ]
    connect_impl = Mock(side_effect=[conn1, conn2])
    _install_fake_mysql(monkeypatch, connect_impl)

    connector = MySQLConnector(
        host="localhost",
        port=3306,
        database="app",
        user="root",
        password="secret",
    )
    await connector.connect()
    tables = await connector.get_schema(schema_name="app")

    assert len(tables) == 1
    assert tables[0].table_name == "users"
    assert tables[0].columns[0].name == "id"


@pytest.mark.asyncio
async def test_get_schema_error_raises(monkeypatch):
    conn1, _ = _build_connection()
    conn2, cursor2 = _build_connection()
    cursor2.execute.side_effect = Exception("schema error")
    connect_impl = Mock(side_effect=[conn1, conn2])
    _install_fake_mysql(monkeypatch, connect_impl)

    connector = MySQLConnector(
        host="localhost",
        port=3306,
        database="app",
        user="root",
        password="secret",
    )
    await connector.connect()
    with pytest.raises(SchemaError, match="schema|introspect"):
        await connector.get_schema(schema_name="app")
