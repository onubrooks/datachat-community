"""
Unit tests for ClickHouseConnector.

Tests the ClickHouse connector with mocked clickhouse-connect client.
"""

from unittest.mock import Mock, patch

import pytest

from backend.connectors.base import (
    ConnectionError,
    QueryError,
    QueryResult,
    SchemaError,
)
from backend.connectors.clickhouse import ClickHouseConnector


@pytest.fixture
def clickhouse_config():
    """ClickHouse connection configuration."""
    return {
        "host": "localhost",
        "port": 8123,
        "database": "default",
        "user": "default",
        "password": "",
        "pool_size": 5,
        "timeout": 30,
    }


@pytest.fixture
def mock_client():
    """Mock clickhouse-connect client."""
    client = Mock()
    client.command = Mock(return_value="24.1.1.1")
    client.close = Mock()

    # Mock query result
    query_result = Mock()
    query_result.column_names = ["id", "name"]
    query_result.result_rows = [(1, "Alice"), (2, "Bob")]
    client.query = Mock(return_value=query_result)

    return client


class TestInitialization:
    """Test ClickHouseConnector initialization."""

    def test_initialization(self, clickhouse_config):
        """Test connector initializes with correct config."""
        connector = ClickHouseConnector(**clickhouse_config)

        assert connector.host == "localhost"
        assert connector.port == 8123
        assert connector.database == "default"
        assert connector.user == "default"
        assert connector.password == ""
        assert connector.pool_size == 5
        assert connector.timeout == 30
        assert connector.is_connected is False

    # Note: Testing import-time errors is complex and not critical for unit tests
    # The ImportError test is better handled in integration tests


class TestConnection:
    """Test connection management."""

    @pytest.mark.asyncio
    async def test_connect_success(self, clickhouse_config, mock_client):
        """Test successful connection."""
        with patch("clickhouse_connect.get_client", return_value=mock_client):
            connector = ClickHouseConnector(**clickhouse_config)
            await connector.connect()

            assert connector.is_connected is True
            assert connector._client is not None
            mock_client.command.assert_called_once_with("SELECT version()")

    @pytest.mark.asyncio
    async def test_connect_idempotent(self, clickhouse_config, mock_client):
        """Test connecting multiple times doesn't create multiple clients."""
        with patch("clickhouse_connect.get_client", return_value=mock_client) as get_client:
            connector = ClickHouseConnector(**clickhouse_config)
            await connector.connect()
            await connector.connect()  # Second call

            # Should only create client once
            assert get_client.call_count == 1

    @pytest.mark.asyncio
    async def test_connect_failure(self, clickhouse_config):
        """Test connection failure raises ConnectionError."""
        with patch(
            "clickhouse_connect.get_client",
            side_effect=Exception("Connection refused"),
        ):
            connector = ClickHouseConnector(**clickhouse_config)

            with pytest.raises(ConnectionError, match="Failed to connect"):
                await connector.connect()

            assert connector.is_connected is False

    @pytest.mark.asyncio
    async def test_close(self, clickhouse_config, mock_client):
        """Test closing connection."""
        with patch("clickhouse_connect.get_client", return_value=mock_client):
            connector = ClickHouseConnector(**clickhouse_config)
            await connector.connect()
            await connector.close()

            assert connector.is_connected is False
            assert connector._client is None
            mock_client.close.assert_called_once()


class TestQueryExecution:
    """Test query execution."""

    @pytest.mark.asyncio
    async def test_execute_simple_query(self, clickhouse_config, mock_client):
        """Test executing a simple query."""
        with patch("clickhouse_connect.get_client", return_value=mock_client):
            connector = ClickHouseConnector(**clickhouse_config)
            await connector.connect()

            result = await connector.execute("SELECT * FROM users")

            assert isinstance(result, QueryResult)
            assert result.row_count == 2
            assert len(result.rows) == 2
            assert result.columns == ["id", "name"]
            assert result.rows[0] == {"id": 1, "name": "Alice"}
            assert result.rows[1] == {"id": 2, "name": "Bob"}
            assert result.execution_time_ms > 0

    @pytest.mark.asyncio
    async def test_execute_with_parameters(self, clickhouse_config, mock_client):
        """Test executing query with parameters."""
        with patch("clickhouse_connect.get_client", return_value=mock_client):
            connector = ClickHouseConnector(**clickhouse_config)
            await connector.connect()

            params = {"id": 123, "date": "2024-01-01"}
            await connector.execute(
                "SELECT * FROM events WHERE id = {id:UInt64} AND date >= {date:Date}",
                params=params,
            )

            # Verify query was called with parameters
            mock_client.query.assert_called()
            call_kwargs = mock_client.query.call_args[1]
            assert call_kwargs["parameters"] == params

    @pytest.mark.asyncio
    async def test_execute_without_connection(self, clickhouse_config):
        """Test executing query without connection raises error."""
        connector = ClickHouseConnector(**clickhouse_config)

        with pytest.raises(ConnectionError, match="Not connected"):
            await connector.execute("SELECT 1")

    @pytest.mark.asyncio
    async def test_execute_query_error(self, clickhouse_config, mock_client):
        """Test query execution error."""
        mock_client.query = Mock(side_effect=Exception("Query error"))

        with patch("clickhouse_connect.get_client", return_value=mock_client):
            connector = ClickHouseConnector(**clickhouse_config)
            await connector.connect()

            with pytest.raises(QueryError, match="Query execution failed"):
                await connector.execute("INVALID SQL")

    @pytest.mark.asyncio
    async def test_execute_custom_timeout(self, clickhouse_config, mock_client):
        """Test query with custom timeout."""
        with patch("clickhouse_connect.get_client", return_value=mock_client):
            connector = ClickHouseConnector(**clickhouse_config)
            await connector.connect()

            await connector.execute("SELECT 1", timeout=60)

            # Verify timeout was set in settings
            call_kwargs = mock_client.query.call_args[1]
            assert call_kwargs["settings"]["max_execution_time"] == 60


class TestSchemaIntrospection:
    """Test schema introspection."""

    @pytest.mark.asyncio
    async def test_get_schema_success(self, clickhouse_config, mock_client):
        """Test successful schema introspection."""
        # Mock tables query result
        tables_result = Mock()
        tables_result.column_names = ["database", "name", "engine"]
        tables_result.result_rows = [("default", "events", "MergeTree")]

        # Mock columns query result
        columns_result = Mock()
        columns_result.column_names = ["name", "type", "default_kind", "default_expression"]
        columns_result.result_rows = [
            ("id", "UInt64", "", None),
            ("timestamp", "DateTime", "", None),
            ("user_id", "Nullable(UInt64)", "", None),
        ]

        # Mock count query result
        count_result = Mock()
        count_result.column_names = ["count()"]
        count_result.result_rows = [(1000,)]

        # Setup query to return different results
        def mock_query(query, **kwargs):
            if "system.tables" in query:
                return tables_result
            elif "system.columns" in query:
                return columns_result
            elif "count()" in query:
                return count_result
            return Mock()

        mock_client.query = Mock(side_effect=mock_query)

        with patch("clickhouse_connect.get_client", return_value=mock_client):
            connector = ClickHouseConnector(**clickhouse_config)
            await connector.connect()

            tables = await connector.get_schema(schema_name="default")

            assert len(tables) == 1
            table = tables[0]

            assert table.schema_name == "default"
            assert table.table_name == "events"
            assert table.table_type == "MergeTree"
            assert table.row_count == 1000
            assert len(table.columns) == 3

            # Check columns
            id_col = table.columns[0]
            assert id_col.name == "id"
            assert id_col.data_type == "UInt64"
            assert id_col.is_nullable is False

            user_col = table.columns[2]
            assert user_col.name == "user_id"
            assert user_col.is_nullable is True  # Nullable(UInt64)

    @pytest.mark.asyncio
    async def test_get_schema_without_connection(self, clickhouse_config):
        """Test schema introspection without connection."""
        connector = ClickHouseConnector(**clickhouse_config)

        with pytest.raises(ConnectionError, match="Not connected"):
            await connector.get_schema()

    @pytest.mark.asyncio
    async def test_get_schema_error(self, clickhouse_config, mock_client):
        """Test schema introspection error."""
        mock_client.query = Mock(side_effect=Exception("Schema error"))

        with patch("clickhouse_connect.get_client", return_value=mock_client):
            connector = ClickHouseConnector(**clickhouse_config)
            await connector.connect()

            with pytest.raises(SchemaError, match="Failed to introspect schema"):
                await connector.get_schema()


class TestContextManager:
    """Test async context manager."""

    @pytest.mark.asyncio
    async def test_context_manager(self, clickhouse_config, mock_client):
        """Test using connector as async context manager."""
        with patch("clickhouse_connect.get_client", return_value=mock_client):
            async with ClickHouseConnector(**clickhouse_config) as connector:
                assert connector.is_connected is True

            # Should be closed after exiting context
            assert connector.is_connected is False
            mock_client.close.assert_called_once()
