"""
Unit Tests for CLI

Tests the DataChat CLI commands.
"""

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import click
import pytest
from click.testing import CliRunner

from backend.cli import (
    _apply_datapoint_scope,
    _apply_display_pagination,
    _emit_entry_event_cli,
    _is_read_only_sql,
    _normalize_target_database,
    _register_cli_connection,
    _render_template_query,
    _resolve_registry_connection_id_for_url,
    _resolve_schema_table_match,
    _resolve_target_database_url,
    _should_exit_chat,
    _split_sql_statements,
    ask,
    cli,
    connect,
    create_pipeline_from_config,
    datapoint,
    setup,
    status,
)
from backend.initialization.initializer import SystemStatus


class TestCLIBasics:
    """Test basic CLI functionality."""

    @pytest.fixture
    def runner(self):
        """Create CLI test runner."""
        return CliRunner()

    def test_cli_help(self, runner):
        """Test that CLI shows help text."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "DataChat" in result.output
        assert "Natural language interface" in result.output

    def test_cli_version(self, runner):
        """Test that CLI shows version."""
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output

    def test_chat_command_exists(self, runner):
        """Test that chat command exists."""
        result = runner.invoke(cli, ["chat", "--help"])
        assert result.exit_code == 0
        assert "Interactive REPL mode" in result.output

    def test_cheat_sheet_command_exists(self, runner):
        """Test that cheat-sheet command exists."""
        result = runner.invoke(cli, ["cheat-sheet", "--help"])
        assert result.exit_code == 0
        assert "quick cli command cheat sheet" in result.output.lower()

    def test_cheat_sheet_command_outputs_reference(self, runner):
        """Test cheat-sheet output includes key command references."""
        result = runner.invoke(cli, ["cheat-sheet"])
        assert result.exit_code == 0
        assert "DataChat CLI Cheat Sheet" in result.output
        assert "datachat ask \"list tables\"" in result.output
        assert "datachat dp sync" in result.output

    def test_ask_command_exists(self, runner):
        """Test that ask command exists."""
        result = runner.invoke(cli, ["ask", "--help"])
        assert result.exit_code == 0
        assert "Ask a single question" in result.output

    def test_connect_command_exists(self, runner):
        """Test that connect command exists."""
        result = runner.invoke(cli, ["connect", "--help"])
        assert result.exit_code == 0
        assert "Set database connection" in result.output

    def test_status_command_exists(self, runner):
        """Test that status command exists."""
        result = runner.invoke(cli, ["status", "--help"])
        assert result.exit_code == 0
        assert "Show connection and system status" in result.output

    def test_setup_command_exists(self, runner):
        """Test that setup command exists."""
        result = runner.invoke(cli, ["setup", "--help"])
        assert result.exit_code == 0
        assert "Guide system initialization" in result.output

    def test_quickstart_command_exists(self, runner):
        """Test that quickstart command exists."""
        result = runner.invoke(cli, ["quickstart", "--help"])
        assert result.exit_code == 0
        assert "guided bootstrap flow" in result.output.lower()

    def test_onboarding_wizard_command_exists(self, runner):
        """Test onboarding wizard command exists."""
        result = runner.invoke(cli, ["onboarding", "wizard", "--help"])
        assert result.exit_code == 0
        assert "onboarding wizard" in result.output.lower()

    def test_train_command_exists(self, runner):
        """Test that train command exists."""
        result = runner.invoke(cli, ["train", "--help"])
        assert result.exit_code == 0
        assert "sync/profile generation flows" in result.output.lower()

    def test_schema_command_exists(self, runner):
        """Test that schema group exists."""
        result = runner.invoke(cli, ["schema", "--help"])
        assert result.exit_code == 0
        assert "Browse live database schema" in result.output

    def test_template_command_exists(self, runner):
        """Test that template group exists."""
        result = runner.invoke(cli, ["template", "--help"])
        assert result.exit_code == 0
        assert "query templates" in result.output.lower()

    def test_session_command_exists(self, runner):
        """Test that session group exists."""
        result = runner.invoke(cli, ["session", "--help"])
        assert result.exit_code == 0
        assert "saved cli sessions" in result.output.lower()

    def test_reset_command_exposes_datapoint_clear_flags(self, runner):
        """Reset command should expose datapoint clear/keep options."""
        result = runner.invoke(cli, ["reset", "--help"])
        assert result.exit_code == 0
        assert "--clear-managed-datapoints" in result.output
        assert "--clear-user-datapoints" in result.output
        assert "--clear-example-datapoints" in result.output

    def test_datapoint_group_exists(self, runner):
        """Test that datapoint command group exists."""
        result = runner.invoke(cli, ["dp", "--help"])
        assert result.exit_code == 0
        assert "Manage DataPoints" in result.output

    def test_exit_phrase_detection_end_and_never_mind(self):
        assert _should_exit_chat("end") is True
        assert _should_exit_chat("never mind, i'll ask later") is True

    def test_split_sql_statements_ignores_comments(self):
        sql = """
        -- comment
        SELECT 1;
        INSERT INTO test_table(id) VALUES (1); -- inline comment
        """
        statements = _split_sql_statements(sql)
        assert statements == [
            "SELECT 1",
            "INSERT INTO test_table(id) VALUES (1)",
        ]

    def test_apply_datapoint_scope_connection(self):
        datapoint = MagicMock()
        datapoint.metadata = {"source": "test"}
        _apply_datapoint_scope([datapoint], connection_id="conn-123")
        assert datapoint.metadata["connection_id"] == "conn-123"
        assert datapoint.metadata["scope"] == "database"

    def test_apply_datapoint_scope_global(self):
        datapoint = MagicMock()
        datapoint.metadata = {"connection_id": "conn-123", "source": "test"}
        _apply_datapoint_scope([datapoint], global_scope=True)
        assert "connection_id" not in datapoint.metadata
        assert datapoint.metadata["scope"] == "global"

    def test_read_only_sql_guard(self):
        assert _is_read_only_sql("SELECT * FROM users LIMIT 10")
        assert _is_read_only_sql("WITH x AS (SELECT 1) SELECT * FROM x")
        assert not _is_read_only_sql("DELETE FROM users")
        assert not _is_read_only_sql("SELECT 1; SELECT 2;")

    def test_display_pagination_slices_rows(self):
        data = {"id": [1, 2, 3, 4, 5], "name": ["a", "b", "c", "d", "e"]}
        page, info = _apply_display_pagination(data, page=2, page_size=2)
        assert page == {"id": [3, 4], "name": ["c", "d"]}
        assert info == {
            "total_rows": 5,
            "page": 2,
            "page_size": 2,
            "start_row": 3,
            "end_row": 4,
            "total_pages": 3,
        }

    def test_render_template_query_uses_default_table(self):
        rendered = _render_template_query("sample-rows")
        assert "grocery_sales_transactions" in rendered

    def test_normalize_target_database_treats_none_like_missing(self):
        assert _normalize_target_database(None) is None
        assert _normalize_target_database("") is None
        assert _normalize_target_database("None") is None
        assert _normalize_target_database("null") is None
        assert _normalize_target_database(" conn-123 ") == "conn-123"

    def test_resolve_schema_table_match_without_schema_uses_unique_match(self):
        table = MagicMock()
        table.schema_name = "analytics"
        table.table_name = "orders"
        resolved = _resolve_schema_table_match(
            [table],
            requested_table="orders",
            requested_schema=None,
        )
        assert resolved is table

    def test_resolve_schema_table_match_without_schema_rejects_ambiguous(self):
        first = MagicMock()
        first.schema_name = "public"
        first.table_name = "orders"
        second = MagicMock()
        second.schema_name = "analytics"
        second.table_name = "orders"
        with pytest.raises(click.ClickException, match="ambiguous"):
            _resolve_schema_table_match(
                [first, second],
                requested_table="orders",
                requested_schema=None,
            )

    def test_resolve_schema_table_match_with_schema_filters_match(self):
        first = MagicMock()
        first.schema_name = "public"
        first.table_name = "orders"
        second = MagicMock()
        second.schema_name = "analytics"
        second.table_name = "orders"
        resolved = _resolve_schema_table_match(
            [first, second],
            requested_table="orders",
            requested_schema="analytics",
        )
        assert resolved is second


class TestDemoCommand:
    """Test demo command behavior."""

    @pytest.fixture
    def runner(self):
        """Create CLI test runner."""
        return CliRunner()

    @staticmethod
    def _make_settings(database_url: str | None):
        """Build minimal settings object for demo command tests."""
        return type(
            "Settings",
            (),
            {
                "database": type("DatabaseSettings", (), {"url": database_url})(),
                "system_database": type("SystemDatabaseSettings", (), {"url": None})(),
            },
        )()

    def test_demo_requires_target_database(self, runner):
        settings = self._make_settings(database_url=None)
        with (
            patch("backend.cli.apply_config_defaults"),
            patch("backend.cli.get_settings", return_value=settings),
            patch("backend.cli.state.get_connection_string", return_value=None),
        ):
            result = runner.invoke(cli, ["demo", "--dataset", "grocery", "--no-workspace"])

        assert result.exit_code != 0
        assert "DATABASE_URL" in result.output

    def test_demo_grocery_uses_target_database(self, runner):
        settings = self._make_settings("postgresql://demo:pw@demo-host:5432/datachat_grocery")

        mock_connector = AsyncMock()
        mock_connector.connect = AsyncMock()
        mock_connector.execute = AsyncMock()
        mock_connector.close = AsyncMock()

        mock_vector_store = AsyncMock()
        mock_vector_store.initialize = AsyncMock()
        mock_vector_store.clear = AsyncMock()
        mock_vector_store.add_datapoints = AsyncMock()

        mock_graph = MagicMock()
        mock_graph.add_datapoint = MagicMock()

        with (
            patch("backend.cli.apply_config_defaults"),
            patch("backend.cli.get_settings", return_value=settings),
            patch("backend.cli.PostgresConnector", return_value=mock_connector) as connector_cls,
            patch("backend.cli.DataPointLoader") as loader_cls,
            patch("backend.cli.VectorStore", return_value=mock_vector_store),
            patch("backend.cli.KnowledgeGraph", return_value=mock_graph),
        ):
            loader_cls.return_value.load_directory.return_value = [MagicMock()]
            result = runner.invoke(
                cli, ["demo", "--dataset", "grocery", "--reset", "--no-workspace"]
            )

        assert result.exit_code == 0
        connector_cls.assert_called_once_with(
            host="demo-host",
            port=5432,
            database="datachat_grocery",
            user="demo",
            password="pw",
        )
        loader_cls.return_value.load_directory.assert_called_once_with(
            Path("datapoints") / "examples" / "grocery_store"
        )
        assert "demo-host:5432/datachat_grocery" in result.output

    def test_demo_fintech_uses_target_database(self, runner):
        settings = self._make_settings("postgresql://demo:pw@demo-host:5432/datachat_fintech")

        mock_connector = AsyncMock()
        mock_connector.connect = AsyncMock()
        mock_connector.execute = AsyncMock()
        mock_connector.close = AsyncMock()

        mock_vector_store = AsyncMock()
        mock_vector_store.initialize = AsyncMock()
        mock_vector_store.clear = AsyncMock()
        mock_vector_store.add_datapoints = AsyncMock()

        mock_graph = MagicMock()
        mock_graph.add_datapoint = MagicMock()

        with (
            patch("backend.cli.apply_config_defaults"),
            patch("backend.cli.get_settings", return_value=settings),
            patch("backend.cli.PostgresConnector", return_value=mock_connector) as connector_cls,
            patch("backend.cli.DataPointLoader") as loader_cls,
            patch("backend.cli.VectorStore", return_value=mock_vector_store),
            patch("backend.cli.KnowledgeGraph", return_value=mock_graph),
        ):
            loader_cls.return_value.load_directory.return_value = [MagicMock()]
            result = runner.invoke(
                cli, ["demo", "--dataset", "fintech", "--reset", "--no-workspace"]
            )

        assert result.exit_code == 0
        connector_cls.assert_called_once_with(
            host="demo-host",
            port=5432,
            database="datachat_fintech",
            user="demo",
            password="pw",
        )
        loader_cls.return_value.load_directory.assert_called_once_with(
            Path("datapoints") / "examples" / "fintech_bank"
        )
        assert "demo-host:5432/datachat_fintech" in result.output


class TestResetCommand:
    """Test reset command target-db behavior."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @staticmethod
    def _make_settings():
        return type(
            "Settings",
            (),
            {
                "database": type("DatabaseSettings", (), {"url": None})(),
                "system_database": type("SystemDatabaseSettings", (), {"url": None})(),
                "chroma": type("ChromaSettings", (), {"persist_dir": Path("./chroma_data_test")})(),
            },
        )()

    def test_reset_include_target_mysql_uses_non_postgres_drop(self, runner):
        settings = self._make_settings()
        connector = AsyncMock()
        connector.connect = AsyncMock()
        connector.execute = AsyncMock()
        connector.close = AsyncMock()

        with (
            patch("backend.cli.apply_config_defaults"),
            patch("backend.cli.get_settings", return_value=settings),
            patch("backend.cli._resolve_system_database_url", return_value=(None, "none")),
            patch(
                "backend.cli._resolve_target_database_url",
                return_value=("mysql://root:password@localhost:3306/datachat_demo", "settings"),
            ),
            patch("backend.cli.create_connector", return_value=connector),
        ):
            result = runner.invoke(
                cli,
                [
                    "reset",
                    "--yes",
                    "--include-target",
                    "--keep-vectors",
                    "--keep-config",
                    "--keep-managed-datapoints",
                    "--keep-user-datapoints",
                ],
            )

        assert result.exit_code == 0
        executed = [call.args[0] for call in connector.execute.await_args_list]
        assert executed == [
            "DROP TABLE IF EXISTS orders",
            "DROP TABLE IF EXISTS users",
        ]

    def test_reset_include_target_clickhouse_uses_non_postgres_drop(self, runner):
        settings = self._make_settings()
        connector = AsyncMock()
        connector.connect = AsyncMock()
        connector.execute = AsyncMock()
        connector.close = AsyncMock()

        with (
            patch("backend.cli.apply_config_defaults"),
            patch("backend.cli.get_settings", return_value=settings),
            patch("backend.cli._resolve_system_database_url", return_value=(None, "none")),
            patch(
                "backend.cli._resolve_target_database_url",
                return_value=("clickhouse://default:@localhost:8123/default", "settings"),
            ),
            patch("backend.cli.create_connector", return_value=connector),
        ):
            result = runner.invoke(
                cli,
                [
                    "reset",
                    "--yes",
                    "--include-target",
                    "--keep-vectors",
                    "--keep-config",
                    "--keep-managed-datapoints",
                    "--keep-user-datapoints",
                ],
            )

        assert result.exit_code == 0
        executed = [call.args[0] for call in connector.execute.await_args_list]
        assert executed == [
            "DROP TABLE IF EXISTS orders",
            "DROP TABLE IF EXISTS users",
        ]

    def test_reset_survives_invalid_runtime_settings(self, runner):
        with (
            patch("backend.cli.apply_config_defaults"),
            patch("backend.cli.get_settings", side_effect=ValueError("invalid settings")),
            patch("backend.cli.clear_settings_cache"),
            patch("backend.cli._resolve_system_database_url", return_value=(None, "none")),
            patch("backend.cli._resolve_target_database_url", return_value=(None, "none")),
        ):
            result = runner.invoke(
                cli,
                [
                    "reset",
                    "--yes",
                    "--keep-vectors",
                    "--keep-config",
                    "--keep-managed-datapoints",
                    "--keep-user-datapoints",
                ],
            )

        assert result.exit_code == 0
        assert "continuing reset with saved config fallbacks" in result.output.lower()
        assert "Reset complete." in result.output


class TestConnectCommand:
    """Test connect command."""

    @pytest.fixture
    def runner(self):
        """Create CLI test runner."""
        return CliRunner()

    @pytest.fixture
    def temp_config_dir(self, tmp_path):
        """Create temporary config directory."""
        return tmp_path / ".datachat"

    def test_connect_saves_connection_string(self, runner, temp_config_dir):
        """Test that connect command saves connection string."""
        # Patch the state object's config_dir and config_file directly
        from backend.cli import state

        with (
            patch.object(state, "config_dir", temp_config_dir),
            patch.object(state, "config_file", temp_config_dir / "config.json"),
        ):
            # Ensure directory exists
            temp_config_dir.mkdir(parents=True, exist_ok=True)

            result = runner.invoke(
                connect,
                ["postgresql://user:pass@localhost:5432/testdb"],
            )

            assert result.exit_code == 0
            assert "Connection string saved" in result.output
            assert "localhost" in result.output
            assert "5432" in result.output
            assert "testdb" in result.output
            assert "user" in result.output

            # Verify config file was created
            config_file = temp_config_dir / "config.json"
            assert config_file.exists()

            with open(config_file) as f:
                config = json.load(f)
                assert config["connection_string"] == "postgresql://user:pass@localhost:5432/testdb"

    def test_connect_validates_connection_string(self, runner):
        """Test that connect command validates connection string format."""
        result = runner.invoke(connect, ["invalid_connection_string"])

        assert result.exit_code == 1
        assert "Invalid connection string format" in result.output

    def test_connect_handles_missing_port(self, runner, temp_config_dir):
        """Test that connect command handles missing port."""
        with patch("pathlib.Path.home", return_value=temp_config_dir.parent):
            result = runner.invoke(
                connect,
                ["postgresql://user:pass@localhost/testdb"],
            )

            assert result.exit_code == 0
            assert "5432" in result.output  # Default port

    def test_connect_registers_in_registry_by_default(self, runner):
        """Connect should attempt registry registration for UI/CLI parity."""
        with patch(
            "backend.cli._register_cli_connection",
            new=AsyncMock(return_value=(True, "Registry: added connection abc")),
        ) as register_mock:
            result = runner.invoke(
                connect,
                ["postgresql://user:pass@localhost:5432/testdb"],
            )

        assert result.exit_code == 0
        assert "Registry: added connection abc" in result.output
        register_mock.assert_awaited_once()

    def test_connect_skips_registry_with_flag(self, runner):
        """Connect should skip registry registration when explicitly disabled."""
        with patch(
            "backend.cli._register_cli_connection",
            new=AsyncMock(return_value=(True, "Registry: added connection abc")),
        ) as register_mock:
            result = runner.invoke(
                connect,
                ["--no-register", "postgresql://user:pass@localhost:5432/testdb"],
            )

        assert result.exit_code == 0
        register_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_register_cli_connection_preserves_existing_name_without_override(self):
        """Existing curated names should not be overwritten unless --name is set."""
        existing = MagicMock()
        existing.name = "Curated Name"
        existing.database_type = "postgresql"
        existing.is_default = True
        existing.connection_id = "conn-123"
        existing.database_url.get_secret_value.return_value = (
            "postgresql://user:pass@localhost:5432/testdb"
        )

        manager = AsyncMock()
        manager.list_connections.return_value = [existing]

        with (
            patch(
                "backend.cli._resolve_system_database_url",
                return_value=("postgresql://system", "settings"),
            ),
            patch("backend.cli.DatabaseConnectionManager", return_value=manager),
        ):
            registered, message = await _register_cli_connection(
                "postgresql://user:pass@localhost:5432/testdb",
                name=None,
                set_default=False,
            )

        assert registered is True
        assert "using existing connection" in message
        manager.update_connection.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_register_cli_connection_updates_existing_name_with_override(self):
        """Explicit --name should update existing connection name."""
        existing = MagicMock()
        existing.name = "Old Name"
        existing.database_type = "postgresql"
        existing.is_default = True
        existing.connection_id = "conn-123"
        existing.database_url.get_secret_value.return_value = (
            "postgresql://user:pass@localhost:5432/testdb"
        )

        manager = AsyncMock()
        manager.list_connections.return_value = [existing]

        with (
            patch(
                "backend.cli._resolve_system_database_url",
                return_value=("postgresql://system", "settings"),
            ),
            patch("backend.cli.DatabaseConnectionManager", return_value=manager),
        ):
            registered, _ = await _register_cli_connection(
                "postgresql://user:pass@localhost:5432/testdb",
                name="New Curated Name",
                set_default=False,
            )

        assert registered is True
        manager.update_connection.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_resolve_registry_connection_id_for_url(self):
        """Should resolve matching registry connection by normalized URL identity."""
        matching = MagicMock()
        matching.connection_id = "conn-123"
        matching.is_default = False
        matching.database_url.get_secret_value.return_value = (
            "postgresql://user:other@localhost/testdb"
        )
        matching_default = MagicMock()
        matching_default.connection_id = "conn-222"
        matching_default.is_default = True
        matching_default.database_url.get_secret_value.return_value = (
            "postgresql://user:pass@localhost:5432/testdb"
        )
        other = MagicMock()
        other.connection_id = "conn-999"
        other.is_default = False
        other.database_url.get_secret_value.return_value = (
            "postgresql://user:pass@localhost:5432/otherdb"
        )
        manager = AsyncMock()
        manager.list_connections.return_value = [other, matching, matching_default]

        with (
            patch(
                "backend.cli._resolve_system_database_url",
                return_value=("postgresql://system", "settings"),
            ),
            patch("backend.cli.DatabaseConnectionManager", return_value=manager),
        ):
            resolved = await _resolve_registry_connection_id_for_url(
                "postgresql://user:pass@localhost:5432/testdb"
            )

        assert resolved == "conn-222"


class TestAskCommand:
    """Test ask command."""

    @pytest.fixture
    def runner(self):
        """Create CLI test runner."""
        return CliRunner()

    @pytest.fixture
    def mock_pipeline(self):
        """Create mock pipeline."""
        pipeline = AsyncMock()
        pipeline.run = AsyncMock(
            return_value={
                "natural_language_answer": "The total revenue is $1,234,567.89",
                "validated_sql": "SELECT SUM(amount) FROM sales",
                "query_result": {"data": {"total": [1234567.89]}},
                "total_latency_ms": 1000.0,
                "llm_calls": 2,
                "retry_count": 0,
            }
        )
        pipeline.connector = AsyncMock()
        pipeline.connector.close = AsyncMock()
        return pipeline

    def test_ask_requires_query_argument(self, runner):
        """Test that ask command requires query argument."""
        result = runner.invoke(ask, [])
        assert result.exit_code != 0

    def test_ask_command_help(self, runner):
        """Test ask command help text."""
        result = runner.invoke(ask, ["--help"])
        assert result.exit_code == 0
        assert "Ask a single question" in result.output
        assert "--template" in result.output
        assert "--target-database" in result.output
        assert "--execution-mode" in result.output

    def test_ask_rejects_query_and_template_together(self, runner):
        result = runner.invoke(cli, ["ask", "show users", "--template", "list-tables"])
        assert result.exit_code != 0
        assert "either QUERY or --template" in result.output

    def test_ask_lists_templates(self, runner):
        result = runner.invoke(cli, ["ask", "--list-templates"])
        assert result.exit_code == 0
        assert "list-tables" in result.output


class TestDataPointCommands:
    """Test DataPoint management commands."""

    @pytest.fixture
    def runner(self):
        """Create CLI test runner."""
        return CliRunner()

    def test_dp_list_command_exists(self, runner):
        """Test that dp list command exists."""
        result = runner.invoke(datapoint, ["list", "--help"])
        assert result.exit_code == 0
        assert "List all DataPoints" in result.output

    def test_dp_add_command_exists(self, runner):
        """Test that dp add command exists."""
        result = runner.invoke(datapoint, ["add", "--help"])
        assert result.exit_code == 0
        assert "Add a DataPoint from a JSON file" in result.output
        assert "--strict-contracts / --no-strict-contracts" in result.output
        assert "--fail-on-contract-warnings" in result.output

    def test_dp_add_validates_type(self, runner, tmp_path):
        """Test that dp add validates DataPoint type."""
        # Create temporary JSON file
        test_file = tmp_path / "test.json"
        test_file.write_text(json.dumps({"type": "Schema", "name": "Test"}))

        result = runner.invoke(datapoint, ["add", "invalid_type", str(test_file)])
        assert result.exit_code != 0

    def test_dp_add_accepts_valid_types(self, runner):
        """Test that dp add accepts valid DataPoint types."""
        for dp_type in ["schema", "business", "process"]:
            result = runner.invoke(datapoint, ["add", dp_type, "--help"])
            assert result.exit_code == 0

    def test_dp_sync_command_exists(self, runner):
        """Test that dp sync command exists."""
        result = runner.invoke(datapoint, ["sync", "--help"])
        assert result.exit_code == 0
        assert "Rebuild vector store and knowledge graph" in result.output

    def test_dp_lint_command_exists(self, runner):
        """Test that dp lint command exists."""
        result = runner.invoke(datapoint, ["lint", "--help"])
        assert result.exit_code == 0
        assert "Lint DataPoint contract quality" in result.output
        assert "--strict-contracts / --no-strict-contracts" in result.output
        assert "--fail-on-contract-warnings" in result.output

    def test_dp_sync_has_datapoints_dir_option(self, runner):
        """Test that dp sync has --datapoints-dir option."""
        result = runner.invoke(datapoint, ["sync", "--help"])
        assert result.exit_code == 0
        assert "--datapoints-dir" in result.output
        assert "--connection-id" in result.output
        assert "--global-scope" in result.output
        assert "--strict-contracts / --no-strict-contracts" in result.output
        assert "--fail-on-contract-warnings" in result.output


class TestStatusCommand:
    """Test status command."""

    @pytest.fixture
    def runner(self):
        """Create CLI test runner."""
        return CliRunner()

    def test_status_command_help(self, runner):
        """Test status command help text."""
        result = runner.invoke(status, ["--help"])
        assert result.exit_code == 0
        assert "Show connection and system status" in result.output


class TestCLIState:
    """Test CLI state management."""

    @pytest.fixture
    def temp_config_dir(self, tmp_path):
        """Create temporary config directory."""
        return tmp_path / ".datachat"

    def test_cli_state_creates_config_dir(self, temp_config_dir):
        """Test that CLI state creates config directory."""
        from backend.cli import CLIState

        with patch("pathlib.Path.home", return_value=temp_config_dir.parent):
            _ = CLIState()  # State creation triggers directory creation
            assert temp_config_dir.exists()

    def test_cli_state_loads_empty_config(self, temp_config_dir):
        """Test that CLI state loads empty config when file doesn't exist."""
        from backend.cli import CLIState

        with patch("pathlib.Path.home", return_value=temp_config_dir.parent):
            state = CLIState()
            config = state.load_config()
            assert config == {}

    def test_cli_state_saves_and_loads_config(self, temp_config_dir):
        """Test that CLI state saves and loads configuration."""
        from backend.cli import CLIState

        with patch("pathlib.Path.home", return_value=temp_config_dir.parent):
            state = CLIState()

            # Save config
            test_config = {"key": "value", "number": 42}
            state.save_config(test_config)

            # Load config
            loaded_config = state.load_config()
            assert loaded_config == test_config

    def test_cli_state_get_connection_string(self, temp_config_dir):
        """Test getting connection string from state."""
        from backend.cli import CLIState

        with patch("pathlib.Path.home", return_value=temp_config_dir.parent):
            state = CLIState()

            # No connection string initially
            assert state.get_connection_string() is None

            # Set connection string
            conn_str = "postgresql://localhost/test"
            state.set_connection_string(conn_str)

            # Get connection string
            assert state.get_connection_string() == conn_str

    def test_cli_state_set_connection_string(self, temp_config_dir):
        """Test setting connection string in state."""
        from backend.cli import CLIState

        with patch("pathlib.Path.home", return_value=temp_config_dir.parent):
            state = CLIState()

            conn_str = "postgresql://user:pass@host:5432/db"
            state.set_connection_string(conn_str)

            # Verify it was saved
            config = state.load_config()
            assert config["connection_string"] == conn_str

    def test_resolve_target_database_url_prefers_settings_over_saved_config(self, temp_config_dir):
        """Target DB resolution should prefer settings/.env over saved CLI config."""
        from backend.cli import CLIState, state

        with patch("pathlib.Path.home", return_value=temp_config_dir.parent):
            temp_state = CLIState()
            temp_state.set_connection_string("postgresql://saved:pw@localhost:5432/saved_db")
            settings = type(
                "Settings",
                (),
                {
                    "database": type(
                        "DatabaseSettings",
                        (),
                        {"url": "postgresql://env:pw@localhost:5432/env_db"},
                    )()
                },
            )()
            with (
                patch.object(state, "config_dir", temp_state.config_dir),
                patch.object(state, "config_file", temp_state.config_file),
            ):
                resolved_url, source = _resolve_target_database_url(settings)
                assert resolved_url == "postgresql://env:pw@localhost:5432/env_db"
                assert source == "settings"

    def test_resolve_target_database_url_falls_back_to_saved_config(self, temp_config_dir):
        """Saved config should be used only when settings URL is missing."""
        from backend.cli import CLIState, state

        with patch("pathlib.Path.home", return_value=temp_config_dir.parent):
            temp_state = CLIState()
            temp_state.set_connection_string("postgresql://saved:pw@localhost:5432/saved_db")
            settings = type(
                "Settings",
                (),
                {"database": type("DatabaseSettings", (), {"url": None})()},
            )()
            with (
                patch.object(state, "config_dir", temp_state.config_dir),
                patch.object(state, "config_file", temp_state.config_file),
            ):
                resolved_url, source = _resolve_target_database_url(settings)
                assert resolved_url == "postgresql://saved:pw@localhost:5432/saved_db"
                assert source == "saved_config"

    def test_emit_entry_event_cli_writes_jsonl(self, temp_config_dir):
        """Entry events should be appended to local CLI telemetry file."""
        from backend.cli import CLIState, state

        with patch("pathlib.Path.home", return_value=temp_config_dir.parent):
            temp_state = CLIState()
            with (
                patch.object(state, "config_dir", temp_state.config_dir),
                patch.object(state, "config_file", temp_state.config_file),
            ):
                _emit_entry_event_cli(
                    flow="phase1_4_quickstart",
                    step="start",
                    status="started",
                    metadata={"dataset": "none"},
                )
                events_path = temp_state.config_dir / "entry_events.jsonl"
                assert events_path.exists()
                line = events_path.read_text(encoding="utf-8").strip()
                payload = json.loads(line)
                assert payload["flow"] == "phase1_4_quickstart"
                assert payload["step"] == "start"
                assert payload["status"] == "started"

    def test_cli_state_persists_sessions(self, temp_config_dir):
        from backend.cli import CLIState

        with patch("pathlib.Path.home", return_value=temp_config_dir.parent):
            cli_state = CLIState()
            cli_state.upsert_session(
                {
                    "session_id": "session-1",
                    "title": "Revenue check",
                    "conversation_history": [{"role": "user", "content": "hello"}],
                    "updated_at": "2026-02-19T00:00:00+00:00",
                }
            )
            loaded = cli_state.get_session("session-1")
            assert loaded is not None
            assert loaded["title"] == "Revenue check"
            assert cli_state.delete_session("session-1") is True
            assert cli_state.get_session("session-1") is None


class TestQuickstartAndTrainCommands:
    """Test quickstart/train wrappers."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_quickstart_non_interactive_requires_database_url(self, runner):
        settings = type(
            "Settings",
            (),
            {"database": type("DatabaseSettings", (), {"url": None})()},
        )()
        with (
            patch("backend.cli.apply_config_defaults"),
            patch("backend.cli.get_settings", return_value=settings),
            patch("backend.cli.state.get_connection_string", return_value=None),
        ):
            result = runner.invoke(cli, ["quickstart", "--non-interactive"])
        assert result.exit_code != 0
        assert "Missing target database URL" in result.output

    def test_train_profile_mode_requires_connection_id(self, runner):
        result = runner.invoke(cli, ["train", "--mode", "profile"])
        assert result.exit_code != 0
        assert "requires --profile-connection-id" in result.output


class TestCLIErrorHandling:
    """Test CLI error handling."""

    @pytest.fixture
    def runner(self):
        """Create CLI test runner."""
        return CliRunner()

    def test_connect_handles_invalid_format(self, runner):
        """Test connect handles invalid connection string format."""
        result = runner.invoke(connect, ["not_a_valid_url"])
        assert result.exit_code == 1
        assert "Invalid connection string format" in result.output

    def test_dp_add_handles_missing_file(self, runner):
        """Test dp add handles missing file."""
        result = runner.invoke(datapoint, ["add", "schema", "nonexistent.json"])
        assert result.exit_code != 0

    def test_dp_add_validates_file_exists(self, runner, tmp_path):
        """Test that dp add validates file exists."""
        # Use Click's built-in path validation
        non_existent = tmp_path / "does_not_exist.json"
        result = runner.invoke(datapoint, ["add", "schema", str(non_existent)])
        assert result.exit_code != 0

    @pytest.mark.asyncio
    async def test_cli_allows_live_schema_when_no_datapoints(self):
        """Ensure CLI allows live schema queries when DataPoints are missing."""
        settings = type(
            "Settings",
            (),
            {
                "database": type(
                    "DatabaseSettings",
                    (),
                    {
                        "url": "postgresql://demo:pw@localhost:5432/datachat_demo",
                        "pool_size": 10,
                        "db_type": "postgresql",
                    },
                )(),
                "system_database": type("SystemDatabaseSettings", (), {"url": None})(),
            },
        )()
        not_initialized = SystemStatus(
            is_initialized=False,
            has_databases=True,
            has_system_database=True,
            has_datapoints=False,
            setup_required=[],
        )
        with patch(
            "backend.cli.SystemInitializer.status",
            new=AsyncMock(return_value=not_initialized),
        ):
            with patch("backend.cli.VectorStore.initialize", new=AsyncMock()):
                with patch("backend.cli.PostgresConnector.connect", new=AsyncMock()):
                    with patch("backend.cli.bootstrap_knowledge_graph_from_datapoints"):
                        with patch("backend.cli.DataChatPipeline") as pipeline_cls:
                            with patch("backend.cli.get_settings", return_value=settings):
                                pipeline = await create_pipeline_from_config()
                            assert pipeline is pipeline_cls.return_value


class TestCLISetup:
    """Test setup command."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_setup_command_runs(self, runner):
        with (
            patch("backend.cli.VectorStore.initialize", new=AsyncMock()),
            patch(
                "backend.cli.SystemInitializer.initialize",
                new=AsyncMock(
                    return_value=(
                        SystemStatus(
                            is_initialized=True,
                            has_databases=True,
                            has_system_database=True,
                            has_datapoints=True,
                            setup_required=[],
                        ),
                        "Initialization completed.",
                    )
                ),
            ),
            patch("click.prompt", return_value="postgresql://user@localhost/db"),
            patch("click.confirm", return_value=False),
        ):
            result = runner.invoke(setup)
            assert result.exit_code == 0


class TestCLIIntegration:
    """Integration-style tests for CLI commands."""

    @pytest.fixture
    def runner(self):
        """Create CLI test runner."""
        return CliRunner()

    def test_full_cli_workflow_help_messages(self, runner):
        """Test that all commands have proper help messages."""
        commands = [
            ["--help"],
            ["chat", "--help"],
            ["ask", "--help"],
            ["schema", "--help"],
            ["template", "--help"],
            ["session", "--help"],
            ["connect", "--help"],
            ["setup", "--help"],
            ["onboarding", "--help"],
            ["onboarding", "wizard", "--help"],
            ["status", "--help"],
            ["dp", "--help"],
            ["dp", "list", "--help"],
            ["dp", "add", "--help"],
            ["dp", "sync", "--help"],
        ]

        for cmd in commands:
            result = runner.invoke(cli, cmd)
            assert result.exit_code == 0, f"Command {cmd} failed"
            assert len(result.output) > 0, f"Command {cmd} has no output"

    def test_command_structure(self, runner):
        """Test that CLI has expected command structure."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0

        # Check main commands exist
        assert "chat" in result.output
        assert "ask" in result.output
        assert "connect" in result.output
        assert "setup" in result.output
        assert "onboarding" in result.output
        assert "status" in result.output
        assert "dp" in result.output
        assert "schema" in result.output
        assert "session" in result.output
        assert "template" in result.output

    def test_datapoint_subcommands(self, runner):
        """Test that dp group has expected subcommands."""
        result = runner.invoke(datapoint, ["--help"])
        assert result.exit_code == 0

        # Check subcommands exist
        assert "list" in result.output
        assert "add" in result.output
        assert "sync" in result.output
