"""
DataChat CLI

Command-line interface for interacting with DataChat.

Usage:
    datachat cheat-sheet                   # Quick command reference
    datachat chat                          # Interactive REPL mode
    datachat ask "What's the revenue?"     # Single query mode
    datachat quickstart                    # Guided one-command bootstrap
    datachat onboarding wizard             # Deep metadata onboarding wizard
    datachat train                         # Thin wrapper for sync/profile flows
    datachat connect "connection_string"   # Set database connection
    datachat dp list                       # List DataPoints
    datachat dp add schema file.json       # Add DataPoint
    datachat dp sync                       # Rebuild vectors and graph
    datachat profile start                 # Start profiling via API
    datachat dp generate                   # Generate DataPoints via API
    datachat dev                           # Run backend + frontend dev servers
    datachat reset                         # Reset system state for testing
    datachat status                        # Show connection status
"""

import os

os.environ.setdefault("ABSL_LOGGING_MIN_LOG_LEVEL", "3")
os.environ.setdefault("ABSL_LOGGING_STDERR_THRESHOLD", "3")
os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "0")
os.environ.setdefault("GRPC_VERBOSITY", "ERROR")
os.environ.setdefault("GRPC_TRACE", "")
os.environ.setdefault("GLOG_minloglevel", "3")
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")

import asyncio
import json
import logging
import re
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlparse
from uuid import uuid4

import click
import httpx
from pydantic import TypeAdapter
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn
from rich.table import Table

from backend.config import clear_settings_cache, get_settings
from backend.connectors.factory import create_connector, infer_database_type
from backend.connectors.postgres import PostgresConnector
from backend.database.manager import DatabaseConnectionManager
from backend.initialization.initializer import SystemInitializer
from backend.knowledge.bootstrap import bootstrap_knowledge_graph_from_datapoints
from backend.knowledge.conflicts import (
    ConflictMode,
    DataPointConflictError,
    resolve_datapoint_conflicts,
)
from backend.knowledge.contracts import DataPointContractReport, validate_contracts
from backend.knowledge.datapoints import DataPointLoader
from backend.knowledge.graph import KnowledgeGraph
from backend.knowledge.retriever import Retriever
from backend.knowledge.vectors import VectorStore
from backend.models.datapoint import DataPoint
from backend.pipeline.orchestrator import DataChatPipeline
from backend.profiling.generator import DataPointGenerator
from backend.profiling.profiler import SchemaProfiler
from backend.settings_store import apply_config_defaults
from backend.sync.orchestrator import save_datapoint_to_disk
from backend.tools import ToolExecutor, initialize_tools
from backend.tools.base import ToolContext

console = Console()
API_BASE_URL = os.getenv("DATA_CHAT_API_URL", "http://localhost:8000")
ENV_DATABASE_CONNECTION_ID = "00000000-0000-0000-0000-00000000dada"
CLI_SESSION_LIMIT = 50
CLI_SESSION_HISTORY_LIMIT = 120
READ_ONLY_SQL_PREFIXES = ("select", "with", "show", "describe", "desc", "explain")
MUTATING_SQL_KEYWORDS = (
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "truncate",
    "create",
    "grant",
    "revoke",
    "merge",
    "call",
    "copy",
)
QUERY_TEMPLATES: dict[str, str] = {
    "list-tables": "List all available tables.",
    "show-columns": "Show columns for {table}.",
    "sample-rows": "Show first 100 rows from {table}.",
    "row-count": "How many rows are in {table}?",
    "top-10": "Show the top 10 records from {table} by the most relevant numeric metric.",
    "trend": "Show a monthly trend from {table} for the last 12 months.",
    "breakdown": "Give me a category breakdown from {table}.",
}
DEFAULT_TEMPLATE_TABLE = "public.grocery_sales_transactions"
CLI_CHEAT_SHEET_SECTIONS: list[tuple[str, list[tuple[str, str]]]] = [
    (
        "Setup",
        [
            ("datachat onboarding wizard", "One-command deep onboarding + metadata generation."),
            ("datachat setup", "Interactive setup for target and system databases."),
            ("datachat status", "Show initialization status and active connections."),
            ("datachat connect <db_url>", "Set default target database URL."),
        ],
    ),
    (
        "Ask and Chat",
        [
            ("datachat ask \"list tables\"", "Run one question and print result."),
            ("datachat ask --list-templates", "Show quick query templates."),
            ("datachat chat", "Start interactive terminal chat session."),
            ("datachat chat --session-id my-run", "Start/resume a named session."),
        ],
    ),
    (
        "Schema and SQL",
        [
            ("datachat schema tables", "List discoverable tables."),
            ("datachat schema columns public.orders", "Show columns for one table."),
            (
                "datachat ask --execution-mode direct_sql \"SELECT * FROM public.orders LIMIT 10\"",
                "Run read-only SQL directly.",
            ),
        ],
    ),
    (
        "DataPoints",
        [
            ("datachat dp sync --datapoints-dir datapoints", "Load/sync DataPoints."),
            ("datachat dp lint --datapoints-dir datapoints", "Validate DataPoint contracts."),
            ("datachat dp pending list", "List generated pending DataPoints."),
            ("datachat dp pending approve-all --latest", "Approve latest pending batch."),
        ],
    ),
    (
        "Sessions and Helpers",
        [
            ("datachat session list", "List saved CLI sessions."),
            ("datachat session resume <id>", "Resume a saved session."),
            ("datachat quickstart", "Guided bootstrap wrapper."),
            ("datachat train --mode sync", "Thin wrapper over sync/profile workflows."),
        ],
    ),
]


def configure_cli_logging() -> None:
    os.environ.setdefault("GRPC_VERBOSITY", "ERROR")
    os.environ.setdefault("GLOG_minloglevel", "2")
    os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
    logging.disable(logging.CRITICAL)
    logging.basicConfig(level=logging.CRITICAL)
    for logger_name in ("backend", "httpx", "openai", "asyncio", "google", "grpc"):
        logging.getLogger(logger_name).setLevel(logging.CRITICAL)


# ============================================================================
# CLI State Management
# ============================================================================


class CLIState:
    """Manage CLI state (connection, pipeline, etc.)."""

    def __init__(self):
        self.refresh_paths()

    def refresh_paths(self) -> None:
        """Refresh config paths (useful when HOME changes in tests)."""
        self.config_dir = Path.home() / ".datachat"
        self.config_file = self.config_dir / "config.json"
        self.sessions_file = self.config_dir / "sessions.json"
        self.config_dir.mkdir(exist_ok=True, mode=0o700)

    def ensure_paths(self) -> None:
        """Ensure config directory exists and is writable."""
        try:
            self.config_dir.mkdir(exist_ok=True, mode=0o700)
            if not os.access(self.config_dir, os.W_OK):
                raise PermissionError("Config directory not writable")
        except OSError:
            self.refresh_paths()

    def load_config(self) -> dict[str, Any]:
        """Load CLI configuration."""
        if self.config_file.exists():
            with open(self.config_file) as f:
                return json.load(f)
        return {}

    def save_config(self, config: dict[str, Any]) -> None:
        """Save CLI configuration."""
        self.ensure_paths()
        with open(self.config_file, "w") as f:
            json.dump(config, f, indent=2)
        try:
            self.config_file.chmod(0o600)
        except OSError:
            pass

    def get_connection_string(self) -> str | None:
        """Get stored connection string."""
        config = self.load_config()
        return config.get("connection_string") or config.get("target_database_url")

    def set_target_database_url(self, connection_string: str) -> None:
        """Set target database URL."""
        config = self.load_config()
        config["target_database_url"] = connection_string
        config["connection_string"] = connection_string
        self.save_config(config)

    def set_connection_string(self, connection_string: str) -> None:
        """Set connection string."""
        self.set_target_database_url(connection_string)

    def get_system_database_url(self) -> str | None:
        """Get stored system database URL."""
        return self.load_config().get("system_database_url")

    def set_system_database_url(self, system_database_url: str) -> None:
        """Set system database URL."""
        config = self.load_config()
        config["system_database_url"] = system_database_url
        self.save_config(config)

    def load_sessions(self) -> list[dict[str, Any]]:
        """Load persisted CLI chat sessions."""
        if not self.sessions_file.exists():
            return []
        try:
            with open(self.sessions_file, encoding="utf-8") as f:
                raw = json.load(f)
        except Exception:
            return []
        return raw if isinstance(raw, list) else []

    def save_sessions(self, sessions: list[dict[str, Any]]) -> None:
        """Persist CLI chat sessions."""
        self.ensure_paths()
        trimmed = sessions[:CLI_SESSION_LIMIT]
        with open(self.sessions_file, "w", encoding="utf-8") as f:
            json.dump(trimmed, f, indent=2)
        try:
            self.sessions_file.chmod(0o600)
        except OSError:
            pass

    def upsert_session(self, payload: dict[str, Any]) -> None:
        """Insert/update a persisted chat session by session_id."""
        session_id = str(payload.get("session_id") or "").strip()
        if not session_id:
            return
        sessions = self.load_sessions()
        sessions = [item for item in sessions if item.get("session_id") != session_id]
        sessions.insert(0, payload)
        self.save_sessions(sessions)

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        """Fetch a session snapshot by id."""
        for item in self.load_sessions():
            if str(item.get("session_id")) == session_id:
                return item
        return None

    def delete_session(self, session_id: str) -> bool:
        """Delete one session by id."""
        sessions = self.load_sessions()
        filtered = [item for item in sessions if str(item.get("session_id")) != session_id]
        if len(filtered) == len(sessions):
            return False
        self.save_sessions(filtered)
        return True

    def clear_sessions(self) -> None:
        """Remove all persisted sessions."""
        self.save_sessions([])


state = CLIState()


# ============================================================================
# Helper Functions
# ============================================================================


def _resolve_target_database_url(settings) -> tuple[str | None, str]:
    """Resolve target database URL with explicit precedence."""
    if settings.database.url:
        return str(settings.database.url), "settings"

    stored = state.get_connection_string()
    if stored:
        return stored, "saved_config"

    return None, "none"


def _resolve_system_database_url(settings) -> tuple[str | None, str]:
    """Resolve system database URL with explicit precedence."""
    if settings.system_database.url:
        return str(settings.system_database.url), "settings"

    stored = state.get_system_database_url()
    if stored:
        return stored, "saved_config"

    return None, "none"


def _resolve_runtime_database_context(settings) -> tuple[str, str | None]:
    """Resolve runtime DB context, preferring URL-derived type."""
    database_url, _ = _resolve_target_database_url(settings)
    if database_url:
        try:
            return infer_database_type(database_url), database_url
        except Exception:
            return settings.database.db_type, database_url
    return settings.database.db_type, None


def _format_connection_target(connection_string: str) -> str:
    """Format a connection string for status output."""
    from urllib.parse import urlparse

    parsed = urlparse(connection_string)
    host = parsed.hostname or "localhost"
    scheme = parsed.scheme.split("+")[0].lower()
    default_ports = {"postgres": 5432, "postgresql": 5432, "mysql": 3306, "clickhouse": 8123}
    port = parsed.port or default_ports.get(scheme, 0)
    database = (
        parsed.path.lstrip("/") if parsed.path else ("default" if scheme == "clickhouse" else "")
    )
    if not database:
        database = "datachat"
    return f"{host}:{port}/{database}"


def _build_postgres_connector(connection_string: str) -> PostgresConnector:
    """Create a Postgres connector from a URL."""
    from urllib.parse import urlparse

    parsed = urlparse(connection_string)
    return PostgresConnector(
        host=parsed.hostname or "localhost",
        port=parsed.port or 5432,
        database=parsed.path.lstrip("/") if parsed.path else "datachat",
        user=parsed.username or "postgres",
        password=parsed.password or "",
    )


def _apply_datapoint_scope(
    datapoints: list[Any],
    *,
    connection_id: str | None = None,
    global_scope: bool = False,
) -> None:
    """
    Apply retrieval scope metadata to DataPoints in-place.

    - connection_id -> metadata.connection_id=<id>, metadata.scope=database
    - global_scope -> metadata.scope=global and remove metadata.connection_id
    """
    for datapoint in datapoints:
        metadata = datapoint.metadata if isinstance(datapoint.metadata, dict) else {}
        if global_scope:
            metadata["scope"] = "global"
            metadata.pop("connection_id", None)
        elif connection_id:
            metadata["scope"] = "database"
            metadata["connection_id"] = str(connection_id)
        datapoint.metadata = metadata


def _default_connection_name(connection_string: str) -> str:
    """Build a stable default registry name from connection URL."""
    parsed = urlparse(connection_string)
    host = parsed.hostname or "localhost"
    database = parsed.path.lstrip("/") if parsed.path else "datachat"
    return f"{host}/{database}"


def _database_identity(database_url: str | None) -> str | None:
    """Normalize a DB URL to a stable identity string for matching."""
    if not database_url:
        return None
    normalized = database_url.replace("postgresql+asyncpg://", "postgresql://").strip()
    if not normalized:
        return None
    parsed = urlparse(normalized)
    if not parsed.scheme or not parsed.hostname:
        return normalized.lower()
    scheme = parsed.scheme.split("+", 1)[0].lower()
    host = (parsed.hostname or "").lower()
    default_ports = {"postgresql": 5432, "postgres": 5432, "mysql": 3306, "clickhouse": 8123}
    port = parsed.port or default_ports.get(scheme)
    username = parsed.username or ""
    database = parsed.path.lstrip("/")
    return f"{scheme}://{username}@{host}:{port}/{database}"


def _same_database_url(left: str | None, right: str | None) -> bool:
    return _database_identity(left) == _database_identity(right)


async def _register_cli_connection(
    connection_string: str,
    *,
    name: str | None,
    set_default: bool,
) -> tuple[bool, str]:
    """
    Register a CLI connection in the system registry when available.

    Returns:
        (registered, message)
    """
    settings = get_settings()
    system_db_url, _ = _resolve_system_database_url(settings)
    if not system_db_url:
        return (
            False,
            "Registry skipped: SYSTEM_DATABASE_URL is not configured.",
        )

    manager = DatabaseConnectionManager(system_database_url=system_db_url)
    try:
        await manager.initialize()
    except Exception as exc:
        return (False, f"Registry unavailable: {exc}")

    try:
        inferred_type = infer_database_type(connection_string)
        explicit_name = name.strip() if name and name.strip() else None
        target_name = explicit_name or _default_connection_name(connection_string)
        existing = None
        for connection in await manager.list_connections():
            if connection.database_url.get_secret_value() == connection_string:
                existing = connection
                break

        if existing is not None:
            updates: dict[str, str | None] = {}
            # Preserve curated existing names unless user explicitly overrides with --name.
            if explicit_name and existing.name != explicit_name:
                updates["name"] = explicit_name
            if existing.database_type != inferred_type:
                updates["database_type"] = inferred_type
            if updates:
                await manager.update_connection(existing.connection_id, updates=updates)
            if set_default and not existing.is_default:
                await manager.set_default(existing.connection_id)
            return (True, f"Registry: using existing connection {existing.connection_id}")

        created = await manager.add_connection(
            name=target_name,
            database_url=connection_string,
            database_type=inferred_type,
            tags=["managed", "cli"],
            description="Added via datachat connect",
            is_default=set_default,
        )
        return (True, f"Registry: added connection {created.connection_id}")
    except Exception as exc:
        return (False, f"Registry skipped: {exc}")
    finally:
        await manager.close()


async def _resolve_registry_connection_id_for_url(connection_string: str) -> str | None:
    """Find a registry connection id matching the provided URL."""
    settings = get_settings()
    system_db_url, _ = _resolve_system_database_url(settings)
    if not system_db_url:
        return None

    manager = DatabaseConnectionManager(system_database_url=system_db_url)
    try:
        await manager.initialize()
        matches = [
            connection
            for connection in await manager.list_connections()
            if _same_database_url(connection.database_url.get_secret_value(), connection_string)
        ]
        if not matches:
            return None
        default_match = next((item for item in matches if item.is_default), None)
        selected = default_match or matches[0]
        return str(selected.connection_id)
    except Exception:
        return None
    finally:
        try:
            await manager.close()
        except Exception:
            pass
    return None


def _emit_entry_event_cli(
    *,
    flow: str,
    step: str,
    status: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Persist lightweight entry-flow telemetry for CLI wrappers."""
    event = {
        "timestamp": datetime.now(UTC).isoformat(),
        "flow": flow,
        "step": step,
        "status": status,
        "source": "cli",
        "metadata": metadata or {},
    }
    try:
        state.ensure_paths()
        events_path = state.config_dir / "entry_events.jsonl"
        with open(events_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True))
            handle.write("\n")
    except Exception:
        # Telemetry should never block command execution.
        return


def _managed_datapoint_dir() -> Path:
    return Path("datapoints") / "managed"


def _list_managed_datapoint_files_for_connection(connection_id: str) -> list[Path]:
    managed_dir = _managed_datapoint_dir()
    if not managed_dir.exists():
        return []
    matches: list[Path] = []
    for path in managed_dir.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            continue
        if str(metadata.get("connection_id")) == str(connection_id):
            matches.append(path)
    return matches


def _remove_managed_datapoints_for_connection(connection_id: str) -> list[str]:
    removed_ids: list[str] = []
    for path in _list_managed_datapoint_files_for_connection(connection_id):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        datapoint_id = str(payload.get("datapoint_id") or path.stem)
        removed_ids.append(datapoint_id)
        path.unlink(missing_ok=True)
    return removed_ids


def _persist_generated_datapoints_for_connection(
    generated,
    *,
    connection_id: str,
    onboarding_flow: str = "wizard_v1",
) -> list[DataPoint]:
    adapter = TypeAdapter(DataPoint)
    persisted: list[DataPoint] = []
    generated_items = [*generated.schema_datapoints, *generated.business_datapoints]
    for item in generated_items:
        payload = dict(item.datapoint)
        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
        metadata["connection_id"] = str(connection_id)
        metadata["scope"] = "database"
        metadata["source_tier"] = "managed"
        metadata["onboarding_flow"] = onboarding_flow
        payload["metadata"] = metadata
        datapoint = adapter.validate_python(payload)
        path = _managed_datapoint_dir() / f"{datapoint.datapoint_id}.json"
        save_datapoint_to_disk(
            datapoint.model_dump(mode="json", by_alias=True),
            path,
        )
        persisted.append(datapoint)
    return persisted


def _split_sql_statements(sql_text: str) -> list[str]:
    """Split SQL script into executable statements."""
    lines: list[str] = []
    for line in sql_text.splitlines():
        # Remove inline comments and blank lines.
        no_comment = line.split("--", 1)[0].strip()
        if no_comment:
            lines.append(no_comment)

    cleaned = "\n".join(lines)
    statements = [stmt.strip() for stmt in cleaned.split(";") if stmt.strip()]
    return statements


async def _execute_sql_script(connector: PostgresConnector, script_path: Path) -> None:
    """Execute a SQL script statement-by-statement via connector.execute."""
    if not script_path.exists():
        raise click.ClickException(f"SQL script not found: {script_path}")

    sql_text = script_path.read_text(encoding="utf-8")
    statements = _split_sql_statements(sql_text)
    if not statements:
        raise click.ClickException(f"No executable SQL statements found in {script_path}")

    for statement in statements:
        await connector.execute(statement)


async def _clear_demo_tables_for_database(
    connector: Any,
    database_type: str,
) -> None:
    """Clear demo tables on supported database engines."""
    drop_statements = {
        "postgresql": [
            "DROP TABLE IF EXISTS orders CASCADE",
            "DROP TABLE IF EXISTS users CASCADE",
        ],
        "mysql": [
            "DROP TABLE IF EXISTS orders",
            "DROP TABLE IF EXISTS users",
        ],
        "clickhouse": [
            "DROP TABLE IF EXISTS orders",
            "DROP TABLE IF EXISTS users",
        ],
    }
    statements = drop_statements.get(database_type)
    if not statements:
        raise click.ClickException(
            f"Target reset is not supported for database type: {database_type}"
        )
    for statement in statements:
        await connector.execute(statement)


async def create_pipeline_from_config(
    *,
    database_type: str | None = None,
    database_url: str | None = None,
) -> DataChatPipeline:
    """Create pipeline from configuration."""
    apply_config_defaults()
    settings = get_settings()

    # Initialize vector store
    vector_store = VectorStore()
    await vector_store.initialize()

    # Initialize knowledge graph
    knowledge_graph = KnowledgeGraph()
    bootstrap_knowledge_graph_from_datapoints(knowledge_graph, datapoints_dir="datapoints")

    # Initialize retriever
    retriever = Retriever(
        vector_store=vector_store,
        knowledge_graph=knowledge_graph,
    )

    # Initialize connector
    # Prefer .env / settings over persisted CLI state so local project config wins.
    connection_string, _ = _resolve_target_database_url(settings)
    effective_database_url = database_url or connection_string
    if not effective_database_url:
        console.print("[red]No target database configured.[/red]")
        console.print(
            "[yellow]Hint: Set DATABASE_URL in .env, use 'datachat connect', "
            "or run 'datachat setup'.[/yellow]"
        )
        raise click.ClickException("Missing target database")

    runtime_db_type, runtime_db_url = _resolve_runtime_database_context(settings)
    effective_database_type = database_type or runtime_db_type
    connector = create_connector(
        database_url=effective_database_url or runtime_db_url or connection_string,
        database_type=effective_database_type,
        pool_size=settings.database.pool_size,
    )

    try:
        await connector.connect()
    except Exception as e:
        console.print(f"[red]Failed to connect to database: {e}[/red]")
        console.print("[yellow]Hint: Use 'datachat connect' to set connection string[/yellow]")
        raise

    initializer = SystemInitializer(
        {
            "connector": connector,
            "vector_store": vector_store,
        }
    )
    status_state = await initializer.status()
    if not status_state.is_initialized:
        if not status_state.has_databases:
            console.print("[red]DataChat requires setup before queries can run.[/red]")
            if not status_state.has_system_database:
                console.print(
                    "[yellow]Note: SYSTEM_DATABASE_URL is not set. Registry/profiling and "
                    "demo data are unavailable.[/yellow]"
                )
            for step in status_state.setup_required:
                console.print(f"[yellow]- {step.title}: {step.description}[/yellow]")
            console.print("[cyan]Hint: Run 'datachat setup' or 'datachat demo' to continue.[/cyan]")
            raise click.ClickException("System not initialized")

        if not status_state.has_datapoints:
            console.print(
                "[yellow]No DataPoints loaded. Continuing with live schema only.[/yellow]"
            )
            console.print(
                "[cyan]Hint: Run 'datachat dp sync' or enable profiling for richer answers.[/cyan]"
            )

    # Create pipeline
    pipeline = DataChatPipeline(
        retriever=retriever,
        connector=connector,
        max_retries=3,
    )

    return pipeline


def _is_read_only_sql(sql_query: str) -> bool:
    """Validate SQL editor input as a single read-only statement."""
    compact = sql_query.strip()
    if not compact:
        return False
    statements = [part.strip() for part in compact.split(";") if part.strip()]
    if len(statements) != 1:
        return False
    statement = statements[0].lower()
    if not statement.startswith(READ_ONLY_SQL_PREFIXES):
        return False
    return not any(re.search(rf"\b{keyword}\b", statement) for keyword in MUTATING_SQL_KEYWORDS)


def _sanitize_table_reference(table: str) -> str:
    """Allow only simple table identifiers for CLI schema/sample shortcuts."""
    candidate = table.strip().strip('"').strip("`")
    if not re.fullmatch(r"[A-Za-z0-9_.]+", candidate):
        raise click.ClickException(
            "Invalid table reference. Use simple schema.table identifiers only."
        )
    return candidate


def _normalize_target_database(target_database: Any) -> str | None:
    """Normalize optional target connection id values from CLI/session payloads."""
    if target_database is None:
        return None
    value = str(target_database).strip()
    if not value or value.lower() in {"none", "null"}:
        return None
    return value


def _resolve_schema_table_match(
    tables: list[Any],
    *,
    requested_table: str,
    requested_schema: str | None = None,
) -> Any:
    """Resolve a table match from schema metadata, handling unqualified names."""
    schema_filter = requested_schema.strip() if requested_schema else None
    matches = [
        item
        for item in tables
        if str(getattr(item, "table_name", "")).lower() == requested_table.lower()
        and (
            schema_filter is None
            or str(getattr(item, "schema_name", "")).lower() == schema_filter.lower()
        )
    ]
    if not matches:
        lookup_name = (
            f"{schema_filter}.{requested_table}" if schema_filter else requested_table
        )
        raise click.ClickException(f"Table not found: {lookup_name}")
    if schema_filter is None and len(matches) > 1:
        options = ", ".join(
            f"{getattr(item, 'schema_name', '?')}.{getattr(item, 'table_name', '?')}"
            for item in matches[:5]
        )
        raise click.ClickException(
            "Table name is ambiguous. Use schema-qualified TABLE or --schema-name. "
            f"Matches: {options}"
        )
    return matches[0]


def _apply_display_pagination(
    data: dict[str, list] | None,
    *,
    page: int,
    page_size: int,
) -> tuple[dict[str, list] | None, dict[str, int] | None]:
    """Slice columnar result data for terminal display pagination."""
    if not data or not isinstance(data, dict):
        return data, None
    normalized_page_size = max(1, page_size)
    normalized_page = max(1, page)
    total_rows = len(next(iter(data.values()), []))
    if total_rows == 0:
        return data, {"total_rows": 0, "page": 1, "page_size": normalized_page_size}
    total_pages = (total_rows + normalized_page_size - 1) // normalized_page_size
    bounded_page = min(normalized_page, max(1, total_pages))
    start = (bounded_page - 1) * normalized_page_size
    end = min(start + normalized_page_size, total_rows)
    paged_data = {key: value[start:end] for key, value in data.items()}
    return paged_data, {
        "total_rows": total_rows,
        "page": bounded_page,
        "page_size": normalized_page_size,
        "start_row": start + 1,
        "end_row": end,
        "total_pages": total_pages,
    }


def _render_template_query(template_id: str, table: str | None = None) -> str:
    """Build a natural-language query from a CLI template id."""
    if template_id not in QUERY_TEMPLATES:
        supported = ", ".join(sorted(QUERY_TEMPLATES))
        raise click.ClickException(
            f"Unknown template '{template_id}'. Supported templates: {supported}."
        )
    pattern = QUERY_TEMPLATES[template_id]
    if "{table}" in pattern:
        return pattern.format(table=(table or DEFAULT_TEMPLATE_TABLE))
    return pattern


def _derive_selected_table(
    explicit_table: str | None,
    selected_schema_table: str | None,
) -> str | None:
    if explicit_table and explicit_table.strip():
        return explicit_table.strip()
    if selected_schema_table and selected_schema_table.strip():
        return selected_schema_table.strip()
    return None


def _session_title_from_history(conversation_history: list[dict[str, str]]) -> str:
    first_user = next(
        (
            item.get("content", "").strip()
            for item in conversation_history
            if item.get("role") == "user" and item.get("content")
        ),
        "",
    )
    if not first_user:
        return "Untitled session"
    compact = re.sub(r"\s+", " ", first_user)
    return f"{compact[:77]}..." if len(compact) > 80 else compact


def _persist_cli_session(
    *,
    session_id: str,
    conversation_history: list[dict[str, str]],
    session_summary: str | None,
    session_state: dict[str, Any] | None,
    target_connection_id: str | None,
    database_type: str,
) -> None:
    """Persist session state for resume/list flows."""
    if not conversation_history:
        return
    payload = {
        "session_id": session_id,
        "title": _session_title_from_history(conversation_history),
        "updated_at": datetime.now(UTC).isoformat(),
        "target_connection_id": target_connection_id,
        "database_type": database_type,
        "conversation_history": conversation_history[-CLI_SESSION_HISTORY_LIMIT:],
        "session_summary": session_summary,
        "session_state": session_state or {},
    }
    state.upsert_session(payload)


async def _resolve_target_database_context(
    settings: Any,
    *,
    target_database: str | None,
) -> tuple[str, str | None, str | None]:
    """Resolve DB type/url and optional connection id for per-query targeting."""
    default_type, default_url = _resolve_runtime_database_context(settings)
    if not target_database:
        return default_type, default_url, None

    requested = target_database.strip()
    if requested == ENV_DATABASE_CONNECTION_ID:
        if not default_url:
            raise click.ClickException(
                "Environment target requested but DATABASE_URL is not configured."
            )
        resolved_type = infer_database_type(default_url)
        return resolved_type, default_url, requested

    system_url, _ = _resolve_system_database_url(settings)
    if not system_url:
        raise click.ClickException(
            "--target-database requires SYSTEM_DATABASE_URL and a registered connection."
        )

    manager = DatabaseConnectionManager(system_database_url=system_url)
    try:
        await manager.initialize()
        connection = await manager.get_connection(requested)
    except KeyError as exc:
        raise click.ClickException(f"Database connection not found: {requested}") from exc
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    finally:
        try:
            await manager.close()
        except Exception:
            pass

    return (
        connection.database_type,
        connection.database_url.get_secret_value(),
        str(connection.connection_id),
    )


async def _run_direct_sql_query(
    *,
    sql_query: str,
    database_type: str,
    database_url: str | None,
) -> dict[str, Any]:
    """Execute a read-only SQL query directly via connector."""
    if not database_url:
        raise click.ClickException("No active database URL available for SQL execution.")
    if not _is_read_only_sql(sql_query):
        raise click.ClickException(
            "Direct SQL mode accepts one read-only statement "
            "(SELECT/WITH/SHOW/DESCRIBE/EXPLAIN)."
        )
    connector = create_connector(
        database_type=database_type,
        database_url=database_url,
    )
    await connector.connect()
    try:
        result = await connector.execute(sql_query)
    finally:
        await connector.close()
    return {
        "natural_language_answer": (
            f"Executed SQL query successfully. Returned {result.row_count} row(s)."
        ),
        "validated_sql": sql_query,
        "generated_sql": sql_query,
        "query_result": {
            "rows": result.rows,
            "columns": result.columns,
            "row_count": result.row_count,
            "execution_time_ms": result.execution_time_ms,
        },
        "answer_source": "sql",
        "answer_confidence": 1.0,
        "clarifying_questions": [],
        "llm_calls": 0,
        "retry_count": 0,
    }


def format_answer(
    answer: str,
    sql: str | None = None,
    data: dict | None = None,
    visualization_note: str | None = None,
    pagination: dict[str, int] | None = None,
) -> None:
    """Format and display answer."""
    # Display answer
    console.print(Panel(Markdown(answer), title="[bold green]Answer[/bold green]"))

    # Display SQL if available
    if sql:
        console.print("\n[bold cyan]Generated SQL:[/bold cyan]")
        console.print(
            Panel(
                sql,
                title="SQL",
                border_style="cyan",
                highlight=True,
            )
        )

    # Display data if available
    if data and isinstance(data, dict):
        console.print("\n[bold cyan]Results:[/bold cyan]")
        table = Table(show_header=True, header_style="bold cyan")

        # Add columns
        for col_name in data.keys():
            table.add_column(col_name)

        # Add rows
        if data:
            num_rows = len(next(iter(data.values())))
            for i in range(num_rows):
                row = [str(data[col][i]) if i < len(data[col]) else "" for col in data]
                table.add_row(*row)

        console.print(table)
        if pagination and pagination.get("total_rows", 0) > pagination.get("page_size", 0):
            console.print(
                "[dim]"
                f"Showing rows {pagination['start_row']}-{pagination['end_row']} "
                f"of {pagination['total_rows']} "
                f"(page {pagination['page']}/{pagination['total_pages']})."
                "[/dim]"
            )

    if visualization_note:
        console.print(f"\n[bold yellow]Visualization note:[/bold yellow] {visualization_note}")


def _build_columnar_data(query_result: dict[str, Any] | None) -> dict[str, list] | None:
    """Build columnar data from query results."""
    if not query_result:
        return None
    data = query_result.get("data")
    if data is not None:
        return data
    rows = query_result.get("rows")
    columns = query_result.get("columns")
    if isinstance(rows, list) and isinstance(columns, list):
        return {col: [row.get(col) for row in rows] for col in columns}
    return None


def _format_source_footer(result: dict[str, Any]) -> str | None:
    source = result.get("answer_source")
    confidence = result.get("answer_confidence")
    if not source:
        return None
    if isinstance(confidence, (int, float)):
        return f"Source: {source} ({confidence:.2f})"
    return f"Source: {source}"


def _print_evidence(result: dict[str, Any]) -> None:
    evidence = result.get("evidence") or []
    if not evidence:
        console.print("[dim]No evidence items available.[/dim]")
        return
    table = Table(title="Evidence", show_header=True, header_style="bold cyan")
    table.add_column("DataPoint")
    table.add_column("Type")
    table.add_column("Reason")
    for item in evidence:
        if isinstance(item, dict):
            table.add_row(
                str(item.get("name") or item.get("datapoint_id") or "unknown"),
                str(item.get("type") or "DataPoint"),
                str(item.get("reason") or ""),
            )
    console.print(table)


def _print_action_trace(result: dict[str, Any]) -> None:
    trace = result.get("action_trace") or []
    if not isinstance(trace, list) or not trace:
        console.print("[dim]No action-loop trace available.[/dim]")
        return

    table = Table(title="Action Loop Trace", show_header=True, header_style="bold cyan")
    table.add_column("Step", justify="right")
    table.add_column("Stage")
    table.add_column("Action")
    table.add_column("Status")
    table.add_column("Stop Reason")
    for item in trace[:30]:
        if not isinstance(item, dict):
            continue
        verification = item.get("verification")
        if isinstance(verification, dict):
            status = str(verification.get("status") or "")
        else:
            status = ""
        table.add_row(
            str(item.get("step", "")),
            str(item.get("stage", "")),
            str(item.get("selected_action", "")),
            status,
            str(item.get("stop_reason") or ""),
        )
    console.print(table)


def _emit_query_output(
    answer: str,
    sql: str | None,
    data: dict | None,
    result: dict[str, Any],
    evidence: bool,
    show_metrics: bool,
    page: int,
    page_size: int,
) -> None:
    paged_data, pagination = _apply_display_pagination(data, page=page, page_size=page_size)
    console.print()
    format_answer(
        answer,
        sql,
        paged_data,
        result.get("visualization_note"),
        pagination=pagination,
    )
    console.print()

    footer = _format_source_footer(result)
    if footer:
        console.print(f"[dim]{footer}[/dim]")
        console.print()

    if show_metrics:
        metrics = Table(show_header=False, box=None)
        metrics.add_row("⏱️  Latency:", f"{result.get('total_latency_ms', 0):.0f}ms")
        metrics.add_row("🤖 LLM Calls:", str(result.get("llm_calls", 0)))
        metrics.add_row("🔄 Retries:", str(result.get("retry_count", 0)))
        formatter_calls = int(result.get("sql_formatter_fallback_calls", 0) or 0)
        formatter_successes = int(result.get("sql_formatter_fallback_successes", 0) or 0)
        metrics.add_row("🧩 SQL Formatter:", f"{formatter_calls} ({formatter_successes} recovered)")
        loop_terminal_state = result.get("loop_terminal_state")
        loop_stop_reason = result.get("loop_stop_reason")
        if loop_terminal_state or loop_stop_reason:
            metrics.add_row(
                "🔁 Loop:",
                f"{loop_terminal_state or 'unknown'} / {loop_stop_reason or 'unknown'}",
            )
        action_trace = result.get("action_trace") or []
        if isinstance(action_trace, list):
            metrics.add_row("🧭 Loop Steps:", str(len(action_trace)))
        loop_shadow = result.get("loop_shadow_decisions") or []
        if isinstance(loop_shadow, list) and loop_shadow:
            metrics.add_row("👥 Shadow Decisions:", str(len(loop_shadow)))
        console.print(metrics)
        console.print()

    clarifying_questions = result.get("clarifying_questions") or []
    if clarifying_questions:
        console.print("[bold]Clarifying questions:[/bold]")
        for question in clarifying_questions:
            console.print(f"- {question}")
        console.print("[dim]Reply with your answer, or type 'exit' to quit.[/dim]")
        console.print()

    if evidence:
        _print_evidence(result)
        console.print()

    if show_metrics and result.get("action_trace"):
        _print_action_trace(result)
        console.print()


def _print_query_output(
    answer: str,
    sql: str | None,
    data: dict | None,
    result: dict[str, Any],
    evidence: bool,
    show_metrics: bool,
    pager: bool,
    page: int,
    page_size: int,
) -> None:
    if pager:
        with console.pager():
            _emit_query_output(
                answer,
                sql,
                data,
                result,
                evidence,
                show_metrics,
                page,
                page_size,
            )
    else:
        _emit_query_output(
            answer,
            sql,
            data,
            result,
            evidence,
            show_metrics,
            page,
            page_size,
        )


def _compose_clarification_answer(questions: list[str]) -> str:
    if not questions:
        return "I need a bit more detail to continue."
    bullets = "\n".join(f"- {q}" for q in questions)
    return f"I need a bit more detail to continue:\n{bullets}"


def _clarification_limit_message(limit: int) -> str:
    return (
        f"I reached the clarification limit ({limit}). "
        "Please ask a fully specified question with table and metric, or type `exit`."
    )


def _should_exit_chat(query: str) -> bool:
    text = query.strip().lower()
    if not text:
        return False
    exit_phrases = {
        "exit",
        "quit",
        "q",
        "end",
        "bye",
        "goodbye",
        "stop",
        "done",
        "done for now",
        "im done",
        "i'm done",
        "im done for now",
        "i'm done for now",
        "lets talk later",
        "let's talk later",
        "talk later",
        "see you later",
        "no further questions",
        "no more questions",
        "nothing else",
        "end chat",
        "end the chat",
        "close chat",
        "close the chat",
        "stop chat",
    }
    if text in exit_phrases:
        return True
    if re.search(r"\bnever\s*mind\b", text):
        return True
    if re.search(r"\b(i'?m|im|we'?re|were)\s+done\b", text):
        return True
    if re.search(r"\b(done for now|done here|that'?s all|all set)\b", text):
        return True
    if re.search(r"\b(let'?s\s+)?talk\s+later\b", text):
        return True
    if re.search(r"\b(talk|see)\s+you\s+later\b", text):
        return True
    if re.search(r"\b(no\s+more|no\s+further)\s+questions\b", text):
        return True
    if re.search(r"\b(end|stop|quit|exit)\b.*\b(chat|conversation)\b", text):
        return True
    return False


def _contains_data_keywords(text: str) -> bool:
    keywords = {
        "table",
        "tables",
        "column",
        "columns",
        "row",
        "rows",
        "schema",
        "database",
        "sql",
        "query",
        "count",
        "sum",
        "average",
        "avg",
        "min",
        "max",
        "join",
        "group",
        "order",
        "select",
        "from",
        "data",
        "dataset",
        "warehouse",
    }
    return any(word in text for word in keywords)


def _maybe_local_intent_response(query: str) -> tuple[str, str, float] | None:
    """Return a non-DB response for obvious non-query intents."""
    text = query.strip().lower()
    if not text:
        return None
    if _should_exit_chat(text):
        return (
            "Got it. Ending the session. If you need more, just start a new chat.",
            "system",
            0.9,
        )
    if text in {"help", "what can you do", "what can you do?"}:
        return (
            "I can help you explore your connected data. Try: list tables, show first 5 rows "
            "from a table, or ask for totals and trends.",
            "system",
            0.8,
        )
    if not _contains_data_keywords(text) and any(
        re.search(pattern, text)
        for pattern in (
            r"\bjoke\b",
            r"\bweather\b",
            r"\bnews\b",
            r"\bsports\b",
            r"\bmovie\b",
            r"\bmusic\b",
            r"\brecipe\b",
            r"\bpoem\b",
            r"\bstory\b",
        )
    ):
        return (
            "I can help with questions about your connected data. Try: list tables, "
            "show first 5 rows from a table, or total sales last month.",
            "system",
            0.8,
        )
    return None


# ============================================================================
# CLI Commands
# ============================================================================


@click.group()
@click.version_option(version="0.1.0", prog_name="DataChat")
def cli():
    """DataChat - Natural language interface for data warehouses."""
    configure_cli_logging()
    pass


@cli.command(name="cheat-sheet")
def cheat_sheet():
    """Show a quick CLI command cheat sheet."""
    console.print(Panel.fit("DataChat CLI Cheat Sheet", style="cyan"))
    for section_title, rows in CLI_CHEAT_SHEET_SECTIONS:
        table = Table(title=section_title, show_header=True, header_style="bold cyan")
        table.add_column("Command", style="green")
        table.add_column("What it does", style="white")
        for command, description in rows:
            table.add_row(command, description)
        console.print(table)
    console.print(
        "[dim]Tip: add --help to any command for full options "
        "(for example: datachat ask --help).[/dim]"
    )


@cli.command()
@click.option("--evidence", is_flag=True, help="Show DataPoint evidence details")
@click.option(
    "--pager/--no-pager",
    default=False,
    help="Show each response in a scrollable pager.",
)
@click.option(
    "--max-clarifications",
    default=3,
    show_default=True,
    type=int,
    help="Maximum clarification prompts before stopping.",
)
@click.option(
    "--synthesize-simple-sql/--no-synthesize-simple-sql",
    default=None,
    help="Override response synthesis for simple SQL answers.",
)
@click.option(
    "--target-database",
    default=None,
    help="Target database connection UUID for this chat session.",
)
@click.option(
    "--execution-mode",
    type=click.Choice(["natural_language", "direct_sql"]),
    default="natural_language",
    show_default=True,
    help="Start chat in natural-language or direct SQL mode.",
)
@click.option(
    "--session-id",
    default=None,
    help="Resume a persisted CLI session by id (or create under this id).",
)
@click.option(
    "--page-size",
    default=10,
    show_default=True,
    type=int,
    help="Rows to display per result page in terminal output.",
)
@click.option(
    "--row-limit",
    default=100,
    show_default=True,
    type=int,
    help="Default SQL row limit policy for generated queries.",
)
@click.option(
    "--auto-approve-tools/--prompt-tool-approval",
    default=False,
    show_default=True,
    help="Auto-approve tool plans when routing requires tool execution.",
)
def chat(
    evidence: bool,
    pager: bool,
    max_clarifications: int,
    synthesize_simple_sql: bool | None,
    target_database: str | None,
    execution_mode: str,
    session_id: str | None,
    page_size: int,
    row_limit: int,
    auto_approve_tools: bool,
):
    """Interactive REPL mode for conversations."""
    normalized_page_size = max(1, page_size)
    normalized_row_limit = max(1, min(row_limit, 1000))
    session_identifier = session_id or f"cli_{uuid4().hex[:12]}"
    saved_session = state.get_session(session_identifier) if session_id else None

    console.print(
        Panel.fit(
            "[bold green]DataChat Interactive Mode[/bold green]\n"
            "Ask questions in natural language or SQL.\n"
            "Type '/mode sql' or '/mode nl' to switch modes. Type 'exit' or 'quit' to leave.",
            border_style="green",
        )
    )

    conversation_history: list[dict[str, str]] = []
    session_summary: str | None = None
    session_state: dict[str, Any] | None = None
    if saved_session:
        conversation_history = list(saved_session.get("conversation_history") or [])
        session_summary = saved_session.get("session_summary")
        session_state = saved_session.get("session_state") or {}
        console.print(
            f"[green]✓ Resumed session {session_identifier}[/green] "
            f"({saved_session.get('title', 'Untitled session')})"
        )
    session_target_database = _normalize_target_database(target_database)
    if session_target_database is None and saved_session:
        session_target_database = _normalize_target_database(
            saved_session.get("target_connection_id")
        )

    mode = execution_mode.lower().strip()
    if mode not in {"natural_language", "direct_sql"}:
        raise click.ClickException("Unsupported --execution-mode value.")

    async def run_chat():
        nonlocal session_summary, session_state, mode
        clarification_attempts = 0
        max_clarifications_limit = max(0, max_clarifications)
        settings = get_settings()
        database_type, database_url, target_connection_id = await _resolve_target_database_context(
            settings,
            target_database=session_target_database,
        )

        pipeline = None
        try:
            while True:
                try:
                    # Get user input
                    prompt_label = "SQL" if mode == "direct_sql" else "You"
                    query = console.input(f"[bold cyan]{prompt_label}:[/bold cyan] ")

                    if not query.strip():
                        continue

                    command_text = query.strip()
                    command_lower = command_text.lower()
                    if command_lower in {"/mode sql", ":mode sql"}:
                        mode = "direct_sql"
                        console.print("[green]Switched to direct SQL mode.[/green]")
                        continue
                    if command_lower in {"/mode nl", "/mode natural", ":mode nl"}:
                        mode = "natural_language"
                        console.print("[green]Switched to natural-language mode.[/green]")
                        continue
                    if command_lower.startswith("/template "):
                        _, _, template_payload = command_text.partition(" ")
                        template_parts = [part for part in template_payload.split() if part]
                        if not template_parts:
                            raise click.ClickException("Usage: /template <template-id> [table]")
                        template_id = template_parts[0]
                        template_table = template_parts[1] if len(template_parts) > 1 else None
                        query = _render_template_query(template_id, table=template_table)
                        mode = "natural_language"
                        console.print(f"[dim]Applied template '{template_id}'.[/dim]")
                    elif command_lower in {"/templates", ":templates"}:
                        table = Table(title="Query Templates", show_header=True, header_style="bold cyan")
                        table.add_column("Template ID")
                        table.add_column("Example")
                        for template_key in sorted(QUERY_TEMPLATES):
                            table.add_row(
                                template_key,
                                _render_template_query(template_key, table=DEFAULT_TEMPLATE_TABLE),
                            )
                        console.print(table)
                        continue

                    if _should_exit_chat(query):
                        console.print("\n[yellow]Goodbye![/yellow]")
                        break

                    if mode == "direct_sql":
                        with console.status("[cyan]Executing SQL...[/cyan]", spinner="dots"):
                            result = await _run_direct_sql_query(
                                sql_query=query,
                                database_type=database_type,
                                database_url=database_url,
                            )
                    else:
                        if pipeline is None:
                            pipeline = await create_pipeline_from_config(
                                database_type=database_type,
                                database_url=database_url,
                            )
                            pipeline.max_clarifications = max(0, max_clarifications)
                            if hasattr(pipeline, "sql"):
                                pipeline.sql._default_row_limit = normalized_row_limit
                                pipeline.sql._max_safe_row_limit = max(100, normalized_row_limit)
                            console.print("[green]✓ Pipeline initialized[/green]\n")

                        # Show loading indicator
                        with console.status("[cyan]Processing...[/cyan]", spinner="dots"):
                            result = await pipeline.run(
                                query=query,
                                conversation_history=conversation_history,
                                session_summary=session_summary,
                                session_state=session_state,
                                database_type=database_type,
                                database_url=database_url,
                                target_connection_id=target_connection_id,
                                synthesize_simple_sql=synthesize_simple_sql,
                                tool_approved=auto_approve_tools,
                            )
                        if result.get("tool_approval_required") and not auto_approve_tools:
                            approval_calls = result.get("tool_approval_calls") or []
                            if approval_calls:
                                call_table = Table(
                                    title="Tool Approval Required",
                                    show_header=True,
                                    header_style="bold yellow",
                                )
                                call_table.add_column("Tool")
                                call_table.add_column("Arguments")
                                for call in approval_calls:
                                    call_table.add_row(
                                        str(call.get("name", "unknown")),
                                        json.dumps(call.get("arguments", {}), sort_keys=True),
                                    )
                                console.print(call_table)
                            approved = click.confirm(
                                "Approve tool execution for this query?",
                                default=False,
                                show_default=True,
                            )
                            if approved:
                                with console.status(
                                    "[cyan]Executing approved tool plan...[/cyan]",
                                    spinner="dots",
                                ):
                                    result = await pipeline.run(
                                        query=query,
                                        conversation_history=conversation_history,
                                        session_summary=session_summary,
                                        session_state=session_state,
                                        database_type=database_type,
                                        database_url=database_url,
                                        target_connection_id=target_connection_id,
                                        synthesize_simple_sql=synthesize_simple_sql,
                                        tool_approved=True,
                                    )

                    # Extract results
                    clarifying_questions = result.get("clarifying_questions") or []
                    answer = result.get("natural_language_answer") or ""
                    if not answer and clarifying_questions:
                        answer = _compose_clarification_answer(clarifying_questions)
                    if not answer:
                        answer = "No answer generated"
                    sql = result.get("validated_sql") or result.get("generated_sql")
                    query_result = result.get("query_result")
                    data = _build_columnar_data(query_result)

                    if clarifying_questions and clarification_attempts >= max_clarifications_limit:
                        result = {
                            **result,
                            "answer_source": "system",
                            "answer_confidence": 0.5,
                            "clarifying_questions": [],
                        }
                        answer = _clarification_limit_message(max_clarifications_limit)
                        sql = None
                        data = None
                        clarifying_questions = []
                    elif clarifying_questions:
                        clarification_attempts += 1
                    else:
                        clarification_attempts = 0

                    # Display results
                    _print_query_output(
                        answer=answer,
                        sql=sql,
                        data=data,
                        result=result,
                        evidence=evidence,
                        show_metrics=True,
                        pager=pager or evidence,
                        page=1,
                        page_size=normalized_page_size,
                    )

                    # Update conversation history
                    conversation_history.append({"role": "user", "content": query})
                    conversation_history.append({"role": "assistant", "content": answer})
                    session_summary = result.get("session_summary")
                    session_state = result.get("session_state")
                    _persist_cli_session(
                        session_id=session_identifier,
                        conversation_history=conversation_history,
                        session_summary=session_summary,
                        session_state=session_state,
                        target_connection_id=target_connection_id,
                        database_type=database_type,
                    )

                except KeyboardInterrupt:
                    console.print("\n[yellow]Interrupted. Type 'exit' to quit.[/yellow]")
                    continue
                except Exception as e:
                    console.print(f"\n[red]Error: {e}[/red]")
                    continue

        except Exception as e:
            console.print(f"[red]Failed to initialize pipeline: {e}[/red]")
            sys.exit(1)
        finally:
            # Cleanup
            if pipeline is not None:
                try:
                    await pipeline.connector.close()
                except Exception:
                    pass

    asyncio.run(run_chat())


@cli.command()
@click.argument("query", required=False)
@click.option("--evidence", is_flag=True, help="Show DataPoint evidence details")
@click.option(
    "--pager/--no-pager",
    default=False,
    help="Show the response in a scrollable pager.",
)
@click.option(
    "--max-clarifications",
    default=3,
    show_default=True,
    type=int,
    help="Maximum clarification prompts before stopping.",
)
@click.option(
    "--synthesize-simple-sql/--no-synthesize-simple-sql",
    default=None,
    help="Override response synthesis for simple SQL answers.",
)
@click.option(
    "--target-database",
    default=None,
    help="Target database connection UUID for this request.",
)
@click.option(
    "--execution-mode",
    type=click.Choice(["natural_language", "direct_sql"]),
    default="natural_language",
    show_default=True,
    help="Run in natural-language mode or direct SQL mode.",
)
@click.option(
    "--template",
    "query_template",
    default=None,
    help="Apply a built-in query template instead of writing the prompt manually.",
)
@click.option(
    "--table",
    "template_table",
    default=None,
    help="Optional table used by templates that require one.",
)
@click.option(
    "--list-templates",
    is_flag=True,
    help="List built-in query templates and exit.",
)
@click.option(
    "--session-id",
    default=None,
    help="Persist this ask flow under a CLI session id.",
)
@click.option(
    "--page",
    default=1,
    show_default=True,
    type=int,
    help="Result page number to display.",
)
@click.option(
    "--page-size",
    default=10,
    show_default=True,
    type=int,
    help="Rows to display per result page.",
)
@click.option(
    "--row-limit",
    default=100,
    show_default=True,
    type=int,
    help="Default SQL row limit policy for generated queries.",
)
@click.option(
    "--auto-approve-tools/--prompt-tool-approval",
    default=False,
    show_default=True,
    help="Auto-approve tool plans when routing requires tool execution.",
)
def ask(
    query: str | None,
    evidence: bool,
    pager: bool,
    max_clarifications: int,
    synthesize_simple_sql: bool | None,
    target_database: str | None,
    execution_mode: str,
    query_template: str | None,
    template_table: str | None,
    list_templates: bool,
    session_id: str | None,
    page: int,
    page_size: int,
    row_limit: int,
    auto_approve_tools: bool,
):
    """Ask a single question and exit."""
    if list_templates:
        table = Table(title="Query Templates", show_header=True, header_style="bold cyan")
        table.add_column("Template ID")
        table.add_column("Example")
        for template_key in sorted(QUERY_TEMPLATES):
            table.add_row(
                template_key,
                _render_template_query(template_key, table=DEFAULT_TEMPLATE_TABLE),
            )
        console.print(table)
        return

    if query and query_template:
        raise click.ClickException("Provide either QUERY or --template, not both.")
    if not query and not query_template:
        raise click.ClickException("Provide QUERY or use --template.")
    effective_query = query.strip() if query else _render_template_query(query_template or "", template_table)
    session_identifier = session_id.strip() if session_id else None
    normalized_page = max(1, page)
    normalized_page_size = max(1, page_size)
    normalized_row_limit = max(1, min(row_limit, 1000))

    async def run_query():
        settings = get_settings()
        database_type, database_url, target_connection_id = await _resolve_target_database_context(
            settings,
            target_database=_normalize_target_database(target_database),
        )
        local_response = _maybe_local_intent_response(effective_query)
        if execution_mode == "natural_language" and local_response:
            answer, source, confidence = local_response
            _print_query_output(
                answer=answer,
                sql=None,
                data=None,
                result={
                    "answer_source": source,
                    "answer_confidence": confidence,
                    "clarifying_questions": [],
                },
                evidence=evidence,
                show_metrics=False,
                pager=pager or evidence,
                page=normalized_page,
                page_size=normalized_page_size,
            )
            return

        conversation_history: list[dict[str, str]] = []
        session_summary: str | None = None
        session_state: dict[str, Any] | None = None
        try:
            pipeline = None
            current_query = effective_query
            clarification_attempts = 0
            max_clarifications_limit = max(0, max_clarifications)

            while True:
                if execution_mode == "direct_sql":
                    with console.status("[cyan]Executing SQL...[/cyan]", spinner="dots"):
                        result = await _run_direct_sql_query(
                            sql_query=current_query,
                            database_type=database_type,
                            database_url=database_url,
                        )
                else:
                    if pipeline is None:
                        pipeline = await create_pipeline_from_config(
                            database_type=database_type,
                            database_url=database_url,
                        )
                        pipeline.max_clarifications = max(0, max_clarifications)
                        if hasattr(pipeline, "sql"):
                            pipeline.sql._default_row_limit = normalized_row_limit
                            pipeline.sql._max_safe_row_limit = max(100, normalized_row_limit)
                    # Show loading with progress
                    with console.status("[cyan]Processing query...[/cyan]", spinner="dots"):
                        result = await pipeline.run(
                            query=current_query,
                            conversation_history=conversation_history,
                            session_summary=session_summary,
                            session_state=session_state,
                            database_type=database_type,
                            database_url=database_url,
                            target_connection_id=target_connection_id,
                            synthesize_simple_sql=synthesize_simple_sql,
                            tool_approved=auto_approve_tools,
                        )
                    if result.get("tool_approval_required") and not auto_approve_tools:
                        approval_calls = result.get("tool_approval_calls") or []
                        if approval_calls:
                            call_table = Table(
                                title="Tool Approval Required",
                                show_header=True,
                                header_style="bold yellow",
                            )
                            call_table.add_column("Tool")
                            call_table.add_column("Arguments")
                            for call in approval_calls:
                                call_table.add_row(
                                    str(call.get("name", "unknown")),
                                    json.dumps(call.get("arguments", {}), sort_keys=True),
                                )
                            console.print(call_table)
                        approved = click.confirm(
                            "Approve tool execution for this query?",
                            default=False,
                            show_default=True,
                        )
                        if approved:
                            with console.status(
                                "[cyan]Executing approved tool plan...[/cyan]",
                                spinner="dots",
                            ):
                                result = await pipeline.run(
                                    query=current_query,
                                    conversation_history=conversation_history,
                                    session_summary=session_summary,
                                    session_state=session_state,
                                    database_type=database_type,
                                    database_url=database_url,
                                    target_connection_id=target_connection_id,
                                    synthesize_simple_sql=synthesize_simple_sql,
                                    tool_approved=True,
                                )

                # Extract results
                clarifying_questions = result.get("clarifying_questions") or []
                answer = result.get("natural_language_answer") or ""
                if not answer and clarifying_questions:
                    answer = _compose_clarification_answer(clarifying_questions)
                if not answer:
                    answer = "No answer generated"
                sql = result.get("validated_sql") or result.get("generated_sql")
                query_result = result.get("query_result")
                data = _build_columnar_data(query_result)

                if clarifying_questions and clarification_attempts >= max_clarifications_limit:
                    result = {
                        **result,
                        "answer_source": "system",
                        "answer_confidence": 0.5,
                        "clarifying_questions": [],
                    }
                    answer = _clarification_limit_message(max_clarifications_limit)
                    sql = None
                    data = None
                    clarifying_questions = []

                # Display results
                _print_query_output(
                    answer=answer,
                    sql=sql,
                    data=data,
                    result=result,
                    evidence=evidence,
                    show_metrics=False,
                    pager=pager or evidence,
                    page=normalized_page,
                    page_size=normalized_page_size,
                )

                conversation_history.extend(
                    [
                        {"role": "user", "content": current_query},
                        {"role": "assistant", "content": answer},
                    ]
                )
                session_summary = result.get("session_summary")
                session_state = result.get("session_state")
                if session_identifier:
                    _persist_cli_session(
                        session_id=session_identifier,
                        conversation_history=conversation_history,
                        session_summary=session_summary,
                        session_state=session_state,
                        target_connection_id=target_connection_id,
                        database_type=database_type,
                    )

                if execution_mode == "direct_sql":
                    break
                if not clarifying_questions or clarification_attempts >= max_clarifications_limit:
                    break

                followup = console.input("[bold cyan]Clarification:[/bold cyan] ").strip()
                if not followup:
                    break
                if _should_exit_chat(followup):
                    console.print("[yellow]Goodbye![/yellow]")
                    break

                current_query = followup
                clarification_attempts += 1

        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")
            sys.exit(1)
        finally:
            if "pipeline" in locals() and pipeline is not None:
                try:
                    await pipeline.connector.close()
                except Exception:
                    pass

    asyncio.run(run_query())


@cli.group(name="template")
def template():
    """Manage built-in query templates."""
    pass


@template.command(name="list")
def list_templates() -> None:
    """List built-in CLI query templates."""
    table = Table(title="Query Templates", show_header=True, header_style="bold cyan")
    table.add_column("Template ID")
    table.add_column("Example")
    for template_key in sorted(QUERY_TEMPLATES):
        table.add_row(
            template_key,
            _render_template_query(template_key, table=DEFAULT_TEMPLATE_TABLE),
        )
    console.print(table)


@cli.group(name="schema")
def schema() -> None:
    """Browse live database schema and sample rows."""
    pass


@schema.command(name="tables")
@click.option("--target-database", default=None, help="Target database connection UUID.")
@click.option("--search", default="", help="Filter tables by schema/table substring.")
@click.option("--schema-name", default=None, help="Schema name to inspect (defaults to all).")
@click.option("--limit", default=200, show_default=True, type=int)
def schema_tables(
    target_database: str | None,
    search: str,
    schema_name: str | None,
    limit: int,
) -> None:
    """List tables for the active or selected database."""

    async def run_tables() -> None:
        settings = get_settings()
        database_type, database_url, _ = await _resolve_target_database_context(
            settings,
            target_database=target_database,
        )
        if not database_url:
            raise click.ClickException("No target database configured.")
        connector = create_connector(database_type=database_type, database_url=database_url)
        await connector.connect()
        try:
            tables = await connector.get_schema(schema_name=schema_name)
        finally:
            await connector.close()

        term = search.strip().lower()
        filtered = []
        for item in tables:
            qualified = f"{item.schema_name}.{item.table_name}"
            if term and term not in qualified.lower():
                continue
            filtered.append(item)
        filtered = sorted(filtered, key=lambda item: (item.schema_name, item.table_name))
        if limit > 0:
            filtered = filtered[:limit]
        if not filtered:
            console.print("[yellow]No tables found for this selection.[/yellow]")
            return

        table = Table(title="Schema Tables", show_header=True, header_style="bold cyan")
        table.add_column("Table")
        table.add_column("Type")
        table.add_column("Columns", justify="right")
        table.add_column("Rows", justify="right")
        for item in filtered:
            table.add_row(
                f"{item.schema_name}.{item.table_name}",
                item.table_type,
                str(len(item.columns)),
                str(item.row_count) if item.row_count is not None else "-",
            )
        console.print(table)

    asyncio.run(run_tables())


@schema.command(name="columns")
@click.argument("table")
@click.option("--target-database", default=None, help="Target database connection UUID.")
@click.option("--schema-name", default=None, help="Schema override when TABLE is unqualified.")
def schema_columns(table: str, target_database: str | None, schema_name: str | None) -> None:
    """List columns for a table."""

    async def run_columns() -> None:
        settings = get_settings()
        database_type, database_url, _ = await _resolve_target_database_context(
            settings,
            target_database=target_database,
        )
        if not database_url:
            raise click.ClickException("No target database configured.")

        qualified = _sanitize_table_reference(table)
        if "." in qualified:
            requested_schema, requested_table = qualified.split(".", 1)
        else:
            requested_schema = schema_name.strip() if schema_name else None
            requested_table = qualified

        connector = create_connector(database_type=database_type, database_url=database_url)
        await connector.connect()
        try:
            tables = await connector.get_schema(schema_name=requested_schema)
        finally:
            await connector.close()
        match = _resolve_schema_table_match(
            tables,
            requested_table=requested_table,
            requested_schema=requested_schema,
        )
        display_name = f"{match.schema_name}.{match.table_name}"

        columns = Table(
            title=f"Columns: {display_name}",
            show_header=True,
            header_style="bold cyan",
        )
        columns.add_column("Column")
        columns.add_column("Type")
        columns.add_column("Nullable")
        columns.add_column("PK")
        columns.add_column("FK")
        for column in match.columns:
            fk_ref = (
                f"{column.foreign_table}.{column.foreign_column}"
                if column.foreign_table and column.foreign_column
                else ""
            )
            columns.add_row(
                column.name,
                column.data_type,
                "yes" if column.is_nullable else "no",
                "yes" if column.is_primary_key else "no",
                fk_ref,
            )
        console.print(columns)

    asyncio.run(run_columns())


@schema.command(name="sample")
@click.argument("table")
@click.option("--target-database", default=None, help="Target database connection UUID.")
@click.option("--rows", default=10, show_default=True, type=int)
@click.option("--offset", default=0, show_default=True, type=int)
@click.option("--page-size", default=10, show_default=True, type=int)
def schema_sample(
    table: str,
    target_database: str | None,
    rows: int,
    offset: int,
    page_size: int,
) -> None:
    """Sample rows from a table."""

    async def run_sample() -> None:
        settings = get_settings()
        database_type, database_url, _ = await _resolve_target_database_context(
            settings,
            target_database=target_database,
        )
        if not database_url:
            raise click.ClickException("No target database configured.")
        table_ref = _sanitize_table_reference(table)
        bounded_rows = max(1, min(rows, 1000))
        bounded_offset = max(0, offset)
        sql = f"SELECT * FROM {table_ref} LIMIT {bounded_rows} OFFSET {bounded_offset}"

        result = await _run_direct_sql_query(
            sql_query=sql,
            database_type=database_type,
            database_url=database_url,
        )
        data = _build_columnar_data(result.get("query_result"))
        _print_query_output(
            answer=result.get("natural_language_answer", ""),
            sql=result.get("validated_sql"),
            data=data,
            result=result,
            evidence=False,
            show_metrics=False,
            pager=False,
            page=1,
            page_size=max(1, page_size),
        )

    asyncio.run(run_sample())


@cli.group(name="session")
def session() -> None:
    """Manage persisted CLI chat sessions."""
    pass


@session.command(name="list")
@click.option("--search", default="", help="Filter sessions by title text.")
@click.option("--limit", default=20, show_default=True, type=int)
def list_sessions(search: str, limit: int) -> None:
    """List saved CLI sessions."""
    sessions = state.load_sessions()
    term = search.strip().lower()
    if term:
        sessions = [item for item in sessions if term in str(item.get("title", "")).lower()]
    if limit > 0:
        sessions = sessions[:limit]
    if not sessions:
        console.print("[yellow]No saved CLI sessions found.[/yellow]")
        return
    table = Table(title="CLI Sessions", show_header=True, header_style="bold cyan")
    table.add_column("Session ID")
    table.add_column("Title")
    table.add_column("Updated")
    table.add_column("Target DB")
    table.add_column("Turns", justify="right")
    for item in sessions:
        history = item.get("conversation_history") or []
        table.add_row(
            str(item.get("session_id", "")),
            str(item.get("title", "Untitled session")),
            str(item.get("updated_at", "")),
            str(item.get("target_connection_id") or "-"),
            str(max(0, len(history) // 2)),
        )
    console.print(table)


@session.command(name="clear")
@click.argument("session_id", required=False)
@click.option("--all", "clear_all", is_flag=True, help="Delete all saved sessions.")
def clear_session(session_id: str | None, clear_all: bool) -> None:
    """Delete one saved session or all sessions."""
    if clear_all:
        state.clear_sessions()
        console.print("[green]✓ Cleared all CLI sessions.[/green]")
        return
    if not session_id:
        raise click.ClickException("Provide SESSION_ID or use --all.")
    deleted = state.delete_session(session_id.strip())
    if not deleted:
        raise click.ClickException(f"Session not found: {session_id}")
    console.print(f"[green]✓ Cleared session {session_id}.[/green]")


@session.command(name="resume")
@click.pass_context
@click.argument("session_id")
@click.option("--target-database", default=None, help="Override target database connection UUID.")
@click.option("--execution-mode", type=click.Choice(["natural_language", "direct_sql"]), default="natural_language")
@click.option("--pager/--no-pager", default=False)
@click.option("--evidence", is_flag=True)
@click.option("--max-clarifications", default=3, type=int, show_default=True)
@click.option("--page-size", default=10, type=int, show_default=True)
@click.option("--row-limit", default=100, type=int, show_default=True)
def resume_session(
    ctx: click.Context,
    session_id: str,
    target_database: str | None,
    execution_mode: str,
    pager: bool,
    evidence: bool,
    max_clarifications: int,
    page_size: int,
    row_limit: int,
) -> None:
    """Resume a saved CLI session in interactive chat mode."""
    snapshot = state.get_session(session_id.strip())
    if snapshot is None:
        raise click.ClickException(f"Session not found: {session_id}")
    effective_target = _normalize_target_database(target_database)
    if effective_target is None:
        effective_target = _normalize_target_database(snapshot.get("target_connection_id"))
    ctx.invoke(
        chat,
        evidence=evidence,
        pager=pager,
        max_clarifications=max_clarifications,
        synthesize_simple_sql=None,
        target_database=effective_target,
        execution_mode=execution_mode,
        session_id=session_id.strip(),
        page_size=page_size,
        row_limit=row_limit,
        auto_approve_tools=False,
    )


@cli.command()
@click.argument("connection_string")
@click.option("--name", default=None, help="Optional registry display name.")
@click.option(
    "--register/--no-register",
    default=True,
    help="Also register this connection in the system registry when available.",
)
@click.option(
    "--set-default/--no-set-default",
    default=True,
    help="When registering, mark this connection as default.",
)
def connect(connection_string: str, name: str | None, register: bool, set_default: bool):
    """Set database connection string.

    Example:
        datachat connect postgresql://user:pass@localhost:5432/dbname
    """
    try:
        # Validate connection string format
        from urllib.parse import urlparse

        parsed = urlparse(connection_string)
        if not parsed.scheme or not parsed.netloc:
            console.print(
                "[red]Invalid connection string format[/red]\n"
                "Expected: postgresql://user:pass@host:port/dbname"
            )
            sys.exit(1)

        # Save connection string for runtime defaults
        state.set_connection_string(connection_string)
        console.print("[green]✓ Connection string saved[/green]")
        console.print(f"Host: {parsed.hostname}")
        console.print(f"Port: {parsed.port or 5432}")
        console.print(f"Database: {parsed.path.lstrip('/')}")
        console.print(f"User: {parsed.username}")
        if register:
            registered, message = asyncio.run(
                _register_cli_connection(
                    connection_string,
                    name=name,
                    set_default=set_default,
                )
            )
            style = "green" if registered else "yellow"
            console.print(f"[{style}]{message}[/{style}]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@cli.command()
def status():
    """Show connection and system status."""

    async def check_status():
        apply_config_defaults()
        table = Table(title="DataChat Status", show_header=True, header_style="bold cyan")
        table.add_column("Component", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Details")

        # Check configuration
        settings = get_settings()
        table.add_row("Configuration", "✓", f"Environment: {settings.environment}")
        system_url, system_source = _resolve_system_database_url(settings)
        if system_url:
            table.add_row(
                "System DB",
                "✓",
                f"{_format_connection_target(system_url)} ({system_source})",
            )
        else:
            table.add_row("System DB", "⚠️", "SYSTEM_DATABASE_URL not set")

        # Check connection string
        connection_string, source = _resolve_target_database_url(settings)
        if connection_string:
            table.add_row(
                "Connection",
                "✓",
                f"{_format_connection_target(connection_string)} ({source})",
            )
        else:
            table.add_row("Connection", "✗", "No target database configured")

        # Check database connection
        try:
            if not connection_string:
                raise RuntimeError("No target database configured")
            connector = create_connector(database_url=connection_string)
            await connector.connect()
            await connector.execute("SELECT 1")
            table.add_row("Database", "✓", "Connected")
            await connector.close()
        except Exception as e:
            table.add_row("Database", "✗", f"Error: {str(e)[:50]}")

        # Check vector store
        try:
            vector_store = VectorStore()
            await vector_store.initialize()
            count = await vector_store.get_count()
            table.add_row("Vector Store", "✓", f"{count} datapoints")
        except Exception as e:
            table.add_row("Vector Store", "✗", f"Error: {str(e)[:50]}")

        # Check knowledge graph
        try:
            graph = KnowledgeGraph()
            stats = graph.get_stats()
            table.add_row(
                "Knowledge Graph",
                "✓",
                f"{stats['total_nodes']} nodes, {stats['total_edges']} edges",
            )
        except Exception as e:
            table.add_row("Knowledge Graph", "✗", f"Error: {str(e)[:50]}")

        console.print(table)

    asyncio.run(check_status())


@cli.command()
@click.option("--backend-port", default=8000, show_default=True, type=int)
@click.option("--frontend-port", default=3000, show_default=True, type=int)
@click.option("--backend-host", default="127.0.0.1", show_default=True)
@click.option("--frontend-host", default="127.0.0.1", show_default=True)
@click.option("--no-backend", is_flag=True, help="Skip starting the backend API server.")
@click.option("--no-frontend", is_flag=True, help="Skip starting the frontend dev server.")
def dev(
    backend_port: int,
    frontend_port: int,
    backend_host: str,
    frontend_host: str,
    no_backend: bool,
    no_frontend: bool,
):
    """Run backend and frontend dev servers in one command."""
    processes: list[subprocess.Popen] = []

    if no_backend and no_frontend:
        raise click.ClickException("Nothing to run. Remove --no-backend or --no-frontend.")

    if not no_backend:
        apply_config_defaults()
        provider_env = os.getenv("LLM_DEFAULT_PROVIDER")
        default_provider = (provider_env or "openai").strip().lower()

        def _is_placeholder_key(value: str | None) -> bool:
            if not value:
                return True
            normalized = value.strip().lower()
            return normalized in {
                "your-key-here",
                "sk-your-key-here",
                "sk-ant-your-key-here",
            } or "your-key-here" in normalized

        key_by_provider = {
            "openai": "LLM_OPENAI_API_KEY",
            "anthropic": "LLM_ANTHROPIC_API_KEY",
            "google": "LLM_GOOGLE_API_KEY",
        }
        required_providers: set[str] = {default_provider}

        for override_key in (
            "LLM_CLASSIFIER_PROVIDER",
            "LLM_SQL_PROVIDER",
            "LLM_FALLBACK_PROVIDER",
        ):
            provider_value = (os.getenv(override_key) or "").strip().lower()
            if provider_value:
                required_providers.add(provider_value)

        missing_key_vars = sorted(
            {
                key_by_provider[provider]
                for provider in required_providers
                if provider in key_by_provider
                and _is_placeholder_key(os.getenv(key_by_provider[provider]))
            }
        )
        if missing_key_vars:
            missing_list = ", ".join(missing_key_vars)
            console.print(
                "[yellow]Warning:[/yellow] Missing LLM API key(s): "
                f"{missing_list}. Backend will still start, but queries may fail until "
                "keys are configured in Settings or env."
            )

    if not no_backend:
        backend_cmd = [
            sys.executable,
            "-m",
            "uvicorn",
            "backend.api.main:app",
            "--reload",
            "--host",
            backend_host,
            "--port",
            str(backend_port),
        ]
        console.print(f"[cyan]Starting backend:[/cyan] {' '.join(backend_cmd)}")
        processes.append(subprocess.Popen(backend_cmd))

    if not no_frontend:
        frontend_dir = Path(__file__).resolve().parents[1] / "frontend"
        if not frontend_dir.exists():
            raise click.ClickException("Frontend directory not found. Run from repo root.")
        frontend_cmd = [
            "npm",
            "run",
            "dev",
            "--",
            "-p",
            str(frontend_port),
            "-H",
            frontend_host,
        ]
        console.print(f"[cyan]Starting frontend:[/cyan] {' '.join(frontend_cmd)}")
        processes.append(subprocess.Popen(frontend_cmd, cwd=str(frontend_dir)))

    try:
        for process in processes:
            process.wait()
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopping dev servers...[/yellow]")
        for process in processes:
            process.terminate()
        for process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()


@cli.command()
@click.option("--target-db", "database_url", help="Target database URL.")
@click.option(
    "--system-db",
    "system_database_url",
    help="System database URL for registry/profiling.",
)
@click.option(
    "--auto-profile/--no-auto-profile",
    default=None,
    help="Auto-profile database (generate DataPoints draft).",
)
@click.option(
    "--max-tables",
    type=int,
    default=None,
    help="Max tables to auto-profile (0 = all).",
)
@click.option(
    "--non-interactive",
    is_flag=True,
    help="Fail instead of prompting for missing values.",
)
def setup(
    database_url: str | None,
    system_database_url: str | None,
    auto_profile: bool | None,
    max_tables: int | None,
    non_interactive: bool,
):
    """Guide system initialization for first-time setup."""

    async def run_setup():
        apply_config_defaults()
        settings = get_settings()
        resolved_default_url, _ = _resolve_target_database_url(settings)
        default_url = resolved_default_url or ""

        console.print(
            Panel.fit(
                "[bold green]DataChat Setup[/bold green]\n"
                "Initialize your database connection and load DataPoints.",
                border_style="green",
            )
        )

        resolved_database_url = (
            database_url
            or default_url
            or (str(settings.database.url) if settings.database.url else "")
        )
        if not resolved_database_url:
            if non_interactive:
                raise click.ClickException("Missing target database URL.")
            resolved_database_url = click.prompt(
                "Target Database URL", default=default_url, show_default=True
            )

        resolved_default_system_url, _ = _resolve_system_database_url(settings)
        resolved_system_database_url = system_database_url or resolved_default_system_url
        if not resolved_system_database_url and not non_interactive:
            resolved_system_database_url = click.prompt(
                "System Database URL (for demo/registry)",
                default="postgresql://datachat:datachat_password@localhost:5432/datachat",
                show_default=True,
            )

        if resolved_database_url:
            state.set_target_database_url(resolved_database_url)
        if resolved_system_database_url:
            state.set_system_database_url(resolved_system_database_url)

        resolved_auto_profile = auto_profile
        if resolved_auto_profile is None:
            if non_interactive:
                resolved_auto_profile = False
            else:
                resolved_auto_profile = click.confirm(
                    "Auto-profile database (generate DataPoints draft)",
                    default=False,
                    show_default=True,
                )

        resolved_max_tables = max_tables
        if resolved_auto_profile:
            if resolved_max_tables is None and not non_interactive:
                resolved_max_tables = click.prompt(
                    "Max tables to auto-profile (0 = all)",
                    default=10,
                    show_default=True,
                    type=int,
                )
            if resolved_max_tables is not None and resolved_max_tables <= 0:
                resolved_max_tables = None

        vector_store = VectorStore()
        await vector_store.initialize()

        initializer = SystemInitializer({"vector_store": vector_store})
        status_state, message = await initializer.initialize(
            database_url=resolved_database_url,
            auto_profile=bool(resolved_auto_profile),
            system_database_url=resolved_system_database_url,
        )

        console.print(f"[green]{message}[/green]")
        if resolved_auto_profile:
            clear_settings_cache()
            apply_config_defaults()
            refreshed_settings = get_settings()
            if not refreshed_settings.system_database.url:
                console.print(
                    "[yellow]Auto-profiling requires SYSTEM_DATABASE_URL. "
                    "Set it in your shell or .env and rerun setup.[/yellow]"
                )
            elif not refreshed_settings.database_credentials_key:
                console.print(
                    "[yellow]Auto-profiling requires DATABASE_CREDENTIALS_KEY. "
                    "Set it in your shell or .env and rerun setup.[/yellow]"
                )
            else:
                try:
                    initialize_tools(refreshed_settings.tools.policy_path)
                    executor = ToolExecutor()
                    ctx = ToolContext(user_id="cli", correlation_id="setup", approved=True)
                    args = {"depth": "metrics_basic", "batch_size": 10}
                    if resolved_max_tables:
                        args["max_tables"] = resolved_max_tables
                    result = await executor.execute(
                        "profile_and_generate_datapoints",
                        args,
                        ctx,
                    )
                    pending_count = result.get("result", {}).get("pending_count")
                    if pending_count is not None:
                        console.print(
                            f"[green]✓ Auto-profiling generated {pending_count} pending DataPoints.[/green]"
                        )
                        if resolved_max_tables and pending_count > resolved_max_tables:
                            console.print(
                                "[dim]Note: each table can generate multiple DataPoints (schema + metrics).[/dim]"
                            )
                    else:
                        console.print("[green]✓ Auto-profiling completed.[/green]")
                except Exception as exc:
                    console.print(f"[yellow]Auto-profiling failed to start: {exc}[/yellow]")
        if status_state.setup_required:
            console.print("[yellow]Remaining setup steps:[/yellow]")
            for step in status_state.setup_required:
                console.print(f"- {step.title}: {step.description}")
            console.print("[cyan]Hint: Run 'datachat dp sync' after adding DataPoints.[/cyan]")
        else:
            console.print("[green]✓ System initialized. You're ready to query.[/green]")

    asyncio.run(run_setup())


@cli.command()
@click.pass_context
@click.option("--database-url", help="Target database URL.")
@click.option(
    "--system-db",
    "system_database_url",
    help="System database URL for registry/profiling/demo flows.",
)
@click.option(
    "--auto-profile/--no-auto-profile",
    default=False,
    show_default=True,
    help="Run setup with auto-profiling enabled.",
)
@click.option(
    "--max-tables",
    default=10,
    show_default=True,
    type=int,
    help="When auto-profiling, maximum tables to include (0 = all).",
)
@click.option(
    "--dataset",
    type=click.Choice(["none", "core", "grocery", "fintech"], case_sensitive=False),
    default="none",
    show_default=True,
    help="Optional demo dataset to load after setup.",
)
@click.option(
    "--persona",
    type=click.Choice(
        ["base", "analyst", "engineer", "platform", "executive"], case_sensitive=False
    ),
    default="base",
    show_default=True,
    help="Persona profile used when --dataset=core.",
)
@click.option(
    "--demo-reset",
    is_flag=True,
    help="When loading a demo dataset, reset tables before seeding.",
)
@click.option(
    "--question",
    default=None,
    help="Optional first question to run after setup/demo.",
)
@click.option(
    "--max-clarifications",
    default=3,
    show_default=True,
    type=int,
    help="Clarification cap for --question follow-ups.",
)
@click.option(
    "--non-interactive",
    is_flag=True,
    help="Fail instead of prompting for missing values.",
)
def quickstart(
    ctx: click.Context,
    database_url: str | None,
    system_database_url: str | None,
    auto_profile: bool,
    max_tables: int,
    dataset: str,
    persona: str,
    demo_reset: bool,
    question: str | None,
    max_clarifications: int,
    non_interactive: bool,
):
    """Run a thin guided bootstrap flow using existing commands."""
    apply_config_defaults()
    settings = get_settings()
    resolved_database_url = database_url or _resolve_target_database_url(settings)[0]

    if not resolved_database_url:
        if non_interactive:
            raise click.ClickException(
                "Missing target database URL. Pass --database-url or configure DATABASE_URL."
            )
        resolved_database_url = click.prompt(
            "Target Database URL",
            default="",
            show_default=False,
        )

    resolved_max_tables = max_tables if max_tables > 0 else None
    _emit_entry_event_cli(
        flow="phase1_4_quickstart",
        step="start",
        status="started",
        metadata={
            "dataset": dataset,
            "auto_profile": auto_profile,
            "question_supplied": bool(question),
        },
    )
    try:
        ctx.invoke(
            connect,
            connection_string=resolved_database_url,
            name=None,
            register=True,
            set_default=True,
        )
        ctx.invoke(
            setup,
            database_url=resolved_database_url,
            system_database_url=system_database_url,
            auto_profile=auto_profile,
            max_tables=resolved_max_tables,
            non_interactive=non_interactive,
        )

        if dataset.lower() != "none":
            _emit_entry_event_cli(
                flow="phase1_4_quickstart",
                step="demo_load",
                status="started",
                metadata={"dataset": dataset, "persona": persona, "reset": demo_reset},
            )
            ctx.invoke(
                demo,
                dataset=dataset,
                persona=persona,
                reset=demo_reset,
                no_workspace=True,
            )
            _emit_entry_event_cli(
                flow="phase1_4_quickstart",
                step="demo_load",
                status="completed",
                metadata={"dataset": dataset},
            )

        if question and question.strip():
            ctx.invoke(
                ask,
                query=question.strip(),
                evidence=False,
                pager=False,
                max_clarifications=max_clarifications,
                synthesize_simple_sql=None,
            )

        _emit_entry_event_cli(
            flow="phase1_4_quickstart",
            step="complete",
            status="completed",
            metadata={"dataset": dataset},
        )
        console.print("[green]✓ Quickstart complete.[/green]")
        console.print(
            "[dim]Next: run 'datachat chat' or open the UI at /databases to continue onboarding.[/dim]"
        )
    except Exception as exc:
        _emit_entry_event_cli(
            flow="phase1_4_quickstart",
            step="complete",
            status="failed",
            metadata={"error": str(exc)},
        )
        raise


@cli.group(name="onboarding")
def onboarding():
    """Guided onboarding flows."""


@onboarding.command(name="wizard")
@click.option("--database-url", default=None, help="Target database URL.")
@click.option(
    "--connection-id",
    default=None,
    help="Existing registry connection ID to onboard (alternative to --database-url).",
)
@click.option("--name", default=None, help="Optional display name for the registry connection.")
@click.option(
    "--database-type",
    default=None,
    type=click.Choice(["postgresql", "mysql", "clickhouse", "sqlite"], case_sensitive=False),
    help="Optional explicit database type override.",
)
@click.option(
    "--system-db",
    "system_database_url",
    default=None,
    help="System database URL used for registry and profiling metadata.",
)
@click.option("--sample-size", default=120, show_default=True, type=int)
@click.option("--max-tables", default=50, show_default=True, type=int)
@click.option(
    "--depth",
    "--metrics-depth",
    "depth",
    default="metrics_full",
    show_default=True,
    type=click.Choice(["schema_only", "metrics_basic", "metrics_full"], case_sensitive=False),
)
@click.option("--batch-size", default=10, show_default=True, type=int)
@click.option("--max-metrics-per-table", default=3, show_default=True, type=int)
@click.option("--non-interactive", is_flag=True, help="Fail instead of prompting for missing values.")
def onboarding_wizard(
    database_url: str | None,
    connection_id: str | None,
    name: str | None,
    database_type: str | None,
    system_database_url: str | None,
    sample_size: int,
    max_tables: int,
    depth: str,
    batch_size: int,
    max_metrics_per_table: int,
    non_interactive: bool,
):
    """
    Connect, deeply analyze schema, generate managed metadata, and index it in one flow.

    This is the fastest "first value" path for evaluators: no hand-authored DataPoints required.
    """

    async def run_wizard() -> None:
        apply_config_defaults()
        settings = get_settings()

        resolved_connection_id = connection_id.strip() if connection_id and connection_id.strip() else None
        if resolved_connection_id and database_url:
            console.print(
                "[yellow]Both --connection-id and --database-url provided; using --connection-id.[/yellow]"
            )

        resolved_database_url = None if resolved_connection_id else (database_url or _resolve_target_database_url(settings)[0])
        if not resolved_connection_id and not resolved_database_url:
            if non_interactive:
                raise click.ClickException("Missing target database URL.")
            resolved_database_url = click.prompt(
                "Target Database URL",
                default="",
                show_default=False,
            )

        resolved_system_database_url = system_database_url or _resolve_system_database_url(settings)[0]
        if not resolved_system_database_url:
            if non_interactive:
                raise click.ClickException(
                    "Missing SYSTEM_DATABASE_URL. Provide --system-db or set SYSTEM_DATABASE_URL."
                )
            resolved_system_database_url = click.prompt(
                "System Database URL",
                default="postgresql://datachat:datachat_password@localhost:5432/datachat",
                show_default=True,
            )

        state.set_system_database_url(resolved_system_database_url)
        if resolved_database_url:
            state.set_target_database_url(resolved_database_url)
            state.set_connection_string(resolved_database_url)
        clear_settings_cache()
        apply_config_defaults()
        settings = get_settings()

        if not settings.system_database.url:
            raise click.ClickException(
                "SYSTEM_DATABASE_URL is required for onboarding wizard (registry + profiling)."
            )
        if not settings.database_credentials_key:
            raise click.ClickException(
                "DATABASE_CREDENTIALS_KEY is required for onboarding wizard."
            )

        manager = DatabaseConnectionManager(system_database_url=str(settings.system_database.url))
        await manager.initialize()
        try:
            explicit_name = name.strip() if name and name.strip() else None

            if resolved_connection_id:
                try:
                    connection = await manager.get_connection(resolved_connection_id)
                except (KeyError, ValueError) as exc:
                    raise click.ClickException(
                        f"Connection not found or invalid: {resolved_connection_id}"
                    ) from exc

                resolved_database_url = connection.database_url.get_secret_value()
                if not resolved_database_url:
                    raise click.ClickException(
                        f"Selected connection has no database URL: {resolved_connection_id}"
                    )

                inferred_db_type = (
                    database_type or connection.database_type or infer_database_type(resolved_database_url)
                ).lower()
                target_name = explicit_name or connection.name

                updates: dict[str, Any] = {}
                if explicit_name and connection.name != target_name:
                    updates["name"] = target_name
                if connection.database_type != inferred_db_type:
                    updates["database_type"] = inferred_db_type
                if updates:
                    connection = await manager.update_connection(
                        connection.connection_id, updates=updates
                    )

                console.print(
                    f"[green]✓ Using selected connection[/green] {connection.connection_id}"
                )
            else:
                inferred_db_type = (database_type or infer_database_type(resolved_database_url)).lower()
                target_name = explicit_name or _default_connection_name(resolved_database_url)

                existing = next(
                    (
                        conn
                        for conn in await manager.list_connections()
                        if _same_database_url(conn.database_url.get_secret_value(), resolved_database_url)
                    ),
                    None,
                )

                if existing:
                    updates = {}
                    if explicit_name and existing.name != target_name:
                        updates["name"] = target_name
                    if existing.database_type != inferred_db_type:
                        updates["database_type"] = inferred_db_type
                    if updates:
                        existing = await manager.update_connection(
                            existing.connection_id, updates=updates
                        )
                    connection = existing
                    console.print(
                        f"[green]✓ Using existing connection[/green] {connection.connection_id}"
                    )
                else:
                    connection = await manager.add_connection(
                        name=target_name,
                        database_url=resolved_database_url,
                        database_type=inferred_db_type,
                        tags=["managed", "wizard"],
                        description="Generated by onboarding wizard",
                        is_default=True,
                    )
                    console.print(
                        f"[green]✓ Created new connection[/green] {connection.connection_id}"
                    )

            if not connection.is_default:
                await manager.set_default(connection.connection_id)

            state.set_target_database_url(resolved_database_url)
            state.set_connection_string(resolved_database_url)

            connection_id_str = str(connection.connection_id)
            selected_max_tables = max_tables if max_tables > 0 else None

            profiler = SchemaProfiler(manager)
            with console.status(
                "[cyan]Profiling schema and sampling column distributions...[/cyan]"
            ):
                profile = await profiler.profile_database(
                    connection_id=connection_id_str,
                    sample_size=sample_size,
                    max_tables=selected_max_tables,
                    max_columns_per_table=120,
                    query_timeout_seconds=8,
                    per_table_timeout_seconds=30,
                    total_timeout_seconds=300,
                )

            generator = DataPointGenerator()
            with console.status(
                "[cyan]Generating managed metadata (summaries, metrics, semantic hints)...[/cyan]"
            ):
                generated = await generator.generate_from_profile(
                    profile,
                    depth=depth,
                    batch_size=batch_size,
                    max_tables=selected_max_tables,
                    max_metrics_per_table=max_metrics_per_table,
                )

            removed_ids = _remove_managed_datapoints_for_connection(connection_id_str)
            datapoints = _persist_generated_datapoints_for_connection(
                generated,
                connection_id=connection_id_str,
            )

            vector_store = VectorStore()
            await vector_store.initialize()
            if removed_ids:
                await vector_store.delete(sorted(set(removed_ids)))
            if datapoints:
                await vector_store.add_datapoints(datapoints)

            schema_rows: list[str] = []
            for item in generated.schema_datapoints[:8]:
                payload = item.datapoint
                table_name = str(payload.get("table_name") or payload.get("name") or "unknown")
                purpose = str(payload.get("business_purpose") or "").strip()
                metadata = payload.get("metadata")
                if isinstance(metadata, dict):
                    display_hints = metadata.get("display_hints") or []
                else:
                    display_hints = []
                display_text = ", ".join(display_hints[:2]) if isinstance(display_hints, list) else ""
                if purpose:
                    line = f"- {table_name}: {purpose}"
                else:
                    line = f"- {table_name}"
                if display_text:
                    line += f" (viz: {display_text})"
                schema_rows.append(line)

            report_lines = [
                "# Onboarding Wizard Report",
                "",
                f"- Connection ID: `{connection_id_str}`",
                f"- Database Type: `{inferred_db_type}`",
                f"- Tables Discovered: `{profile.total_tables_discovered}`",
                f"- Tables Profiled: `{profile.tables_profiled}`",
                f"- Tables Failed: `{profile.tables_failed}`",
                f"- Generated Schema DataPoints: `{len(generated.schema_datapoints)}`",
                f"- Generated Business DataPoints: `{len(generated.business_datapoints)}`",
                f"- Replaced Managed DataPoints: `{len(removed_ids)}`",
                f"- Indexed New DataPoints: `{len(datapoints)}`",
                "",
                "## Semantic Table Highlights",
                "",
                *(schema_rows or ["- No schema summaries generated."]),
            ]
            reports_dir = Path("reports")
            reports_dir.mkdir(parents=True, exist_ok=True)
            report_path = reports_dir / f"onboarding_wizard_{connection_id_str}.md"
            report_path.write_text("\n".join(report_lines), encoding="utf-8")

            summary = Table(title="Onboarding Wizard", show_header=False, box=None)
            summary.add_row("[green]✓ Complete[/green]")
            summary.add_row("Connection ID:", connection_id_str)
            summary.add_row("Tables profiled:", str(profile.tables_profiled))
            summary.add_row("Generated metadata:", str(len(datapoints)))
            summary.add_row("Report:", str(report_path))
            console.print(summary)
            console.print(
                "[dim]Next: ask questions immediately; live prompts now include richer schema semantics.[/dim]"
            )
        finally:
            await manager.close()

    asyncio.run(run_wizard())


@cli.command()
@click.pass_context
@click.option(
    "--mode",
    type=click.Choice(["sync", "profile"], case_sensitive=False),
    default="sync",
    show_default=True,
    help="Training helper mode.",
)
@click.option(
    "--datapoints-dir",
    default="datapoints",
    show_default=True,
    help="DataPoint directory for mode=sync.",
)
@click.option("--connection-id", default=None, help="Database scope connection ID for sync mode.")
@click.option("--global-scope", is_flag=True, help="Mark synced DataPoints as global scope.")
@click.option(
    "--strict-contracts/--no-strict-contracts",
    default=True,
    show_default=True,
    help="Validate DataPoint contracts during sync mode.",
)
@click.option(
    "--fail-on-contract-warnings",
    is_flag=True,
    help="Treat contract warnings as errors during sync mode.",
)
@click.option(
    "--conflict-mode",
    type=click.Choice(["error", "prefer_user", "prefer_managed", "prefer_latest"]),
    default="error",
    show_default=True,
    help="Conflict resolution policy for duplicate semantic definitions in sync mode.",
)
@click.option(
    "--profile-connection-id",
    default=None,
    help="Connection ID for mode=profile.",
)
@click.option("--sample-size", default=100, show_default=True, type=int)
@click.option("--tables", multiple=True, help="Optional profile/generation table filters.")
@click.option(
    "--generate-after-profile/--no-generate-after-profile",
    default=False,
    show_default=True,
    help="Start DataPoint generation from the latest profile in mode=profile.",
)
@click.option(
    "--depth",
    type=click.Choice(["schema_only", "metrics_basic", "metrics_full"]),
    default="metrics_basic",
    show_default=True,
    help="Generation depth for --generate-after-profile.",
)
@click.option("--batch-size", default=10, show_default=True, type=int)
@click.option("--max-tables", default=None, type=int)
@click.option("--max-metrics-per-table", default=3, show_default=True, type=int)
def train(
    ctx: click.Context,
    mode: str,
    datapoints_dir: str,
    connection_id: str | None,
    global_scope: bool,
    strict_contracts: bool,
    fail_on_contract_warnings: bool,
    conflict_mode: str,
    profile_connection_id: str | None,
    sample_size: int,
    tables: tuple[str, ...],
    generate_after_profile: bool,
    depth: str,
    batch_size: int,
    max_tables: int | None,
    max_metrics_per_table: int,
):
    """Thin wrapper over existing sync/profile generation flows."""
    normalized_mode = mode.lower()
    _emit_entry_event_cli(
        flow="phase1_4_train",
        step="start",
        status="started",
        metadata={"mode": normalized_mode},
    )
    try:
        if normalized_mode == "sync":
            ctx.invoke(
                sync_datapoints,
                datapoints_dir=datapoints_dir,
                connection_id=connection_id,
                global_scope=global_scope,
                strict_contracts=strict_contracts,
                fail_on_contract_warnings=fail_on_contract_warnings,
                conflict_mode=conflict_mode,
            )
        else:
            if not profile_connection_id:
                raise click.ClickException("mode=profile requires --profile-connection-id.")
            ctx.invoke(
                start_profile,
                connection_id=profile_connection_id,
                sample_size=sample_size,
                tables=tables,
            )
            if generate_after_profile:
                ctx.invoke(
                    generate_datapoints_cli,
                    profile_id=None,
                    connection_id=profile_connection_id,
                    depth=depth,
                    tables=tables,
                    batch_size=batch_size,
                    max_tables=max_tables,
                    max_metrics_per_table=max_metrics_per_table,
                )

        _emit_entry_event_cli(
            flow="phase1_4_train",
            step="complete",
            status="completed",
            metadata={"mode": normalized_mode},
        )
    except Exception as exc:
        _emit_entry_event_cli(
            flow="phase1_4_train",
            step="complete",
            status="failed",
            metadata={"mode": normalized_mode, "error": str(exc)},
        )
        raise


@cli.command()
@click.option("--include-target", is_flag=True, help="Also clear target database tables.")
@click.option(
    "--drop-all-target",
    is_flag=True,
    help="Drop all tables in the target database (dangerous). Requires --include-target.",
)
@click.option("--keep-config", is_flag=True, help="Keep ~/.datachat/config.json.")
@click.option("--keep-vectors", is_flag=True, help="Keep local vector store on disk.")
@click.option(
    "--clear-managed-datapoints/--keep-managed-datapoints",
    default=True,
    show_default=True,
    help="Clear local DataPoint files under datapoints/managed.",
)
@click.option(
    "--clear-user-datapoints/--keep-user-datapoints",
    default=True,
    show_default=True,
    help="Clear local DataPoint files under datapoints/user.",
)
@click.option(
    "--clear-example-datapoints/--keep-example-datapoints",
    default=False,
    show_default=True,
    help=(
        "Also clear sample/reference DataPoint files under datapoints/examples and datapoints/demo."
    ),
)
@click.option("--yes", is_flag=True, help="Skip confirmation prompts (use with caution).")
def reset(
    include_target: bool,
    drop_all_target: bool,
    keep_config: bool,
    keep_vectors: bool,
    clear_managed_datapoints: bool,
    clear_user_datapoints: bool,
    clear_example_datapoints: bool,
    yes: bool,
):
    """Reset system state for testing or clean setup."""

    async def run_reset() -> None:
        apply_config_defaults()
        settings = get_settings()

        if drop_all_target and not include_target:
            raise click.ClickException("--drop-all-target requires --include-target.")

        if not yes:
            console.print(
                Panel.fit(
                    "[bold red]Reset DataChat State[/bold red]\n"
                    "This clears system registry/profiling, local vectors, and saved config.\n"
                    "By default it also clears datapoints/managed and datapoints/user.",
                    border_style="red",
                )
            )
            if not click.confirm("Continue?", default=False, show_default=True):
                console.print("[yellow]Reset cancelled.[/yellow]")
                return

        system_db_url, _ = _resolve_system_database_url(settings)
        if system_db_url:
            from urllib.parse import urlparse

            parsed = urlparse(system_db_url)
            connector = PostgresConnector(
                host=parsed.hostname or "localhost",
                port=parsed.port or 5432,
                database=parsed.path.lstrip("/") if parsed.path else "datachat",
                user=parsed.username or "postgres",
                password=parsed.password or "",
            )
            await connector.connect()
            try:
                await connector.execute(
                    "TRUNCATE database_connections, profiling_jobs, "
                    "profiling_profiles, pending_datapoints"
                )
                console.print("[green]✓ System DB state cleared[/green]")
            finally:
                await connector.close()
        else:
            console.print("[yellow]System DB not configured; skipped registry reset.[/yellow]")

        if include_target:
            target_db_url, _ = _resolve_target_database_url(settings)
            if not target_db_url:
                console.print("[yellow]Target DB not configured; skipped target reset.[/yellow]")
            else:
                target_db_type = infer_database_type(target_db_url)
                connector = create_connector(
                    database_url=target_db_url,
                    database_type=target_db_type,
                )
                await connector.connect()
                try:
                    if drop_all_target:
                        if target_db_type != "postgresql":
                            raise click.ClickException(
                                "--drop-all-target currently supports PostgreSQL only."
                            )
                        await connector.execute(
                            """
                            DO $$
                            DECLARE r RECORD;
                            BEGIN
                              FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public')
                              LOOP
                                EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename)
                                || ' CASCADE';
                              END LOOP;
                            END $$;
                            """
                        )
                        console.print("[green]✓ Target DB tables dropped[/green]")
                    else:
                        await _clear_demo_tables_for_database(connector, target_db_type)
                        console.print("[green]✓ Target DB demo tables cleared[/green]")
                finally:
                    await connector.close()

        if not keep_vectors:
            vector_store = VectorStore()
            await vector_store.initialize()
            await vector_store.clear()
            shutil.rmtree(settings.chroma.persist_dir, ignore_errors=True)
            console.print("[green]✓ Local vector store cleared[/green]")

        if clear_managed_datapoints:
            managed_dir = Path("datapoints") / "managed"
            if managed_dir.exists():
                shutil.rmtree(managed_dir, ignore_errors=True)
                console.print("[green]✓ Managed DataPoints cleared[/green]")

        if clear_user_datapoints:
            user_dir = Path("datapoints") / "user"
            if user_dir.exists():
                shutil.rmtree(user_dir, ignore_errors=True)
                console.print("[green]✓ User DataPoints cleared[/green]")

        if clear_example_datapoints:
            examples_dir = Path("datapoints") / "examples"
            demo_dir = Path("datapoints") / "demo"
            if examples_dir.exists():
                shutil.rmtree(examples_dir, ignore_errors=True)
                console.print("[green]✓ Example DataPoints cleared[/green]")
            if demo_dir.exists():
                shutil.rmtree(demo_dir, ignore_errors=True)
                console.print("[green]✓ Demo DataPoints cleared[/green]")

        if not keep_config:
            state.refresh_paths()
            if state.config_file.exists():
                try:
                    state.config_file.unlink()
                    console.print("[green]✓ Saved config cleared[/green]")
                except OSError:
                    console.print("[yellow]Failed to remove saved config.[/yellow]")

        console.print("[green]Reset complete.[/green]")

    asyncio.run(run_reset())


@cli.command()
@click.option(
    "--dataset",
    type=click.Choice(["core", "grocery", "fintech"], case_sensitive=False),
    default="core",
    show_default=True,
    help="Demo dataset to seed and load.",
)
@click.option(
    "--persona",
    type=click.Choice(
        ["base", "analyst", "engineer", "platform", "executive"], case_sensitive=False
    ),
    default="base",
    show_default=True,
    help="Persona-specific demo setup to load.",
)
@click.option("--reset", is_flag=True, help="Drop and re-seed demo tables.")
@click.option("--no-workspace", is_flag=True, help="Skip workspace indexing (if available).")
def demo(dataset: str, persona: str, reset: bool, no_workspace: bool):
    """Seed demo tables and load demo DataPoints."""

    async def run_demo():
        apply_config_defaults()
        settings = get_settings()
        target_database_url, _ = _resolve_target_database_url(settings)
        if not target_database_url:
            raise click.ClickException(
                "DATABASE_URL (or saved target connection from 'datachat connect') must be set to run the demo."
            )

        dataset_name = dataset.lower()
        persona_name = persona.lower()
        target_desc = _format_connection_target(target_database_url)

        connector = _build_postgres_connector(target_database_url)

        console.print(
            f"[cyan]Seeding demo dataset '{dataset_name}' into target DB ({target_desc})...[/cyan]"
        )
        await connector.connect()
        try:
            if dataset_name == "grocery":
                if reset:
                    await _execute_sql_script(connector, Path("scripts") / "grocery_seed.sql")
                else:
                    has_grocery = await connector.execute(
                        "SELECT to_regclass('public.grocery_stores') IS NOT NULL AS exists"
                    )
                    exists = bool(has_grocery.rows and has_grocery.rows[0].get("exists"))
                    if not exists:
                        await _execute_sql_script(connector, Path("scripts") / "grocery_seed.sql")
                    else:
                        console.print(
                            "[yellow]Grocery tables already exist. Use --reset to re-seed.[/yellow]"
                        )
            elif dataset_name == "fintech":
                if reset:
                    await _execute_sql_script(connector, Path("scripts") / "fintech_seed.sql")
                else:
                    has_fintech = await connector.execute(
                        "SELECT to_regclass('public.bank_customers') IS NOT NULL AS exists"
                    )
                    exists = bool(has_fintech.rows and has_fintech.rows[0].get("exists"))
                    if not exists:
                        await _execute_sql_script(connector, Path("scripts") / "fintech_seed.sql")
                    else:
                        console.print(
                            "[yellow]Fintech tables already exist. Use --reset to re-seed.[/yellow]"
                        )
            else:
                if reset:
                    await connector.execute("DROP TABLE IF EXISTS orders")
                    await connector.execute("DROP TABLE IF EXISTS users")

                await connector.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        email TEXT NOT NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        is_active BOOLEAN NOT NULL DEFAULT TRUE
                    );
                    """
                )
                await connector.execute(
                    """
                    CREATE TABLE IF NOT EXISTS orders (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER NOT NULL REFERENCES users(id),
                        amount NUMERIC(12,2) NOT NULL,
                        status TEXT NOT NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                        order_date DATE NOT NULL DEFAULT CURRENT_DATE
                    );
                    """
                )

                user_count = await connector.execute("SELECT COUNT(*) AS count FROM users")
                if user_count.rows and user_count.rows[0]["count"] == 0:
                    await connector.execute(
                        """
                        INSERT INTO users (email, is_active)
                        VALUES
                            ('alice@example.com', TRUE),
                            ('bob@example.com', TRUE),
                            ('charlie@example.com', FALSE)
                        """
                    )

                order_count = await connector.execute("SELECT COUNT(*) AS count FROM orders")
                if order_count.rows and order_count.rows[0]["count"] == 0:
                    await connector.execute(
                        """
                        INSERT INTO orders (user_id, amount, status, order_date)
                        VALUES
                            (1, 120.50, 'completed', CURRENT_DATE - INTERVAL '12 days'),
                            (1, 75.00, 'completed', CURRENT_DATE - INTERVAL '6 days'),
                            (2, 200.00, 'completed', CURRENT_DATE - INTERVAL '2 days'),
                            (3, 15.00, 'refunded', CURRENT_DATE - INTERVAL '20 days')
                        """
                    )
        finally:
            await connector.close()

        if dataset_name == "grocery":
            datapoints_dir = Path("datapoints") / "examples" / "grocery_store"
            workspace_root = Path("workspace_demo") / "grocery"
            suggested_query = "List all grocery stores"
        elif dataset_name == "fintech":
            datapoints_dir = Path("datapoints") / "examples" / "fintech_bank"
            workspace_root = Path("workspace_demo") / "fintech"
            suggested_query = "What is total deposits?"
        else:
            base_dir = Path("datapoints") / "demo"
            datapoints_dir = base_dir
            if persona_name != "base":
                persona_dir = base_dir / persona_name
                if persona_dir.exists():
                    datapoints_dir = persona_dir
                else:
                    console.print(
                        f"[yellow]Persona DataPoints not found at {persona_dir}. Falling back to base demo.[/yellow]"
                    )
            workspace_root = Path("workspace_demo") / persona_name
            suggested_query = "How many users are active?"

        if not datapoints_dir.exists():
            console.print(f"[red]Demo DataPoints not found at {datapoints_dir}[/red]")
            raise click.ClickException("Missing demo DataPoints.")

        console.print("[cyan]Loading demo DataPoints...[/cyan]")
        loader = DataPointLoader()
        datapoints = loader.load_directory(datapoints_dir)
        if not datapoints:
            raise click.ClickException("No demo DataPoints loaded.")
        _apply_datapoint_scope(
            datapoints,
            connection_id=(
                await _resolve_registry_connection_id_for_url(target_database_url)
                or ENV_DATABASE_CONNECTION_ID
            ),
            global_scope=False,
        )

        vector_store = VectorStore()
        await vector_store.initialize()
        await vector_store.clear()
        await vector_store.add_datapoints(datapoints)

        graph = KnowledgeGraph()
        for datapoint in datapoints:
            graph.add_datapoint(datapoint)

        if not no_workspace:
            if workspace_root.exists():
                console.print(
                    "[yellow]Workspace indexing is not implemented yet. "
                    "Found workspace demo content but skipped indexing.[/yellow]"
                )
            else:
                console.print(
                    f"[yellow]Workspace demo folder not found at {workspace_root} (skipping).[/yellow]"
                )

        console.print(f'[green]✓ Demo data loaded. Try: datachat ask "{suggested_query}"[/green]')

    asyncio.run(run_demo())


# ============================================================================
# DataPoint Commands
# ============================================================================


@cli.group(name="profile")
def profile():
    """Manage profiling jobs via API."""
    pass


@profile.command(name="start")
@click.option("--connection-id", required=True, help="Database connection UUID.")
@click.option("--sample-size", default=100, show_default=True, type=int)
@click.option("--tables", multiple=True, help="Optional table names to profile.")
def start_profile(connection_id: str, sample_size: int, tables: tuple[str, ...]):
    """Start profiling for a registered database connection."""
    payload = {"sample_size": sample_size, "tables": list(tables) or None}
    try:
        response = httpx.post(
            f"{API_BASE_URL}/api/v1/databases/{connection_id}/profile",
            json=payload,
            timeout=30.0,
        )
        response.raise_for_status()
        data = response.json()
        console.print(f"[green]✓ Profiling started[/green] job_id={data['job_id']}")
    except Exception as exc:
        console.print(f"[red]Failed to start profiling: {exc}[/red]")
        sys.exit(1)


@profile.command(name="status")
@click.argument("job_id")
def profile_status(job_id: str):
    """Check profiling job status."""
    try:
        response = httpx.get(f"{API_BASE_URL}/api/v1/profiling/jobs/{job_id}", timeout=15.0)
        response.raise_for_status()
        data = response.json()
        console.print(json.dumps(data, indent=2))
    except Exception as exc:
        console.print(f"[red]Failed to fetch status: {exc}[/red]")
        sys.exit(1)


@cli.group(name="dp")
def datapoint():
    """Manage DataPoints (knowledge base)."""
    pass


# ============================================================================
# Tool Commands
# ============================================================================


@cli.group(name="tools")
def tools():
    """Manage tool execution and reports."""
    pass


@tools.command(name="list")
def list_tools():
    """List available tools."""
    try:
        response = httpx.get(f"{API_BASE_URL}/api/v1/tools", timeout=15.0)
        response.raise_for_status()
        data = response.json()
        table = Table(title="Tools", show_header=True, header_style="bold cyan")
        table.add_column("Name")
        table.add_column("Category")
        table.add_column("Approval")
        table.add_column("Enabled")
        for tool in data:
            table.add_row(
                tool.get("name", ""),
                tool.get("category", ""),
                "yes" if tool.get("requires_approval") else "no",
                "yes" if tool.get("enabled") else "no",
            )
        console.print(table)
    except Exception as exc:
        console.print(f"[red]Failed to list tools: {exc}[/red]")
        sys.exit(1)


@tools.command(name="run")
@click.argument("name")
@click.option("--approve", is_flag=True, help="Approve tool execution.")
def run_tool(name: str, approve: bool):
    """Run a tool via the API."""
    if not approve:
        approve = click.confirm(
            "This tool may trigger profiling or other actions. Proceed?", default=False
        )
    payload = {"name": name, "arguments": {}, "approved": approve}
    try:
        response = httpx.post(f"{API_BASE_URL}/api/v1/tools/execute", json=payload, timeout=60.0)
        response.raise_for_status()
        console.print(json.dumps(response.json(), indent=2))
    except Exception as exc:
        console.print(f"[red]Tool execution failed: {exc}[/red]")
        sys.exit(1)


@tools.command(name="quality-report")
def quality_report():
    """Run DataPoint quality report."""
    payload = {"name": "datapoint_quality_report", "arguments": {"limit": 10}}
    try:
        response = httpx.post(f"{API_BASE_URL}/api/v1/tools/execute", json=payload, timeout=30.0)
        response.raise_for_status()
        console.print(json.dumps(response.json(), indent=2))
    except Exception as exc:
        console.print(f"[red]Failed to run quality report: {exc}[/red]")
        sys.exit(1)


@datapoint.command(name="list")
@click.option(
    "--type",
    "dp_type",
    type=click.Choice(["Schema", "Business", "Process", "Query"]),
    help="Filter by DataPoint type",
)
def list_datapoints(dp_type: str | None):
    """List all DataPoints in the knowledge base."""

    async def run_list():
        try:
            vector_store = VectorStore()
            await vector_store.initialize()

            filter_metadata = None
            if dp_type:
                filter_metadata = {"type": dp_type}

            results = await vector_store.search(
                query="",
                top_k=1000,
                filter_metadata=filter_metadata,
            )

            if not results:
                console.print("[yellow]No DataPoints found[/yellow]")
                return

            table = Table(
                title=f"DataPoints ({len(results)} found)",
                show_header=True,
                header_style="bold cyan",
            )
            table.add_column("ID", style="cyan")
            table.add_column("Type", style="green")
            table.add_column("Name")
            table.add_column("Score", justify="right")

            for result in results:
                metadata = result.get("metadata", {})
                score = result.get("distance")
                score_text = f"{score:.3f}" if isinstance(score, (int, float)) else "-"
                table.add_row(
                    metadata.get("datapoint_id", "unknown"),
                    metadata.get("type", "unknown"),
                    metadata.get("name", "unknown"),
                    score_text,
                )

            console.print(table)

        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")
            sys.exit(1)

    asyncio.run(run_list())


@datapoint.group(name="pending")
def pending_datapoints():
    """Review pending DataPoints (requires backend)."""


def _fetch_pending_datapoints() -> list[dict[str, Any]]:
    response = httpx.get(f"{API_BASE_URL}/api/v1/datapoints/pending", timeout=15.0)
    response.raise_for_status()
    data = response.json()
    return data.get("pending", [])


def _resolve_default_connection_id() -> str:
    response = httpx.get(f"{API_BASE_URL}/api/v1/databases", timeout=15.0)
    response.raise_for_status()
    connections = response.json()
    if not isinstance(connections, list) or not connections:
        raise click.ClickException(
            "No database connections found. Run 'datachat setup' or add one in the UI."
        )
    default = next((item for item in connections if item.get("is_default")), None)
    connection = default or connections[0]
    connection_id = connection.get("connection_id")
    if not connection_id:
        raise click.ClickException("Default connection is missing an ID.")
    return str(connection_id)


def _resolve_latest_profile_id(connection_id: str) -> str:
    response = httpx.get(
        f"{API_BASE_URL}/api/v1/profiling/jobs/connection/{connection_id}/latest",
        timeout=15.0,
    )
    response.raise_for_status()
    data = response.json()
    if not data:
        raise click.ClickException(
            "No profiling job found. Run 'datachat profile start' or 'datachat setup --auto-profile'."
        )
    profile_id = data.get("profile_id")
    if not profile_id:
        raise click.ClickException("Latest profiling job is missing a profile_id.")
    return str(profile_id)
    pass


@pending_datapoints.command(name="list")
def list_pending_datapoints():
    """List pending DataPoints awaiting approval."""
    try:
        pending = _fetch_pending_datapoints()
        if not pending:
            console.print("[yellow]No pending DataPoints found[/yellow]")
            return

        table = Table(title="Pending DataPoints", show_header=True, header_style="bold cyan")
        table.add_column("Pending ID")
        table.add_column("Type")
        table.add_column("Name")
        table.add_column("Confidence", justify="right")
        table.add_column("Status")
        for item in pending:
            datapoint = item.get("datapoint", {}) if isinstance(item, dict) else {}
            table.add_row(
                str(item.get("pending_id", "")),
                str(datapoint.get("type", "")),
                str(datapoint.get("name") or datapoint.get("datapoint_id") or ""),
                f"{item.get('confidence', 0):.2f}" if item.get("confidence") is not None else "-",
                str(item.get("status", "")),
            )
        console.print(table)
    except Exception as exc:
        console.print(f"[red]Failed to list pending DataPoints: {exc}[/red]")
        sys.exit(1)


@pending_datapoints.command(name="approve")
@click.argument("pending_id")
@click.option("--note", help="Optional review note.")
def approve_pending_datapoint(pending_id: str, note: str | None):
    """Approve a pending DataPoint."""
    payload = {"review_note": note} if note else None
    try:
        response = httpx.post(
            f"{API_BASE_URL}/api/v1/datapoints/pending/{pending_id}/approve",
            json=payload,
            timeout=15.0,
        )
        response.raise_for_status()
        console.print("[green]✓ Approved DataPoint[/green]")
    except Exception as exc:
        console.print(f"[red]Failed to approve DataPoint: {exc}[/red]")
        sys.exit(1)


@pending_datapoints.command(name="reject")
@click.argument("pending_id")
@click.option("--note", help="Optional review note.")
def reject_pending_datapoint(pending_id: str, note: str | None):
    """Reject a pending DataPoint."""
    payload = {"review_note": note} if note else None
    try:
        response = httpx.post(
            f"{API_BASE_URL}/api/v1/datapoints/pending/{pending_id}/reject",
            json=payload,
            timeout=15.0,
        )
        response.raise_for_status()
        console.print("[green]✓ Rejected DataPoint[/green]")
    except Exception as exc:
        console.print(f"[red]Failed to reject DataPoint: {exc}[/red]")
        sys.exit(1)


@pending_datapoints.command(name="approve-all")
@click.option("--profile-id", help="Approve pending items for a specific profile.")
@click.option(
    "--latest",
    is_flag=True,
    help="Approve pending items for the latest profile on the default connection.",
)
def approve_all_pending(profile_id: str | None, latest: bool):
    """Bulk-approve pending DataPoints."""
    if profile_id and latest:
        raise click.ClickException("Use either --profile-id or --latest, not both.")
    try:
        if latest and not profile_id:
            connection_id = _resolve_default_connection_id()
            profile_id = _resolve_latest_profile_id(connection_id)

        if profile_id:
            pending = [
                item
                for item in _fetch_pending_datapoints()
                if str(item.get("profile_id")) == profile_id
            ]
            if not pending:
                console.print("[yellow]No pending DataPoints found for that profile.[/yellow]")
                return
            approved = 0
            for item in pending:
                pending_id = item.get("pending_id")
                if not pending_id:
                    continue
                response = httpx.post(
                    f"{API_BASE_URL}/api/v1/datapoints/pending/{pending_id}/approve",
                    timeout=15.0,
                )
                response.raise_for_status()
                approved += 1
            console.print(f"[green]✓ Approved {approved} DataPoints[/green]")
        else:
            response = httpx.post(
                f"{API_BASE_URL}/api/v1/datapoints/pending/bulk-approve", timeout=30.0
            )
            response.raise_for_status()
            data = response.json()
            pending = data.get("pending", [])
            console.print(f"[green]✓ Approved {len(pending)} DataPoints[/green]")
    except Exception as exc:
        console.print(f"[red]Failed to bulk-approve DataPoints: {exc}[/red]")
        sys.exit(1)


@datapoint.command(name="generate")
@click.option("--profile-id", help="Profiling profile UUID.")
@click.option("--connection-id", help="Connection UUID for latest profiling job lookup.")
@click.option(
    "--depth",
    type=click.Choice(["schema_only", "metrics_basic", "metrics_full"]),
    default="metrics_basic",
    show_default=True,
)
@click.option("--tables", multiple=True, help="Optional table names to include.")
@click.option("--batch-size", default=10, show_default=True, type=int)
@click.option("--max-tables", default=None, type=int)
@click.option("--max-metrics-per-table", default=3, show_default=True, type=int)
def generate_datapoints_cli(
    profile_id: str | None,
    connection_id: str | None,
    depth: str,
    tables: tuple[str, ...],
    batch_size: int,
    max_tables: int | None,
    max_metrics_per_table: int,
):
    """Start DataPoint generation for a profiling profile."""
    if max_tables is not None and max_tables <= 0:
        max_tables = None
    if not profile_id:
        try:
            resolved_connection_id = connection_id or _resolve_default_connection_id()
            profile_id = _resolve_latest_profile_id(resolved_connection_id)
            console.print(
                f"[dim]Using latest profile {profile_id} for connection {resolved_connection_id}.[/dim]"
            )
        except click.ClickException as exc:
            console.print(f"[red]{exc}[/red]")
            sys.exit(1)
    payload = {
        "profile_id": profile_id,
        "tables": list(tables) or None,
        "depth": depth,
        "batch_size": batch_size,
        "max_tables": max_tables,
        "max_metrics_per_table": max_metrics_per_table,
    }
    try:
        response = httpx.post(
            f"{API_BASE_URL}/api/v1/datapoints/generate",
            json=payload,
            timeout=30.0,
        )
        response.raise_for_status()
        data = response.json()
        console.print(f"[green]✓ Generation started[/green] job_id={data['job_id']}")
    except Exception as exc:
        console.print(f"[red]Failed to start generation: {exc}[/red]")
        sys.exit(1)


@datapoint.command(name="generate-status")
@click.argument("job_id")
def generation_status(job_id: str):
    """Check DataPoint generation job status."""
    try:
        response = httpx.get(
            f"{API_BASE_URL}/api/v1/datapoints/generate/jobs/{job_id}",
            timeout=15.0,
        )
        response.raise_for_status()
        data = response.json()
        console.print(json.dumps(data, indent=2))
    except Exception as exc:
        console.print(f"[red]Failed to fetch generation status: {exc}[/red]")
        sys.exit(1)


@datapoint.command(name="add")
@click.argument("datapoint_type", type=click.Choice(["schema", "business", "process", "query"]))
@click.argument("file", type=click.Path(exists=True))
@click.option(
    "--strict-contracts/--no-strict-contracts",
    default=True,
    show_default=True,
    help="Treat advisory contract gaps as errors.",
)
@click.option(
    "--fail-on-contract-warnings",
    is_flag=True,
    default=False,
    help="Fail when contract warnings are present.",
)
def add_datapoint(
    datapoint_type: str,
    file: str,
    strict_contracts: bool,
    fail_on_contract_warnings: bool,
):
    """Add a DataPoint from a JSON file.

    DATAPOINT_TYPE: schema, business, or process
    FILE: Path to JSON file
    """

    async def run_add():
        try:
            # Load DataPoint
            console.print(f"[cyan]Loading DataPoint from {file}...[/cyan]")
            loader = DataPointLoader()
            datapoint = loader.load_file(Path(file))

            # Validate type matches
            if datapoint.type.lower() != datapoint_type.lower():
                console.print(
                    f"[red]Error: DataPoint type '{datapoint.type}' "
                    f"doesn't match specified type '{datapoint_type}'[/red]"
                )
                sys.exit(1)

            reports = validate_contracts([datapoint], strict=strict_contracts)
            if not _print_contract_reports(
                reports,
                fail_on_warnings=fail_on_contract_warnings,
            ):
                sys.exit(1)

            # Add to vector store
            console.print("[cyan]Adding to vector store...[/cyan]")
            vector_store = VectorStore()
            await vector_store.initialize()
            await vector_store.add_datapoints([datapoint])

            # Add to knowledge graph
            console.print("[cyan]Adding to knowledge graph...[/cyan]")
            graph = KnowledgeGraph()
            graph.add_datapoint(datapoint)

            console.print(f"[green]✓ DataPoint '{datapoint.name}' added successfully[/green]")
            console.print(f"ID: {datapoint.datapoint_id}")
            console.print(f"Type: {datapoint.type}")

        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")
            sys.exit(1)

    asyncio.run(run_add())


@datapoint.command(name="sync")
@click.option(
    "--datapoints-dir",
    default="datapoints",
    help="Directory containing DataPoint JSON files",
)
@click.option(
    "--connection-id",
    default=None,
    help=(
        "Attach DataPoints to a specific database connection id "
        "(for per-database retrieval scoping)."
    ),
)
@click.option(
    "--global-scope",
    is_flag=True,
    default=False,
    help="Mark synced DataPoints as global/shared across all databases.",
)
@click.option(
    "--strict-contracts/--no-strict-contracts",
    default=True,
    show_default=True,
    help="Treat advisory contract gaps as errors.",
)
@click.option(
    "--fail-on-contract-warnings",
    is_flag=True,
    default=False,
    help="Fail when contract warnings are present.",
)
@click.option(
    "--conflict-mode",
    type=click.Choice(["error", "prefer_user", "prefer_managed", "prefer_latest"]),
    default="error",
    show_default=True,
    help="Conflict resolution policy for duplicate semantic definitions.",
)
def sync_datapoints(
    datapoints_dir: str,
    connection_id: str | None,
    global_scope: bool,
    strict_contracts: bool,
    fail_on_contract_warnings: bool,
    conflict_mode: str,
):
    """Rebuild vector store and knowledge graph from DataPoints directory."""

    async def run_sync():
        try:
            datapoints_path = Path(datapoints_dir)
            if not datapoints_path.exists():
                console.print(f"[red]Directory not found: {datapoints_dir}[/red]")
                sys.exit(1)

            # Load all DataPoints
            console.print(f"[cyan]Loading DataPoints from {datapoints_dir}...[/cyan]")
            loader = DataPointLoader()
            datapoints = loader.load_directory(datapoints_path)
            stats = loader.get_stats()

            if stats["failed_count"] > 0:
                console.print(
                    f"[yellow]⚠ {stats['failed_count']} DataPoints failed to load[/yellow]"
                )
                for error in stats["failed_files"]:
                    console.print(f"  [red]• {error['path']}: {error['error']}[/red]")

            if not datapoints:
                console.print("[yellow]No valid DataPoints found[/yellow]")
                return

            if connection_id and global_scope:
                raise click.ClickException(
                    "--connection-id and --global-scope are mutually exclusive."
                )
            if connection_id or global_scope:
                _apply_datapoint_scope(
                    datapoints,
                    connection_id=connection_id,
                    global_scope=global_scope,
                )
                if global_scope:
                    console.print("[dim]Applied scope: global[/dim]")
                else:
                    console.print(f"[dim]Applied scope: database ({connection_id})[/dim]")

            console.print(f"[green]✓ Loaded {len(datapoints)} DataPoints[/green]")
            reports = validate_contracts(datapoints, strict=strict_contracts)
            if not _print_contract_reports(
                reports,
                fail_on_warnings=fail_on_contract_warnings,
            ):
                sys.exit(1)

            try:
                resolved_conflict_mode = cast(ConflictMode, conflict_mode)
                resolution = resolve_datapoint_conflicts(
                    datapoints,
                    mode=resolved_conflict_mode,
                )
            except DataPointConflictError as exc:
                console.print(f"[red]Conflict validation failed: {exc}[/red]")
                sys.exit(1)

            datapoints = resolution.datapoints
            if resolution.conflicts and conflict_mode != "error":
                console.print(
                    f"[yellow]Resolved {len(resolution.conflicts)} semantic conflict(s) "
                    f"with --conflict-mode={conflict_mode}.[/yellow]"
                )
                for conflict in resolution.conflicts[:5]:
                    if not conflict.resolved_datapoint_id:
                        continue
                    console.print(
                        f"[dim]• {conflict.key} -> {conflict.resolved_datapoint_id}[/dim]"
                    )
                if len(resolution.conflicts) > 5:
                    console.print(
                        f"[dim]… +{len(resolution.conflicts) - 5} more conflict decisions[/dim]"
                    )

            # Rebuild vector store
            console.print("\n[cyan]Rebuilding vector store...[/cyan]")
            vector_store = VectorStore()
            await vector_store.initialize()

            # Clear existing
            await vector_store.clear()

            # Add all datapoints with progress
            progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            )

            with progress:
                task = progress.add_task("Adding to vector store...", total=len(datapoints))
                await vector_store.add_datapoints(datapoints)
                progress.update(task, completed=len(datapoints))

            # Rebuild knowledge graph
            console.print("[cyan]Rebuilding knowledge graph...[/cyan]")
            graph = KnowledgeGraph()

            with progress:
                task = progress.add_task("Adding to knowledge graph...", total=len(datapoints))
                for datapoint in datapoints:
                    graph.add_datapoint(datapoint)
                    progress.update(task, advance=1)

            # Display summary
            console.print()
            summary = Table(show_header=False, box=None)
            summary.add_row("[green]✓ Sync complete[/green]")
            summary.add_row("DataPoints loaded:", f"[cyan]{len(datapoints)}[/cyan]")
            summary.add_row("Vector store:", f"[cyan]{await vector_store.get_count()}[/cyan]")

            stats = graph.get_stats()
            summary.add_row(
                "Knowledge graph:",
                f"[cyan]{stats['total_nodes']} nodes, {stats['total_edges']} edges[/cyan]",
            )

            console.print(summary)

        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")
            import traceback

            console.print(f"[red]{traceback.format_exc()}[/red]")
            sys.exit(1)

    asyncio.run(run_sync())


@datapoint.command(name="lint")
@click.option(
    "--datapoints-dir",
    default="datapoints",
    help="Directory containing DataPoint JSON files",
)
@click.option(
    "--strict-contracts/--no-strict-contracts",
    default=True,
    show_default=True,
    help="Treat advisory contract gaps as errors.",
)
@click.option(
    "--fail-on-contract-warnings",
    is_flag=True,
    default=False,
    help="Fail when contract warnings are present.",
)
def lint_datapoints(
    datapoints_dir: str,
    strict_contracts: bool,
    fail_on_contract_warnings: bool,
):
    """Lint DataPoint contract quality without mutating vector store/graph."""

    async def run_lint():
        try:
            datapoints_path = Path(datapoints_dir)
            if not datapoints_path.exists():
                console.print(f"[red]Directory not found: {datapoints_dir}[/red]")
                sys.exit(1)

            loader = DataPointLoader()
            datapoints = loader.load_directory(datapoints_path)
            stats = loader.get_stats()

            if stats["failed_count"] > 0:
                console.print(
                    f"[yellow]⚠ {stats['failed_count']} DataPoints failed to load[/yellow]"
                )
                for error in stats["failed_files"]:
                    console.print(f"  [red]• {error['path']}: {error['error']}[/red]")
            if not datapoints:
                console.print("[yellow]No valid DataPoints found[/yellow]")
                sys.exit(1)

            reports = validate_contracts(datapoints, strict=strict_contracts)
            if not _print_contract_reports(
                reports,
                fail_on_warnings=fail_on_contract_warnings,
            ):
                sys.exit(1)
        except Exception as exc:
            console.print(f"[red]Error: {exc}[/red]")
            sys.exit(1)

    asyncio.run(run_lint())


def _print_contract_reports(
    reports: list[DataPointContractReport],
    *,
    fail_on_warnings: bool,
) -> bool:
    """Print contract validation output and return pass/fail."""
    total_errors = 0
    total_warnings = 0
    for report in reports:
        for issue in report.issues:
            label = issue.severity.upper()
            field_hint = f" ({issue.field})" if issue.field else ""
            message = f"[{label}] {report.datapoint_id}: {issue.code}{field_hint} - {issue.message}"
            if issue.severity == "error":
                total_errors += 1
                console.print(f"[red]{message}[/red]")
            else:
                total_warnings += 1
                console.print(f"[yellow]{message}[/yellow]")

    if total_errors == 0 and total_warnings == 0:
        console.print("[green]✓ Contract lint passed with no issues[/green]")
        return True

    summary = (
        "Contract lint summary: "
        f"errors={total_errors}, warnings={total_warnings}, datapoints={len(reports)}"
    )
    if total_errors > 0:
        console.print(f"[red]{summary}[/red]")
        return False
    if fail_on_warnings and total_warnings > 0:
        console.print(f"[red]{summary} (failing on warnings)[/red]")
        return False
    console.print(f"[yellow]{summary}[/yellow]")
    return True


# ============================================================================
# Entry Point
# ============================================================================


def main():
    """Main entry point for CLI."""
    cli()


if __name__ == "__main__":
    main()
