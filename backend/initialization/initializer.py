"""
System Initialization

Provides initialization status checks and guided setup helpers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backend.connectors.base import ConnectionError
from backend.connectors.factory import create_connector, infer_database_type
from backend.knowledge.graph import KnowledgeGraph
from backend.knowledge.retriever import Retriever
from backend.knowledge.vectors import VectorStore
from backend.pipeline.orchestrator import DataChatPipeline


@dataclass(frozen=True)
class SetupStep:
    """Represents a required setup step."""

    step: str
    title: str
    description: str
    action: str


@dataclass(frozen=True)
class SystemStatus:
    """System initialization status."""

    is_initialized: bool
    has_databases: bool
    has_system_database: bool
    has_datapoints: bool
    setup_required: list[SetupStep]


class SystemInitializer:
    """Initialization workflow for DataChat."""

    def __init__(self, app_state: dict[str, Any]) -> None:
        self._app_state = app_state

    async def _check_database(self) -> bool:
        connector = self._app_state.get("connector")
        if connector is None:
            return False
        try:
            await connector.connect()
        except ConnectionError:
            return False
        return True

    async def _check_datapoints(self) -> bool:
        vector_store: VectorStore | None = self._app_state.get("vector_store")
        if vector_store is None:
            return False
        try:
            count = await vector_store.get_count()
        except Exception:
            return False
        return count > 0

    async def _check_system_database(self) -> bool:
        database_manager = self._app_state.get("database_manager")
        profiling_store = self._app_state.get("profiling_store")
        return database_manager is not None or profiling_store is not None

    async def status(self) -> SystemStatus:
        has_databases = await self._check_database()
        has_system_database = await self._check_system_database()
        has_datapoints = await self._check_datapoints()
        setup_required: list[SetupStep] = []

        if not has_databases:
            setup_required.append(
                SetupStep(
                    step="database_connection",
                    title="Connect a target database",
                    description="Provide the database you want DataChat to query.",
                    action="configure_database",
                )
            )

        if not has_system_database:
            setup_required.append(
                SetupStep(
                    step="system_database",
                    title="System database (optional)",
                    description=(
                        "Configure SYSTEM_DATABASE_URL to enable registry/profiling "
                        "and run the demo dataset."
                    ),
                    action="configure_system_database",
                )
            )

        if not has_datapoints:
            setup_required.append(
                SetupStep(
                    step="datapoints",
                    title="Load DataPoints (Recommended)",
                    description=(
                        "Optional but recommended: add DataPoints describing your schema "
                        "and business logic for higher answer quality "
                        "(or run datachat demo for sample data)."
                    ),
                    action="load_datapoints",
                )
            )

        return SystemStatus(
            # Credentials-only mode: once a target DB is connected, queries can run.
            # DataPoints are optional enrichment.
            is_initialized=has_databases,
            has_databases=has_databases,
            has_system_database=has_system_database,
            has_datapoints=has_datapoints,
            setup_required=setup_required,
        )

    async def initialize(
        self, database_url: str | None, auto_profile: bool, system_database_url: str | None
    ) -> tuple[SystemStatus, str]:
        message = "Initialization completed."

        if system_database_url:
            from backend.database.manager import DatabaseConnectionManager
            from backend.profiling.store import ProfilingStore

            database_manager = DatabaseConnectionManager(system_database_url=system_database_url)
            await database_manager.initialize()
            self._app_state["database_manager"] = database_manager

            profiling_store = ProfilingStore(database_url=system_database_url)
            await profiling_store.initialize()
            self._app_state["profiling_store"] = profiling_store

        if database_url:
            db_type = infer_database_type(database_url)
            connector = create_connector(
                database_url=database_url,
                database_type=db_type,
            )
            await connector.connect()
            self._app_state["connector"] = connector

            vector_store: VectorStore | None = self._app_state.get("vector_store")
            knowledge_graph: KnowledgeGraph | None = self._app_state.get("knowledge_graph")
            if vector_store and knowledge_graph:
                retriever = Retriever(vector_store=vector_store, knowledge_graph=knowledge_graph)
                self._app_state["pipeline"] = DataChatPipeline(
                    retriever=retriever,
                    connector=connector,
                    max_retries=3,
                )

            database_manager = self._app_state.get("database_manager")
            if database_manager:
                existing = None
                for connection in await database_manager.list_connections():
                    if connection.database_url.get_secret_value() == database_url:
                        existing = connection
                        break
                if existing is None:
                    await database_manager.add_connection(
                        name="Primary Database",
                        database_url=database_url,
                        database_type=db_type,
                        tags=["setup"],
                        description="Added during setup",
                        is_default=True,
                    )

        if auto_profile and database_url:
            profiling_store = self._app_state.get("profiling_store")
            database_manager = self._app_state.get("database_manager")
            if profiling_store and database_manager:
                try:
                    existing = None
                    for connection in await database_manager.list_connections():
                        if connection.database_url.get_secret_value() == database_url:
                            existing = connection
                            break

                    if existing is None:
                        existing = await database_manager.add_connection(
                            name="Primary Database",
                            database_url=database_url,
                            database_type=db_type,
                            tags=["auto-profiled"],
                            description="Auto-profiled during setup",
                            is_default=True,
                        )

                    from backend.profiling.models import ProfilingProgress
                    from backend.profiling.profiler import SchemaProfiler

                    job = await profiling_store.create_job(existing.connection_id)

                    async def run_profile_job() -> None:
                        profiler = SchemaProfiler(database_manager)

                        async def progress_callback(
                            total: int,
                            completed: int,
                            failed: int = 0,
                            skipped: int = 0,
                        ) -> None:
                            await profiling_store.update_job(
                                job.job_id,
                                progress=ProfilingProgress(
                                    total_tables=total,
                                    tables_completed=completed,
                                    tables_failed=failed,
                                    tables_skipped=skipped,
                                ),
                            )

                        try:
                            await profiling_store.update_job(job.job_id, status="running")
                            profile = await profiler.profile_database(
                                str(existing.connection_id),
                                progress_callback=progress_callback,
                                max_tables=50,
                                max_columns_per_table=100,
                                query_timeout_seconds=5,
                                per_table_timeout_seconds=20,
                                total_timeout_seconds=180,
                            )
                            await profiling_store.save_profile(profile)
                            await profiling_store.update_job(
                                job.job_id,
                                status="completed",
                                profile_id=profile.profile_id,
                                progress=ProfilingProgress(
                                    total_tables=profile.total_tables_discovered,
                                    tables_completed=profile.tables_profiled,
                                    tables_failed=profile.tables_failed,
                                    tables_skipped=profile.tables_skipped,
                                ),
                            )
                        except Exception as exc:
                            await profiling_store.update_job(
                                job.job_id, status="failed", error=str(exc)
                            )

                    import asyncio

                    asyncio.create_task(run_profile_job())
                    message = (
                        "Initialization completed. Auto-profiling started; "
                        f"job_id={job.job_id}."
                    )
                except Exception as exc:
                    message = (
                        "Initialization completed, but auto-profiling failed to start: "
                        f"{exc}."
                    )
            else:
                message = (
                    "Initialization completed, but auto-profiling is unavailable. "
                    "Set SYSTEM_DATABASE_URL and DATABASE_CREDENTIALS_KEY to enable profiling."
                )

        return await self.status(), message
