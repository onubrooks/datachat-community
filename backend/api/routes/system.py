"""
System Routes

Initialization status and guided setup endpoints.
"""

import logging
import shutil
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request, status

from backend.config import get_settings
from backend.connectors.postgres import PostgresConnector
from backend.initialization.initializer import SystemInitializer
from backend.knowledge.vectors import VectorStore
from backend.models.api import (
    EntryEventRequest,
    EntryEventResponse,
    SystemInitializeRequest,
    SystemInitializeResponse,
    SystemStatusResponse,
)
from backend.settings_store import (
    SYSTEM_DB_KEY,
    TARGET_DB_KEY,
    apply_config_defaults,
    clear_config,
    set_value,
)

router = APIRouter()
logger = logging.getLogger(__name__)


class SystemResetResponse(SystemStatusResponse):
    message: str


@router.post("/system/entry-event", response_model=EntryEventResponse)
async def system_entry_event(payload: EntryEventRequest) -> EntryEventResponse:
    """Record lightweight setup/entry telemetry events."""
    logger.info(
        "entry_event",
        extra={
            "flow": payload.flow,
            "step": payload.step,
            "status": payload.status,
            "source": payload.source,
            "metadata": payload.metadata or {},
        },
    )
    return EntryEventResponse(ok=True)


@router.get("/system/status", response_model=SystemStatusResponse)
async def system_status(request: Request) -> SystemStatusResponse:
    """Return current initialization status."""
    from backend.api.main import app_state

    initializer = SystemInitializer(app_state)
    status_state = await initializer.status()
    return SystemStatusResponse(
        is_initialized=status_state.is_initialized,
        has_databases=status_state.has_databases,
        has_system_database=status_state.has_system_database,
        has_datapoints=status_state.has_datapoints,
        setup_required=[
            {
                "step": step.step,
                "title": step.title,
                "description": step.description,
                "action": step.action,
            }
            for step in status_state.setup_required
        ],
    )


@router.post("/system/initialize", response_model=SystemInitializeResponse)
async def system_initialize(
    request: Request, payload: SystemInitializeRequest
) -> SystemInitializeResponse:
    """Run guided initialization."""
    from backend.api.main import app_state

    initializer = SystemInitializer(app_state)

    try:
        if payload.database_url:
            set_value(TARGET_DB_KEY, payload.database_url)
        if payload.system_database_url:
            set_value(SYSTEM_DB_KEY, payload.system_database_url)
        apply_config_defaults()
        status_state, message = await initializer.initialize(
            database_url=payload.database_url,
            auto_profile=payload.auto_profile,
            system_database_url=payload.system_database_url,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc

    return SystemInitializeResponse(
        message=message,
        is_initialized=status_state.is_initialized,
        has_databases=status_state.has_databases,
        has_system_database=status_state.has_system_database,
        has_datapoints=status_state.has_datapoints,
        setup_required=[
            {
                "step": step.step,
                "title": step.title,
                "description": step.description,
                "action": step.action,
            }
            for step in status_state.setup_required
        ],
    )


@router.post("/system/reset", response_model=SystemResetResponse)
async def system_reset(request: Request) -> SystemResetResponse:
    """Reset system registry/profiling state and local caches."""
    from backend.api.main import app_state

    apply_config_defaults()
    settings = get_settings()

    if settings.system_database.url:
        from urllib.parse import urlparse

        parsed = urlparse(str(settings.system_database.url))
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
                "TRUNCATE database_connections, profiling_jobs, profiling_profiles, "
                "pending_datapoints, datapoint_generation_jobs, ui_feedback, ui_conversations"
            )
        finally:
            await connector.close()

    vector_store = app_state.get("vector_store")
    if vector_store is None:
        vector_store = VectorStore()
        await vector_store.initialize()
    await vector_store.clear()
    shutil.rmtree(settings.chroma.persist_dir, ignore_errors=True)

    managed_dir = Path("datapoints") / "managed"
    if managed_dir.exists():
        shutil.rmtree(managed_dir, ignore_errors=True)

    clear_config()

    apply_config_defaults()
    initializer = SystemInitializer(app_state)
    status_state = await initializer.status()
    return SystemResetResponse(
        message="System reset complete.",
        is_initialized=status_state.is_initialized,
        has_databases=status_state.has_databases,
        has_system_database=status_state.has_system_database,
        has_datapoints=status_state.has_datapoints,
        setup_required=[
            {
                "step": step.step,
                "title": step.title,
                "description": step.description,
                "action": step.action,
            }
            for step in status_state.setup_required
        ],
    )
