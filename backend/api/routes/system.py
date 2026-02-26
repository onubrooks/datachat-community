"""
System Routes

Initialization status and guided setup endpoints.
"""

import logging
import os
import shutil
from pathlib import Path
from typing import Literal

from cryptography.fernet import Fernet
from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field

from backend.config import clear_settings_cache, get_settings
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
    DATABASE_CREDENTIALS_KEY,
    LLM_ANTHROPIC_API_KEY,
    LLM_ANTHROPIC_MODEL,
    LLM_ANTHROPIC_MODEL_MINI,
    LLM_DEFAULT_PROVIDER_KEY,
    LLM_GOOGLE_API_KEY,
    LLM_GOOGLE_MODEL,
    LLM_GOOGLE_MODEL_MINI,
    LLM_LOCAL_MODEL,
    LLM_OPENAI_API_KEY,
    LLM_OPENAI_MODEL,
    LLM_OPENAI_MODEL_MINI,
    LLM_TEMPERATURE,
    SYSTEM_DB_KEY,
    TARGET_DB_KEY,
    apply_config_defaults,
    clear_config,
    is_placeholder_database_url,
    load_config,
    set_value,
    set_values,
)

router = APIRouter()
logger = logging.getLogger(__name__)


class SystemResetResponse(SystemStatusResponse):
    message: str


class RuntimeSettingsResponse(BaseModel):
    target_database_url: str | None = None
    system_database_url: str | None = None
    llm_default_provider: str = "openai"
    llm_openai_model: str | None = None
    llm_openai_model_mini: str | None = None
    llm_anthropic_model: str | None = None
    llm_anthropic_model_mini: str | None = None
    llm_google_model: str | None = None
    llm_google_model_mini: str | None = None
    llm_local_model: str | None = None
    llm_temperature: str | None = None
    database_credentials_key_present: bool = False
    llm_openai_api_key_present: bool = False
    llm_anthropic_api_key_present: bool = False
    llm_google_api_key_present: bool = False
    database_credentials_key_preview: str | None = None
    llm_openai_api_key_preview: str | None = None
    llm_anthropic_api_key_preview: str | None = None
    llm_google_api_key_preview: str | None = None
    source: dict[str, str] = Field(default_factory=dict)
    runtime_valid: bool = True
    runtime_error: str | None = None


class RuntimeSettingsUpdateRequest(BaseModel):
    target_database_url: str | None = None
    system_database_url: str | None = None
    llm_default_provider: Literal["openai", "anthropic", "google", "local"] | None = None
    llm_openai_model: str | None = None
    llm_openai_model_mini: str | None = None
    llm_anthropic_model: str | None = None
    llm_anthropic_model_mini: str | None = None
    llm_google_model: str | None = None
    llm_google_model_mini: str | None = None
    llm_local_model: str | None = None
    llm_temperature: str | None = None
    database_credentials_key: str | None = None
    llm_openai_api_key: str | None = None
    llm_anthropic_api_key: str | None = None
    llm_google_api_key: str | None = None
    generate_database_credentials_key: bool = False


def _preview_secret(value: str | None) -> str | None:
    if not value:
        return None
    stripped = value.strip()
    if len(stripped) <= 8:
        return "*" * len(stripped)
    return f"{stripped[:4]}...{stripped[-4:]}"


def _resolve_runtime_setting(
    config: dict[str, str],
    *,
    env_var: str,
    config_key: str,
    default: str | None = None,
    ignore_placeholder_database_url: bool = False,
) -> tuple[str | None, str]:
    env_value = os.getenv(env_var)
    if ignore_placeholder_database_url and is_placeholder_database_url(env_value):
        env_value = None
    if env_value:
        return env_value, "env"
    config_value = config.get(config_key)
    if ignore_placeholder_database_url and is_placeholder_database_url(str(config_value)):
        config_value = None
    if config_value:
        return str(config_value), "config"
    return default, "default" if default is not None else "missing"


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


@router.get("/system/settings", response_model=RuntimeSettingsResponse)
async def system_settings() -> RuntimeSettingsResponse:
    apply_config_defaults()
    config = load_config()

    target_url, target_source = _resolve_runtime_setting(
        config,
        env_var="DATABASE_URL",
        config_key=TARGET_DB_KEY,
        ignore_placeholder_database_url=True,
    )
    system_url, system_source = _resolve_runtime_setting(
        config,
        env_var="SYSTEM_DATABASE_URL",
        config_key=SYSTEM_DB_KEY,
        ignore_placeholder_database_url=True,
    )
    llm_provider, provider_source = _resolve_runtime_setting(
        config,
        env_var="LLM_DEFAULT_PROVIDER",
        config_key=LLM_DEFAULT_PROVIDER_KEY,
        default="openai",
    )
    credentials_key, credentials_source = _resolve_runtime_setting(
        config,
        env_var="DATABASE_CREDENTIALS_KEY",
        config_key=DATABASE_CREDENTIALS_KEY,
    )
    openai_key, openai_source = _resolve_runtime_setting(
        config, env_var="LLM_OPENAI_API_KEY", config_key=LLM_OPENAI_API_KEY
    )
    anthropic_key, anthropic_source = _resolve_runtime_setting(
        config, env_var="LLM_ANTHROPIC_API_KEY", config_key=LLM_ANTHROPIC_API_KEY
    )
    google_key, google_source = _resolve_runtime_setting(
        config, env_var="LLM_GOOGLE_API_KEY", config_key=LLM_GOOGLE_API_KEY
    )
    openai_model, openai_model_source = _resolve_runtime_setting(
        config, env_var="LLM_OPENAI_MODEL", config_key=LLM_OPENAI_MODEL
    )
    openai_model_mini, openai_model_mini_source = _resolve_runtime_setting(
        config, env_var="LLM_OPENAI_MODEL_MINI", config_key=LLM_OPENAI_MODEL_MINI
    )
    anthropic_model, anthropic_model_source = _resolve_runtime_setting(
        config, env_var="LLM_ANTHROPIC_MODEL", config_key=LLM_ANTHROPIC_MODEL
    )
    anthropic_model_mini, anthropic_model_mini_source = _resolve_runtime_setting(
        config,
        env_var="LLM_ANTHROPIC_MODEL_MINI",
        config_key=LLM_ANTHROPIC_MODEL_MINI,
    )
    google_model, google_model_source = _resolve_runtime_setting(
        config, env_var="LLM_GOOGLE_MODEL", config_key=LLM_GOOGLE_MODEL
    )
    google_model_mini, google_model_mini_source = _resolve_runtime_setting(
        config, env_var="LLM_GOOGLE_MODEL_MINI", config_key=LLM_GOOGLE_MODEL_MINI
    )
    local_model, local_model_source = _resolve_runtime_setting(
        config, env_var="LLM_LOCAL_MODEL", config_key=LLM_LOCAL_MODEL
    )
    llm_temperature, llm_temperature_source = _resolve_runtime_setting(
        config, env_var="LLM_TEMPERATURE", config_key=LLM_TEMPERATURE
    )

    runtime_valid = True
    runtime_error = None
    try:
        clear_settings_cache()
        get_settings()
    except Exception as exc:
        runtime_valid = False
        runtime_error = str(exc)
    finally:
        clear_settings_cache()

    return RuntimeSettingsResponse(
        target_database_url=target_url,
        system_database_url=system_url,
        llm_default_provider=(llm_provider or "openai"),
        llm_openai_model=openai_model,
        llm_openai_model_mini=openai_model_mini,
        llm_anthropic_model=anthropic_model,
        llm_anthropic_model_mini=anthropic_model_mini,
        llm_google_model=google_model,
        llm_google_model_mini=google_model_mini,
        llm_local_model=local_model,
        llm_temperature=llm_temperature,
        database_credentials_key_present=bool(credentials_key),
        llm_openai_api_key_present=bool(openai_key),
        llm_anthropic_api_key_present=bool(anthropic_key),
        llm_google_api_key_present=bool(google_key),
        database_credentials_key_preview=_preview_secret(credentials_key),
        llm_openai_api_key_preview=_preview_secret(openai_key),
        llm_anthropic_api_key_preview=_preview_secret(anthropic_key),
        llm_google_api_key_preview=_preview_secret(google_key),
        source={
            "target_database_url": target_source,
            "system_database_url": system_source,
            "llm_default_provider": provider_source,
            "database_credentials_key": credentials_source,
            "llm_openai_api_key": openai_source,
            "llm_anthropic_api_key": anthropic_source,
            "llm_google_api_key": google_source,
            "llm_openai_model": openai_model_source,
            "llm_openai_model_mini": openai_model_mini_source,
            "llm_anthropic_model": anthropic_model_source,
            "llm_anthropic_model_mini": anthropic_model_mini_source,
            "llm_google_model": google_model_source,
            "llm_google_model_mini": google_model_mini_source,
            "llm_local_model": local_model_source,
            "llm_temperature": llm_temperature_source,
        },
        runtime_valid=runtime_valid,
        runtime_error=runtime_error,
    )


@router.put("/system/settings", response_model=RuntimeSettingsResponse)
async def update_system_settings(payload: RuntimeSettingsUpdateRequest) -> RuntimeSettingsResponse:
    updates: dict[str, str | None] = {}
    if payload.target_database_url is not None:
        target_url = payload.target_database_url.strip()
        updates[TARGET_DB_KEY] = None if is_placeholder_database_url(target_url) else target_url
    if payload.system_database_url is not None:
        system_url = payload.system_database_url.strip()
        updates[SYSTEM_DB_KEY] = None if is_placeholder_database_url(system_url) else system_url
    if payload.llm_default_provider is not None:
        updates[LLM_DEFAULT_PROVIDER_KEY] = payload.llm_default_provider.strip().lower()
    if payload.llm_openai_model is not None:
        updates[LLM_OPENAI_MODEL] = payload.llm_openai_model.strip()
    if payload.llm_openai_model_mini is not None:
        updates[LLM_OPENAI_MODEL_MINI] = payload.llm_openai_model_mini.strip()
    if payload.llm_anthropic_model is not None:
        updates[LLM_ANTHROPIC_MODEL] = payload.llm_anthropic_model.strip()
    if payload.llm_anthropic_model_mini is not None:
        updates[LLM_ANTHROPIC_MODEL_MINI] = payload.llm_anthropic_model_mini.strip()
    if payload.llm_google_model is not None:
        updates[LLM_GOOGLE_MODEL] = payload.llm_google_model.strip()
    if payload.llm_google_model_mini is not None:
        updates[LLM_GOOGLE_MODEL_MINI] = payload.llm_google_model_mini.strip()
    if payload.llm_local_model is not None:
        updates[LLM_LOCAL_MODEL] = payload.llm_local_model.strip()
    if payload.llm_temperature is not None:
        updates[LLM_TEMPERATURE] = payload.llm_temperature.strip()
    if payload.database_credentials_key is not None:
        updates[DATABASE_CREDENTIALS_KEY] = payload.database_credentials_key.strip()
    if payload.llm_openai_api_key is not None:
        updates[LLM_OPENAI_API_KEY] = payload.llm_openai_api_key.strip()
    if payload.llm_anthropic_api_key is not None:
        updates[LLM_ANTHROPIC_API_KEY] = payload.llm_anthropic_api_key.strip()
    if payload.llm_google_api_key is not None:
        updates[LLM_GOOGLE_API_KEY] = payload.llm_google_api_key.strip()

    if payload.generate_database_credentials_key:
        updates[DATABASE_CREDENTIALS_KEY] = Fernet.generate_key().decode()

    if updates:
        set_values(updates)
    apply_config_defaults()
    clear_settings_cache()
    return await system_settings()


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
    sanitized_database_url = payload.database_url
    sanitized_system_database_url = payload.system_database_url
    if is_placeholder_database_url(sanitized_database_url):
        sanitized_database_url = None
    if is_placeholder_database_url(sanitized_system_database_url):
        sanitized_system_database_url = None

    try:
        status_state, message = await initializer.initialize(
            database_url=sanitized_database_url,
            auto_profile=payload.auto_profile,
            system_database_url=sanitized_system_database_url,
        )
        if sanitized_database_url is not None:
            set_value(TARGET_DB_KEY, sanitized_database_url)
        if sanitized_system_database_url is not None:
            set_value(SYSTEM_DB_KEY, sanitized_system_database_url)
        apply_config_defaults()
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

    # Clear active runtime state so initialization status reflects the reset.
    active_connector = app_state.get("connector")
    if active_connector is not None:
        try:
            await active_connector.close()
        except Exception:
            pass
    app_state["connector"] = None
    app_state["pipeline"] = None

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
