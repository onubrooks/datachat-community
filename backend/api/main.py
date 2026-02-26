"""
FastAPI Application

Main FastAPI application for DataChat with:
- Lifespan management for resource initialization/cleanup
- CORS middleware for frontend integration
- Global exception handlers for agent errors
- Health and chat endpoints

Usage:
    uvicorn backend.api.main:app --reload --port 8000
"""

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from backend.agents.base import AgentError
from backend.api import websocket
from backend.api.routes import (
    chat,
    conversations,
    databases,
    datapoints,
    feedback,
    health,
    profiling,
    system,
    tools,
)
from backend.config import get_settings
from backend.connectors.base import BaseConnector, QueryError
from backend.connectors.base import ConnectionError as ConnectorConnectionError
from backend.connectors.factory import create_connector
from backend.knowledge.bootstrap import bootstrap_knowledge_graph_from_datapoints
from backend.knowledge.graph import KnowledgeGraph
from backend.knowledge.retriever import Retriever
from backend.knowledge.vectors import VectorStore
from backend.pipeline.orchestrator import DataChatPipeline

logger = logging.getLogger(__name__)

# Global state for pipeline and components
app_state = {
    "pipeline": None,
    "vector_store": None,
    "knowledge_graph": None,
    "connector": None,
    "database_manager": None,
    "profiling_store": None,
    "feedback_store": None,
    "conversation_store": None,
    "sync_orchestrator": None,
    "datapoint_watcher": None,
}


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Lifespan context manager for startup and shutdown.

    Initializes:
    - Vector store (Chroma)
    - Knowledge graph (NetworkX)
    - Database connector (PostgreSQL)
    - Pipeline orchestrator
    """
    from backend.settings_store import apply_config_defaults

    apply_config_defaults()
    config = get_settings()
    logger.info("Starting DataChat API server...")

    try:
        # Initialize vector store
        logger.info("Initializing vector store...")
        vector_store = VectorStore()
        await vector_store.initialize()
        app_state["vector_store"] = vector_store

        # Initialize knowledge graph
        logger.info("Initializing knowledge graph...")
        knowledge_graph = KnowledgeGraph()
        bootstrap_knowledge_graph_from_datapoints(knowledge_graph, datapoints_dir="datapoints")
        app_state["knowledge_graph"] = knowledge_graph

        # Initialize retriever
        logger.info("Initializing retriever...")
        retriever = Retriever(
            vector_store=vector_store,
            knowledge_graph=knowledge_graph,
        )

        # Initialize database connector
        logger.info("Initializing database connector...")
        if config.database.url:
            db_url_str = str(config.database.url)
            connector = create_connector(
                database_url=db_url_str,
                pool_size=config.database.pool_size,
            )
            try:
                await connector.connect()
                app_state["connector"] = connector
            except ConnectorConnectionError as e:
                logger.warning(
                    "Target database connection unavailable at startup: %s. "
                    "API continues in setup mode.",
                    e,
                )
                app_state["connector"] = None
        else:
            logger.warning("DATABASE_URL not set; target database connector not initialized.")
            app_state["connector"] = None

        # Initialize database connection registry
        logger.info("Initializing database registry...")
        try:
            from backend.database.manager import DatabaseConnectionManager

            if config.system_database.url:
                database_manager = DatabaseConnectionManager()
                await database_manager.initialize()
                app_state["database_manager"] = database_manager
            else:
                logger.warning("SYSTEM_DATABASE_URL not set; database registry disabled.")
                app_state["database_manager"] = None
        except Exception as e:
            logger.warning(f"Database registry unavailable: {e}")
            app_state["database_manager"] = None

        # Initialize profiling store
        logger.info("Initializing profiling store...")
        try:
            from backend.profiling.store import ProfilingStore

            if config.system_database.url:
                profiling_store = ProfilingStore()
                await profiling_store.initialize()
                app_state["profiling_store"] = profiling_store
            else:
                logger.warning("SYSTEM_DATABASE_URL not set; profiling store disabled.")
                app_state["profiling_store"] = None
        except Exception as e:
            logger.warning(f"Profiling store unavailable: {e}")
            app_state["profiling_store"] = None

        # Initialize feedback store
        logger.info("Initializing feedback store...")
        try:
            from backend.feedback.store import FeedbackStore

            if config.system_database.url:
                feedback_store = FeedbackStore()
                await feedback_store.initialize()
                app_state["feedback_store"] = feedback_store
            else:
                logger.warning("SYSTEM_DATABASE_URL not set; feedback store disabled.")
                app_state["feedback_store"] = None
        except Exception as e:
            logger.warning(f"Feedback store unavailable: {e}")
            app_state["feedback_store"] = None

        # Initialize conversation store
        logger.info("Initializing conversation store...")
        try:
            from backend.conversations.store import ConversationStore

            if config.system_database.url:
                conversation_store = ConversationStore()
                await conversation_store.initialize()
                app_state["conversation_store"] = conversation_store
            else:
                logger.warning("SYSTEM_DATABASE_URL not set; conversation store disabled.")
                app_state["conversation_store"] = None
        except Exception as e:
            logger.warning(f"Conversation store unavailable: {e}")
            app_state["conversation_store"] = None

        # Initialize pipeline
        logger.info("Initializing pipeline orchestrator...")
        if app_state["connector"] is not None:
            pipeline = DataChatPipeline(
                retriever=retriever,
                connector=app_state["connector"],
                max_retries=3,
            )
            app_state["pipeline"] = pipeline
        else:
            logger.warning("Pipeline not initialized; target database is missing.")
            app_state["pipeline"] = None

        # Initialize sync orchestrator and watcher
        logger.info("Initializing sync orchestrator...")
        try:
            from backend.sync.orchestrator import SyncOrchestrator
            from backend.sync.watcher import DataPointWatcher

            loop = asyncio.get_running_loop()
            sync_orchestrator = SyncOrchestrator(
                vector_store=vector_store,
                knowledge_graph=knowledge_graph,
                loop=loop,
            )
            app_state["sync_orchestrator"] = sync_orchestrator

            if config.sync_watcher_enabled:
                watcher = DataPointWatcher(
                    datapoints_dir="datapoints",
                    on_change=sync_orchestrator.enqueue_sync_all,
                    debounce_seconds=5.0,
                )
                watcher.start()
                app_state["datapoint_watcher"] = watcher
            else:
                logger.info("Datapoint watcher disabled via SYNC_WATCHER_ENABLED")
                app_state["datapoint_watcher"] = None
        except Exception as e:
            logger.warning(f"Sync watcher unavailable: {e}")
            app_state["sync_orchestrator"] = None
            app_state["datapoint_watcher"] = None

        logger.info("DataChat API server started successfully")

        yield  # Application runs here

    finally:
        # Cleanup on shutdown
        logger.info("Shutting down DataChat API server...")

        if app_state["connector"]:
            try:
                await app_state["connector"].close()
                logger.info("Database connector closed")
            except Exception as e:
                logger.error(f"Error closing connector: {e}")

        if app_state["database_manager"]:
            try:
                await app_state["database_manager"].close()
                logger.info("Database registry closed")
            except Exception as e:
                logger.error(f"Error closing database registry: {e}")

        if app_state["profiling_store"]:
            try:
                await app_state["profiling_store"].close()
                logger.info("Profiling store closed")
            except Exception as e:
                logger.error(f"Error closing profiling store: {e}")

        if app_state["feedback_store"]:
            try:
                await app_state["feedback_store"].close()
                logger.info("Feedback store closed")
            except Exception as e:
                logger.error(f"Error closing feedback store: {e}")

        if app_state["conversation_store"]:
            try:
                await app_state["conversation_store"].close()
                logger.info("Conversation store closed")
            except Exception as e:
                logger.error(f"Error closing conversation store: {e}")

        if app_state["datapoint_watcher"]:
            try:
                app_state["datapoint_watcher"].stop()
                logger.info("Datapoint watcher stopped")
            except Exception as e:
                logger.error(f"Error stopping datapoint watcher: {e}")

        logger.info("DataChat API server shut down complete")


# Create FastAPI app
app = FastAPI(
    title="DataChat API",
    description="AI-powered natural language interface for data warehouses",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS middleware for frontend
config = get_settings()
cors_origins_env = os.getenv("CORS_ORIGINS", "")
cors_origins = (
    [origin.strip() for origin in cors_origins_env.split(",") if origin.strip()]
    if cors_origins_env
    else ["http://localhost:3000", "http://localhost:3001"]
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Exception handlers
@app.exception_handler(AgentError)
async def agent_error_handler(request: Request, exc: AgentError) -> JSONResponse:
    """Handle agent errors with context."""
    logger.error(
        f"Agent error: {exc}",
        extra={
            "agent": exc.agent if hasattr(exc, "agent") else "unknown",
            "recoverable": exc.recoverable if hasattr(exc, "recoverable") else False,
        },
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "agent_error",
            "message": str(exc),
            "agent": exc.agent if hasattr(exc, "agent") else "unknown",
            "recoverable": exc.recoverable if hasattr(exc, "recoverable") else False,
        },
    )


@app.exception_handler(ConnectorConnectionError)
async def connection_error_handler(request: Request, exc: ConnectorConnectionError) -> JSONResponse:
    """Handle database connection errors."""
    logger.error(f"Database connection error: {exc}")
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "error": "connection_error",
            "message": "Database connection failed. Please try again later.",
        },
    )


@app.exception_handler(QueryError)
async def query_error_handler(request: Request, exc: QueryError) -> JSONResponse:
    """Handle query execution errors."""
    logger.error(f"Query execution error: {exc}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "query_error",
            "message": "Failed to execute query. Please check your request.",
        },
    )


# Include routers
app.include_router(health.router, prefix="/api/v1", tags=["health"])
app.include_router(chat.router, prefix="/api/v1", tags=["chat"])
app.include_router(system.router, prefix="/api/v1", tags=["system"])
app.include_router(databases.router, prefix="/api/v1", tags=["databases"])
app.include_router(profiling.router, prefix="/api/v1", tags=["profiling"])
app.include_router(datapoints.router, prefix="/api/v1", tags=["datapoints"])
app.include_router(feedback.router, prefix="/api/v1", tags=["feedback"])
app.include_router(conversations.router, prefix="/api/v1", tags=["conversations"])
app.include_router(tools.router, prefix="/api/v1", tags=["tools"])
app.include_router(websocket.router, tags=["websocket"])


# Root endpoint
@app.get("/")
async def root() -> dict[str, str]:
    """Root endpoint with API information."""
    return {
        "name": "DataChat API",
        "version": "0.1.0",
        "description": "Natural language interface for data warehouses",
        "docs": "/docs",
    }


def get_pipeline() -> DataChatPipeline:
    """Get the initialized pipeline instance."""
    if app_state["pipeline"] is None:
        raise RuntimeError("Pipeline not initialized")
    return app_state["pipeline"]


def get_connector() -> BaseConnector:
    """Get the initialized database connector."""
    if app_state["connector"] is None:
        raise RuntimeError("Connector not initialized")
    return app_state["connector"]
