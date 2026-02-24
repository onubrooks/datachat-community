"""
Health Check Routes

FastAPI endpoints for service health and readiness checks.
"""

import logging
from datetime import UTC, datetime

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from backend.models.api import HealthResponse, ReadinessResponse

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health", response_model=HealthResponse, status_code=status.HTTP_200_OK)
async def health() -> HealthResponse:
    """
    Basic liveness check.

    Returns 200 OK if the service is running.
    This endpoint should always succeed if the application is alive.

    Returns:
        HealthResponse with status and version
    """
    return HealthResponse(
        status="healthy",
        version="0.1.0",
        timestamp=datetime.now(UTC).isoformat(),
    )


@router.get("/ready", status_code=status.HTTP_200_OK)
async def readiness() -> JSONResponse:
    """
    Readiness check for service dependencies.

    Checks:
    - Database connection is active
    - Vector store is initialized
    - Pipeline is initialized

    Returns:
        200 OK if all checks pass
        503 Service Unavailable if any check fails
    """
    from backend.api.main import app_state

    checks: dict[str, bool] = {}
    all_ready = True

    # Check database connection
    try:
        if app_state["connector"] is not None:
            # Try a simple query to verify connection
            await app_state["connector"].execute("SELECT 1")
            checks["database"] = True
            logger.debug("Database check: OK")
        else:
            checks["database"] = False
            all_ready = False
            logger.warning("Database check: FAILED (connector not initialized)")
    except Exception as e:
        checks["database"] = False
        all_ready = False
        logger.warning(f"Database check: FAILED ({e})")

    # Check vector store
    try:
        if app_state["vector_store"] is not None:
            # Simple check that vector store is initialized
            count = await app_state["vector_store"].get_count()
            checks["vector_store"] = True
            logger.debug(f"Vector store check: OK ({count} datapoints)")
        else:
            checks["vector_store"] = False
            all_ready = False
            logger.warning("Vector store check: FAILED (not initialized)")
    except Exception as e:
        checks["vector_store"] = False
        all_ready = False
        logger.warning(f"Vector store check: FAILED ({e})")

    # Check pipeline
    checks["pipeline"] = app_state["pipeline"] is not None
    if not checks["pipeline"]:
        all_ready = False
        logger.warning("Pipeline check: FAILED (not initialized)")
    else:
        logger.debug("Pipeline check: OK")

    # Build response
    response_data = ReadinessResponse(
        status="ready" if all_ready else "not_ready",
        version="0.1.0",
        timestamp=datetime.now(UTC).isoformat(),
        checks=checks,
    )

    # Return 503 if not ready, 200 if ready
    status_code = status.HTTP_200_OK if all_ready else status.HTTP_503_SERVICE_UNAVAILABLE

    return JSONResponse(
        status_code=status_code,
        content=response_data.model_dump(),
    )
