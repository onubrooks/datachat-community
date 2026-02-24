# Multi-stage build for DataChat backend
# Uses Python 3.11 slim with uv for fast dependency installation

# =============================================================================
# Stage 1: Builder - Install dependencies
# =============================================================================
FROM python:3.11-slim AS builder

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies to virtual environment from lockfile
RUN uv venv /app/.venv && \
    uv pip sync --python /app/.venv/bin/python --frozen --no-cache

# =============================================================================
# Stage 2: Runtime - Run application
# =============================================================================
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH" \
    ENVIRONMENT=production

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 datachat && \
    mkdir -p /app && \
    chown -R datachat:datachat /app

# Set working directory
WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder --chown=datachat:datachat /app/.venv /app/.venv

# Copy application code
COPY --chown=datachat:datachat . .

# Switch to non-root user
USER datachat

# Create directories for data persistence
RUN mkdir -p /app/chroma_data /app/logs

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/health || exit 1

# Run application with uvicorn
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
