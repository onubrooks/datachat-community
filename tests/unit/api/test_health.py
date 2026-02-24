"""
Unit Tests for Health Check Endpoints

Tests the /api/v1/health and /api/v1/ready endpoints.
"""

from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from backend.api.main import app, app_state


class TestHealthEndpoint:
    """Test suite for health check endpoint."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        return TestClient(app)

    def test_health_returns_200(self, client):
        """Test that health endpoint returns 200 OK."""
        response = client.get("/api/v1/health")
        assert response.status_code == 200

    def test_health_returns_correct_structure(self, client):
        """Test that health endpoint returns correct response structure."""
        response = client.get("/api/v1/health")
        data = response.json()

        assert "status" in data
        assert "version" in data
        assert "timestamp" in data

        assert data["status"] == "healthy"
        assert data["version"] == "0.1.0"
        assert isinstance(data["timestamp"], str)

    def test_health_always_succeeds(self, client):
        """Test that health endpoint always returns success."""
        # Make multiple requests
        for _ in range(3):
            response = client.get("/api/v1/health")
            assert response.status_code == 200
            assert response.json()["status"] == "healthy"


class TestReadinessEndpoint:
    """Test suite for readiness check endpoint."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        return TestClient(app)

    def test_ready_returns_200_when_all_checks_pass(self, client):
        """Test that ready endpoint returns 200 when all dependencies are ready."""
        # Mock all components as initialized and healthy
        mock_connector = AsyncMock()
        mock_connector.execute = AsyncMock(return_value=None)
        mock_vs = AsyncMock()
        mock_vs.get_count = AsyncMock(return_value=10)
        mock_pipeline = AsyncMock()

        # Temporarily update app_state
        original_state = app_state.copy()
        app_state["connector"] = mock_connector
        app_state["vector_store"] = mock_vs
        app_state["pipeline"] = mock_pipeline

        try:
            response = client.get("/api/v1/ready")
            assert response.status_code == 200
        finally:
            # Restore original state
            app_state.update(original_state)

    def test_ready_returns_503_when_checks_fail(self, client):
        """Test that ready endpoint returns 503 when dependencies are not ready."""
        # Temporarily set all components to None
        original_state = app_state.copy()
        app_state["connector"] = None
        app_state["vector_store"] = None
        app_state["pipeline"] = None

        try:
            response = client.get("/api/v1/ready")
            assert response.status_code == 503
        finally:
            # Restore original state
            app_state.update(original_state)

    def test_ready_returns_correct_structure(self, client):
        """Test that ready endpoint returns correct response structure."""
        # Mock components
        mock_connector = AsyncMock()
        mock_connector.execute = AsyncMock(return_value=None)
        mock_vs = AsyncMock()
        mock_vs.get_count = AsyncMock(return_value=10)
        mock_pipeline = AsyncMock()

        # Temporarily update app_state
        original_state = app_state.copy()
        app_state["connector"] = mock_connector
        app_state["vector_store"] = mock_vs
        app_state["pipeline"] = mock_pipeline

        try:
            response = client.get("/api/v1/ready")
            data = response.json()

            assert "status" in data
            assert "version" in data
            assert "timestamp" in data
            assert "checks" in data

            assert data["version"] == "0.1.0"
            assert isinstance(data["checks"], dict)
        finally:
            # Restore original state
            app_state.update(original_state)

    def test_ready_checks_database_connection(self, client):
        """Test that ready endpoint checks database connection."""
        # Mock healthy components
        mock_connector = AsyncMock()
        mock_connector.execute = AsyncMock(return_value=None)
        mock_vs = AsyncMock()
        mock_vs.get_count = AsyncMock(return_value=10)
        mock_pipeline = AsyncMock()

        # Temporarily update app_state
        original_state = app_state.copy()
        app_state["connector"] = mock_connector
        app_state["vector_store"] = mock_vs
        app_state["pipeline"] = mock_pipeline

        try:
            response = client.get("/api/v1/ready")
            data = response.json()

            # Check that database check was performed
            assert "database" in data["checks"]
            assert data["checks"]["database"] is True
            assert mock_connector.execute.called
        finally:
            # Restore original state
            app_state.update(original_state)

    def test_ready_checks_vector_store(self, client):
        """Test that ready endpoint checks vector store."""
        # Mock healthy components
        mock_connector = AsyncMock()
        mock_connector.execute = AsyncMock(return_value=None)
        mock_vs = AsyncMock()
        mock_vs.get_count = AsyncMock(return_value=10)
        mock_pipeline = AsyncMock()

        # Temporarily update app_state
        original_state = app_state.copy()
        app_state["connector"] = mock_connector
        app_state["vector_store"] = mock_vs
        app_state["pipeline"] = mock_pipeline

        try:
            response = client.get("/api/v1/ready")
            data = response.json()

            # Check that vector store check was performed
            assert "vector_store" in data["checks"]
            assert data["checks"]["vector_store"] is True
            assert mock_vs.get_count.called
        finally:
            # Restore original state
            app_state.update(original_state)

    def test_ready_checks_pipeline(self, client):
        """Test that ready endpoint checks pipeline initialization."""
        # Mock healthy components
        mock_connector = AsyncMock()
        mock_connector.execute = AsyncMock(return_value=None)
        mock_vs = AsyncMock()
        mock_vs.get_count = AsyncMock(return_value=10)
        mock_pipeline = AsyncMock()

        # Temporarily update app_state
        original_state = app_state.copy()
        app_state["connector"] = mock_connector
        app_state["vector_store"] = mock_vs
        app_state["pipeline"] = mock_pipeline

        try:
            response = client.get("/api/v1/ready")
            data = response.json()

            # Check that pipeline check was performed
            assert "pipeline" in data["checks"]
            assert data["checks"]["pipeline"] is True
        finally:
            # Restore original state
            app_state.update(original_state)

    def test_ready_handles_database_connection_failure(self, client):
        """Test that ready endpoint handles database connection failures."""
        # Mock failing database
        mock_connector = AsyncMock()
        mock_connector.execute = AsyncMock(side_effect=Exception("Connection failed"))
        mock_vs = AsyncMock()
        mock_vs.get_count = AsyncMock(return_value=10)
        mock_pipeline = AsyncMock()

        # Temporarily update app_state
        original_state = app_state.copy()
        app_state["connector"] = mock_connector
        app_state["vector_store"] = mock_vs
        app_state["pipeline"] = mock_pipeline

        try:
            response = client.get("/api/v1/ready")
            data = response.json()

            # Check that database check failed
            assert response.status_code == 503
            assert data["status"] == "not_ready"
            assert data["checks"]["database"] is False
        finally:
            # Restore original state
            app_state.update(original_state)

    def test_ready_handles_vector_store_failure(self, client):
        """Test that ready endpoint handles vector store failures."""
        # Mock failing vector store
        mock_connector = AsyncMock()
        mock_connector.execute = AsyncMock(return_value=None)
        mock_vs = AsyncMock()
        mock_vs.get_count = AsyncMock(side_effect=Exception("Vector store error"))
        mock_pipeline = AsyncMock()

        # Temporarily update app_state
        original_state = app_state.copy()
        app_state["connector"] = mock_connector
        app_state["vector_store"] = mock_vs
        app_state["pipeline"] = mock_pipeline

        try:
            response = client.get("/api/v1/ready")
            data = response.json()

            # Check that vector store check failed
            assert response.status_code == 503
            assert data["status"] == "not_ready"
            assert data["checks"]["vector_store"] is False
        finally:
            # Restore original state
            app_state.update(original_state)

    def test_ready_handles_uninitialized_pipeline(self, client):
        """Test that ready endpoint handles uninitialized pipeline."""
        # Mock healthy database and vector store, but no pipeline
        mock_connector = AsyncMock()
        mock_connector.execute = AsyncMock(return_value=None)
        mock_vs = AsyncMock()
        mock_vs.get_count = AsyncMock(return_value=10)

        # Temporarily update app_state
        original_state = app_state.copy()
        app_state["connector"] = mock_connector
        app_state["vector_store"] = mock_vs
        app_state["pipeline"] = None  # Not initialized

        try:
            response = client.get("/api/v1/ready")
            data = response.json()

            # Check that pipeline check failed
            assert response.status_code == 503
            assert data["status"] == "not_ready"
            assert data["checks"]["pipeline"] is False
        finally:
            # Restore original state
            app_state.update(original_state)
