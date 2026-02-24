"""
Integration tests for system initialization workflow.
"""

import os

import pytest
from fastapi.testclient import TestClient

from backend.api.main import app


@pytest.mark.integration
def test_initialization_workflow_end_to_end():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL not set for integration test.")

    client = TestClient(app)

    response = client.get("/api/v1/system/status")
    assert response.status_code == 200

    init_response = client.post(
        "/api/v1/system/initialize",
        json={"database_url": database_url, "auto_profile": False},
    )
    assert init_response.status_code == 200

    status = init_response.json()
    assert "is_initialized" in status


@pytest.mark.integration
def test_initialize_rejects_invalid_database_url():
    client = TestClient(app)
    response = client.post(
        "/api/v1/system/initialize",
        json={"database_url": "not-a-url", "auto_profile": False},
    )
    assert response.status_code == 400
