"""Integration tests for auto-profiling workflow."""

from __future__ import annotations

import os
import time

import pytest
from fastapi.testclient import TestClient

from backend.api.main import app


@pytest.mark.integration
def test_profiling_job_end_to_end():
    database_url = os.getenv("DATABASE_URL")
    credentials_key = os.getenv("DATABASE_CREDENTIALS_KEY")
    if not database_url or not credentials_key:
        pytest.skip("DATABASE_URL or DATABASE_CREDENTIALS_KEY not set.")

    with TestClient(app) as client:
        create = client.post(
            "/api/v1/databases",
            json={
                "name": "Integration DB",
                "database_url": database_url,
                "database_type": "postgresql",
                "tags": ["integration"],
                "description": "Integration test",
                "is_default": True,
            },
        )
        assert create.status_code in {200, 201}
        connection_id = create.json()["connection_id"]

        job_response = client.post(
            f"/api/v1/databases/{connection_id}/profile",
            json={"sample_size": 10},
        )
        assert job_response.status_code == 202
        job_id = job_response.json()["job_id"]

        status_payload = None
        for _ in range(20):
            status_response = client.get(f"/api/v1/profiling/jobs/{job_id}")
            assert status_response.status_code == 200
            status_payload = status_response.json()
            if status_payload["status"] in {"completed", "failed"}:
                break
            time.sleep(0.5)

        assert status_payload
        assert status_payload["status"] == "completed"
        assert status_payload["profile_id"]


@pytest.mark.integration
def test_generate_and_approve_datapoints():
    database_url = os.getenv("DATABASE_URL")
    credentials_key = os.getenv("DATABASE_CREDENTIALS_KEY")
    llm_key = os.getenv("LLM_OPENAI_API_KEY")
    if not database_url or not credentials_key or not llm_key:
        pytest.skip("DATABASE_URL, DATABASE_CREDENTIALS_KEY, or LLM_OPENAI_API_KEY missing.")

    with TestClient(app) as client:
        create = client.post(
            "/api/v1/databases",
            json={
                "name": "Integration DB",
                "database_url": database_url,
                "database_type": "postgresql",
                "tags": ["integration"],
                "description": "Integration test",
                "is_default": True,
            },
        )
        assert create.status_code in {200, 201}
        connection_id = create.json()["connection_id"]

        job_response = client.post(
            f"/api/v1/databases/{connection_id}/profile",
            json={"sample_size": 5},
        )
        assert job_response.status_code == 202
        job_id = job_response.json()["job_id"]

        status_payload = None
        for _ in range(20):
            status_response = client.get(f"/api/v1/profiling/jobs/{job_id}")
            status_payload = status_response.json()
            if status_payload["status"] in {"completed", "failed"}:
                break
            time.sleep(0.5)

        assert status_payload
        assert status_payload["status"] == "completed"

        generate = client.post(
            "/api/v1/datapoints/generate",
            json={"profile_id": status_payload["profile_id"]},
        )
        assert generate.status_code == 200
        pending = generate.json()["pending"]
        assert pending

        approve = client.post(f"/api/v1/datapoints/pending/{pending[0]['pending_id']}/approve")
        assert approve.status_code == 200
