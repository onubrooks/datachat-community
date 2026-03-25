from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from backend.runs.store import RunStore


@pytest.mark.asyncio
async def test_get_run_includes_quality_findings_with_run_id() -> None:
    run_id = uuid4()
    store = RunStore(database_url="postgresql://postgres:@localhost:5432/datachat")

    class FakePool:
        async def fetchrow(self, query, *args):
            return {
                "run_id": run_id,
                "run_type": "chat",
                "status": "completed",
                "route": "sql",
                "connection_id": None,
                "conversation_id": None,
                "correlation_id": str(run_id),
                "failure_class": None,
                "confidence": 0.9,
                "warning_count": 0,
                "error_count": 0,
                "latency_ms": 120.0,
                "summary_json": {"query": "How many orders?"},
                "output_json": {"answer_source": "sql"},
                "started_at": datetime.now(UTC),
                "completed_at": datetime.now(UTC),
                "created_at": datetime.now(UTC),
                "updated_at": datetime.now(UTC),
            }

        async def fetch(self, query, *args):
            if "FROM ai_run_steps" in query:
                return []
            if "FROM ai_quality_findings" in query:
                return [
                    {
                        "run_id": run_id,
                        "finding_id": uuid4(),
                        "finding_type": "advisory",
                        "severity": "warning",
                        "category": "retrieval",
                        "code": "retrieval_miss",
                        "message": "No datapoints were retrieved for this run.",
                        "entity_type": None,
                        "entity_id": None,
                        "details_json": {"query": "How many orders?"},
                        "created_at": datetime.now(UTC),
                    }
                ]
            raise AssertionError(f"Unexpected query: {query}")

    store._pool = FakePool()
    payload = await store.get_run(run_id)

    assert payload is not None
    assert payload["run_id"] == str(run_id)
    assert payload["quality_findings"][0]["run_id"] == str(run_id)
    assert payload["quality_findings"][0]["code"] == "retrieval_miss"
