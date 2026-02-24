"""Unit tests for DataPointGenerator."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from backend.llm.models import LLMResponse
from backend.profiling.generator import DataPointGenerator
from backend.profiling.models import ColumnProfile, DatabaseProfile, TableProfile


class FakeLLM:
    def __init__(self, responses: list[str]):
        self._responses = responses
        self._calls = 0

    async def generate(self, _request):
        content = self._responses[self._calls]
        self._calls += 1
        return LLMResponse(
            content=content,
            model="mock",
            provider="mock",
            usage={"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            finish_reason="stop",
        )


def _sample_profile():
    table = TableProfile(
        schema="public",
        name="orders",
        row_count=100,
        columns=[
            ColumnProfile(
                name="order_id",
                data_type="integer",
                nullable=False,
                sample_values=["1", "2"],
            ),
            ColumnProfile(
                name="total_amount",
                data_type="numeric",
                nullable=False,
                sample_values=["10.5", "20.0"],
            ),
            ColumnProfile(
                name="created_at",
                data_type="timestamp",
                nullable=False,
                sample_values=["2024-01-01"],
            ),
        ],
        relationships=[],
        sample_size=2,
    )
    return DatabaseProfile(
        profile_id=uuid4(),
        connection_id=uuid4(),
        tables=[table],
        created_at=datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_generates_schema_datapoints_from_profile():
    profile = _sample_profile()
    llm = FakeLLM(
        [
            '{"business_purpose": "Track customer orders", "columns": {"order_id": "Order id", "total_amount": "Total", "created_at": "Date"}, "common_queries": ["SUM(total_amount)"], "gotchas": [], "freshness": "T-1", "confidence": 0.8}',
            '{"orders": {"metrics": []}}',
        ]
    )

    generator = DataPointGenerator(llm_provider=llm)
    generated = await generator.generate_from_profile(profile, depth="metrics_full")

    assert generated.schema_datapoints
    datapoint = generated.schema_datapoints[0].datapoint
    assert datapoint["type"] == "Schema"
    assert "Track customer orders" in datapoint["business_purpose"]
    assert datapoint["metadata"]["connection_id"] == str(profile.connection_id)
    assert datapoint["metadata"]["semantic_role"] == "fact_event"
    assert "display_hints" in datapoint["metadata"]


@pytest.mark.asyncio
async def test_suggests_metrics_from_numeric_columns():
    profile = _sample_profile()
    llm = FakeLLM(
        [
            '{"business_purpose": "Orders", "columns": {"order_id": "Order id", "total_amount": "Total", "created_at": "Date"}}',
            '{"orders": {"metrics": [{"name": "Total Order Value", "calculation": "SUM(total_amount)", "aggregation": "SUM", "unit": "USD", "confidence": 0.75}]}}',
        ]
    )

    generator = DataPointGenerator(llm_provider=llm)
    generated = await generator.generate_from_profile(profile, depth="metrics_full")

    assert generated.business_datapoints
    metric = generated.business_datapoints[0].datapoint
    assert metric["type"] == "Business"
    assert metric["aggregation"] == "SUM"
    assert metric["metadata"]["connection_id"] == str(profile.connection_id)
    assert metric["metadata"]["metric_kind"] == "additive_measure"
    assert metric["metadata"]["default_visualization"] in {"line", "bar", "kpi", "table"}


@pytest.mark.asyncio
async def test_identifies_time_series_patterns():
    profile = _sample_profile()
    llm = FakeLLM(
        [
            '{"business_purpose": "Orders", "columns": {"order_id": "Order id", "total_amount": "Total", "created_at": "Date"}}',
            '{"orders": {"metrics": []}}',
        ]
    )

    generator = DataPointGenerator(llm_provider=llm)
    generated = await generator.generate_from_profile(profile, depth="metrics_full")

    datapoint = generated.schema_datapoints[0].datapoint
    common_queries = datapoint.get("common_queries", [])
    assert any("DATE_TRUNC" in query for query in common_queries)


@pytest.mark.asyncio
async def test_returns_confidence_scores():
    profile = _sample_profile()
    llm = FakeLLM(
        [
            '{"business_purpose": "Orders", "columns": {"order_id": "Order id", "total_amount": "Total", "created_at": "Date"}, "confidence": 0.9}',
            '{"orders": {"metrics": []}}',
        ]
    )

    generator = DataPointGenerator(llm_provider=llm)
    generated = await generator.generate_from_profile(profile, depth="metrics_full")

    assert generated.schema_datapoints[0].confidence == 0.9


@pytest.mark.asyncio
async def test_skips_failed_tables_in_partial_profiles():
    profile = _sample_profile()
    failed_table = TableProfile(
        schema="public",
        name="broken_table",
        row_count=None,
        columns=[],
        relationships=[],
        sample_size=2,
        status="failed",
        error="profiling timed out",
    )
    profile.tables.append(failed_table)

    llm = FakeLLM(
        [
            '{"business_purpose": "Orders", "columns": {"order_id": "Order id", "total_amount": "Total", "created_at": "Date"}, "confidence": 0.9}',
            '{"orders": {"metrics": []}}',
        ]
    )
    generator = DataPointGenerator(llm_provider=llm)
    generated = await generator.generate_from_profile(profile, depth="metrics_full")

    schema_table_names = [item.datapoint.get("table_name") for item in generated.schema_datapoints]
    assert "public.orders" in schema_table_names
    assert "public.broken_table" not in schema_table_names
