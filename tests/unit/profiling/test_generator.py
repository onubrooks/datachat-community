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
                distinct_count=100,
            ),
            ColumnProfile(
                name="total_amount",
                data_type="numeric",
                nullable=False,
                sample_values=["10.5", "20.0"],
                distinct_count=87,
                min_value="10.5",
                max_value="20.0",
            ),
            ColumnProfile(
                name="created_at",
                data_type="timestamp",
                nullable=False,
                sample_values=["2024-01-01"],
                distinct_count=100,
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
    assert datapoint["metadata"]["grain"] == "row-level"
    assert datapoint["metadata"]["exclusions"]
    assert datapoint["metadata"]["confidence_notes"]
    assert datapoint["metadata"]["profiled_columns"]
    order_id_column = next(col for col in datapoint["key_columns"] if col["name"] == "order_id")
    assert order_id_column["sample_values"] == ["1", "2"]
    assert order_id_column["distinct_count"] == 100


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
    assert metric["metadata"]["grain"] == "table-level"
    assert metric["metadata"]["exclusions"]
    assert metric["metadata"]["confidence_notes"]


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


@pytest.mark.asyncio
async def test_negative_row_count_is_sanitized_for_schema_datapoints():
    profile = _sample_profile()
    profile.tables[0].row_count = -1

    generator = DataPointGenerator(llm_provider=FakeLLM([]))
    generated = await generator.generate_from_profile(profile, depth="metrics_basic")

    assert generated.schema_datapoints
    datapoint = generated.schema_datapoints[0].datapoint
    assert datapoint.get("row_count") is None


def test_schema_prompt_includes_profile_stats():
    profile = _sample_profile()
    generator = DataPointGenerator(llm_provider=FakeLLM([]))

    prompt = generator._build_schema_prompt(profile.tables[0])

    assert "distinct_count" in prompt
    assert "min_value" in prompt
    assert "max_value" in prompt


def test_metric_prompt_includes_distinct_and_samples():
    profile = _sample_profile()
    table = profile.tables[0]
    numeric_columns = [column for column in table.columns if "numeric" in column.data_type]
    generator = DataPointGenerator(llm_provider=FakeLLM([]))

    prompt = generator._build_metric_prompt(table, numeric_columns)

    assert "distinct_count" in prompt
    assert "sample_values" in prompt


def test_derive_table_purpose_is_useful_for_non_finance_domain():
    table = TableProfile(
        schema="public",
        name="support_tickets",
        row_count=42,
        columns=[
            ColumnProfile(
                name="ticket_id",
                data_type="integer",
                nullable=False,
                sample_values=["1001", "1002"],
            ),
            ColumnProfile(
                name="ticket_status",
                data_type="text",
                nullable=False,
                sample_values=["open", "closed"],
            ),
        ],
        relationships=[],
        sample_size=2,
    )
    generator = DataPointGenerator(llm_provider=FakeLLM([]))

    purpose = generator._derive_table_purpose(table)

    assert "support cases" in purpose.lower()


@pytest.mark.asyncio
async def test_metrics_batch_prefers_schema_qualified_table_keys():
    profile = DatabaseProfile(
        profile_id=uuid4(),
        connection_id=uuid4(),
        tables=[
            TableProfile(
                schema="public",
                name="orders",
                row_count=100,
                columns=[
                    ColumnProfile(
                        name="amount",
                        data_type="numeric",
                        nullable=False,
                        sample_values=["10.5", "20.0"],
                    )
                ],
                relationships=[],
                sample_size=2,
            ),
            TableProfile(
                schema="analytics",
                name="orders",
                row_count=200,
                columns=[
                    ColumnProfile(
                        name="gross_amount",
                        data_type="numeric",
                        nullable=False,
                        sample_values=["50.0", "75.0"],
                    )
                ],
                relationships=[],
                sample_size=2,
            ),
        ],
        created_at=datetime.now(UTC),
    )
    llm = FakeLLM(
        [
            '{"business_purpose": "Orders", "columns": {"amount": "Amount"}}',
            '{"business_purpose": "Orders analytics", "columns": {"gross_amount": "Gross amount"}}',
            '{"public.orders": {"metrics": [{"name": "Public Orders Total", "calculation": "SUM(amount)", "aggregation": "SUM", "unit": "USD", "confidence": 0.7}]}, "analytics.orders": {"metrics": [{"name": "Analytics Orders Total", "calculation": "SUM(gross_amount)", "aggregation": "SUM", "unit": "USD", "confidence": 0.8}]}}',
        ]
    )

    generator = DataPointGenerator(llm_provider=llm)
    generated = await generator.generate_from_profile(profile, depth="metrics_full")

    calculations = {item.datapoint["name"]: item.datapoint["calculation"] for item in generated.business_datapoints}
    assert calculations["Public Orders Total"] == "SUM(amount)"
    assert calculations["Analytics Orders Total"] == "SUM(gross_amount)"


def test_numeric_type_detection_includes_real_double_and_number():
    generator = DataPointGenerator(llm_provider=FakeLLM([]))

    assert generator._is_numeric_type("double precision") is True
    assert generator._is_numeric_type("real") is True
    assert generator._is_numeric_type("number(18,2)") is True
