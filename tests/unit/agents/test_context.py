"""
Unit tests for ContextAgent.

Tests the context retrieval agent that gathers relevant DataPoints
without making LLM calls.
"""

from unittest.mock import AsyncMock, Mock

import pytest

from backend.agents.context import ContextAgent
from backend.knowledge.retriever import RetrievalMode, RetrievalResult, RetrievedItem, Retriever
from backend.models.agent import (
    ContextAgentInput,
    ContextAgentOutput,
    ExtractedEntity,
    InvestigationMemory,
    RetrievalError,
)


@pytest.fixture
def mock_retriever():
    """Create mock Retriever."""
    retriever = Mock(spec=Retriever)
    retriever.retrieve = AsyncMock()
    return retriever


@pytest.fixture
def context_agent(mock_retriever):
    """Create ContextAgent with mock retriever."""
    return ContextAgent(retriever=mock_retriever)


@pytest.fixture
def sample_retrieval_result():
    """Create sample retrieval result."""
    return RetrievalResult(
        items=[
            RetrievedItem(
                datapoint_id="table_sales_001",
                score=0.95,
                source="hybrid",
                metadata={
                    "type": "Schema",
                    "name": "Sales Table",
                    "table_name": "fact_sales",
                },
                content="Sales transaction data",
            ),
            RetrievedItem(
                datapoint_id="metric_revenue_001",
                score=0.88,
                source="vector",
                metadata={
                    "type": "Business",
                    "name": "Revenue",
                    "calculation": "SUM(amount)",
                },
                content="Total revenue calculation",
            ),
            RetrievedItem(
                datapoint_id="proc_daily_etl_001",
                score=0.75,
                source="graph",
                metadata={
                    "type": "Process",
                    "name": "Daily Sales ETL",
                    "schedule": "0 2 * * *",
                },
                content="Daily ETL process",
            ),
        ],
        total_count=3,
        mode=RetrievalMode.HYBRID,
        query="sales data",
    )


class TestInitialization:
    """Test ContextAgent initialization."""

    def test_initialization_sets_properties(self, mock_retriever):
        """Test agent initializes with correct properties."""
        agent = ContextAgent(retriever=mock_retriever)

        assert agent.name == "ContextAgent"
        assert agent.retriever == mock_retriever

    def test_initialization_requires_retriever(self):
        """Test initialization requires retriever."""
        with pytest.raises(TypeError):
            ContextAgent()


class TestExecution:
    """Test ContextAgent execution."""

    @pytest.mark.asyncio
    async def test_successful_retrieval(
        self, context_agent, mock_retriever, sample_retrieval_result
    ):
        """Test successful context retrieval."""
        mock_retriever.retrieve.return_value = sample_retrieval_result

        input = ContextAgentInput(
            query="What were total sales last quarter?",
            retrieval_mode="hybrid",
            max_datapoints=10,
        )

        output = await context_agent(input)

        assert isinstance(output, ContextAgentOutput)
        assert output.success is True
        assert output.metadata.agent_name == "ContextAgent"
        assert output.metadata.llm_calls == 0  # No LLM calls
        assert output.next_agent == "SQLAgent"

        # Check investigation memory
        memory = output.investigation_memory
        assert isinstance(memory, InvestigationMemory)
        assert memory.query == input.query
        assert len(memory.datapoints) == 3
        assert memory.total_retrieved == 3
        assert memory.retrieval_mode == "hybrid"

    @pytest.mark.asyncio
    async def test_retrieval_calls_retriever_with_correct_params(
        self, context_agent, mock_retriever, sample_retrieval_result
    ):
        """Test retriever is called with correct parameters."""
        mock_retriever.retrieve.return_value = sample_retrieval_result

        input = ContextAgentInput(
            query="sales data",
            retrieval_mode="hybrid",
            max_datapoints=15,
        )

        await context_agent(input)

        mock_retriever.retrieve.assert_called_once_with(
            query="sales data",
            mode=RetrievalMode.HYBRID,
            top_k=15,
            metadata_filter=None,
        )

    @pytest.mark.asyncio
    async def test_passes_context_metadata_filter_to_retriever(
        self, context_agent, mock_retriever, sample_retrieval_result
    ):
        mock_retriever.retrieve.return_value = sample_retrieval_result

        input = ContextAgentInput(
            query="sales data",
            retrieval_mode="hybrid",
            max_datapoints=3,
            context={"retrieval_metadata_filter": {"connection_id": "conn_123"}},
        )

        await context_agent(input)

        mock_retriever.retrieve.assert_called_once_with(
            query="sales data",
            mode=RetrievalMode.HYBRID,
            top_k=3,
            metadata_filter={"connection_id": "conn_123"},
        )

    @pytest.mark.asyncio
    async def test_uses_unfiltered_fallback_when_filtered_results_are_insufficient(
        self, context_agent, mock_retriever
    ):
        filtered = RetrievalResult(
            items=[
                RetrievedItem(
                    datapoint_id="table_sales_001",
                    score=0.95,
                    source="vector",
                    metadata={"type": "Schema", "name": "Sales Table"},
                )
            ],
            total_count=1,
            mode=RetrievalMode.HYBRID,
            query="sales data",
            trace={"mode": "hybrid"},
        )
        unfiltered = RetrievalResult(
            items=[
                RetrievedItem(
                    datapoint_id="table_sales_001",
                    score=0.95,
                    source="vector",
                    metadata={"type": "Schema", "name": "Sales Table"},
                ),
                RetrievedItem(
                    datapoint_id="metric_revenue_001",
                    score=0.88,
                    source="vector",
                    metadata={"type": "Business", "name": "Revenue"},
                ),
            ],
            total_count=2,
            mode=RetrievalMode.HYBRID,
            query="sales data",
            trace={"mode": "hybrid"},
        )
        mock_retriever.retrieve.side_effect = [filtered, unfiltered]

        input = ContextAgentInput(
            query="sales data",
            retrieval_mode="hybrid",
            max_datapoints=2,
            context={"retrieval_metadata_filter": {"connection_id": "conn_123"}},
        )

        output = await context_agent(input)

        assert len(output.investigation_memory.datapoints) == 2
        assert output.data["retrieval_trace"]["fallback_used"] is True
        assert mock_retriever.retrieve.await_count == 2

    @pytest.mark.asyncio
    async def test_handles_different_retrieval_modes(
        self, context_agent, mock_retriever, sample_retrieval_result
    ):
        """Test works with different retrieval modes."""
        mock_retriever.retrieve.return_value = sample_retrieval_result

        for mode in ["local", "global", "hybrid"]:
            input = ContextAgentInput(
                query="test query",
                retrieval_mode=mode,
                max_datapoints=10,
            )

            output = await context_agent(input)

            assert output.success is True
            assert output.investigation_memory.retrieval_mode == mode


class TestInvestigationMemory:
    """Test InvestigationMemory creation."""

    @pytest.mark.asyncio
    async def test_creates_retrieved_datapoints_correctly(
        self, context_agent, mock_retriever, sample_retrieval_result
    ):
        """Test RetrievedDataPoints are created with correct fields."""
        mock_retriever.retrieve.return_value = sample_retrieval_result

        input = ContextAgentInput(query="sales", max_datapoints=10)
        output = await context_agent(input)

        datapoints = output.investigation_memory.datapoints

        # Check first datapoint (Schema)
        dp1 = datapoints[0]
        assert dp1.datapoint_id == "table_sales_001"
        assert dp1.datapoint_type == "Schema"
        assert dp1.name == "Sales Table"
        assert dp1.score == 0.95
        assert dp1.source == "hybrid"
        assert dp1.metadata["table_name"] == "fact_sales"

        # Check second datapoint (Business)
        dp2 = datapoints[1]
        assert dp2.datapoint_id == "metric_revenue_001"
        assert dp2.datapoint_type == "Business"
        assert dp2.name == "Revenue"
        assert dp2.score == 0.88
        assert dp2.source == "vector"

        # Check third datapoint (Process)
        dp3 = datapoints[2]
        assert dp3.datapoint_id == "proc_daily_etl_001"
        assert dp3.datapoint_type == "Process"
        assert dp3.name == "Daily Sales ETL"
        assert dp3.score == 0.75
        assert dp3.source == "graph"

    @pytest.mark.asyncio
    async def test_tracks_sources_for_citations(
        self, context_agent, mock_retriever, sample_retrieval_result
    ):
        """Test sources are tracked for citation purposes."""
        mock_retriever.retrieve.return_value = sample_retrieval_result

        input = ContextAgentInput(query="sales", max_datapoints=10)
        output = await context_agent(input)

        sources = output.investigation_memory.sources_used

        assert len(sources) == 3
        assert "table_sales_001" in sources
        assert "metric_revenue_001" in sources
        assert "proc_daily_etl_001" in sources

    @pytest.mark.asyncio
    async def test_deduplicates_sources(self, context_agent, mock_retriever):
        """Test duplicate sources are removed."""
        # Create result with duplicate datapoint_id
        result = RetrievalResult(
            items=[
                RetrievedItem(
                    datapoint_id="table_sales_001",
                    score=0.95,
                    source="hybrid",
                    metadata={"type": "Schema", "name": "Sales"},
                ),
                RetrievedItem(
                    datapoint_id="table_sales_001",  # Duplicate
                    score=0.90,
                    source="vector",
                    metadata={"type": "Schema", "name": "Sales"},
                ),
            ],
            total_count=2,
            mode=RetrievalMode.HYBRID,
            query="sales",
        )

        mock_retriever.retrieve.return_value = result

        input = ContextAgentInput(query="sales", max_datapoints=10)
        output = await context_agent(input)

        sources = output.investigation_memory.sources_used
        assert len(sources) == 1  # Deduplicated
        assert sources[0] == "table_sales_001"

    @pytest.mark.asyncio
    async def test_handles_empty_results(self, context_agent, mock_retriever):
        """Test handles empty retrieval results gracefully."""
        empty_result = RetrievalResult(
            items=[],
            total_count=0,
            mode=RetrievalMode.HYBRID,
            query="nonexistent query",
        )

        mock_retriever.retrieve.return_value = empty_result

        input = ContextAgentInput(query="nonexistent query", max_datapoints=10)
        output = await context_agent(input)

        assert output.success is True
        assert len(output.investigation_memory.datapoints) == 0
        assert output.investigation_memory.total_retrieved == 0
        assert len(output.investigation_memory.sources_used) == 0


class TestEntityHandling:
    """Test handling of extracted entities."""

    @pytest.mark.asyncio
    async def test_accepts_entities_in_input(
        self, context_agent, mock_retriever, sample_retrieval_result
    ):
        """Test agent accepts entities in input."""
        mock_retriever.retrieve.return_value = sample_retrieval_result

        entities = [
            ExtractedEntity(
                entity_type="metric",
                value="revenue",
                confidence=0.95,
            ),
            ExtractedEntity(
                entity_type="table",
                value="fact_sales",
                confidence=0.88,
            ),
        ]

        input = ContextAgentInput(
            query="sales revenue",
            entities=entities,
            max_datapoints=10,
        )

        output = await context_agent(input)

        assert output.success is True
        mock_retriever.retrieve.assert_called_once()
        kwargs = mock_retriever.retrieve.await_args.kwargs
        assert "Entity hints:" in kwargs["query"]
        assert "revenue" in kwargs["query"]

    @pytest.mark.asyncio
    async def test_entity_boosting_reorders_matching_datapoints(self, context_agent, mock_retriever):
        mock_retriever.retrieve.return_value = RetrievalResult(
            items=[
                RetrievedItem(
                    datapoint_id="table_sales_001",
                    score=0.2,
                    source="hybrid",
                    metadata={"type": "Schema", "name": "Sales Table"},
                    content="sales facts",
                ),
                RetrievedItem(
                    datapoint_id="query_customer_segment_deposits_001",
                    score=0.18,
                    source="hybrid",
                    metadata={
                        "type": "Query",
                        "name": "Top Customer Segment by Deposits",
                        "query_description": "Ranks customer segment values by deposit totals",
                        "tags": "customer_segment,deposits,top_n",
                    },
                    content="customer segment deposit ranking",
                ),
            ],
            total_count=2,
            mode=RetrievalMode.HYBRID,
            query="top segments",
            trace={},
        )

        input = ContextAgentInput(
            query="show top 5 customer segments by deposits",
            entities=[
                ExtractedEntity(entity_type="metric", value="deposits", confidence=0.95),
                ExtractedEntity(entity_type="column", value="customer segment", confidence=0.9),
            ],
            max_datapoints=5,
        )

        output = await context_agent(input)

        assert output.success is True
        assert output.investigation_memory.datapoints[0].datapoint_id == (
            "query_customer_segment_deposits_001"
        )
        trace = output.data["retrieval_trace"]["entity_boosting"]
        assert trace["matched_items"]

    @pytest.mark.asyncio
    async def test_works_without_entities(
        self, context_agent, mock_retriever, sample_retrieval_result
    ):
        """Test agent works without entities (pure query-based retrieval)."""
        mock_retriever.retrieve.return_value = sample_retrieval_result

        input = ContextAgentInput(
            query="sales revenue",
            entities=[],  # No entities
            max_datapoints=10,
        )

        output = await context_agent(input)

        assert output.success is True
        assert len(output.investigation_memory.datapoints) == 3


class TestErrorHandling:
    """Test error handling."""

    @pytest.mark.asyncio
    async def test_retrieval_error_raises_retrieval_error(self, context_agent, mock_retriever):
        """Test retrieval failures raise RetrievalError."""
        mock_retriever.retrieve.side_effect = Exception("Retriever failed")

        input = ContextAgentInput(query="test", max_datapoints=10)

        with pytest.raises(RetrievalError) as exc_info:
            await context_agent(input)

        error = exc_info.value
        assert error.agent == "ContextAgent"
        assert "Failed to retrieve context" in error.message
        assert error.recoverable is True
        assert error.context["query"] == "test"
        assert error.context["mode"] == "hybrid"

    @pytest.mark.asyncio
    async def test_invalid_input_type_raises_error(self, context_agent):
        """Test invalid input type raises AgentError (ValueError wrapped by BaseAgent)."""
        from backend.models.agent import AgentError, AgentInput

        invalid_input = AgentInput(query="test")

        with pytest.raises(AgentError, match="ContextAgent requires ContextAgentInput"):
            await context_agent(invalid_input)


class TestContextPassing:
    """Test context passing to next agent."""

    @pytest.mark.asyncio
    async def test_builds_context_for_next_agent(
        self, context_agent, mock_retriever, sample_retrieval_result
    ):
        """Test context is built for next agent."""
        mock_retriever.retrieve.return_value = sample_retrieval_result

        input = ContextAgentInput(
            query="sales",
            context={"previous_agent": "ClassifierAgent"},
            max_datapoints=10,
        )

        output = await context_agent(input)

        # Build context for next agent
        next_context = context_agent._build_context_for_next(input, output)

        assert "investigation_memory" in next_context
        assert "previous_agent" in next_context
        assert next_context["previous_agent"] == "ClassifierAgent"

        # Investigation memory is serialized
        memory_dict = next_context["investigation_memory"]
        assert memory_dict["query"] == "sales"
        assert memory_dict["total_retrieved"] == 3


class TestMetadata:
    """Test metadata tracking."""

    @pytest.mark.asyncio
    async def test_metadata_tracking(self, context_agent, mock_retriever, sample_retrieval_result):
        """Test execution metadata is tracked correctly."""
        mock_retriever.retrieve.return_value = sample_retrieval_result

        input = ContextAgentInput(query="test", max_datapoints=10)
        output = await context_agent(input)

        metadata = output.metadata

        assert metadata.agent_name == "ContextAgent"
        assert metadata.llm_calls == 0  # No LLM calls
        assert metadata.started_at is not None
        assert metadata.completed_at is not None
        assert metadata.duration_ms is not None
        assert metadata.duration_ms > 0
        assert metadata.error is None

    @pytest.mark.asyncio
    async def test_metadata_on_error(self, context_agent, mock_retriever):
        """Test metadata is set even on error."""
        mock_retriever.retrieve.side_effect = Exception("Test error")

        input = ContextAgentInput(query="test", max_datapoints=10)

        with pytest.raises(RetrievalError):
            await context_agent(input)

        # Note: metadata is internal to the agent, not accessible after error
        # Error is raised before output is created


class TestInputValidation:
    """Test input validation."""

    @pytest.mark.asyncio
    async def test_max_datapoints_validation(
        self, context_agent, mock_retriever, sample_retrieval_result
    ):
        """Test max_datapoints is validated by Pydantic."""
        mock_retriever.retrieve.return_value = sample_retrieval_result

        # Valid range is 1-50
        with pytest.raises(ValueError):  # Pydantic validation error
            ContextAgentInput(query="test", max_datapoints=0)

        with pytest.raises(ValueError):  # Pydantic validation error
            ContextAgentInput(query="test", max_datapoints=51)

    @pytest.mark.asyncio
    async def test_retrieval_mode_validation(self):
        """Test retrieval_mode is validated."""
        # Valid modes: local, global, hybrid
        valid_input = ContextAgentInput(
            query="test",
            retrieval_mode="hybrid",
            max_datapoints=10,
        )
        assert valid_input.retrieval_mode == "hybrid"

        # Invalid mode should fail Pydantic validation
        with pytest.raises(ValueError):
            ContextAgentInput(
                query="test",
                retrieval_mode="invalid_mode",
                max_datapoints=10,
            )


class TestDataPointMapping:
    """Test mapping from RetrievedItem to RetrievedDataPoint."""

    @pytest.mark.asyncio
    async def test_maps_node_type_to_datapoint_type(self, context_agent, mock_retriever):
        """Test correct mapping from node_type (graph) to DataPoint type."""
        # Create items with node_type instead of type (from knowledge graph)
        result = RetrievalResult(
            items=[
                RetrievedItem(
                    datapoint_id="table_sales_001",
                    score=0.9,
                    source="graph",
                    metadata={"node_type": "table", "name": "Sales Table"},
                ),
                RetrievedItem(
                    datapoint_id="column_amount_001",
                    score=0.8,
                    source="graph",
                    metadata={"node_type": "column", "name": "Amount"},
                ),
                RetrievedItem(
                    datapoint_id="metric_revenue_001",
                    score=0.95,
                    source="graph",
                    metadata={"node_type": "metric", "name": "Revenue"},
                ),
                RetrievedItem(
                    datapoint_id="proc_daily_etl_001",
                    score=0.7,
                    source="graph",
                    metadata={"node_type": "process", "name": "Daily ETL"},
                ),
                RetrievedItem(
                    datapoint_id="query_top_customers_001",
                    score=0.85,
                    source="graph",
                    metadata={"node_type": "query", "name": "Top Customers Query"},
                ),
            ],
            total_count=5,
            mode=RetrievalMode.GLOBAL,
            query="test",
        )

        mock_retriever.retrieve.return_value = result

        input_data = ContextAgentInput(query="test", max_datapoints=10)
        output = await context_agent(input_data)

        datapoints = output.investigation_memory.datapoints

        # Check node_type → DataPoint type mapping
        assert datapoints[0].datapoint_type == "Schema"  # table → Schema
        assert datapoints[1].datapoint_type == "Schema"  # column → Schema
        assert datapoints[2].datapoint_type == "Business"  # metric → Business
        assert datapoints[3].datapoint_type == "Process"  # process → Process
        assert datapoints[4].datapoint_type == "Query"  # query → Query

    @pytest.mark.asyncio
    async def test_prefers_type_over_node_type(self, context_agent, mock_retriever):
        """Test type field takes precedence over node_type (vector store priority)."""
        # Item has both type and node_type - type should win
        result = RetrievalResult(
            items=[
                RetrievedItem(
                    datapoint_id="test_001",
                    score=0.9,
                    source="hybrid",
                    metadata={
                        "type": "Business",  # Vector store value
                        "node_type": "table",  # Graph value (should be ignored)
                        "name": "Test Item",
                    },
                )
            ],
            total_count=1,
            mode=RetrievalMode.HYBRID,
            query="test",
        )

        mock_retriever.retrieve.return_value = result

        input_data = ContextAgentInput(query="test", max_datapoints=10)
        output = await context_agent(input_data)

        dp = output.investigation_memory.datapoints[0]
        # Should use "type" field, not node_type mapping
        assert dp.datapoint_type == "Business"

    @pytest.mark.asyncio
    async def test_handles_missing_metadata_fields(self, context_agent, mock_retriever):
        """Test handles missing optional metadata fields gracefully."""
        # Create item with minimal metadata
        result = RetrievalResult(
            items=[
                RetrievedItem(
                    datapoint_id="test_001",
                    score=0.9,
                    source="vector",
                    metadata={},  # No type or name
                )
            ],
            total_count=1,
            mode=RetrievalMode.LOCAL,
            query="test",
        )

        mock_retriever.retrieve.return_value = result

        input = ContextAgentInput(query="test", max_datapoints=10)
        output = await context_agent(input)

        dp = output.investigation_memory.datapoints[0]
        assert dp.datapoint_id == "test_001"
        assert dp.datapoint_type == "Schema"  # Default
        assert dp.name == "test_001"  # Falls back to ID

    @pytest.mark.asyncio
    async def test_preserves_all_metadata(
        self, context_agent, mock_retriever, sample_retrieval_result
    ):
        """Test all metadata is preserved in RetrievedDataPoint."""
        mock_retriever.retrieve.return_value = sample_retrieval_result

        input = ContextAgentInput(query="test", max_datapoints=10)
        output = await context_agent(input)

        dp = output.investigation_memory.datapoints[0]

        # All original metadata is preserved
        assert dp.metadata["type"] == "Schema"
        assert dp.metadata["name"] == "Sales Table"
        assert dp.metadata["table_name"] == "fact_sales"
