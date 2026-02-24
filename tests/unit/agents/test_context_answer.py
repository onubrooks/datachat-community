import pytest

from backend.agents.context_answer import ContextAnswerAgent
from backend.llm.models import LLMResponse, LLMUsage
from backend.models.agent import (
    ContextAnswerAgentInput,
    InvestigationMemory,
    RetrievedDataPoint,
)


def _schema_memory() -> InvestigationMemory:
    return InvestigationMemory(
        query="what tables exist?",
        datapoints=[
            RetrievedDataPoint(
                datapoint_id="table_users_001",
                datapoint_type="Schema",
                name="Users",
                score=0.9,
                source="vector",
                metadata={
                    "table_name": "public.users",
                    "key_columns": [
                        {"name": "id", "type": "bigint"},
                        {"name": "email", "type": "text"},
                    ],
                },
            ),
            RetrievedDataPoint(
                datapoint_id="table_orders_001",
                datapoint_type="Schema",
                name="Orders",
                score=0.85,
                source="vector",
                metadata={
                    "table_name": "public.orders",
                    "key_columns": [
                        {"name": "id", "type": "bigint"},
                        {"name": "amount", "type": "numeric"},
                    ],
                },
            ),
        ],
        total_retrieved=2,
        retrieval_mode="hybrid",
        sources_used=["table_users_001", "table_orders_001"],
    )


@pytest.mark.asyncio
async def test_context_answer_agent_uses_deterministic_catalog_for_table_list(
    mock_async_function,
):
    agent = ContextAnswerAgent()
    agent.llm.generate = mock_async_function(
        return_value=LLMResponse(
            content='{"answer":"fallback","confidence":0.1,"evidence":[],"needs_sql":false}',
            model="mock",
            usage=LLMUsage(prompt_tokens=1, completion_tokens=1, total_tokens=2),
            finish_reason="stop",
            provider="mock",
        )
    )

    input_data = ContextAnswerAgentInput(
        query="what tables exist?",
        conversation_history=[],
        investigation_memory=_schema_memory(),
        intent="exploration",
        context_confidence=0.9,
    )

    output = await agent.execute(input_data)

    assert "table(s)" in output.context_answer.answer
    assert "public.users" in output.context_answer.answer
    assert output.context_answer.needs_sql is False
    assert output.metadata.llm_calls == 0
    agent.llm.generate.assert_not_called()


@pytest.mark.asyncio
async def test_context_answer_agent_lists_columns_without_llm(mock_async_function):
    agent = ContextAnswerAgent()
    agent.llm.generate = mock_async_function()

    input_data = ContextAnswerAgentInput(
        query="show columns in public.orders",
        conversation_history=[],
        investigation_memory=_schema_memory(),
        intent="exploration",
        context_confidence=0.9,
    )

    output = await agent.execute(input_data)

    assert "Columns in `public.orders`" in output.context_answer.answer
    assert "amount (numeric)" in output.context_answer.answer
    assert output.context_answer.needs_sql is False
    assert output.metadata.llm_calls == 0
    agent.llm.generate.assert_not_called()


@pytest.mark.asyncio
async def test_context_answer_agent_falls_back_to_llm_for_non_catalog_query(
    mock_async_function,
):
    agent = ContextAnswerAgent()
    response = LLMResponse(
        content='{"answer":"Use the users table.","confidence":0.82,"evidence":[{"datapoint_id":"table_users_001","name":"Users","type":"Schema","reason":"Table list"}],"needs_sql":false,"clarifying_questions":[]}',
        model="mock",
        usage=LLMUsage(prompt_tokens=1, completion_tokens=1, total_tokens=2),
        finish_reason="stop",
        provider="mock",
    )
    agent.llm.generate = mock_async_function(return_value=response)

    input_data = ContextAnswerAgentInput(
        query="which table contains user emails?",
        conversation_history=[],
        investigation_memory=_schema_memory(),
        intent="exploration",
        context_confidence=0.9,
    )

    output = await agent.execute(input_data)

    assert output.context_answer.answer == "Use the users table."
    assert output.context_answer.confidence == 0.82
    assert output.context_answer.evidence[0].datapoint_id == "table_users_001"
    assert output.context_answer.needs_sql is False
    assert output.metadata.llm_calls == 1


@pytest.mark.asyncio
async def test_context_answer_agent_gates_low_confidence_semantic_answer(
    mock_async_function,
):
    agent = ContextAnswerAgent()
    response = LLMResponse(
        content='{"answer":"Maybe revenue is in users.","confidence":0.21,"evidence":[],"needs_sql":false,"clarifying_questions":[]}',
        model="mock",
        usage=LLMUsage(prompt_tokens=1, completion_tokens=1, total_tokens=2),
        finish_reason="stop",
        provider="mock",
    )
    agent.llm.generate = mock_async_function(return_value=response)

    input_data = ContextAnswerAgentInput(
        query="How is revenue trending?",
        conversation_history=[],
        investigation_memory=_schema_memory(),
        intent="exploration",
        context_confidence=0.2,
    )

    output = await agent.execute(input_data)

    assert output.context_answer.confidence == 0.2
    assert output.context_answer.clarifying_questions
    question = output.context_answer.clarifying_questions[0].lower()
    assert "revenue" in question
    assert "public.users" in question
    assert "not confident enough" in output.context_answer.answer.lower()


def test_context_summary_includes_query_datapoint_details():
    agent = ContextAnswerAgent()
    memory = InvestigationMemory(
        query="top customers by concentration risk",
        datapoints=[
            RetrievedDataPoint(
                datapoint_id="query_bank_concentration_001",
                datapoint_type="Query",
                name="Top Customers by Deposit Concentration",
                score=0.92,
                source="vector",
                metadata={
                    "query_description": "Ranks customers by share of total balances.",
                    "related_tables": "public.bank_accounts,public.bank_customers",
                    "parameters": '{"limit":{"type":"integer","default":10}}',
                },
            )
        ],
        total_retrieved=1,
        retrieval_mode="hybrid",
        sources_used=["query_bank_concentration_001"],
    )

    summary = agent._build_context_summary(memory)

    assert "Query Pattern: Ranks customers by share of total balances." in summary
    assert "Related Tables: public.bank_accounts, public.bank_customers" in summary
    assert "Parameters: limit" in summary
