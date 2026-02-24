"""
Unit tests for BaseAgent

Tests the base agent framework including:
- Abstract class enforcement
- Timing and metadata tracking
- Error handling and retry logic
- Logging behavior
"""

import asyncio
import time

import pytest

from backend.agents.base import BaseAgent
from backend.models.agent import (
    AgentError,
    AgentInput,
    AgentOutput,
    LLMError,
    ValidationError,
)


class TestBaseAgentInstantiation:
    """Test that BaseAgent enforces abstract class pattern."""

    def test_cannot_instantiate_base_agent_directly(self):
        """BaseAgent is abstract and cannot be instantiated."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            BaseAgent(name="TestAgent")

    def test_subclass_must_implement_execute(self):
        """Subclass without execute() cannot be instantiated."""

        class IncompleteAgent(BaseAgent):
            pass

        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteAgent(name="IncompleteAgent")

    def test_valid_subclass_can_be_instantiated(self):
        """Subclass with execute() can be instantiated."""

        class ValidAgent(BaseAgent):
            async def execute(self, input: AgentInput) -> AgentOutput:
                return AgentOutput(success=True, data={}, metadata=self._create_metadata())

        agent = ValidAgent(name="ValidAgent")
        assert agent.name == "ValidAgent"
        assert agent.max_retries == 3  # Default value
        assert agent.timeout_seconds is None  # Default value


class TestAgentConfiguration:
    """Test agent initialization and configuration."""

    def test_agent_initialization_with_defaults(self, valid_agent):
        """Agent initializes with default configuration."""
        assert valid_agent.name == "TestAgent"
        assert valid_agent.max_retries == 3
        assert valid_agent.timeout_seconds is None

    def test_agent_initialization_with_custom_config(self):
        """Agent can be initialized with custom configuration."""

        class CustomAgent(BaseAgent):
            async def execute(self, input: AgentInput) -> AgentOutput:
                return AgentOutput(success=True, data={}, metadata=self._create_metadata())

        agent = CustomAgent(name="CustomAgent", max_retries=5, timeout_seconds=30.0)

        assert agent.name == "CustomAgent"
        assert agent.max_retries == 5
        assert agent.timeout_seconds == 30.0


class TestAgentExecution:
    """Test agent execution and the __call__ wrapper."""

    @pytest.mark.asyncio
    async def test_successful_execution(self, valid_agent, sample_input):
        """Agent executes successfully and returns output."""
        output = await valid_agent(sample_input)

        assert isinstance(output, AgentOutput)
        assert output.success is True
        assert output.data == {"result": "success"}
        assert output.metadata.agent_name == "TestAgent"

    @pytest.mark.asyncio
    async def test_execution_tracks_timing(self, valid_agent, sample_input):
        """Agent tracks execution duration in metadata."""
        output = await valid_agent(sample_input)

        assert output.metadata.duration_ms is not None
        assert output.metadata.duration_ms > 0
        assert output.metadata.started_at is not None
        assert output.metadata.completed_at is not None

    @pytest.mark.asyncio
    async def test_execution_with_delay_tracks_accurate_timing(self, sample_input):
        """Timing accurately reflects execution duration."""

        class SlowAgent(BaseAgent):
            async def execute(self, input: AgentInput) -> AgentOutput:
                await asyncio.sleep(0.1)  # 100ms delay
                return AgentOutput(success=True, data={}, metadata=self._create_metadata())

        agent = SlowAgent(name="SlowAgent")
        output = await agent(sample_input)

        # Should be at least 100ms
        assert output.metadata.duration_ms >= 100
        # But not unreasonably long (< 200ms with overhead)
        assert output.metadata.duration_ms < 200

    @pytest.mark.asyncio
    async def test_execution_preserves_agent_output(self, sample_input):
        """Agent output data is preserved through wrapper."""

        class DataAgent(BaseAgent):
            async def execute(self, input: AgentInput) -> AgentOutput:
                return AgentOutput(
                    success=True,
                    data={"query": input.query, "processed": True, "items": [1, 2, 3]},
                    metadata=self._create_metadata(),
                    next_agent="NextAgent",
                )

        agent = DataAgent(name="DataAgent")
        output = await agent(sample_input)

        assert output.data["query"] == "Test query"
        assert output.data["processed"] is True
        assert output.data["items"] == [1, 2, 3]
        assert output.next_agent == "NextAgent"


class TestErrorHandling:
    """Test error handling and error metadata."""

    @pytest.mark.asyncio
    async def test_agent_error_captures_agent_name(self, sample_input):
        """AgentError includes agent name in message."""

        class ErrorAgent(BaseAgent):
            async def execute(self, input: AgentInput) -> AgentOutput:
                raise AgentError(agent=self.name, message="Something went wrong", recoverable=False)

        agent = ErrorAgent(name="ErrorAgent")

        with pytest.raises(AgentError) as exc_info:
            await agent(sample_input)

        assert "ErrorAgent" in str(exc_info.value)
        assert "Something went wrong" in str(exc_info.value)
        assert exc_info.value.agent == "ErrorAgent"

    @pytest.mark.asyncio
    async def test_non_recoverable_error_fails_immediately(self, sample_input):
        """Non-recoverable errors don't retry."""

        class FailAgent(BaseAgent):
            async def execute(self, input: AgentInput) -> AgentOutput:
                raise ValidationError(agent=self.name, message="Invalid input")

        agent = FailAgent(name="FailAgent")

        with pytest.raises(ValidationError) as exc_info:
            await agent(sample_input)

        assert exc_info.value.recoverable is False
        assert "Invalid input" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_error_includes_metadata_with_duration(self, sample_input):
        """Failed execution still tracks timing metadata."""

        class TimedErrorAgent(BaseAgent):
            async def execute(self, input: AgentInput) -> AgentOutput:
                await asyncio.sleep(0.05)  # 50ms delay
                raise AgentError(agent=self.name, message="Error after delay", recoverable=False)

        agent = TimedErrorAgent(name="TimedErrorAgent")

        with pytest.raises(AgentError):
            await agent(sample_input)

        # Metadata should be set even on failure
        assert agent._metadata.duration_ms is not None
        assert agent._metadata.duration_ms >= 50
        assert agent._metadata.error is not None

    @pytest.mark.asyncio
    async def test_unexpected_error_wrapped_in_agent_error(self, sample_input):
        """Unexpected exceptions are wrapped in AgentError."""

        class BuggyAgent(BaseAgent):
            async def execute(self, input: AgentInput) -> AgentOutput:
                raise ValueError("Unexpected bug!")

        agent = BuggyAgent(name="BuggyAgent")

        with pytest.raises(AgentError) as exc_info:
            await agent(sample_input)

        assert exc_info.value.agent == "BuggyAgent"
        assert "Unexpected error" in exc_info.value.message
        assert exc_info.value.recoverable is False
        assert exc_info.value.context["error_type"] == "ValueError"


class TestRetryLogic:
    """Test retry behavior for recoverable errors."""

    @pytest.mark.asyncio
    async def test_recoverable_error_retries(self, sample_input):
        """Recoverable errors trigger retry logic."""

        class RetryAgent(BaseAgent):
            def __init__(self):
                super().__init__(name="RetryAgent", max_retries=3)
                self.attempts = 0

            async def execute(self, input: AgentInput) -> AgentOutput:
                self.attempts += 1
                if self.attempts < 3:
                    raise LLMError(
                        agent=self.name,
                        message="API timeout",
                    )
                return AgentOutput(
                    success=True, data={"attempts": self.attempts}, metadata=self._create_metadata()
                )

        agent = RetryAgent()
        output = await agent(sample_input)

        assert output.success is True
        assert output.data["attempts"] == 3  # Succeeded on 3rd attempt

    @pytest.mark.asyncio
    async def test_retry_exhaustion_raises_error(self, sample_input):
        """Error is raised after max retries exhausted."""

        class AlwaysFailAgent(BaseAgent):
            def __init__(self):
                super().__init__(name="AlwaysFailAgent", max_retries=2)
                self.attempts = 0

            async def execute(self, input: AgentInput) -> AgentOutput:
                self.attempts += 1
                raise LLMError(agent=self.name, message="Persistent failure")

        agent = AlwaysFailAgent()

        with pytest.raises(LLMError):
            await agent(sample_input)

        # Should have tried 1 + max_retries times
        assert agent.attempts == 3  # Initial + 2 retries

    @pytest.mark.asyncio
    async def test_retry_uses_exponential_backoff(self, sample_input):
        """Retry delays use exponential backoff."""

        class BackoffAgent(BaseAgent):
            def __init__(self):
                super().__init__(name="BackoffAgent", max_retries=2)
                self.attempts = 0
                self.attempt_times = []

            async def execute(self, input: AgentInput) -> AgentOutput:
                self.attempts += 1
                self.attempt_times.append(time.perf_counter())

                if self.attempts < 3:
                    raise LLMError(agent=self.name, message="Retry me")

                return AgentOutput(success=True, data={}, metadata=self._create_metadata())

        agent = BackoffAgent()
        await agent(sample_input)

        # Check delays between attempts
        assert len(agent.attempt_times) == 3

        # First retry should wait ~1s
        delay1 = agent.attempt_times[1] - agent.attempt_times[0]
        assert 0.9 < delay1 < 1.2

        # Second retry should wait ~2s
        delay2 = agent.attempt_times[2] - agent.attempt_times[1]
        assert 1.9 < delay2 < 2.2


class TestLLMTracking:
    """Test LLM call and token tracking."""

    @pytest.mark.asyncio
    async def test_track_llm_call_without_tokens(self, sample_input):
        """LLM calls are tracked in metadata."""

        class LLMAgent(BaseAgent):
            async def execute(self, input: AgentInput) -> AgentOutput:
                self._track_llm_call()
                self._track_llm_call()
                return AgentOutput(success=True, data={}, metadata=self._create_metadata())

        agent = LLMAgent(name="LLMAgent")
        output = await agent(sample_input)

        assert output.metadata.llm_calls == 2
        assert output.metadata.tokens_used is None

    @pytest.mark.asyncio
    async def test_track_llm_call_with_tokens(self, sample_input):
        """Token usage is accumulated across calls."""

        class TokenAgent(BaseAgent):
            async def execute(self, input: AgentInput) -> AgentOutput:
                self._track_llm_call(tokens=100)
                self._track_llm_call(tokens=150)
                self._track_llm_call(tokens=50)
                return AgentOutput(success=True, data={}, metadata=self._create_metadata())

        agent = TokenAgent(name="TokenAgent")
        output = await agent(sample_input)

        assert output.metadata.llm_calls == 3
        assert output.metadata.tokens_used == 300


class TestContextPassing:
    """Test context dictionary passing between agents."""

    @pytest.mark.asyncio
    async def test_build_context_for_next(self, valid_agent, sample_input):
        """Context can be updated for next agent."""

        original_context = {"step": 1, "data": "original"}

        new_context = valid_agent._build_context_for_next(
            original_context, step=2, new_field="added"
        )

        assert new_context["step"] == 2  # Updated
        assert new_context["data"] == "original"  # Preserved
        assert new_context["new_field"] == "added"  # Added

        # Original should be unchanged
        assert original_context["step"] == 1

    @pytest.mark.asyncio
    async def test_context_flows_through_input(self, sample_input):
        """Context from input is accessible in execute."""

        class ContextAgent(BaseAgent):
            async def execute(self, input: AgentInput) -> AgentOutput:
                return AgentOutput(
                    success=True,
                    data={"received_context": input.context.copy()},
                    metadata=self._create_metadata(),
                )

        agent = ContextAgent(name="ContextAgent")

        input_with_context = AgentInput(
            query="Test", context={"previous_step": "classification", "entities": ["sales"]}
        )

        output = await agent(input_with_context)

        assert output.data["received_context"]["previous_step"] == "classification"
        assert output.data["received_context"]["entities"] == ["sales"]


class TestLogging:
    """Test logging behavior."""

    @pytest.mark.asyncio
    async def test_execution_logs_start_and_completion(self, valid_agent, sample_input, caplog):
        """Agent logs execution start and completion."""
        import logging

        with caplog.at_level(logging.INFO):
            await valid_agent(sample_input)

        # Check for start log
        start_logs = [r for r in caplog.records if "Starting TestAgent" in r.message]
        assert len(start_logs) == 1

        # Check for completion log
        complete_logs = [r for r in caplog.records if "Completed TestAgent" in r.message]
        assert len(complete_logs) == 1

    @pytest.mark.asyncio
    async def test_error_logs_warning_on_retry(self, sample_input, caplog):
        """Recoverable errors log warnings on retry attempts."""
        import logging

        class RetryAgent(BaseAgent):
            def __init__(self):
                super().__init__(name="RetryAgent", max_retries=1)
                self.attempts = 0

            async def execute(self, input: AgentInput) -> AgentOutput:
                self.attempts += 1
                if self.attempts == 1:
                    raise LLMError(agent=self.name, message="First attempt fails")
                return AgentOutput(success=True, data={}, metadata=self._create_metadata())

        agent = RetryAgent()

        with caplog.at_level(logging.WARNING):
            await agent(sample_input)

        warning_logs = [r for r in caplog.records if "Agent error in RetryAgent" in r.message]
        assert len(warning_logs) >= 1

    @pytest.mark.asyncio
    async def test_fatal_error_logs_error(self, sample_input, caplog):
        """Non-recoverable errors log at ERROR level."""
        import logging

        class FatalAgent(BaseAgent):
            async def execute(self, input: AgentInput) -> AgentOutput:
                raise ValidationError(agent=self.name, message="Fatal error")

        agent = FatalAgent(name="FatalAgent")

        with caplog.at_level(logging.ERROR):
            with pytest.raises(ValidationError):
                await agent(sample_input)

        error_logs = [r for r in caplog.records if "Failed FatalAgent" in r.message]
        assert len(error_logs) == 1


class TestInputValidation:
    """Test input validation hook."""

    def test_default_validate_input_does_nothing(self, valid_agent, sample_input):
        """Default _validate_input implementation does nothing."""
        # Should not raise
        valid_agent._validate_input(sample_input)

    @pytest.mark.asyncio
    async def test_validate_input_hook(self, sample_input):
        """_validate_input can be overridden for custom validation."""

        class ValidatingAgent(BaseAgent):
            def _validate_input(self, input: AgentInput) -> None:
                if not input.query:
                    raise ValidationError(agent=self.name, message="Query cannot be empty")

            async def execute(self, input: AgentInput) -> AgentOutput:
                self._validate_input(input)
                return AgentOutput(success=True, data={}, metadata=self._create_metadata())

        agent = ValidatingAgent(name="ValidatingAgent")

        # Valid input succeeds
        await agent(sample_input)

        # Invalid input fails
        invalid_input = AgentInput(query="", context={})
        with pytest.raises(ValidationError, match="Query cannot be empty"):
            await agent(invalid_input)


class TestErrorModels:
    """Test error model methods and specialized error types."""

    def test_agent_error_to_dict(self):
        """AgentError.to_dict() converts error to dictionary."""
        error = AgentError(
            agent="TestAgent",
            message="Something failed",
            recoverable=True,
            context={"key": "value"},
        )

        error_dict = error.to_dict()

        assert error_dict["agent"] == "TestAgent"
        assert error_dict["message"] == "Something failed"
        assert error_dict["recoverable"] is True
        assert error_dict["context"]["key"] == "value"
        assert error_dict["type"] == "AgentError"

    def test_database_error_with_recoverable_flag(self):
        """DatabaseError can have custom recoverable flag."""
        from backend.models.agent import DatabaseError

        # Non-recoverable database error
        error1 = DatabaseError(agent="TestAgent", message="Connection lost", recoverable=False)
        assert error1.recoverable is False

        # Recoverable database error
        error2 = DatabaseError(agent="TestAgent", message="Temporary timeout", recoverable=True)
        assert error2.recoverable is True

    def test_retrieval_error_is_recoverable(self):
        """RetrievalError is recoverable by default."""
        from backend.models.agent import RetrievalError

        error = RetrievalError(agent="ContextAgent", message="Vector store unavailable")
        assert error.recoverable is True
        assert "ContextAgent" in str(error)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def valid_agent():
    """Create a valid test agent."""

    class TestAgent(BaseAgent):
        async def execute(self, input: AgentInput) -> AgentOutput:
            return AgentOutput(
                success=True, data={"result": "success"}, metadata=self._create_metadata()
            )

    return TestAgent(name="TestAgent")


@pytest.fixture
def sample_input():
    """Create sample agent input."""
    return AgentInput(query="Test query", conversation_history=[], context={})
