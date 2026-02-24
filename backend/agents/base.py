"""
Base Agent Framework

Abstract base class for all agents in the DataChat pipeline.
Provides consistent interface, timing, logging, and error handling.

Usage:
    class MyAgent(BaseAgent):
        def __init__(self):
            super().__init__(name="MyAgent")

        async def execute(self, input: AgentInput) -> AgentOutput:
            # Agent-specific logic here
            return AgentOutput(
                success=True,
                data={"result": "value"},
                metadata=self._create_metadata()
            )
"""

import logging
import time
from abc import ABC, abstractmethod
from typing import Any

from backend.models.agent import (
    AgentError,
    AgentInput,
    AgentMetadata,
    AgentOutput,
)

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    """
    Abstract base class for all agents in the DataChat pipeline.

    Responsibilities:
        - Define standard interface via execute() method
        - Provide timing and performance tracking
        - Handle logging and error propagation
        - Manage execution metadata

    Attributes:
        name: Unique identifier for this agent
        max_retries: Maximum number of retry attempts on recoverable errors
        timeout_seconds: Maximum execution time before timeout

    Design Pattern:
        All agents follow the same pattern:
        1. Validate input
        2. Execute core logic
        3. Return typed output with metadata

        The __call__ method wraps execute() with:
        - Performance timing
        - Error handling and logging
        - Metadata collection
    """

    def __init__(self, name: str, max_retries: int = 3, timeout_seconds: float | None = None):
        """
        Initialize base agent.

        Args:
            name: Unique identifier for this agent (e.g., "ClassifierAgent")
            max_retries: Number of retry attempts for recoverable errors
            timeout_seconds: Optional timeout for agent execution
        """
        self.name = name
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self._metadata = self._create_metadata()

        logger.info(
            f"Initialized {self.name}",
            extra={
                "agent": self.name,
                "max_retries": max_retries,
                "timeout_seconds": timeout_seconds,
            },
        )

    @abstractmethod
    async def execute(self, input: AgentInput) -> AgentOutput:
        """
        Execute the agent's core logic.

        This method MUST be implemented by all concrete agents.
        It should contain the agent-specific processing logic.

        Args:
            input: Typed input data for the agent

        Returns:
            AgentOutput: Typed output data with success status and metadata

        Raises:
            AgentError: On execution failures (recoverable or not)
        """
        pass  # pragma: no cover - abstract method

    async def __call__(self, input: AgentInput) -> AgentOutput:
        """
        Execute the agent with timing, logging, and error handling.

        This method wraps execute() and should NOT be overridden.
        It provides consistent behavior across all agents:
        - Logs execution start/end
        - Tracks timing metrics
        - Handles errors gracefully
        - Manages retry logic for recoverable errors

        Args:
            input: Typed input data for the agent

        Returns:
            AgentOutput: Result of agent execution with metadata

        Raises:
            AgentError: If all retry attempts fail or error is not recoverable
        """
        start_time = time.perf_counter()
        attempt = 0
        last_error: AgentError | None = None

        logger.info(
            f"Starting {self.name}",
            extra={
                "agent": self.name,
                "query": input.query[:100],  # Truncate for logging
                "context_keys": list(input.context.keys()),
            },
        )

        while attempt <= self.max_retries:
            try:
                # Reset metadata for this attempt
                self._metadata = self._create_metadata()

                # Execute core logic
                output = await self.execute(input)

                # Calculate duration and finalize metadata
                duration_ms = (time.perf_counter() - start_time) * 1000
                self._metadata.mark_complete()
                self._metadata.duration_ms = duration_ms

                # Update output metadata
                output.metadata = self._metadata

                logger.info(
                    f"Completed {self.name}",
                    extra={
                        "agent": self.name,
                        "success": output.success,
                        "duration_ms": duration_ms,
                        "attempt": attempt + 1,
                        "llm_calls": self._metadata.llm_calls,
                    },
                )

                return output

            except AgentError as e:
                attempt += 1
                last_error = e

                logger.warning(
                    f"Agent error in {self.name}",
                    extra={
                        "agent": self.name,
                        "error": str(e),
                        "recoverable": e.recoverable,
                        "attempt": attempt,
                        "max_retries": self.max_retries,
                        "context": e.context,
                    },
                    exc_info=True,
                )

                # If error is not recoverable or we've exhausted retries, raise
                if not e.recoverable or attempt > self.max_retries:
                    # Calculate final duration
                    duration_ms = (time.perf_counter() - start_time) * 1000
                    self._metadata.mark_complete()
                    self._metadata.duration_ms = duration_ms
                    self._metadata.error = str(e)

                    logger.error(
                        f"Failed {self.name} after {attempt} attempts",
                        extra={
                            "agent": self.name,
                            "error": str(e),
                            "duration_ms": duration_ms,
                            "attempts": attempt,
                        },
                    )
                    raise

                # Wait before retry (exponential backoff)
                wait_time = 2 ** (attempt - 1)  # 1s, 2s, 4s, ...
                logger.info(
                    f"Retrying {self.name} in {wait_time}s",
                    extra={"agent": self.name, "wait_time": wait_time},
                )
                await self._sleep(wait_time)

            except Exception as e:
                # Unexpected error - wrap in AgentError and raise
                duration_ms = (time.perf_counter() - start_time) * 1000
                self._metadata.mark_complete()
                self._metadata.duration_ms = duration_ms
                self._metadata.error = str(e)

                logger.error(
                    f"Unexpected error in {self.name}",
                    extra={
                        "agent": self.name,
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "duration_ms": duration_ms,
                    },
                    exc_info=True,
                )

                raise AgentError(
                    agent=self.name,
                    message=f"Unexpected error: {str(e)}",
                    recoverable=False,
                    context={"error_type": type(e).__name__},
                ) from e

        # Should never reach here, but for type safety
        if last_error:  # pragma: no cover - defensive programming
            raise last_error
        raise AgentError(  # pragma: no cover - defensive programming
            agent=self.name, message="Unknown error - retry loop exhausted", recoverable=False
        )

    def _create_metadata(self) -> AgentMetadata:
        """
        Create fresh metadata object for tracking execution.

        Returns:
            AgentMetadata: New metadata instance with agent name
        """
        return AgentMetadata(agent_name=self.name)

    def _track_llm_call(self, tokens: int | None = None) -> None:
        """
        Track an LLM API call in metadata.

        Call this method whenever your agent makes an LLM request
        to maintain accurate metrics.

        Args:
            tokens: Optional token count for this call
        """
        self._metadata.llm_calls += 1
        if tokens:
            current_tokens = self._metadata.tokens_used or 0
            self._metadata.tokens_used = current_tokens + tokens

        logger.debug(
            f"LLM call tracked for {self.name}",
            extra={
                "agent": self.name,
                "total_llm_calls": self._metadata.llm_calls,
                "tokens_this_call": tokens,
                "total_tokens": self._metadata.tokens_used,
            },
        )

    async def _sleep(self, seconds: float) -> None:
        """
        Async sleep utility for retry backoff.

        Args:
            seconds: Number of seconds to sleep
        """
        import asyncio

        await asyncio.sleep(seconds)

    def _validate_input(self, input: AgentInput) -> None:  # noqa: B027
        """
        Validate input data before execution.

        Override this method to add agent-specific validation.
        Raise ValidationError if input is invalid.

        This is intentionally not abstract - agents can optionally override it.

        Args:
            input: Input data to validate

        Raises:
            ValidationError: If input data is invalid
        """
        pass  # Base implementation does no validation

    def _build_context_for_next(
        self, current_context: dict[str, Any], **updates: Any
    ) -> dict[str, Any]:
        """
        Build context dictionary to pass to next agent in pipeline.

        Args:
            current_context: Context from current input
            **updates: Key-value pairs to add/update in context

        Returns:
            Updated context dictionary
        """
        new_context = current_context.copy()
        new_context.update(updates)
        return new_context
