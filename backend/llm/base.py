"""
Base LLM Provider

Abstract base class defining the interface for all LLM providers.
Ensures consistent API across OpenAI, Anthropic, Google, Local, etc.
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from backend.llm.models import (
    LLMRequest,
    LLMResponse,
    LLMStreamChunk,
    ModelInfo,
)

logger = logging.getLogger(__name__)


class BaseLLMProvider(ABC):
    """
    Abstract base class for LLM providers.

    All LLM providers (OpenAI, Anthropic, Google, Local) must implement
    this interface to ensure consistent behavior across the application.

    Attributes:
        provider_name: Unique identifier for this provider
        temperature: Default sampling temperature
        max_tokens: Default maximum tokens to generate
        timeout: Request timeout in seconds
    """

    def __init__(
        self,
        provider_name: str,
        temperature: float = 0.0,
        max_tokens: int = 2000,
        timeout: int = 30,
    ):
        """
        Initialize base provider.

        Args:
            provider_name: Provider identifier (e.g., "openai", "anthropic")
            temperature: Default temperature for responses
            max_tokens: Default max tokens for responses
            timeout: Request timeout in seconds
        """
        self.provider_name = provider_name
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.timeout = timeout

        logger.info(
            f"Initialized {provider_name} provider",
            extra={
                "provider": provider_name,
                "temperature": temperature,
                "max_tokens": max_tokens,
            },
        )

    @abstractmethod
    async def generate(self, request: LLMRequest) -> LLMResponse:
        """
        Generate a completion from the LLM.

        This is the main method for getting LLM responses. Each provider
        must implement this to handle their specific API.

        Args:
            request: LLM request with messages and parameters

        Returns:
            LLMResponse with generated content and metadata

        Raises:
            Exception: Provider-specific errors (API errors, timeouts, etc.)
        """
        pass  # pragma: no cover - abstract method

    @abstractmethod
    async def stream(self, request: LLMRequest) -> AsyncIterator[LLMStreamChunk]:
        """
        Stream a completion from the LLM.

        Yields chunks of generated text as they're produced, enabling
        real-time streaming to the user.

        Args:
            request: LLM request with messages and parameters

        Yields:
            LLMStreamChunk: Chunks of generated text

        Raises:
            Exception: Provider-specific errors
        """
        pass  # pragma: no cover - abstract method

    @abstractmethod
    def count_tokens(self, text: str) -> int:
        """
        Count tokens in a text string.

        Used for tracking usage and ensuring prompts fit within
        context windows.

        Args:
            text: Text to count tokens for

        Returns:
            Number of tokens in the text
        """
        pass  # pragma: no cover - abstract method

    @abstractmethod
    def get_model_info(self, model_name: str | None = None) -> ModelInfo:
        """
        Get information about a model.

        Args:
            model_name: Specific model to get info for (None = default model)

        Returns:
            ModelInfo with model capabilities and limits
        """
        pass  # pragma: no cover - abstract method

    def _apply_defaults(self, request: LLMRequest) -> LLMRequest:
        """
        Apply default values to request if not specified.

        Args:
            request: Original request

        Returns:
            Request with defaults applied
        """
        if request.temperature is None:
            request.temperature = self.temperature
        if request.max_tokens is None:
            request.max_tokens = self.max_tokens
        return request

    def _log_request(self, request: LLMRequest) -> None:
        """Log request details for debugging."""
        logger.debug(
            f"{self.provider_name} request",
            extra={
                "provider": self.provider_name,
                "message_count": len(request.messages),
                "temperature": request.temperature,
                "max_tokens": request.max_tokens,
                "stream": request.stream,
            },
        )

    def _log_response(self, response: LLMResponse) -> None:
        """Log response details for debugging."""
        logger.debug(
            f"{self.provider_name} response",
            extra={
                "provider": self.provider_name,
                "model": response.model,
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
                "total_tokens": response.usage.total_tokens,
                "finish_reason": response.finish_reason,
            },
        )
