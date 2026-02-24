"""
Anthropic LLM Provider

Implementation of BaseLLMProvider for Anthropic's Claude models.
Supports Claude 3.5 Sonnet, Claude 3.5 Haiku, etc.
"""

import logging
from collections.abc import AsyncIterator

from backend.llm.base import BaseLLMProvider
from backend.llm.models import (
    LLMRequest,
    LLMResponse,
    LLMStreamChunk,
    LLMUsage,
    ModelInfo,
)

logger = logging.getLogger(__name__)


class AnthropicProvider(BaseLLMProvider):
    """
    Anthropic (Claude) LLM provider implementation.

    Supports Claude 3.5 Sonnet, Claude 3.5 Haiku, and other Claude models.
    Uses the anthropic Python SDK.
    """

    def __init__(
        self,
        api_key: str,
        model: str = "claude-3-5-sonnet-20241022",
        temperature: float = 0.0,
        max_tokens: int = 2000,
        timeout: int = 30,
    ):
        """
        Initialize Anthropic provider.

        Args:
            api_key: Anthropic API key
            model: Default model to use
            temperature: Default temperature
            max_tokens: Default max tokens
            timeout: Request timeout
        """
        super().__init__(
            provider_name="anthropic",
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
        )

        self.model = model
        self.api_key = api_key

        try:
            from anthropic import AsyncAnthropic

            self.client = AsyncAnthropic(api_key=api_key, timeout=float(timeout))
        except ImportError:
            logger.warning("anthropic package not installed. Install with: pip install anthropic")
            self.client = None

        logger.info(f"Anthropic provider initialized with model: {model}", extra={"model": model})

    async def generate(self, request: LLMRequest) -> LLMResponse:
        """Generate completion using Anthropic API."""
        if not self.client:
            raise ImportError("anthropic package not installed")

        request = self._apply_defaults(request)
        self._log_request(request)

        # Convert messages - Anthropic requires system message separate
        system_message = None
        messages = []
        for msg in request.messages:
            if msg.role == "system":
                system_message = msg.content
            else:
                messages.append({"role": msg.role, "content": msg.content})

        response = await self.client.messages.create(
            model=request.model or self.model,
            max_tokens=request.max_tokens,
            temperature=request.temperature,
            system=system_message,
            messages=messages,
        )

        llm_response = LLMResponse(
            content=response.content[0].text,
            model=response.model,
            usage=LLMUsage(
                prompt_tokens=response.usage.input_tokens,
                completion_tokens=response.usage.output_tokens,
                total_tokens=response.usage.input_tokens + response.usage.output_tokens,
            ),
            finish_reason=self._map_finish_reason(response.stop_reason),
            provider="anthropic",
            metadata={"id": response.id},
        )

        self._log_response(llm_response)
        return llm_response

    async def stream(self, request: LLMRequest) -> AsyncIterator[LLMStreamChunk]:
        """Stream completion using Anthropic API."""
        if not self.client:
            raise ImportError("anthropic package not installed")

        request = self._apply_defaults(request)
        self._log_request(request)

        system_message = None
        messages = []
        for msg in request.messages:
            if msg.role == "system":
                system_message = msg.content
            else:
                messages.append({"role": msg.role, "content": msg.content})

        async with self.client.messages.stream(
            model=request.model or self.model,
            max_tokens=request.max_tokens,
            temperature=request.temperature,
            system=system_message,
            messages=messages,
        ) as stream:
            async for chunk in stream.text_stream:
                yield LLMStreamChunk(content=chunk, finish_reason=None)

    def count_tokens(self, text: str) -> int:
        """
        Count tokens for Anthropic models.

        Uses rough approximation. For exact counts, use Anthropic's API.
        """
        # Rough approximation: ~4 characters per token
        return len(text) // 4

    def get_model_info(self, model_name: str | None = None) -> ModelInfo:
        """Get Anthropic model information."""
        model = model_name or self.model

        model_info_map = {
            "claude-3-5-sonnet-20241022": ModelInfo(
                name="claude-3-5-sonnet-20241022",
                provider="anthropic",
                context_window=200000,
                max_output=8192,
                cost_per_1k_input=0.003,
                cost_per_1k_output=0.015,
                capabilities=["function-calling", "vision"],
            ),
            "claude-3-5-haiku-20241022": ModelInfo(
                name="claude-3-5-haiku-20241022",
                provider="anthropic",
                context_window=200000,
                max_output=8192,
                cost_per_1k_input=0.001,
                cost_per_1k_output=0.005,
                capabilities=["function-calling"],
            ),
        }

        return model_info_map.get(
            model,
            ModelInfo(
                name=model,
                provider="anthropic",
                context_window=200000,
                max_output=8192,
                capabilities=[],
            ),
        )

    def _map_finish_reason(self, reason: str | None) -> str:
        """Map Anthropic stop reason to standard format."""
        if reason == "end_turn":
            return "stop"
        elif reason == "max_tokens":
            return "length"
        else:
            return "stop"
