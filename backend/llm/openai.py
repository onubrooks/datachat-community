"""
OpenAI LLM Provider

Implementation of BaseLLMProvider for OpenAI's GPT models.
Supports GPT-4o, GPT-4o-mini, GPT-3.5-turbo, etc.
"""

import logging
from collections.abc import AsyncIterator

import openai
from openai import AsyncOpenAI

from backend.llm.base import BaseLLMProvider
from backend.llm.models import (
    LLMRequest,
    LLMResponse,
    LLMStreamChunk,
    LLMUsage,
    ModelInfo,
)

logger = logging.getLogger(__name__)


class OpenAIProvider(BaseLLMProvider):
    """
    OpenAI LLM provider implementation.

    Supports all OpenAI chat models including GPT-4o, GPT-4o-mini, and GPT-3.5-turbo.
    Uses the official openai Python SDK with async support.
    """

    def __init__(
        self,
        api_key: str,
        model: str = "gpt-4o",
        temperature: float = 0.0,
        max_tokens: int = 2000,
        timeout: int = 30,
    ):
        """
        Initialize OpenAI provider.

        Args:
            api_key: OpenAI API key
            model: Default model to use
            temperature: Default temperature
            max_tokens: Default max tokens
            timeout: Request timeout
        """
        super().__init__(
            provider_name="openai",
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
        )

        self.model = model
        self.client = AsyncOpenAI(
            api_key=api_key,
            timeout=float(timeout),
        )

        logger.info(f"OpenAI provider initialized with model: {model}", extra={"model": model})

    async def generate(self, request: LLMRequest) -> LLMResponse:
        """
        Generate completion using OpenAI API.

        Args:
            request: LLM request

        Returns:
            LLMResponse with generated content

        Raises:
            openai.APIError: On API errors
            openai.APITimeoutError: On timeout
        """
        request = self._apply_defaults(request)
        self._log_request(request)

        try:
            # Convert messages to OpenAI format
            messages = [{"role": msg.role, "content": msg.content} for msg in request.messages]

            # Call OpenAI API
            response = await self.client.chat.completions.create(
                model=request.model or self.model,
                messages=messages,
                temperature=request.temperature,
                max_tokens=request.max_tokens,
                **request.metadata,  # Additional OpenAI parameters
            )

            # Convert to our format
            llm_response = LLMResponse(
                content=response.choices[0].message.content or "",
                model=response.model,
                usage=LLMUsage(
                    prompt_tokens=response.usage.prompt_tokens,
                    completion_tokens=response.usage.completion_tokens,
                    total_tokens=response.usage.total_tokens,
                ),
                finish_reason=self._map_finish_reason(response.choices[0].finish_reason),
                provider="openai",
                metadata={
                    "id": response.id,
                    "created": response.created,
                    "system_fingerprint": response.system_fingerprint,
                },
            )

            self._log_response(llm_response)
            return llm_response

        except openai.APITimeoutError as e:
            logger.error(f"OpenAI API timeout: {e}")
            raise
        except openai.APIError as e:
            logger.error(f"OpenAI API error: {e}")
            raise

    async def stream(self, request: LLMRequest) -> AsyncIterator[LLMStreamChunk]:
        """
        Stream completion using OpenAI API.

        Args:
            request: LLM request

        Yields:
            LLMStreamChunk with content chunks

        Raises:
            openai.APIError: On API errors
        """
        request = self._apply_defaults(request)
        request.stream = True
        self._log_request(request)

        try:
            messages = [{"role": msg.role, "content": msg.content} for msg in request.messages]

            stream = await self.client.chat.completions.create(
                model=request.model or self.model,
                messages=messages,
                temperature=request.temperature,
                max_tokens=request.max_tokens,
                stream=True,
                **request.metadata,
            )

            async for chunk in stream:
                if chunk.choices[0].delta.content:
                    yield LLMStreamChunk(
                        content=chunk.choices[0].delta.content,
                        finish_reason=self._map_finish_reason(chunk.choices[0].finish_reason)
                        if chunk.choices[0].finish_reason
                        else None,
                        metadata={"id": chunk.id},
                    )

        except openai.APIError as e:
            logger.error(f"OpenAI streaming error: {e}")
            raise

    def count_tokens(self, text: str) -> int:
        """
        Count tokens using tiktoken.

        Args:
            text: Text to count

        Returns:
            Token count
        """
        try:
            import tiktoken

            # Get encoding for current model
            try:
                encoding = tiktoken.encoding_for_model(self.model)
            except KeyError:
                # Fallback to cl100k_base (used by gpt-4, gpt-3.5-turbo)
                encoding = tiktoken.get_encoding("cl100k_base")

            return len(encoding.encode(text))

        except ImportError:
            # Rough approximation if tiktoken not available
            # ~4 characters per token average
            return len(text) // 4
        except Exception:
            # Fallback when tiktoken can't load encodings (e.g., offline)
            return len(text) // 4

    def get_model_info(self, model_name: str | None = None) -> ModelInfo:
        """
        Get OpenAI model information.

        Args:
            model_name: Model to get info for (defaults to self.model)

        Returns:
            ModelInfo with model details
        """
        model = model_name or self.model

        # Model information database
        model_info_map = {
            "gpt-4o": ModelInfo(
                name="gpt-4o",
                provider="openai",
                context_window=128000,
                max_output=16384,
                cost_per_1k_input=0.0025,
                cost_per_1k_output=0.010,
                capabilities=["function-calling", "vision", "json-mode"],
            ),
            "gpt-4o-mini": ModelInfo(
                name="gpt-4o-mini",
                provider="openai",
                context_window=128000,
                max_output=16384,
                cost_per_1k_input=0.00015,
                cost_per_1k_output=0.0006,
                capabilities=["function-calling", "vision", "json-mode"],
            ),
            "gpt-3.5-turbo": ModelInfo(
                name="gpt-3.5-turbo",
                provider="openai",
                context_window=16385,
                max_output=4096,
                cost_per_1k_input=0.0005,
                cost_per_1k_output=0.0015,
                capabilities=["function-calling", "json-mode"],
            ),
        }

        return model_info_map.get(
            model,
            # Default fallback
            ModelInfo(
                name=model,
                provider="openai",
                context_window=128000,
                max_output=4096,
                capabilities=[],
            ),
        )

    def _map_finish_reason(self, reason: str | None) -> str:
        """Map OpenAI finish reason to our standard format."""
        if reason == "stop":
            return "stop"
        elif reason == "length":
            return "length"
        elif reason == "content_filter":
            return "content_filter"
        else:
            return "stop"  # Default
