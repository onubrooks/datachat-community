"""
Google LLM Provider

Implementation of BaseLLMProvider for Google's Gemini models.
Supports Gemini 1.5 Pro, Gemini 1.5 Flash, etc.
"""

import logging
import warnings
from collections.abc import AsyncIterator
from typing import Any

from backend.llm.base import BaseLLMProvider
from backend.llm.models import (
    LLMRequest,
    LLMResponse,
    LLMStreamChunk,
    LLMUsage,
    ModelInfo,
)

logger = logging.getLogger(__name__)


class GoogleProvider(BaseLLMProvider):
    """
    Google (Gemini) LLM provider implementation.

    Supports Gemini 1.5 Pro and Gemini 1.5 Flash models.
    Uses the google-generativeai Python SDK.
    """

    def __init__(
        self,
        api_key: str,
        model: str = "gemini-1.5-pro",
        temperature: float = 0.0,
        max_tokens: int = 2000,
        timeout: int = 30,
    ):
        """Initialize Google provider."""
        super().__init__(
            provider_name="google",
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
        )

        self.model = model
        self.api_key = api_key

        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", FutureWarning)
                import google.generativeai as genai

            genai.configure(api_key=api_key)
            self.genai = genai
            self.client = genai.GenerativeModel(model)
        except ImportError:
            logger.warning(
                "google-generativeai package not installed. "
                "Install with: pip install google-generativeai"
            )
            self.genai = None
            self.client = None

        logger.info(f"Google provider initialized with model: {model}", extra={"model": model})

    async def generate(self, request: LLMRequest) -> LLMResponse:
        """Generate completion using Google Gemini API."""
        if not self.genai:
            raise ImportError("google-generativeai package not installed")

        request = self._apply_defaults(request)
        self._log_request(request)

        model_name = request.model or self.model
        client = self.genai.GenerativeModel(model_name)

        # Convert messages to Gemini format
        prompt_parts = []
        for msg in request.messages:
            if msg.role == "system":
                prompt_parts.append(f"System: {msg.content}")
            elif msg.role == "user":
                prompt_parts.append(f"User: {msg.content}")
            elif msg.role == "assistant":
                prompt_parts.append(f"Assistant: {msg.content}")

        prompt = "\n\n".join(prompt_parts)

        response = await client.generate_content_async(
            prompt,
            generation_config=self.genai.types.GenerationConfig(
                temperature=request.temperature,
                max_output_tokens=request.max_tokens,
            ),
        )
        response_text = self._extract_response_text(response)
        finish_reason = self._extract_finish_reason(response)

        # Estimate token usage (Gemini doesn't always provide exact counts)
        prompt_tokens = self.count_tokens(prompt)
        completion_tokens = self.count_tokens(response_text)

        llm_response = LLMResponse(
            content=response_text,
            model=model_name,
            usage=LLMUsage(
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=prompt_tokens + completion_tokens,
            ),
            finish_reason=finish_reason,
            provider="google",
            metadata={"raw_finish_reason": self._extract_raw_finish_reason(response)},
        )

        self._log_response(llm_response)
        return llm_response

    async def stream(self, request: LLMRequest) -> AsyncIterator[LLMStreamChunk]:
        """Stream completion using Google Gemini API."""
        if not self.genai:
            raise ImportError("google-generativeai package not installed")

        request = self._apply_defaults(request)
        self._log_request(request)

        model_name = request.model or self.model
        client = self.genai.GenerativeModel(model_name)

        prompt_parts = []
        for msg in request.messages:
            if msg.role == "system":
                prompt_parts.append(f"System: {msg.content}")
            else:
                prompt_parts.append(f"{msg.role.capitalize()}: {msg.content}")

        prompt = "\n\n".join(prompt_parts)

        response = await client.generate_content_async(
            prompt,
            generation_config=self.genai.types.GenerationConfig(
                temperature=request.temperature,
                max_output_tokens=request.max_tokens,
            ),
            stream=True,
        )

        async for chunk in response:
            if chunk.text:
                yield LLMStreamChunk(content=chunk.text, finish_reason=None)

    def count_tokens(self, text: str) -> int:
        """Count tokens for Google models."""
        # Rough approximation
        return len(text) // 4

    def _extract_response_text(self, response: Any) -> str:
        text = getattr(response, "text", "")
        if isinstance(text, str):
            return text
        if text is None:
            return ""
        return str(text)

    def _extract_raw_finish_reason(self, response: Any) -> str:
        candidates = getattr(response, "candidates", None)
        if not candidates:
            return ""
        first = candidates[0] if len(candidates) > 0 else None
        if first is None:
            return ""
        reason = getattr(first, "finish_reason", "")
        return str(reason or "")

    def _extract_finish_reason(self, response: Any) -> str:
        raw_reason = self._extract_raw_finish_reason(response).lower()
        if any(token in raw_reason for token in ("max_tokens", "length")):
            return "length"
        if any(token in raw_reason for token in ("safety", "blocked", "recitation")):
            return "content_filter"
        if "error" in raw_reason:
            return "error"
        return "stop"

    def get_model_info(self, model_name: str | None = None) -> ModelInfo:
        """Get Google model information."""
        model = model_name or self.model

        model_info_map = {
            "gemini-1.5-pro": ModelInfo(
                name="gemini-1.5-pro",
                provider="google",
                context_window=1048576,  # 1M tokens
                max_output=8192,
                cost_per_1k_input=0.00125,
                cost_per_1k_output=0.005,
                capabilities=["vision", "function-calling"],
            ),
            "gemini-1.5-flash": ModelInfo(
                name="gemini-1.5-flash",
                provider="google",
                context_window=1048576,
                max_output=8192,
                cost_per_1k_input=0.000075,
                cost_per_1k_output=0.0003,
                capabilities=["vision", "function-calling"],
            ),
        }

        return model_info_map.get(
            model,
            ModelInfo(
                name=model,
                provider="google",
                context_window=1048576,
                max_output=8192,
                capabilities=[],
            ),
        )
