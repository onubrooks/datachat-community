"""
Local LLM Provider

Implementation of BaseLLMProvider for local models.
Supports Ollama, vLLM, llama.cpp server, and any OpenAI-compatible endpoint.
"""

import logging
from collections.abc import AsyncIterator

import httpx

from backend.llm.base import BaseLLMProvider
from backend.llm.models import (
    LLMRequest,
    LLMResponse,
    LLMStreamChunk,
    LLMUsage,
    ModelInfo,
)

logger = logging.getLogger(__name__)


class LocalProvider(BaseLLMProvider):
    """
    Local LLM provider implementation.

    Supports local model servers like Ollama, vLLM, and llama.cpp that
    expose an OpenAI-compatible API.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:11434",
        model: str = "llama3.1:8b",
        temperature: float = 0.0,
        max_tokens: int = 2000,
        timeout: int = 30,
    ):
        """
        Initialize local provider.

        Args:
            base_url: Base URL for local model server
            model: Model name (e.g., "llama3.1:8b" for Ollama)
            temperature: Default temperature
            max_tokens: Default max tokens
            timeout: Request timeout
        """
        super().__init__(
            provider_name="local",
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
        )

        self.base_url = base_url.rstrip("/")
        self.model = model
        self.client = httpx.AsyncClient(timeout=float(timeout))

        logger.info(
            f"Local provider initialized: {base_url} with model: {model}",
            extra={"base_url": base_url, "model": model},
        )

    async def generate(self, request: LLMRequest) -> LLMResponse:
        """
        Generate completion using local model server.

        Uses OpenAI-compatible /v1/chat/completions endpoint.
        """
        request = self._apply_defaults(request)
        self._log_request(request)

        # Build OpenAI-compatible request
        payload = {
            "model": request.model or self.model,
            "messages": [{"role": msg.role, "content": msg.content} for msg in request.messages],
            "temperature": request.temperature,
            "max_tokens": request.max_tokens,
        }

        # Try Ollama format first, then OpenAI-compatible format
        try:
            response = await self._call_ollama(payload)
        except Exception:
            response = await self._call_openai_compatible(payload)

        llm_response = LLMResponse(
            content=response.get("message", {}).get("content", "")
            or response.get("choices", [{}])[0].get("message", {}).get("content", ""),
            model=response.get("model", self.model),
            usage=LLMUsage(
                prompt_tokens=response.get("prompt_eval_count", 0)
                or response.get("usage", {}).get("prompt_tokens", 0),
                completion_tokens=response.get("eval_count", 0)
                or response.get("usage", {}).get("completion_tokens", 0),
                total_tokens=0,  # Calculated below
            ),
            finish_reason="stop",
            provider="local",
            metadata={"base_url": self.base_url},
        )

        llm_response.usage.total_tokens = (
            llm_response.usage.prompt_tokens + llm_response.usage.completion_tokens
        )

        self._log_response(llm_response)
        return llm_response

    async def stream(self, request: LLMRequest) -> AsyncIterator[LLMStreamChunk]:
        """Stream completion using local model server."""
        request = self._apply_defaults(request)
        self._log_request(request)

        payload = {
            "model": request.model or self.model,
            "messages": [{"role": msg.role, "content": msg.content} for msg in request.messages],
            "temperature": request.temperature,
            "max_tokens": request.max_tokens,
            "stream": True,
        }

        # Try Ollama streaming
        try:
            async with self.client.stream(
                "POST",
                f"{self.base_url}/api/chat",
                json=payload,
            ) as response:
                async for line in response.aiter_lines():
                    if line.strip():
                        import json

                        chunk_data = json.loads(line)
                        if content := chunk_data.get("message", {}).get("content"):
                            yield LLMStreamChunk(content=content, finish_reason=None)
        except Exception:
            # Fallback to OpenAI-compatible streaming
            async with self.client.stream(
                "POST",
                f"{self.base_url}/v1/chat/completions",
                json=payload,
            ) as response:
                async for line in response.aiter_lines():
                    if line.startswith("data: ") and not line.endswith("[DONE]"):
                        import json

                        chunk_data = json.loads(line[6:])
                        if (
                            content := chunk_data.get("choices", [{}])[0]
                            .get("delta", {})
                            .get("content")
                        ):
                            yield LLMStreamChunk(content=content, finish_reason=None)

    def count_tokens(self, text: str) -> int:
        """Count tokens (rough approximation for local models)."""
        return len(text) // 4

    def get_model_info(self, model_name: str | None = None) -> ModelInfo:
        """Get local model information."""
        model = model_name or self.model

        # Default info for unknown local models
        return ModelInfo(
            name=model,
            provider="local",
            context_window=4096,  # Conservative default
            max_output=2048,
            capabilities=[],
        )

    async def _call_ollama(self, payload: dict) -> dict:
        """Call Ollama-specific endpoint."""
        response = await self.client.post(
            f"{self.base_url}/api/chat",
            json=payload,
        )
        response.raise_for_status()
        return response.json()

    async def _call_openai_compatible(self, payload: dict) -> dict:
        """Call OpenAI-compatible endpoint."""
        response = await self.client.post(
            f"{self.base_url}/v1/chat/completions",
            json=payload,
        )
        response.raise_for_status()
        return response.json()
