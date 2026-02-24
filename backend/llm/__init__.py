"""
LLM Provider Module

Multi-provider LLM abstraction layer supporting OpenAI, Anthropic, Google, and Local models.

Usage:
    from backend.llm import LLMProviderFactory, LLMRequest, LLMMessage
    from backend.config import get_settings

    config = get_settings()
    provider = LLMProviderFactory.create_default_provider(config)

    request = LLMRequest(
        messages=[LLMMessage(role="user", content="Hello!")],
    )

    response = await provider.generate(request)
    print(response.content)
"""

from backend.llm.anthropic import AnthropicProvider
from backend.llm.base import BaseLLMProvider
from backend.llm.factory import LLMProviderFactory
from backend.llm.google import GoogleProvider
from backend.llm.local import LocalProvider
from backend.llm.models import (
    LLMMessage,
    LLMRequest,
    LLMResponse,
    LLMStreamChunk,
    LLMUsage,
    ModelInfo,
)
from backend.llm.openai import OpenAIProvider

__all__ = [
    # Base classes
    "BaseLLMProvider",
    # Models
    "LLMMessage",
    "LLMRequest",
    "LLMResponse",
    "LLMStreamChunk",
    "LLMUsage",
    "ModelInfo",
    # Factory
    "LLMProviderFactory",
    # Providers
    "OpenAIProvider",
    "AnthropicProvider",
    "GoogleProvider",
    "LocalProvider",
]
