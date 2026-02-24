"""
LLM Request and Response Models

Pydantic models for LLM provider interactions.
Provider-agnostic models that work across OpenAI, Anthropic, Google, etc.
"""

from typing import Any, Literal

from pydantic import BaseModel, Field


class LLMMessage(BaseModel):
    """Single message in an LLM conversation."""

    role: Literal["system", "user", "assistant"] = Field(..., description="Message role")
    content: str = Field(..., description="Message content", min_length=1)


class LLMRequest(BaseModel):
    """Request to an LLM provider."""

    messages: list[LLMMessage] = Field(..., description="Conversation messages", min_length=1)
    temperature: float | None = Field(
        None, ge=0.0, le=2.0, description="Sampling temperature (overrides default)"
    )
    max_tokens: int | None = Field(
        None, gt=0, description="Maximum tokens to generate (overrides default)"
    )
    stream: bool = Field(default=False, description="Whether to stream the response")
    model: str | None = Field(None, description="Specific model to use (overrides default)")
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional provider-specific parameters"
    )


class LLMUsage(BaseModel):
    """Token usage information."""

    prompt_tokens: int = Field(..., ge=0, description="Number of tokens in the prompt")
    completion_tokens: int = Field(..., ge=0, description="Number of tokens in the completion")
    total_tokens: int = Field(..., ge=0, description="Total tokens used")


class LLMResponse(BaseModel):
    """Response from an LLM provider."""

    content: str = Field(..., description="Generated text content")
    model: str = Field(..., description="Model that generated the response")
    usage: LLMUsage = Field(..., description="Token usage information")
    finish_reason: Literal["stop", "length", "content_filter", "error"] = Field(
        ..., description="Reason the generation stopped"
    )
    provider: str = Field(
        ..., description="Provider that handled the request (openai, anthropic, etc.)"
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional provider-specific response data"
    )


class LLMStreamChunk(BaseModel):
    """Streaming response chunk from an LLM provider."""

    content: str = Field(..., description="Chunk of generated text")
    finish_reason: Literal["stop", "length", "content_filter", "error"] | None = Field(
        None, description="Reason if this is the final chunk"
    )
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional chunk metadata")


class ModelInfo(BaseModel):
    """Information about a specific model."""

    name: str = Field(..., description="Model name/identifier")
    provider: str = Field(..., description="Provider name")
    context_window: int = Field(..., gt=0, description="Maximum context window in tokens")
    max_output: int = Field(..., gt=0, description="Maximum output tokens")
    cost_per_1k_input: float | None = Field(
        None, ge=0.0, description="Cost per 1000 input tokens (USD)"
    )
    cost_per_1k_output: float | None = Field(
        None, ge=0.0, description="Cost per 1000 output tokens (USD)"
    )
    capabilities: list[str] = Field(
        default_factory=list, description="Model capabilities (e.g., 'function-calling', 'vision')"
    )
