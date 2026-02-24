"""
Tests for LLM request/response models.

Tests Pydantic models for LLM provider interactions.
"""

import pytest
from pydantic import ValidationError

from backend.llm.models import (
    LLMMessage,
    LLMRequest,
    LLMResponse,
    LLMStreamChunk,
    LLMUsage,
    ModelInfo,
)


class TestLLMMessage:
    """Test LLMMessage model."""

    def test_valid_message(self):
        """Test creating valid message."""
        msg = LLMMessage(role="user", content="Hello!")
        assert msg.role == "user"
        assert msg.content == "Hello!"

    def test_all_roles(self):
        """Test all valid roles."""
        for role in ["system", "user", "assistant"]:
            msg = LLMMessage(role=role, content="Test")
            assert msg.role == role

    def test_invalid_role(self):
        """Test invalid role is rejected."""
        with pytest.raises(ValidationError):
            LLMMessage(role="invalid", content="Test")

    def test_empty_content_rejected(self):
        """Test empty content is rejected."""
        with pytest.raises(ValidationError):
            LLMMessage(role="user", content="")


class TestLLMRequest:
    """Test LLMRequest model."""

    def test_valid_request(self):
        """Test creating valid request."""
        request = LLMRequest(
            messages=[LLMMessage(role="user", content="Hello!")],
            temperature=0.7,
            max_tokens=100,
        )
        assert len(request.messages) == 1
        assert request.temperature == 0.7
        assert request.max_tokens == 100
        assert request.stream is False

    def test_empty_messages_rejected(self):
        """Test empty messages list is rejected."""
        with pytest.raises(ValidationError):
            LLMRequest(messages=[])

    def test_temperature_bounds(self):
        """Test temperature validation."""
        # Valid temperatures
        LLMRequest(
            messages=[LLMMessage(role="user", content="Test")],
            temperature=0.0,
        )
        LLMRequest(
            messages=[LLMMessage(role="user", content="Test")],
            temperature=2.0,
        )

        # Invalid temperatures
        with pytest.raises(ValidationError):
            LLMRequest(
                messages=[LLMMessage(role="user", content="Test")],
                temperature=-0.1,
            )
        with pytest.raises(ValidationError):
            LLMRequest(
                messages=[LLMMessage(role="user", content="Test")],
                temperature=2.1,
            )

    def test_max_tokens_positive(self):
        """Test max_tokens must be positive."""
        with pytest.raises(ValidationError):
            LLMRequest(
                messages=[LLMMessage(role="user", content="Test")],
                max_tokens=0,
            )
        with pytest.raises(ValidationError):
            LLMRequest(
                messages=[LLMMessage(role="user", content="Test")],
                max_tokens=-1,
            )

    def test_optional_fields(self):
        """Test optional fields have correct defaults."""
        request = LLMRequest(messages=[LLMMessage(role="user", content="Test")])
        assert request.temperature is None
        assert request.max_tokens is None
        assert request.stream is False
        assert request.model is None
        assert request.metadata == {}

    def test_metadata(self):
        """Test custom metadata."""
        request = LLMRequest(
            messages=[LLMMessage(role="user", content="Test")],
            metadata={"custom": "value", "foo": 123},
        )
        assert request.metadata["custom"] == "value"
        assert request.metadata["foo"] == 123


class TestLLMUsage:
    """Test LLMUsage model."""

    def test_valid_usage(self):
        """Test creating valid usage."""
        usage = LLMUsage(
            prompt_tokens=10,
            completion_tokens=20,
            total_tokens=30,
        )
        assert usage.prompt_tokens == 10
        assert usage.completion_tokens == 20
        assert usage.total_tokens == 30

    def test_non_negative_tokens(self):
        """Test token counts must be non-negative."""
        # Valid zero tokens
        LLMUsage(prompt_tokens=0, completion_tokens=0, total_tokens=0)

        # Invalid negative tokens
        with pytest.raises(ValidationError):
            LLMUsage(prompt_tokens=-1, completion_tokens=0, total_tokens=0)
        with pytest.raises(ValidationError):
            LLMUsage(prompt_tokens=0, completion_tokens=-1, total_tokens=0)
        with pytest.raises(ValidationError):
            LLMUsage(prompt_tokens=0, completion_tokens=0, total_tokens=-1)


class TestLLMResponse:
    """Test LLMResponse model."""

    def test_valid_response(self):
        """Test creating valid response."""
        response = LLMResponse(
            content="Hello!",
            model="gpt-4o",
            usage=LLMUsage(
                prompt_tokens=10,
                completion_tokens=5,
                total_tokens=15,
            ),
            finish_reason="stop",
            provider="openai",
        )
        assert response.content == "Hello!"
        assert response.model == "gpt-4o"
        assert response.finish_reason == "stop"
        assert response.provider == "openai"

    def test_all_finish_reasons(self):
        """Test all valid finish reasons."""
        for reason in ["stop", "length", "content_filter", "error"]:
            response = LLMResponse(
                content="Test",
                model="test",
                usage=LLMUsage(
                    prompt_tokens=1,
                    completion_tokens=1,
                    total_tokens=2,
                ),
                finish_reason=reason,
                provider="test",
            )
            assert response.finish_reason == reason

    def test_invalid_finish_reason(self):
        """Test invalid finish reason is rejected."""
        with pytest.raises(ValidationError):
            LLMResponse(
                content="Test",
                model="test",
                usage=LLMUsage(
                    prompt_tokens=1,
                    completion_tokens=1,
                    total_tokens=2,
                ),
                finish_reason="invalid",
                provider="test",
            )

    def test_metadata(self):
        """Test response metadata."""
        response = LLMResponse(
            content="Test",
            model="test",
            usage=LLMUsage(
                prompt_tokens=1,
                completion_tokens=1,
                total_tokens=2,
            ),
            finish_reason="stop",
            provider="test",
            metadata={"id": "test-123", "custom": "value"},
        )
        assert response.metadata["id"] == "test-123"
        assert response.metadata["custom"] == "value"


class TestLLMStreamChunk:
    """Test LLMStreamChunk model."""

    def test_valid_chunk(self):
        """Test creating valid stream chunk."""
        chunk = LLMStreamChunk(content="Hello")
        assert chunk.content == "Hello"
        assert chunk.finish_reason is None

    def test_final_chunk(self):
        """Test chunk with finish reason."""
        chunk = LLMStreamChunk(
            content="",
            finish_reason="stop",
        )
        assert chunk.content == ""
        assert chunk.finish_reason == "stop"

    def test_chunk_metadata(self):
        """Test chunk metadata."""
        chunk = LLMStreamChunk(
            content="Test",
            metadata={"id": "chunk-1"},
        )
        assert chunk.metadata["id"] == "chunk-1"


class TestModelInfo:
    """Test ModelInfo model."""

    def test_valid_model_info(self):
        """Test creating valid model info."""
        info = ModelInfo(
            name="gpt-4o",
            provider="openai",
            context_window=128000,
            max_output=16384,
            cost_per_1k_input=0.0025,
            cost_per_1k_output=0.010,
            capabilities=["function-calling", "vision"],
        )
        assert info.name == "gpt-4o"
        assert info.provider == "openai"
        assert info.context_window == 128000
        assert info.max_output == 16384
        assert info.cost_per_1k_input == 0.0025
        assert info.cost_per_1k_output == 0.010
        assert "function-calling" in info.capabilities

    def test_positive_context_window(self):
        """Test context window must be positive."""
        with pytest.raises(ValidationError):
            ModelInfo(
                name="test",
                provider="test",
                context_window=0,
                max_output=100,
            )

    def test_positive_max_output(self):
        """Test max output must be positive."""
        with pytest.raises(ValidationError):
            ModelInfo(
                name="test",
                provider="test",
                context_window=100,
                max_output=0,
            )

    def test_optional_costs(self):
        """Test cost fields are optional."""
        info = ModelInfo(
            name="test",
            provider="test",
            context_window=100,
            max_output=100,
        )
        assert info.cost_per_1k_input is None
        assert info.cost_per_1k_output is None

    def test_non_negative_costs(self):
        """Test costs must be non-negative."""
        with pytest.raises(ValidationError):
            ModelInfo(
                name="test",
                provider="test",
                context_window=100,
                max_output=100,
                cost_per_1k_input=-0.01,
            )
