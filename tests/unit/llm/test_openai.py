"""
Tests for OpenAI Provider.

Tests OpenAI provider implementation with mocked API calls.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.llm.models import LLMMessage, LLMRequest
from backend.llm.openai import OpenAIProvider


@pytest.fixture
def provider():
    """Create OpenAI provider instance."""
    return OpenAIProvider(
        api_key="sk-test-key-1234567890abcdefghij",
        model="gpt-4o",
        temperature=0.0,
        max_tokens=2000,
        timeout=30,
    )


class TestOpenAIProviderInit:
    """Test OpenAI provider initialization."""

    def test_initialization(self, provider):
        """Test provider initializes correctly."""
        assert provider.model == "gpt-4o"
        assert provider.temperature == 0.0
        assert provider.max_tokens == 2000
        assert provider.timeout == 30
        assert provider.provider_name == "openai"

    def test_client_created(self, provider):
        """Test AsyncOpenAI client is created."""
        assert provider.client is not None


class TestGenerate:
    """Test generate method."""

    @pytest.mark.asyncio
    async def test_successful_generation(self, provider):
        """Test successful completion generation."""
        # Mock OpenAI response
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Hello! How can I help?"
        mock_response.choices[0].finish_reason = "stop"
        mock_response.model = "gpt-4o"
        mock_response.usage.prompt_tokens = 10
        mock_response.usage.completion_tokens = 5
        mock_response.usage.total_tokens = 15
        mock_response.id = "chatcmpl-123"
        mock_response.created = 1234567890
        mock_response.system_fingerprint = "fp_123"

        with patch.object(
            provider.client.chat.completions,
            "create",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            request = LLMRequest(
                messages=[LLMMessage(role="user", content="Hello!")],
            )
            response = await provider.generate(request)

            assert response.content == "Hello! How can I help?"
            assert response.model == "gpt-4o"
            assert response.usage.prompt_tokens == 10
            assert response.usage.completion_tokens == 5
            assert response.usage.total_tokens == 15
            assert response.finish_reason == "stop"
            assert response.provider == "openai"

    @pytest.mark.asyncio
    async def test_applies_defaults(self, provider):
        """Test request defaults are applied."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Test"
        mock_response.choices[0].finish_reason = "stop"
        mock_response.model = "gpt-4o"
        mock_response.usage.prompt_tokens = 1
        mock_response.usage.completion_tokens = 1
        mock_response.usage.total_tokens = 2
        mock_response.id = "test"
        mock_response.created = 1234567890
        mock_response.system_fingerprint = "fp_123"

        mock_create = AsyncMock(return_value=mock_response)

        with patch.object(provider.client.chat.completions, "create", mock_create):
            request = LLMRequest(
                messages=[LLMMessage(role="user", content="Test")],
            )
            await provider.generate(request)

            # Check that defaults were applied
            call_kwargs = mock_create.call_args.kwargs
            assert call_kwargs["temperature"] == 0.0
            assert call_kwargs["max_tokens"] == 2000

    @pytest.mark.asyncio
    async def test_request_overrides_defaults(self, provider):
        """Test request can override defaults."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Test"
        mock_response.choices[0].finish_reason = "stop"
        mock_response.model = "gpt-4o"
        mock_response.usage.prompt_tokens = 1
        mock_response.usage.completion_tokens = 1
        mock_response.usage.total_tokens = 2
        mock_response.id = "test"
        mock_response.created = 1234567890
        mock_response.system_fingerprint = "fp_123"

        mock_create = AsyncMock(return_value=mock_response)

        with patch.object(provider.client.chat.completions, "create", mock_create):
            request = LLMRequest(
                messages=[LLMMessage(role="user", content="Test")],
                temperature=0.7,
                max_tokens=500,
            )
            await provider.generate(request)

            call_kwargs = mock_create.call_args.kwargs
            assert call_kwargs["temperature"] == 0.7
            assert call_kwargs["max_tokens"] == 500


class TestStream:
    """Test stream method."""

    @pytest.mark.asyncio
    async def test_successful_streaming(self, provider):
        """Test successful streaming."""
        # Mock stream chunks
        mock_chunks = [
            MagicMock(
                choices=[
                    MagicMock(
                        delta=MagicMock(content="Hello"),
                        finish_reason=None,
                    )
                ],
                id="chunk-1",
            ),
            MagicMock(
                choices=[
                    MagicMock(
                        delta=MagicMock(content=" world"),
                        finish_reason=None,
                    )
                ],
                id="chunk-2",
            ),
            MagicMock(
                choices=[
                    MagicMock(
                        delta=MagicMock(content=None),
                        finish_reason="stop",
                    )
                ],
                id="chunk-3",
            ),
        ]

        async def mock_stream():
            for chunk in mock_chunks:
                yield chunk

        with patch.object(
            provider.client.chat.completions,
            "create",
            new_callable=AsyncMock,
            return_value=mock_stream(),
        ):
            request = LLMRequest(
                messages=[LLMMessage(role="user", content="Hello!")],
            )

            chunks = []
            async for chunk in provider.stream(request):
                chunks.append(chunk)

            assert len(chunks) == 2  # Only chunks with content
            assert chunks[0].content == "Hello"
            assert chunks[1].content == " world"


class TestCountTokens:
    """Test count_tokens method."""

    def test_count_tokens_with_tiktoken(self, provider):
        """Test token counting with tiktoken."""
        text = "Hello, world!"
        count = provider.count_tokens(text)
        assert count > 0
        assert isinstance(count, int)

    def test_count_tokens_fallback(self, provider):
        """Test token counting fallback without tiktoken."""

        # Mock tiktoken.encoding_for_model to raise ImportError
        def mock_import_error(*args, **kwargs):
            raise ImportError("tiktoken not available")

        with patch("tiktoken.encoding_for_model", side_effect=ImportError):
            text = "Hello, world!"
            count = provider.count_tokens(text)
            # Fallback: len(text) // 4
            assert count == len(text) // 4


class TestGetModelInfo:
    """Test get_model_info method."""

    def test_gpt4o_info(self, provider):
        """Test GPT-4o model info."""
        info = provider.get_model_info("gpt-4o")
        assert info.name == "gpt-4o"
        assert info.provider == "openai"
        assert info.context_window == 128000
        assert info.max_output == 16384
        assert info.cost_per_1k_input == 0.0025
        assert info.cost_per_1k_output == 0.010
        assert "function-calling" in info.capabilities

    def test_gpt4o_mini_info(self, provider):
        """Test GPT-4o-mini model info."""
        info = provider.get_model_info("gpt-4o-mini")
        assert info.name == "gpt-4o-mini"
        assert info.context_window == 128000
        assert info.cost_per_1k_input == 0.00015

    def test_gpt35_turbo_info(self, provider):
        """Test GPT-3.5-turbo model info."""
        info = provider.get_model_info("gpt-3.5-turbo")
        assert info.name == "gpt-3.5-turbo"
        assert info.context_window == 16385

    def test_default_model_info(self, provider):
        """Test defaults to provider's model."""
        info = provider.get_model_info()
        assert info.name == "gpt-4o"

    def test_unknown_model_fallback(self, provider):
        """Test unknown model gets fallback info."""
        info = provider.get_model_info("gpt-99")
        assert info.name == "gpt-99"
        assert info.provider == "openai"
        assert info.context_window == 128000


class TestErrorHandling:
    """Test error handling."""

    @pytest.mark.asyncio
    async def test_api_timeout_error(self, provider):
        """Test handling of API timeout errors."""
        import openai

        with patch.object(
            provider.client.chat.completions,
            "create",
            new_callable=AsyncMock,
            side_effect=openai.APITimeoutError("Request timed out"),
        ):
            request = LLMRequest(
                messages=[LLMMessage(role="user", content="Test")],
            )

            with pytest.raises(openai.APITimeoutError):
                await provider.generate(request)

    @pytest.mark.asyncio
    async def test_api_error(self, provider):
        """Test handling of general API errors."""
        from unittest.mock import Mock

        import openai

        # Create a mock request for the error
        mock_request = Mock()
        mock_request.method = "POST"
        mock_request.url = "https://api.openai.com/v1/chat/completions"

        with patch.object(
            provider.client.chat.completions,
            "create",
            new_callable=AsyncMock,
            side_effect=openai.APIError("API error occurred", request=mock_request, body=None),
        ):
            request = LLMRequest(
                messages=[LLMMessage(role="user", content="Test")],
            )

            with pytest.raises(openai.APIError):
                await provider.generate(request)

    @pytest.mark.asyncio
    async def test_streaming_api_error(self, provider):
        """Test handling of streaming API errors."""
        from unittest.mock import Mock

        import openai

        # Create a mock request for the error
        mock_request = Mock()
        mock_request.method = "POST"
        mock_request.url = "https://api.openai.com/v1/chat/completions"

        with patch.object(
            provider.client.chat.completions,
            "create",
            new_callable=AsyncMock,
            side_effect=openai.APIError("Streaming error", request=mock_request, body=None),
        ):
            request = LLMRequest(
                messages=[LLMMessage(role="user", content="Test")],
            )

            with pytest.raises(openai.APIError):
                async for _ in provider.stream(request):
                    pass


class TestTokenCounting:
    """Test token counting edge cases."""

    def test_count_tokens_unknown_model_fallback(self, provider):
        """Test token counting with unknown model uses fallback encoding."""
        # Create provider with unknown model
        provider.model = "unknown-model-9999"

        text = "Hello, world!"
        count = provider.count_tokens(text)

        # Should still return a count using fallback encoding
        assert count > 0
        assert isinstance(count, int)


class TestFinishReasonMapping:
    """Test finish reason mapping."""

    def test_map_stop(self, provider):
        """Test 'stop' finish reason."""
        assert provider._map_finish_reason("stop") == "stop"

    def test_map_length(self, provider):
        """Test 'length' finish reason."""
        assert provider._map_finish_reason("length") == "length"

    def test_map_content_filter(self, provider):
        """Test 'content_filter' finish reason."""
        assert provider._map_finish_reason("content_filter") == "content_filter"

    def test_map_unknown(self, provider):
        """Test unknown finish reason defaults to 'stop'."""
        assert provider._map_finish_reason("unknown") == "stop"
        assert provider._map_finish_reason(None) == "stop"
