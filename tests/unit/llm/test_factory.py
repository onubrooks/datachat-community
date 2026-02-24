"""
Tests for LLM Provider Factory.

Tests provider creation, configuration, and agent-specific overrides.
"""

import pytest

from backend.config import LLMSettings
from backend.llm.anthropic import AnthropicProvider
from backend.llm.factory import LLMProviderFactory
from backend.llm.google import GoogleProvider
from backend.llm.local import LocalProvider
from backend.llm.openai import OpenAIProvider


@pytest.fixture
def mock_config():
    """Mock LLM configuration with all providers configured."""
    return LLMSettings(
        default_provider="openai",
        classifier_provider="anthropic",
        sql_provider="openai",
        fallback_provider="google",
        openai_api_key="sk-test-openai-key-1234567890",
        openai_model="gpt-4o",
        openai_model_mini="gpt-4o-mini",
        anthropic_api_key="sk-ant-test-anthropic-key-1234567890",
        anthropic_model="claude-3-5-sonnet-20241022",
        anthropic_model_mini="claude-3-5-haiku-20241022",
        google_api_key="test-google-key-1234567890",
        google_model="gemini-1.5-pro",
        google_model_mini="gemini-1.5-flash",
        local_base_url="http://localhost:11434",
        local_model="llama3.1:8b",
        temperature=0.0,
        max_tokens=2000,
        timeout=30,
    )


class TestProviderRegistry:
    """Test provider registry."""

    def test_all_providers_registered(self):
        """Test all providers are in registry."""
        assert "openai" in LLMProviderFactory.PROVIDERS
        assert "anthropic" in LLMProviderFactory.PROVIDERS
        assert "google" in LLMProviderFactory.PROVIDERS
        assert "local" in LLMProviderFactory.PROVIDERS

    def test_provider_classes(self):
        """Test provider classes are correct."""
        assert LLMProviderFactory.PROVIDERS["openai"] == OpenAIProvider
        assert LLMProviderFactory.PROVIDERS["anthropic"] == AnthropicProvider
        assert LLMProviderFactory.PROVIDERS["google"] == GoogleProvider
        assert LLMProviderFactory.PROVIDERS["local"] == LocalProvider


class TestCreateProvider:
    """Test create_provider method."""

    def test_create_openai_provider(self, mock_config):
        """Test creating OpenAI provider."""
        provider = LLMProviderFactory.create_provider("openai", mock_config)
        assert isinstance(provider, OpenAIProvider)
        assert provider.model == "gpt-4o"
        assert provider.temperature == 0.0

    def test_create_openai_mini(self, mock_config):
        """Test creating OpenAI provider with mini model."""
        provider = LLMProviderFactory.create_provider("openai", mock_config, model_type="mini")
        assert isinstance(provider, OpenAIProvider)
        assert provider.model == "gpt-4o-mini"

    def test_create_anthropic_provider(self, mock_config):
        """Test creating Anthropic provider."""
        provider = LLMProviderFactory.create_provider("anthropic", mock_config)
        assert isinstance(provider, AnthropicProvider)
        assert provider.model == "claude-3-5-sonnet-20241022"

    def test_create_anthropic_mini(self, mock_config):
        """Test creating Anthropic provider with mini model."""
        provider = LLMProviderFactory.create_provider("anthropic", mock_config, model_type="mini")
        assert isinstance(provider, AnthropicProvider)
        assert provider.model == "claude-3-5-haiku-20241022"

    def test_create_google_provider(self, mock_config):
        """Test creating Google provider."""
        provider = LLMProviderFactory.create_provider("google", mock_config)
        assert isinstance(provider, GoogleProvider)
        assert provider.model == "gemini-1.5-pro"

    def test_create_google_mini(self, mock_config):
        """Test creating Google provider with mini model."""
        provider = LLMProviderFactory.create_provider("google", mock_config, model_type="mini")
        assert isinstance(provider, GoogleProvider)
        assert provider.model == "gemini-1.5-flash"

    def test_create_local_provider(self, mock_config):
        """Test creating Local provider."""
        provider = LLMProviderFactory.create_provider("local", mock_config)
        assert isinstance(provider, LocalProvider)
        assert provider.model == "llama3.1:8b"
        assert provider.base_url == "http://localhost:11434"

    def test_unknown_provider(self, mock_config):
        """Test unknown provider raises ValueError."""
        with pytest.raises(ValueError, match="Unknown provider type"):
            LLMProviderFactory.create_provider("unknown", mock_config)

    def test_missing_openai_api_key(self, mock_config):
        """Test creating OpenAI provider without API key fails."""
        mock_config.openai_api_key = None
        with pytest.raises(ValueError, match="OpenAI API key is required"):
            LLMProviderFactory.create_provider("openai", mock_config)

    def test_missing_anthropic_api_key(self, mock_config):
        """Test creating Anthropic provider without API key fails."""
        mock_config.anthropic_api_key = None
        with pytest.raises(ValueError, match="Anthropic API key is required"):
            LLMProviderFactory.create_provider("anthropic", mock_config)

    def test_missing_google_api_key(self, mock_config):
        """Test creating Google provider without API key fails."""
        mock_config.google_api_key = None
        with pytest.raises(ValueError, match="Google API key is required"):
            LLMProviderFactory.create_provider("google", mock_config)


class TestCreateDefaultProvider:
    """Test create_default_provider method."""

    def test_creates_default_provider(self, mock_config):
        """Test creates provider based on default_provider."""
        provider = LLMProviderFactory.create_default_provider(mock_config)
        assert isinstance(provider, OpenAIProvider)

    def test_uses_main_model_by_default(self, mock_config):
        """Test uses main model by default."""
        provider = LLMProviderFactory.create_default_provider(mock_config)
        assert provider.model == "gpt-4o"

    def test_can_use_mini_model(self, mock_config):
        """Test can specify mini model."""
        provider = LLMProviderFactory.create_default_provider(mock_config, model_type="mini")
        assert provider.model == "gpt-4o-mini"

    def test_respects_default_provider_setting(self, mock_config):
        """Test respects different default_provider."""
        mock_config.default_provider = "anthropic"
        provider = LLMProviderFactory.create_default_provider(mock_config)
        assert isinstance(provider, AnthropicProvider)


class TestCreateAgentProvider:
    """Test create_agent_provider method."""

    def test_classifier_uses_override(self, mock_config):
        """Test classifier agent uses classifier_provider override."""
        provider = LLMProviderFactory.create_agent_provider("classifier", mock_config)
        assert isinstance(provider, AnthropicProvider)

    def test_sql_uses_override(self, mock_config):
        """Test SQL agent uses sql_provider override."""
        provider = LLMProviderFactory.create_agent_provider("sql", mock_config)
        assert isinstance(provider, OpenAIProvider)

    def test_fallback_to_default(self, mock_config):
        """Test falls back to default_provider when no override."""
        provider = LLMProviderFactory.create_agent_provider("context", mock_config)
        assert isinstance(provider, OpenAIProvider)

    def test_uses_mini_model(self, mock_config):
        """Test can specify mini model for agent."""
        provider = LLMProviderFactory.create_agent_provider(
            "classifier", mock_config, model_type="mini"
        )
        assert isinstance(provider, AnthropicProvider)
        assert provider.model == "claude-3-5-haiku-20241022"

    def test_nonexistent_override_uses_default(self, mock_config):
        """Test nonexistent override attribute uses default."""
        # "executor" agent doesn't have executor_provider override
        provider = LLMProviderFactory.create_agent_provider("executor", mock_config)
        assert isinstance(provider, OpenAIProvider)

    def test_none_override_uses_default(self, mock_config):
        """Test None override value uses default."""
        mock_config.classifier_provider = None
        provider = LLMProviderFactory.create_agent_provider("classifier", mock_config)
        assert isinstance(provider, OpenAIProvider)


class TestProviderConfiguration:
    """Test provider configuration is passed correctly."""

    def test_temperature_passed(self, mock_config):
        """Test temperature is passed to provider."""
        mock_config.temperature = 0.7
        provider = LLMProviderFactory.create_provider("openai", mock_config)
        assert provider.temperature == 0.7

    def test_max_tokens_passed(self, mock_config):
        """Test max_tokens is passed to provider."""
        mock_config.max_tokens = 1000
        provider = LLMProviderFactory.create_provider("openai", mock_config)
        assert provider.max_tokens == 1000

    def test_timeout_passed(self, mock_config):
        """Test timeout is passed to provider."""
        mock_config.timeout = 60
        provider = LLMProviderFactory.create_provider("openai", mock_config)
        assert provider.timeout == 60

    def test_local_base_url_passed(self, mock_config):
        """Test local base_url is passed correctly."""
        mock_config.local_base_url = "http://localhost:8080"
        provider = LLMProviderFactory.create_provider("local", mock_config)
        assert provider.base_url == "http://localhost:8080"
