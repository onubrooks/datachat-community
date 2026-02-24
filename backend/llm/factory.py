"""
LLM Provider Factory

Factory and registry for creating LLM provider instances based on configuration.
Supports OpenAI, Anthropic, Google, and Local providers with fallback logic.
"""

import logging
from typing import Literal

from backend.config import LLMSettings
from backend.llm.anthropic import AnthropicProvider
from backend.llm.base import BaseLLMProvider
from backend.llm.google import GoogleProvider
from backend.llm.local import LocalProvider
from backend.llm.openai import OpenAIProvider

logger = logging.getLogger(__name__)


class LLMProviderFactory:
    """
    Factory for creating LLM provider instances.

    Handles provider selection, configuration, and fallback logic.
    Supports OpenAI, Anthropic, Google, and Local providers.
    """

    # Registry of available providers
    PROVIDERS = {
        "openai": OpenAIProvider,
        "anthropic": AnthropicProvider,
        "google": GoogleProvider,
        "local": LocalProvider,
    }

    @staticmethod
    def create_provider(
        provider_type: Literal["openai", "anthropic", "google", "local"],
        config: LLMSettings,
        model_type: Literal["main", "mini"] = "main",
    ) -> BaseLLMProvider:
        """
        Create an LLM provider instance.

        Args:
            provider_type: Type of provider to create
            config: LLM configuration settings
            model_type: Use main model or mini model (default: main)

        Returns:
            Configured provider instance

        Raises:
            ValueError: If provider type is unknown or required config is missing
        """
        if provider_type not in LLMProviderFactory.PROVIDERS:
            raise ValueError(
                f"Unknown provider type: {provider_type}. "
                f"Available providers: {list(LLMProviderFactory.PROVIDERS.keys())}"
            )

        logger.info(
            f"Creating {provider_type} provider with {model_type} model",
            extra={"provider": provider_type, "model_type": model_type},
        )

        # Create provider based on type
        if provider_type == "openai":
            return LLMProviderFactory._create_openai(config, model_type)
        elif provider_type == "anthropic":
            return LLMProviderFactory._create_anthropic(config, model_type)
        elif provider_type == "google":
            return LLMProviderFactory._create_google(config, model_type)
        elif provider_type == "local":
            return LLMProviderFactory._create_local(config, model_type)

        # This should never be reached due to the check above
        raise ValueError(f"Provider {provider_type} not implemented")  # pragma: no cover

    @staticmethod
    def create_default_provider(
        config: LLMSettings,
        model_type: Literal["main", "mini"] = "main",
    ) -> BaseLLMProvider:
        """
        Create provider using default_provider from config.

        Args:
            config: LLM configuration
            model_type: Use main or mini model

        Returns:
            Default provider instance
        """
        return LLMProviderFactory.create_provider(
            config.default_provider,
            config,
            model_type,
        )

    @staticmethod
    def create_agent_provider(
        agent_name: str,
        config: LLMSettings,
        model_type: Literal["main", "mini"] = "main",
    ) -> BaseLLMProvider:
        """
        Create provider for a specific agent with override support.

        Checks for agent-specific provider override (e.g., classifier_provider)
        and falls back to default_provider if not specified.

        Args:
            agent_name: Name of the agent (e.g., "classifier", "sql")
            config: LLM configuration
            model_type: Use main or mini model

        Returns:
            Provider instance for the agent
        """
        # Check for agent-specific override
        override_attr = f"{agent_name}_provider"
        provider_type = getattr(config, override_attr, None) or config.default_provider

        logger.info(
            f"Creating provider for {agent_name} agent",
            extra={
                "agent": agent_name,
                "provider": provider_type,
                "has_override": hasattr(config, override_attr)
                and getattr(config, override_attr) is not None,
            },
        )

        return LLMProviderFactory.create_provider(
            provider_type,
            config,
            model_type,
        )

    @staticmethod
    def _create_openai(
        config: LLMSettings,
        model_type: Literal["main", "mini"],
    ) -> OpenAIProvider:
        """Create OpenAI provider instance."""
        if not config.openai_api_key:
            raise ValueError("OpenAI API key is required but not configured")

        model = config.openai_model if model_type == "main" else config.openai_model_mini

        return OpenAIProvider(
            api_key=config.openai_api_key,
            model=model,
            temperature=config.temperature,
            max_tokens=config.max_tokens,
            timeout=config.timeout,
        )

    @staticmethod
    def _create_anthropic(
        config: LLMSettings,
        model_type: Literal["main", "mini"],
    ) -> AnthropicProvider:
        """Create Anthropic provider instance."""
        if not config.anthropic_api_key:
            raise ValueError("Anthropic API key is required but not configured")

        model = config.anthropic_model if model_type == "main" else config.anthropic_model_mini

        return AnthropicProvider(
            api_key=config.anthropic_api_key,
            model=model,
            temperature=config.temperature,
            max_tokens=config.max_tokens,
            timeout=config.timeout,
        )

    @staticmethod
    def _create_google(
        config: LLMSettings,
        model_type: Literal["main", "mini"],
    ) -> GoogleProvider:
        """Create Google provider instance."""
        if not config.google_api_key:
            raise ValueError("Google API key is required but not configured")

        model = config.google_model if model_type == "main" else config.google_model_mini

        return GoogleProvider(
            api_key=config.google_api_key,
            model=model,
            temperature=config.temperature,
            max_tokens=config.max_tokens,
            timeout=config.timeout,
        )

    @staticmethod
    def _create_local(
        config: LLMSettings,
        model_type: Literal["main", "mini"],
    ) -> LocalProvider:
        """Create Local provider instance."""
        # Local provider doesn't distinguish between main/mini models
        # but accepts model_type for interface consistency
        return LocalProvider(
            base_url=config.local_base_url,
            model=config.local_model,
            temperature=config.temperature,
            max_tokens=config.max_tokens,
            timeout=config.timeout,
        )
