"""
Unit tests for configuration module.

Tests settings loading, validation, nested configuration, and caching.
"""

import pytest
from pydantic import ValidationError

from backend.config import (
    ChromaSettings,
    DatabaseSettings,
    LLMSettings,
    LoggingSettings,
    Settings,
    clear_settings_cache,
    get_settings,
)


class TestLLMSettings:
    """Test LLM configuration."""

    def test_valid_llm_settings(self, monkeypatch):
        """Valid LLM settings load correctly."""
        monkeypatch.setenv("LLM_OPENAI_API_KEY", "sk-test-key-1234567890abcdefghij")
        monkeypatch.delenv("LLM_SQL_FORMATTER_MODEL", raising=False)

        settings = LLMSettings()

        assert settings.openai_api_key == "sk-test-key-1234567890abcdefghij"
        assert settings.openai_model == "gpt-4o"
        assert settings.openai_model_mini == "gpt-4o-mini"
        assert settings.temperature == 0.0
        assert settings.max_tokens == 2000

    def test_custom_llm_settings(self, monkeypatch):
        """Custom LLM settings override defaults."""
        monkeypatch.setenv("LLM_OPENAI_API_KEY", "sk-custom-key-1234567890xyz")
        monkeypatch.setenv("LLM_OPENAI_MODEL", "gpt-4-turbo")
        monkeypatch.setenv("LLM_SQL_FORMATTER_MODEL", "gpt-4o-mini")
        monkeypatch.setenv("LLM_TEMPERATURE", "0.7")
        monkeypatch.setenv("LLM_MAX_TOKENS", "4000")

        settings = LLMSettings()

        assert settings.openai_model == "gpt-4-turbo"
        assert settings.sql_formatter_model == "gpt-4o-mini"
        assert settings.temperature == 0.7
        assert settings.max_tokens == 4000

    def test_api_key_validation_requires_sk_prefix(self, monkeypatch):
        """API key must start with 'sk-'."""
        monkeypatch.setenv("LLM_OPENAI_API_KEY", "invalid-key-1234567890abcdef")

        with pytest.raises(ValidationError, match="must start with 'sk-'"):
            LLMSettings()

    def test_api_key_minimum_length(self, monkeypatch):
        """API key must have minimum length."""
        monkeypatch.setenv("LLM_OPENAI_API_KEY", "sk-short")

        with pytest.raises(ValidationError):
            LLMSettings()

    def test_temperature_validation(self, monkeypatch):
        """Temperature must be between 0.0 and 2.0."""
        monkeypatch.setenv("LLM_OPENAI_API_KEY", "sk-test-key-1234567890abcdefghij")
        monkeypatch.setenv("LLM_TEMPERATURE", "3.0")

        with pytest.raises(ValidationError, match="less than or equal to 2"):
            LLMSettings()

    def test_max_tokens_validation(self, monkeypatch):
        """Max tokens must be positive and within limits."""
        monkeypatch.setenv("LLM_OPENAI_API_KEY", "sk-test-key-1234567890abcdefghij")
        monkeypatch.setenv("LLM_MAX_TOKENS", "20000")

        with pytest.raises(ValidationError, match="less than or equal to 16"):
            LLMSettings()

    def test_anthropic_key_validation(self, monkeypatch):
        """Anthropic API key must start with 'sk-ant-'."""
        monkeypatch.setenv("LLM_DEFAULT_PROVIDER", "anthropic")
        monkeypatch.setenv("LLM_ANTHROPIC_API_KEY", "sk-invalid-key-1234567890")

        with pytest.raises(ValidationError, match="must start with 'sk-ant-'"):
            LLMSettings()


class TestDatabaseSettings:
    """Test database configuration."""

    def test_valid_database_settings(self, monkeypatch):
        """Valid database settings load correctly."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/testdb")

        settings = DatabaseSettings()

        assert str(settings.url) == "postgresql://user:pass@localhost:5432/testdb"
        assert settings.pool_size == 5
        assert settings.max_overflow == 10
        assert settings.echo is False

    def test_database_url_optional(self, monkeypatch):
        """Database URL can be omitted in config-only environments."""
        monkeypatch.setenv("DATABASE_URL", "")

        settings = DatabaseSettings()

        assert settings.url is None

    def test_asyncpg_url_scheme(self, monkeypatch):
        """PostgreSQL+asyncpg scheme is valid."""
        monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost:5432/testdb")

        settings = DatabaseSettings()

        assert "asyncpg" in str(settings.url)

    def test_mysql_url_scheme(self, monkeypatch):
        """MySQL scheme is valid."""
        monkeypatch.setenv("DATABASE_URL", "mysql://user:pass@localhost:3306/testdb")

        settings = DatabaseSettings()

        assert str(settings.url) == "mysql://user:pass@localhost:3306/testdb"

    def test_clickhouse_url_scheme(self, monkeypatch):
        """ClickHouse scheme is valid."""
        monkeypatch.setenv("DATABASE_URL", "clickhouse://default:@localhost:8123/default")

        settings = DatabaseSettings()

        assert str(settings.url) == "clickhouse://default@localhost:8123/default"

    def test_invalid_database_scheme(self, monkeypatch):
        """Unsupported schemes are rejected."""
        monkeypatch.setenv("DATABASE_URL", "sqlite:///tmp/test.db")

        with pytest.raises(ValidationError):
            DatabaseSettings()

    def test_custom_pool_settings(self, monkeypatch):
        """Custom pool settings override defaults."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/testdb")
        monkeypatch.setenv("DATABASE_POOL_SIZE", "10")
        monkeypatch.setenv("DATABASE_MAX_OVERFLOW", "20")
        monkeypatch.setenv("DATABASE_ECHO", "true")

        settings = DatabaseSettings()

        assert settings.pool_size == 10
        assert settings.max_overflow == 20
        assert settings.echo is True

    def test_pool_size_validation(self, monkeypatch):
        """Pool size must be within valid range."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/testdb")
        monkeypatch.setenv("DATABASE_POOL_SIZE", "25")

        with pytest.raises(ValidationError, match="less than or equal to 20"):
            DatabaseSettings()


class TestChromaSettings:
    """Test Chroma vector store configuration."""

    def test_valid_chroma_settings(self, tmp_path, monkeypatch):
        """Valid Chroma settings load correctly."""
        persist_dir = tmp_path / "chroma"
        monkeypatch.setenv("CHROMA_PERSIST_DIR", str(persist_dir))

        settings = ChromaSettings()

        assert settings.persist_dir == persist_dir
        assert settings.persist_dir.exists()  # Should be created
        assert settings.collection_name == "datachat_knowledge"
        assert settings.embedding_model == "text-embedding-3-small"
        assert settings.chunk_size == 512
        assert settings.chunk_overlap == 50

    def test_custom_chroma_settings(self, tmp_path, monkeypatch):
        """Custom Chroma settings override defaults."""
        persist_dir = tmp_path / "custom_chroma"
        monkeypatch.setenv("CHROMA_PERSIST_DIR", str(persist_dir))
        monkeypatch.setenv("CHROMA_COLLECTION_NAME", "custom_collection")
        monkeypatch.setenv("CHROMA_CHUNK_SIZE", "1024")
        monkeypatch.setenv("CHROMA_CHUNK_OVERLAP", "100")
        monkeypatch.setenv("CHROMA_TOP_K", "10")

        settings = ChromaSettings()

        assert settings.collection_name == "custom_collection"
        assert settings.chunk_size == 1024
        assert settings.chunk_overlap == 100
        assert settings.top_k == 10

    def test_persist_dir_created_if_not_exists(self, tmp_path, monkeypatch):
        """Persist directory is created if it doesn't exist."""
        persist_dir = tmp_path / "nested" / "path" / "chroma"
        monkeypatch.setenv("CHROMA_PERSIST_DIR", str(persist_dir))

        assert not persist_dir.exists()

        settings = ChromaSettings()

        assert settings.persist_dir.exists()
        assert settings.persist_dir.is_dir()

    def test_chunk_overlap_validation(self, tmp_path, monkeypatch):
        """Chunk overlap must be less than chunk size."""
        monkeypatch.setenv("CHROMA_PERSIST_DIR", str(tmp_path))
        monkeypatch.setenv("CHROMA_CHUNK_SIZE", "100")
        monkeypatch.setenv("CHROMA_CHUNK_OVERLAP", "150")

        with pytest.raises(ValidationError, match="must be less than"):
            ChromaSettings()

    def test_chunk_overlap_equal_to_size_invalid(self, tmp_path, monkeypatch):
        """Chunk overlap equal to size is invalid."""
        monkeypatch.setenv("CHROMA_PERSIST_DIR", str(tmp_path))
        monkeypatch.setenv("CHROMA_CHUNK_SIZE", "100")
        monkeypatch.setenv("CHROMA_CHUNK_OVERLAP", "100")

        with pytest.raises(ValidationError, match="must be less than"):
            ChromaSettings()


class TestLoggingSettings:
    """Test logging configuration."""

    def test_valid_logging_settings(self):
        """Valid logging settings load correctly."""
        settings = LoggingSettings()

        assert settings.level == "INFO"
        assert "%(asctime)s" in settings.format
        assert settings.file is None

    def test_custom_log_level(self, monkeypatch):
        """Custom log level can be set."""
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")

        settings = LoggingSettings()

        assert settings.level == "DEBUG"

    def test_log_file_path(self, tmp_path, monkeypatch):
        """Log file path can be configured."""
        log_file = tmp_path / "app.log"
        monkeypatch.setenv("LOG_FILE", str(log_file))

        settings = LoggingSettings()

        assert settings.file == log_file

    def test_invalid_log_level(self, monkeypatch):
        """Invalid log level is rejected."""
        monkeypatch.setenv("LOG_LEVEL", "INVALID")

        with pytest.raises(ValidationError):
            LoggingSettings()

    def test_configure_logging(self, monkeypatch, caplog):
        """configure() sets up Python logging."""
        import logging

        monkeypatch.setenv("LOG_LEVEL", "WARNING")

        settings = LoggingSettings()
        settings.configure()

        # Check that logging level is set
        assert logging.root.level == logging.WARNING

    def test_configure_logging_with_file(self, tmp_path, monkeypatch):
        """configure() sets up file logging when file path is provided."""
        import logging

        log_file = tmp_path / "test.log"
        monkeypatch.setenv("LOG_FILE", str(log_file))

        settings = LoggingSettings()
        settings.configure()

        # Verify file handler is added
        file_handlers = [h for h in logging.root.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) > 0


class TestSettings:
    """Test main Settings class."""

    def test_valid_settings_with_all_nested(self, mock_env_vars):
        """Settings load with all nested configurations."""
        settings = Settings()

        assert settings.environment == "development"
        assert settings.app_name == "DataChat"
        assert settings.llm.openai_api_key == "sk-test-key-1234567890abcdefghij"
        assert "postgresql" in str(settings.database.url)
        assert settings.chroma.collection_name == "datachat_knowledge"
        assert settings.logging.level == "INFO"

    def test_environment_properties(self, mock_env_vars):
        """Environment check properties work correctly."""
        settings = Settings()

        assert settings.is_development is True
        assert settings.is_staging is False
        assert settings.is_production is False

    def test_production_environment(self, mock_env_vars, monkeypatch):
        """Production environment is detected."""
        monkeypatch.setenv("ENVIRONMENT", "production")

        settings = Settings()

        assert settings.is_production is True
        assert settings.is_development is False

    def test_staging_environment(self, mock_env_vars, monkeypatch):
        """Staging environment is detected."""
        monkeypatch.setenv("ENVIRONMENT", "staging")

        settings = Settings()

        assert settings.is_staging is True
        assert settings.is_production is False

    def test_custom_api_settings(self, mock_env_vars, monkeypatch):
        """Custom API host and port can be set."""
        monkeypatch.setenv("API_HOST", "127.0.0.1")
        monkeypatch.setenv("API_PORT", "9000")

        settings = Settings()

        assert settings.api_host == "127.0.0.1"
        assert settings.api_port == 9000

    def test_debug_mode(self, mock_env_vars, monkeypatch):
        """Debug mode can be enabled."""
        monkeypatch.setenv("DEBUG", "true")

        settings = Settings()

        assert settings.debug is True

    def test_invalid_environment(self, mock_env_vars, monkeypatch):
        """Invalid environment is rejected."""
        monkeypatch.setenv("ENVIRONMENT", "invalid")

        with pytest.raises(ValidationError):
            Settings()


class TestGetSettings:
    """Test settings factory function."""

    def test_get_settings_returns_settings(self, mock_env_vars):
        """get_settings() returns Settings instance."""
        clear_settings_cache()

        settings = get_settings()

        assert isinstance(settings, Settings)

    def test_get_settings_cached(self, mock_env_vars):
        """get_settings() returns cached instance."""
        clear_settings_cache()

        settings1 = get_settings()
        settings2 = get_settings()

        assert settings1 is settings2  # Same instance

    def test_clear_settings_cache(self, mock_env_vars, monkeypatch):
        """clear_settings_cache() forces reload."""
        clear_settings_cache()

        settings1 = get_settings()
        assert settings1.environment == "development"

        # Change environment and clear cache
        monkeypatch.setenv("ENVIRONMENT", "production")
        clear_settings_cache()

        settings2 = get_settings()
        assert settings2.environment == "production"
        assert settings1 is not settings2  # Different instances


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_env_vars(monkeypatch, tmp_path):
    """Set up mock environment variables for testing."""
    monkeypatch.setenv("LLM_OPENAI_API_KEY", "sk-test-key-1234567890abcdefghij")
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/testdb")
    monkeypatch.setenv("CHROMA_PERSIST_DIR", str(tmp_path / "chroma"))
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    monkeypatch.setenv("ENVIRONMENT", "development")

    yield

    # Cleanup
    clear_settings_cache()
