"""
Application Configuration

Pydantic-based settings management using environment variables.
Supports nested configuration, validation, and caching.

Usage:
    from backend.config import get_settings

    settings = get_settings()
    print(settings.openai.api_key)
    print(settings.database.url)
"""

import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

from dotenv import load_dotenv
from pydantic import AnyUrl, Field, PostgresDsn, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class LLMSettings(BaseSettings):
    """LLM provider configuration."""

    # Provider selection
    default_provider: Literal["openai", "anthropic", "google", "local"] = Field(
        default="openai", description="Default LLM provider"
    )
    classifier_provider: Literal["openai", "anthropic", "google", "local"] | None = Field(
        None, description="Provider for ClassifierAgent (defaults to default_provider)"
    )
    sql_provider: Literal["openai", "anthropic", "google", "local"] | None = Field(
        None, description="Provider for SQLAgent (defaults to default_provider)"
    )
    sql_formatter_model: str | None = Field(
        None,
        description=(
            "Optional model override used only for SQL JSON formatting fallback. "
            "When unset, SQLAgent uses the configured mini model."
        ),
    )
    fallback_provider: Literal["openai", "anthropic", "google", "local"] | None = Field(
        None, description="Fallback provider if primary fails"
    )

    # OpenAI configuration
    openai_api_key: str | None = Field(
        None,
        description="OpenAI API key",
        min_length=20,
    )
    openai_model: str = Field(default="gpt-4o", description="OpenAI model for complex tasks")
    openai_model_mini: str = Field(default="gpt-4o-mini", description="OpenAI lightweight model")

    # Anthropic configuration
    anthropic_api_key: str | None = Field(
        None,
        description="Anthropic API key",
        min_length=20,
    )
    anthropic_model: str = Field(
        default="claude-3-5-sonnet-20241022", description="Anthropic model for complex tasks"
    )
    anthropic_model_mini: str = Field(
        default="claude-3-5-haiku-20241022", description="Anthropic lightweight model"
    )

    # Google configuration
    google_api_key: str | None = Field(None, description="Google AI API key")
    google_model: str = Field(
        default="gemini-1.5-pro", description="Google model for complex tasks"
    )
    google_model_mini: str = Field(
        default="gemini-1.5-flash", description="Google lightweight model"
    )

    # Local model configuration
    local_base_url: str = Field(
        default="http://localhost:11434",
        description="Base URL for local model server (Ollama, vLLM, etc.)",
    )
    local_model: str = Field(default="llama3.1:8b", description="Local model name")

    # Common settings
    temperature: float = Field(
        default=0.0,
        ge=0.0,
        le=2.0,
        description="Temperature for LLM responses (0.0 = deterministic)",
    )
    max_tokens: int = Field(
        default=2000,
        gt=0,
        le=16000,
        description="Maximum tokens per LLM response",
    )
    timeout: int = Field(
        default=30,
        gt=0,
        description="Request timeout in seconds",
    )
    require_provider_keys_on_startup: bool = Field(
        default=False,
        description=(
            "When true, startup fails if selected providers are missing API keys. "
            "When false, startup succeeds and keys can be added later via Settings."
        ),
    )

    model_config = SettingsConfigDict(
        env_prefix="LLM_",
        env_file=".env",
        extra="ignore",
    )

    @field_validator("openai_api_key")
    @classmethod
    def validate_openai_key(cls, v: str | None) -> str | None:
        """Validate OpenAI API key format."""
        if v and not v.startswith("sk-"):
            raise ValueError("OpenAI API key must start with 'sk-'")
        return v

    @field_validator("anthropic_api_key")
    @classmethod
    def validate_anthropic_key(cls, v: str | None) -> str | None:
        """Validate Anthropic API key format."""
        if v and not v.startswith("sk-ant-"):
            raise ValueError("Anthropic API key must start with 'sk-ant-'")
        return v

    @model_validator(mode="after")
    def validate_provider_keys(self) -> "LLMSettings":
        """Validate provider key availability for selected providers."""
        provider_key_map = {
            "openai": self.openai_api_key,
            "anthropic": self.anthropic_api_key,
            "google": self.google_api_key,
        }

        selected_providers = {
            self.default_provider,
            self.classifier_provider,
            self.sql_provider,
            self.fallback_provider,
        }

        missing_providers = sorted(
            provider
            for provider in selected_providers
            if provider in provider_key_map and not provider_key_map[provider]
        )

        if missing_providers and self.require_provider_keys_on_startup:
            provider = missing_providers[0]
            raise ValueError(
                f"API key required for {provider} provider. Set LLM_{provider.upper()}_API_KEY"
            )

        if missing_providers:
            logging.getLogger(__name__).warning(
                "Missing API key(s) for provider(s): %s. "
                "Startup continues; queries using those providers will fail until keys are set.",
                ", ".join(missing_providers),
            )

        return self


class DatabaseSettings(BaseSettings):
    """Target database configuration."""

    db_type: Literal["postgresql", "clickhouse", "mysql"] = Field(
        default="postgresql",
        description="Target database type for SQL generation and validation.",
        validation_alias="DATABASE_TYPE",
    )
    url: AnyUrl | None = Field(
        None,
        description="Target database connection URL (the database you query)",
    )
    pool_size: int = Field(
        default=5,
        gt=0,
        le=20,
        description="Database connection pool size",
    )
    max_overflow: int = Field(
        default=10,
        ge=0,
        description="Maximum overflow connections",
    )
    pool_timeout: int = Field(
        default=30,
        gt=0,
        description="Connection pool timeout in seconds",
    )
    echo: bool = Field(
        default=False,
        description="Echo SQL queries to logs (useful for debugging)",
    )

    model_config = SettingsConfigDict(
        env_prefix="DATABASE_",
        env_file=".env",
        extra="ignore",
    )

    @field_validator("url", mode="before")
    @classmethod
    def normalize_url(cls, v: str | AnyUrl | None) -> str | AnyUrl | None:
        """Treat empty strings as missing."""
        if v == "":
            return None
        return v

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: AnyUrl | None) -> AnyUrl | None:
        """Validate supported database URL schemes."""
        if v is None:
            return v
        parsed = urlparse(str(v))
        scheme = parsed.scheme.split("+")[0].lower() if parsed.scheme else ""
        if scheme not in {"postgres", "postgresql", "clickhouse", "mysql"}:
            raise ValueError("DATABASE_URL must use postgresql, clickhouse, or mysql scheme.")
        if not parsed.hostname:
            raise ValueError("DATABASE_URL must include a host.")
        return v


class SystemDatabaseSettings(BaseSettings):
    """System database configuration (registry/profiling/demo)."""

    url: PostgresDsn | None = Field(
        None,
        description="System PostgreSQL connection URL (registry/profiling/demo)",
    )

    model_config = SettingsConfigDict(
        env_prefix="SYSTEM_DATABASE_",
        env_file=".env",
        extra="ignore",
    )

    @field_validator("url", mode="before")
    @classmethod
    def normalize_url(cls, v: str | PostgresDsn | None) -> str | PostgresDsn | None:
        """Treat empty strings as missing."""
        if v == "":
            return None
        return v

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: PostgresDsn | None) -> PostgresDsn | None:
        """Validate system database URL scheme."""
        if v is None:
            return v
        return v


class ChromaSettings(BaseSettings):
    """Chroma vector store configuration."""

    persist_dir: Path = Field(
        default=Path("./chroma_data"),
        description="Directory for Chroma vector store persistence",
    )
    collection_name: str = Field(
        default="datachat_knowledge",
        description="Name of the Chroma collection",
    )
    embedding_model: str = Field(
        default="text-embedding-3-small",
        description="OpenAI embedding model",
    )
    chunk_size: int = Field(
        default=512,
        gt=0,
        le=8192,
        description="Text chunk size for embeddings",
    )
    chunk_overlap: int = Field(
        default=50,
        ge=0,
        description="Overlap between text chunks",
    )
    top_k: int = Field(
        default=5,
        gt=0,
        le=20,
        description="Number of top results to retrieve",
    )

    model_config = SettingsConfigDict(
        env_prefix="CHROMA_",
        env_file=".env",
        extra="ignore",
    )

    @field_validator("persist_dir")
    @classmethod
    def validate_persist_dir(cls, v: Path) -> Path:
        """Ensure persist directory exists."""
        v.mkdir(parents=True, exist_ok=True)
        return v.resolve()

    @model_validator(mode="after")
    def validate_chunk_overlap(self) -> "ChromaSettings":
        """Ensure chunk overlap is less than chunk size."""
        if self.chunk_overlap >= self.chunk_size:
            raise ValueError(
                f"chunk_overlap ({self.chunk_overlap}) must be less than "
                f"chunk_size ({self.chunk_size})"
            )
        return self


class LoggingSettings(BaseSettings):
    """Logging configuration."""

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Application log level",
    )
    format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log message format",
    )
    date_format: str = Field(
        default="%Y-%m-%d %H:%M:%S",
        description="Log timestamp format",
    )
    file: Path | None = Field(
        default=None,
        description="Optional log file path (None = stdout only)",
    )

    model_config = SettingsConfigDict(
        env_prefix="LOG_",
        env_file=".env",
        extra="ignore",
    )

    def configure(self) -> None:
        """Configure Python logging with these settings."""
        handlers: list[logging.Handler] = [logging.StreamHandler()]

        if self.file:
            self.file.parent.mkdir(parents=True, exist_ok=True)
            handlers.append(logging.FileHandler(self.file))

        logging.basicConfig(
            level=getattr(logging, self.level),
            format=self.format,
            datefmt=self.date_format,
            handlers=handlers,
            force=True,  # Override any existing configuration
        )


class ToolsSettings(BaseSettings):
    """Tooling configuration."""

    enabled: bool = Field(default=True, description="Enable tool planner/executor")
    planner_enabled: bool = Field(
        default=True, description="Enable LLM tool planner"
    )
    policy_path: str = Field(
        default="config/tools.yaml",
        description="Path to tool policy configuration",
    )

    model_config = SettingsConfigDict(
        env_prefix="TOOLS_",
        env_file=".env",
        extra="ignore",
    )


class PipelineSettings(BaseSettings):
    """Pipeline performance and behavior settings."""

    sql_two_stage_enabled: bool = Field(
        default=False,
        description="Try mini model first for SQL generation, escalate to main model when needed.",
    )
    sql_two_stage_confidence_threshold: float = Field(
        default=0.78,
        ge=0.0,
        le=1.0,
        description="Minimum confidence for accepting first-pass mini SQL output.",
    )
    sql_formatter_fallback_enabled: bool = Field(
        default=True,
        description=(
            "Use mini-model formatter fallback when SQL LLM output is malformed "
            "(for example missing `sql` field)."
        ),
    )
    sql_prompt_budget_enabled: bool = Field(
        default=False,
        description="Apply prompt budgeting to SQL schema context sections.",
    )
    sql_prompt_max_tables: int = Field(
        default=80,
        ge=10,
        le=500,
        description="Maximum tables to include in live schema context.",
    )
    sql_prompt_focus_tables: int = Field(
        default=8,
        ge=1,
        le=50,
        description="Maximum ranked tables to include for non-table-list queries.",
    )
    sql_prompt_max_columns_per_table: int = Field(
        default=18,
        ge=3,
        le=100,
        description="Maximum columns per table in SQL prompt context.",
    )
    sql_prompt_max_context_chars: int = Field(
        default=12000,
        ge=2000,
        le=50000,
        description="Maximum schema context characters injected into SQL prompt.",
    )
    synthesize_simple_sql_answers: bool = Field(
        default=True,
        description="Run response synthesis for simple SQL answers.",
    )
    classifier_deep_low_confidence_threshold: float = Field(
        default=0.6,
        ge=0.0,
        le=1.0,
        description="Deep classifier triggers below this confidence threshold.",
    )
    classifier_deep_min_query_length: int = Field(
        default=1,
        ge=1,
        le=500,
        description="Minimum query length to allow deep-classification fallback.",
    )
    intent_llm_confidence_threshold: float = Field(
        default=0.45,
        ge=0.0,
        le=1.0,
        description=(
            "When intent LLM returns data_query below this confidence for ambiguous input, "
            "route to clarification."
        ),
    )
    context_answer_confidence_threshold: float = Field(
        default=0.7,
        ge=0.0,
        le=1.0,
        description=(
            "Minimum context confidence for routing ambiguous semantic questions "
            "to ContextAnswer instead of SQL."
        ),
    )
    semantic_sql_clarification_confidence_threshold: float = Field(
        default=0.55,
        ge=0.0,
        le=1.0,
        description=(
            "Minimum SQL generation confidence for accepting semantic SQL without "
            "a clarification round."
        ),
    )
    clarification_confirmation_enabled: bool = Field(
        default=True,
        description=(
            "Confirm clarification prompts with an LLM gate before showing them "
            "to the user."
        ),
    )
    clarification_confirmation_confidence_threshold: float = Field(
        default=0.6,
        ge=0.0,
        le=1.0,
        description=(
            "Minimum confidence required from clarification confirmation gate "
            "before asking user clarifying questions."
        ),
    )
    sql_table_resolver_enabled: bool = Field(
        default=True,
        description=(
            "Use a lightweight LLM table resolver to pick likely source tables "
            "before SQL generation."
        ),
    )
    sql_table_resolver_confidence_threshold: float = Field(
        default=0.55,
        ge=0.0,
        le=1.0,
        description="Confidence threshold below which SQL resolver asks clarification.",
    )
    sql_table_resolver_max_tables: int = Field(
        default=20,
        ge=5,
        le=80,
        description="Maximum candidate tables sent to SQL resolver prompt.",
    )
    visualization_planner_enabled: bool = Field(
        default=True,
        description=(
            "Use a lightweight LLM planner for chart suggestion, with rule-based "
            "guardrails and fallback."
        ),
    )
    visualization_planner_confidence_threshold: float = Field(
        default=0.55,
        ge=0.0,
        le=1.0,
        description="Minimum confidence required to trust LLM visualization planner output.",
    )
    visualization_planner_max_rows_sample: int = Field(
        default=10,
        ge=2,
        le=50,
        description="Maximum rows to send to LLM visualization planner.",
    )
    visualization_planner_max_columns_sample: int = Field(
        default=12,
        ge=2,
        le=40,
        description="Maximum columns to send to LLM visualization planner.",
    )
    visualization_planner_timeout_ms: int = Field(
        default=1500,
        ge=200,
        le=10000,
        description="Timeout budget for LLM visualization planner.",
    )
    ambiguous_query_max_tokens: int = Field(
        default=3,
        ge=1,
        le=12,
        description="Maximum tokens treated as ambiguous when no data keywords are present.",
    )
    selective_tool_planner_enabled: bool = Field(
        default=False,
        description="Run tool planner only for likely tool/action requests.",
    )
    schema_snapshot_cache_enabled: bool = Field(
        default=True,
        description="Cache schema snapshot by database for reuse across queries.",
    )
    schema_snapshot_cache_ttl_seconds: int = Field(
        default=21600,
        ge=0,
        le=604800,
        description=(
            "Schema snapshot cache TTL in seconds. "
            "Set to 0 for no expiry while process is running."
        ),
    )
    sql_operator_templates_enabled: bool = Field(
        default=True,
        description="Inject semantic analytic operator hints into SQL generation prompts.",
    )
    sql_operator_templates_max: int = Field(
        default=8,
        ge=2,
        le=20,
        description="Maximum operator templates to inject into a single SQL prompt.",
    )
    query_compiler_enabled: bool = Field(
        default=True,
        description="Enable semantic query compiler stage before SQL generation.",
    )
    query_compiler_llm_enabled: bool = Field(
        default=True,
        description="Allow mini-LLM refinement for ambiguous query-compiler table choices.",
    )
    query_compiler_llm_max_candidates: int = Field(
        default=10,
        ge=2,
        le=20,
        description="Maximum table candidates passed to query-compiler LLM refinement.",
    )
    query_compiler_confidence_threshold: float = Field(
        default=0.72,
        ge=0.0,
        le=1.0,
        description="Minimum deterministic query-compiler confidence before skipping LLM refinement.",
    )
    visualization_llm_enabled: bool = Field(
        default=True,
        description="Enable mini-LLM visualization recommendation with deterministic guardrails.",
    )
    visualization_llm_row_sample: int = Field(
        default=8,
        ge=2,
        le=30,
        description="Maximum result rows sampled for visualization recommendation prompts.",
    )
    action_loop_enabled: bool = Field(
        default=True,
        description="Enable DYN-001 bounded action-loop trace/controller.",
    )
    action_loop_shadow_mode: bool = Field(
        default=True,
        description=(
            "Run action-loop controller in shadow mode (record decisions without changing routing)."
        ),
    )
    action_loop_max_steps: int = Field(
        default=12,
        ge=1,
        le=64,
        description="Maximum action-loop steps per request.",
    )
    action_loop_max_latency_ms: int = Field(
        default=45000,
        ge=1000,
        le=600000,
        description="Maximum action-loop latency budget per request.",
    )
    action_loop_max_llm_tokens: int = Field(
        default=120000,
        ge=1000,
        le=1000000,
        description="Maximum total LLM token budget across the action loop.",
    )
    action_loop_max_clarifications: int = Field(
        default=3,
        ge=0,
        le=20,
        description="Maximum clarification rounds allowed inside the action loop.",
    )

    model_config = SettingsConfigDict(
        env_prefix="PIPELINE_",
        env_file=".env",
        extra="ignore",
    )


class Settings(BaseSettings):
    """
    Main application settings.

    Loads configuration from environment variables and .env file.
    Settings are nested by domain (llm, database, chroma, logging).

    Environment Variables:
        ENVIRONMENT: Deployment environment (development, staging, production)
        APP_NAME: Application name for logging and metrics
        DEBUG: Enable debug mode
        API_HOST: API server host
        API_PORT: API server port
        SYNC_WATCHER_ENABLED: Enable filesystem DataPoint watcher
        LLM_*: LLM provider configuration (see LLMSettings)
        DATABASE_*: Target database configuration (see DatabaseSettings)
        SYSTEM_DATABASE_*: System database configuration (see SystemDatabaseSettings)
        CHROMA_*: Vector store configuration (see ChromaSettings)
        LOG_*: Logging configuration (see LoggingSettings)

    Example:
        >>> settings = get_settings()
        >>> settings.llm.default_provider
        'openai'
        >>> settings.llm.openai_api_key
        'sk-...'
        >>> settings.is_production
        False
    """

    # Application settings
    environment: Literal["development", "staging", "production"] = Field(
        default="development",
        description="Deployment environment",
    )
    app_name: str = Field(
        default="DataChat",
        description="Application name",
    )
    debug: bool = Field(
        default=False,
        description="Enable debug mode",
    )
    api_host: str = Field(
        default="0.0.0.0",
        description="API server host",
    )
    api_port: int = Field(
        default=8000,
        gt=0,
        le=65535,
        description="API server port",
    )
    sync_watcher_enabled: bool = Field(
        default=True,
        description="Enable filesystem watcher for DataPoints (single-node deployments)",
    )

    # Nested settings
    llm: LLMSettings = Field(default_factory=LLMSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    system_database: SystemDatabaseSettings = Field(default_factory=SystemDatabaseSettings)
    chroma: ChromaSettings = Field(default_factory=ChromaSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    tools: ToolsSettings = Field(default_factory=ToolsSettings)
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)
    database_credentials_key: str | None = Field(
        default=None,
        description="Fernet key for encrypting stored database credentials.",
        validation_alias="DATABASE_CREDENTIALS_KEY",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment == "development"

    @property
    def is_staging(self) -> bool:
        """Check if running in staging environment."""
        return self.environment == "staging"

    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == "production"

    @model_validator(mode="after")
    def configure_logging(self) -> "Settings":
        """Configure logging when settings are loaded."""
        self.logging.configure()
        return self

    def model_post_init(self, __context) -> None:
        """Log configuration on initialization."""
        logger = logging.getLogger(__name__)
        logger.info(
            f"Settings loaded for {self.app_name} ({self.environment})",
            extra={
                "environment": self.environment,
                "debug": self.debug,
                "llm_provider": self.llm.default_provider,
                "database_pool_size": self.database.pool_size,
                "chroma_collection": self.chroma.collection_name,
            },
        )


_DOTENV_PATH = Path(__file__).resolve().parents[1] / ".env"


def _apply_dotenv_precedence() -> None:
    env_source = os.getenv("DATA_CHAT_ENV_SOURCE", "dotenv").lower()
    if env_source not in {"dotenv", "envfile", "file"}:
        return
    if _DOTENV_PATH.exists():
        load_dotenv(_DOTENV_PATH, override=True)


@lru_cache
def get_settings() -> Settings:
    """
    Get cached application settings.

    Uses functools.lru_cache to ensure settings are loaded only once.
    This is the recommended way to access settings throughout the application.

    Returns:
        Settings: Singleton settings instance

    Example:
        >>> from backend.config import get_settings
        >>> settings = get_settings()
        >>> print(settings.openai.api_key)
    """
    _apply_dotenv_precedence()
    return Settings()


def clear_settings_cache() -> None:
    """
    Clear the settings cache.

    Useful for testing when you need to reload settings with different
    environment variables.

    Example:
        >>> import os
        >>> os.environ["ENVIRONMENT"] = "production"
        >>> clear_settings_cache()
        >>> settings = get_settings()  # Reloads with new env vars
    """
    get_settings.cache_clear()
