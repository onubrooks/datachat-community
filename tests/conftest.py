"""
Pytest configuration and shared fixtures.

This module provides fixtures and configuration used across all tests.
"""

import asyncio
import logging
import os

import pytest

os.environ.setdefault(
    "LLM_OPENAI_API_KEY",
    "sk-test-key-1234567890-abcdefghijklmnop",
)
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://user:pass@localhost:5432/testdb",
)
os.environ.setdefault("DATA_CHAT_ENV_SOURCE", "system")

# ============================================================================
# Pytest Configuration
# ============================================================================


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests (requires API keys and external services)",
    )


def pytest_configure(config):
    """Configure pytest with custom markers and settings."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test (may require external services)"
    )
    config.addinivalue_line("markers", "slow: mark test as slow (takes more than 1 second)")
    config.addinivalue_line("markers", "unit: mark test as unit test (default)")


def pytest_collection_modifyitems(config, items):
    """Skip integration tests unless explicitly enabled."""
    if config.getoption("--run-integration"):
        return
    skip_integration = pytest.mark.skip(
        reason="Integration tests disabled (use --run-integration to enable)."
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)


# ============================================================================
# Event Loop Configuration
# ============================================================================


@pytest.fixture(scope="session")
def event_loop():
    """
    Create an event loop for the test session.

    This ensures all async tests use the same event loop,
    which is important for proper cleanup.
    """
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# ============================================================================
# Logging Configuration
# ============================================================================


@pytest.fixture(autouse=True)
def configure_test_logging(caplog):
    """
    Configure logging for tests.

    Sets up log capture and configures log levels.
    This fixture runs automatically for all tests.
    """
    caplog.set_level(logging.DEBUG)
    yield
    # Cleanup happens automatically


@pytest.fixture
def disable_logging():
    """
    Disable logging for specific tests.

    Use this for tests that generate excessive logs.

    Usage:
        def test_something(disable_logging):
            # Logs are disabled here
            pass
    """
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


# ============================================================================
# Test Utilities
# ============================================================================


@pytest.fixture
def mock_async_function():
    """
    Create a mock async function.

    Returns a factory that creates configured async mocks.

    Usage:
        def test_something(mock_async_function):
            mock = mock_async_function(return_value="result")
            result = await mock()
            assert result == "result"
    """
    from unittest.mock import AsyncMock

    def _create_mock(**kwargs):
        return AsyncMock(**kwargs)

    return _create_mock


# ============================================================================
# Common Test Data
# ============================================================================


@pytest.fixture
def sample_query() -> str:
    """Sample user query for testing."""
    return "What were our total sales last quarter?"


@pytest.fixture
def sample_queries() -> list[str]:
    """Collection of sample queries for testing."""
    return [
        "What were our total sales last quarter?",
        "Show me top 10 products by revenue",
        "How many customers did we acquire in 2023?",
        "What is the average order value?",
        "Compare this year's revenue to last year",
    ]


# ============================================================================
# Environment and Configuration
# ============================================================================


@pytest.fixture(autouse=True)
def reset_environment_per_test(monkeypatch):
    """
    Ensure clean environment for each test.

    This fixture runs automatically and prevents environment
    pollution between tests.
    """
    # Add any environment cleanup here
    yield
    # Cleanup happens automatically via monkeypatch


@pytest.fixture(autouse=True)
def mock_openai_api_key(monkeypatch):
    """
    Mock OpenAI API key for tests that require it.

    This prevents tests from attempting real API calls.
    Runs automatically for all tests.
    """
    # Clear the settings cache to force reload with new env vars
    from backend.config import get_settings

    get_settings.cache_clear()

    test_key = "sk-test-key-1234567890-abcdefghijklmnop"  # 20+ chars
    monkeypatch.setenv("LLM_OPENAI_API_KEY", test_key)
    yield test_key

    # Clear cache after test
    get_settings.cache_clear()


@pytest.fixture
def mock_database_url(monkeypatch):
    """
    Mock database URL for tests.

    Uses an in-memory SQLite database for testing.
    """
    db_url = "sqlite:///:memory:"
    monkeypatch.setenv("DATABASE_URL", db_url)
    yield db_url


# ============================================================================
# Async Helpers
# ============================================================================


@pytest.fixture
async def async_context_manager():
    """
    Factory for creating async context managers in tests.

    Usage:
        async def test_something(async_context_manager):
            async with async_context_manager():
                # Test code here
                pass
    """
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _context():
        # Setup
        yield
        # Teardown

    return _context


# ============================================================================
# Timing Helpers
# ============================================================================


@pytest.fixture
def assert_timing():
    """
    Helper for asserting execution time bounds.

    Usage:
        def test_performance(assert_timing):
            with assert_timing(min_ms=100, max_ms=500):
                # Code that should take 100-500ms
                time.sleep(0.2)
    """
    import time
    from contextlib import contextmanager

    @contextmanager
    def _assert_timing(min_ms: float = 0, max_ms: float = float("inf")):
        start = time.perf_counter()
        yield
        duration_ms = (time.perf_counter() - start) * 1000

        assert duration_ms >= min_ms, f"Execution too fast: {duration_ms:.2f}ms < {min_ms}ms"
        assert duration_ms <= max_ms, f"Execution too slow: {duration_ms:.2f}ms > {max_ms}ms"

    return _assert_timing


# ============================================================================
# Mock LLM Provider
# ============================================================================


@pytest.fixture
def mock_llm_provider():
    """
    Mock LLM provider for testing agents.

    Provides a simple interface for setting responses.

    Usage:
        def test_agent(mock_llm_provider):
            mock_llm_provider.set_response("test response")
            result = await agent.execute(input)
    """
    from unittest.mock import AsyncMock

    from backend.llm.models import LLMResponse, LLMUsage

    class MockLLMProvider:
        def __init__(self):
            self.generate = AsyncMock()
            self.stream = AsyncMock()
            self.count_tokens = AsyncMock(return_value=100)
            self.get_model_info = AsyncMock(return_value={"name": "mock-model"})

        def set_response(self, response: str):
            """Set the response that generate() will return."""
            self.generate.return_value = LLMResponse(
                content=response,
                model="mock-model",
                usage=LLMUsage(prompt_tokens=1, completion_tokens=1, total_tokens=2),
                finish_reason="stop",
                provider="mock",
                metadata={},
            )

    return MockLLMProvider()


# ============================================================================
# Mock Database Connectors
# ============================================================================


@pytest.fixture
def mock_postgres_connector():
    """
    Mock PostgreSQL connector for testing.

    Usage:
        def test_query(mock_postgres_connector):
            mock_postgres_connector.execute.return_value = QueryResult(...)
            result = await connector.execute("SELECT 1")
    """
    from unittest.mock import AsyncMock

    connector = AsyncMock()
    connector.connect = AsyncMock()
    connector.close = AsyncMock()
    connector.execute = AsyncMock()
    connector.get_schema = AsyncMock()

    return connector
