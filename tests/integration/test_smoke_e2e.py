"""Smoke E2E integration flow for launch-critical paths.

Flow:
1) onboarding wizard
2) one ask
3) one train datapoint API flow
4) reset

Engine selection is driven by CI matrix via environment variables.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Callable
from urllib.parse import urlparse

import asyncpg
import clickhouse_connect
import mysql.connector
import pytest
from click.testing import CliRunner
from fastapi.testclient import TestClient

from backend.config import clear_settings_cache


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        pytest.skip(f"{name} is required for smoke E2E test.")
    return value


def _retry(operation: Callable[[], None], *, attempts: int = 30, delay_seconds: float = 1.0) -> None:
    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            operation()
            return
        except Exception as exc:  # pragma: no cover - exercised in CI timing windows
            last_error = exc
            time.sleep(delay_seconds)
    if last_error is not None:
        raise last_error


def _quote_postgres_identifier(value: str) -> str:
    return f"\"{value.replace('\"', '\"\"')}\""


def _ensure_postgres_database(url: str) -> None:
    async def _run() -> None:
        parsed = urlparse(url.replace("postgresql+asyncpg://", "postgresql://"))
        db_name = parsed.path.lstrip("/")
        if not db_name:
            raise RuntimeError("PostgreSQL URL must include a database name.")

        admin_url = (
            f"postgresql://{parsed.username or 'postgres'}:{parsed.password or ''}"
            f"@{parsed.hostname}:{parsed.port or 5432}/postgres"
        )
        conn = await asyncpg.connect(admin_url)
        try:
            exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", db_name)
            if not exists:
                quoted_db = _quote_postgres_identifier(db_name)
                await conn.execute(f"CREATE DATABASE {quoted_db}")
        finally:
            await conn.close()

    _retry(lambda: asyncio.run(_run()))


def _prepare_postgres_target(url: str) -> str:
    _ensure_postgres_database(url)

    async def _run() -> None:
        conn = await asyncpg.connect(url.replace("postgresql+asyncpg://", "postgresql://"))
        try:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS public.smoke_orders (
                    order_id SERIAL PRIMARY KEY,
                    customer_name TEXT NOT NULL,
                    amount NUMERIC(12,2) NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """
            )
            count = await conn.fetchval("SELECT COUNT(*) FROM public.smoke_orders")
            if int(count or 0) == 0:
                await conn.execute(
                    """
                    INSERT INTO public.smoke_orders (customer_name, amount)
                    VALUES
                        ('alice', 12.50),
                        ('bob', 20.00),
                        ('carol', 33.25)
                    """
                )
        finally:
            await conn.close()

    _retry(lambda: asyncio.run(_run()))
    return "public.smoke_orders"


def _prepare_mysql_target(url: str) -> str:
    parsed = urlparse(url)
    db_name = parsed.path.lstrip("/")
    if not db_name:
        raise RuntimeError("MySQL URL must include a database name.")

    def _seed_mysql() -> None:
        root_conn = mysql.connector.connect(
            host=parsed.hostname or "127.0.0.1",
            port=parsed.port or 3306,
            user=parsed.username or "root",
            password=parsed.password or "",
            autocommit=True,
        )
        try:
            cursor = root_conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
            cursor.close()
        finally:
            root_conn.close()

        conn = mysql.connector.connect(
            host=parsed.hostname or "127.0.0.1",
            port=parsed.port or 3306,
            user=parsed.username or "root",
            password=parsed.password or "",
            database=db_name,
            autocommit=True,
        )
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS smoke_orders (
                    order_id INT PRIMARY KEY AUTO_INCREMENT,
                    customer_name VARCHAR(255) NOT NULL,
                    amount DECIMAL(12,2) NOT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cursor.execute("SELECT COUNT(*) FROM smoke_orders")
            (count,) = cursor.fetchone()
            if int(count or 0) == 0:
                cursor.execute(
                    """
                    INSERT INTO smoke_orders (customer_name, amount)
                    VALUES
                        ('alice', 12.50),
                        ('bob', 20.00),
                        ('carol', 33.25)
                    """
                )
            cursor.close()
        finally:
            conn.close()

    _retry(_seed_mysql)

    return f"{db_name}.smoke_orders"


def _prepare_clickhouse_target(url: str) -> str:
    parsed = urlparse(url)
    db_name = parsed.path.lstrip("/") or "default"
    def _seed_clickhouse() -> None:
        client = clickhouse_connect.get_client(
            host=parsed.hostname or "127.0.0.1",
            port=parsed.port or 8123,
            username=parsed.username or "default",
            password=parsed.password or "",
            database="default",
        )
        try:
            client.command(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
            client.command(f"DROP TABLE IF EXISTS `{db_name}`.smoke_orders")
            client.command(
                f"""
                CREATE TABLE `{db_name}`.smoke_orders (
                    order_id UInt64,
                    customer_name String,
                    amount Float64,
                    created_at DateTime
                )
                ENGINE = MergeTree
                ORDER BY (created_at, order_id)
                """
            )
            client.command(
                f"""
                INSERT INTO `{db_name}`.smoke_orders
                VALUES
                    (1, 'alice', 12.5, now()),
                    (2, 'bob', 20.0, now()),
                    (3, 'carol', 33.25, now())
                """
            )
        finally:
            client.close()

    _retry(_seed_clickhouse)
    return f"{db_name}.smoke_orders"


def _prepare_target_database(engine: str, target_url: str) -> str:
    if engine == "postgresql":
        return _prepare_postgres_target(target_url)
    if engine == "mysql":
        return _prepare_mysql_target(target_url)
    if engine == "clickhouse":
        return _prepare_clickhouse_target(target_url)
    raise RuntimeError(f"Unsupported SMOKE_ENGINE: {engine}")


def _poll_sync_until_terminal(client: TestClient, timeout_seconds: float = 30.0) -> dict:
    deadline = time.time() + timeout_seconds
    last_payload: dict = {}
    while time.time() < deadline:
        response = client.get("/api/v1/sync/status")
        assert response.status_code == 200, response.text
        payload = response.json()
        last_payload = payload
        if payload.get("status") in {"idle", "completed", "failed"}:
            return payload
        time.sleep(0.25)
    return last_payload


@pytest.mark.integration
@pytest.mark.slow
def test_smoke_e2e_onboarding_ask_train_reset(monkeypatch: pytest.MonkeyPatch):
    engine = _required_env("SMOKE_ENGINE").lower()
    target_url = _required_env("SMOKE_TARGET_DATABASE_URL")
    system_db_url = _required_env("SYSTEM_DATABASE_URL")
    credentials_key = _required_env("DATABASE_CREDENTIALS_KEY")

    # Ensure system DB and target DB are ready before running the end-to-end flow.
    _ensure_postgres_database(system_db_url)
    related_table = _prepare_target_database(engine, target_url)

    # Keep smoke deterministic and keyless by forcing local provider; onboarding depth avoids LLM calls.
    monkeypatch.setenv("DATABASE_TYPE", engine)
    monkeypatch.setenv("DATABASE_URL", target_url)
    monkeypatch.setenv("SYSTEM_DATABASE_URL", system_db_url)
    monkeypatch.setenv("DATABASE_CREDENTIALS_KEY", credentials_key)
    monkeypatch.setenv("LLM_DEFAULT_PROVIDER", "local")
    monkeypatch.setenv("LLM_REQUIRE_PROVIDER_KEYS_ON_STARTUP", "false")
    clear_settings_cache()

    from backend.api.main import app
    from backend.cli import cli

    runner = CliRunner()

    # 1) Onboarding wizard
    onboarding = runner.invoke(
        cli,
        [
            "onboarding",
            "wizard",
            "--database-url",
            target_url,
            "--system-db",
            system_db_url,
            "--metrics-depth",
            "metrics_basic",
            "--max-tables",
            "5",
            "--sample-size",
            "25",
            "--batch-size",
            "5",
            "--max-metrics-per-table",
            "1",
            "--non-interactive",
        ],
    )
    assert onboarding.exit_code == 0, onboarding.output
    assert "Complete" in onboarding.output

    # 2) One ask (direct SQL keeps the smoke test deterministic).
    ask = runner.invoke(
        cli,
        [
            "ask",
            f"SELECT COUNT(*) AS smoke_count FROM {related_table}",
            "--execution-mode",
            "direct_sql",
        ],
    )
    assert ask.exit_code == 0, ask.output
    assert "smoke_count" in ask.output.lower() or "answer" in ask.output.lower()

    with TestClient(app) as client:
        # 3) Train datapoint API flow (create managed query datapoint + wait for sync terminal).
        datapoint_id = f"query_smoke_training_{engine}_001"
        create_payload = {
            "datapoint_id": datapoint_id,
            "type": "Query",
            "name": f"Smoke Trained Query ({engine})",
            "owner": "smoke-test@datachat.local",
            "tags": ["smoke", "training", "query-template"],
            "metadata": {
                "source": "smoke_e2e",
                "source_tier": "user",
                "scope": "database",
                "grain": "row-level",
                "exclusions": "No exclusions for smoke flow.",
                "confidence_notes": "Smoke integration datapoint for launch validation.",
            },
            "description": "Smoke-trained query datapoint used by CI launch flow.",
            "sql_template": f"SELECT COUNT(*) AS smoke_count FROM {related_table}",
            "parameters": {},
            "related_tables": [related_table],
        }
        create_response = client.post("/api/v1/datapoints", json=create_payload)
        assert create_response.status_code == 201, create_response.text
        assert create_response.json()["datapoint_id"] == datapoint_id

        sync_payload = _poll_sync_until_terminal(client)
        assert sync_payload.get("status") != "failed", sync_payload

        get_response = client.get(f"/api/v1/datapoints/{datapoint_id}")
        assert get_response.status_code == 200, get_response.text

    # 4) Reset
    reset = runner.invoke(
        cli,
        [
            "reset",
            "--yes",
            "--include-target",
            "--drop-all-target",
        ],
    )
    assert reset.exit_code == 0, reset.output
    assert "reset complete" in reset.output.lower()
