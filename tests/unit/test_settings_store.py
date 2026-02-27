"""Unit tests for settings store precedence behavior."""

from __future__ import annotations

import json
import os
from pathlib import Path

from backend import settings_store


def _read_json(path: Path) -> dict:
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def test_apply_config_defaults_uses_dotenv_and_updates_config(tmp_path, monkeypatch):
    """When enabled, project .env should override stale env/config values."""
    config_dir = tmp_path / ".datachat"
    config_path = config_dir / "config.json"
    dotenv_path = tmp_path / ".env"

    config_dir.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        json.dumps(
            {
                settings_store.TARGET_DB_KEY: "postgresql://u:p@localhost:5432/stale_db",
                settings_store.SYSTEM_DB_KEY: "postgresql://u:p@localhost:5432/stale_system",
            }
        ),
        encoding="utf-8",
    )
    dotenv_path.write_text(
        "\n".join(
            [
                "DATABASE_URL=postgresql://u:p@localhost:5432/env_target",
                "SYSTEM_DATABASE_URL=postgresql://u:p@localhost:5432/env_system",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(settings_store, "CONFIG_DIR", config_dir)
    monkeypatch.setattr(settings_store, "CONFIG_PATH", config_path)
    monkeypatch.setattr(settings_store, "DOTENV_PATH", dotenv_path)
    monkeypatch.setenv("DATA_CHAT_ENV_SOURCE", "dotenv")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/shell_target")
    monkeypatch.setenv("SYSTEM_DATABASE_URL", "postgresql://u:p@localhost:5432/shell_system")

    settings_store.apply_config_defaults()

    assert os.getenv("DATABASE_URL") == "postgresql://u:p@localhost:5432/env_target"
    assert os.getenv("SYSTEM_DATABASE_URL") == "postgresql://u:p@localhost:5432/env_system"

    config = _read_json(config_path)
    assert config[settings_store.TARGET_DB_KEY] == "postgresql://u:p@localhost:5432/env_target"
    assert config[settings_store.SYSTEM_DB_KEY] == "postgresql://u:p@localhost:5432/env_system"


def test_apply_config_defaults_respects_env_source_when_not_dotenv(tmp_path, monkeypatch):
    """If dotenv precedence is disabled, existing environment values should remain."""
    config_dir = tmp_path / ".datachat"
    config_path = config_dir / "config.json"
    dotenv_path = tmp_path / ".env"

    config_dir.mkdir(parents=True, exist_ok=True)
    config_path.write_text("{}", encoding="utf-8")
    dotenv_path.write_text(
        "DATABASE_URL=postgresql://u:p@localhost:5432/from_dotenv\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(settings_store, "CONFIG_DIR", config_dir)
    monkeypatch.setattr(settings_store, "CONFIG_PATH", config_path)
    monkeypatch.setattr(settings_store, "DOTENV_PATH", dotenv_path)
    monkeypatch.setenv("DATA_CHAT_ENV_SOURCE", "environment")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/from_shell")

    settings_store.apply_config_defaults()

    assert os.getenv("DATABASE_URL") == "postgresql://u:p@localhost:5432/from_shell"
    config = _read_json(config_path)
    assert config[settings_store.TARGET_DB_KEY] == "postgresql://u:p@localhost:5432/from_shell"


def test_is_placeholder_database_url_detects_sample_values():
    assert settings_store.is_placeholder_database_url(
        "postgresql://user:pass@localhost:5432/testdb"
    )
    assert settings_store.is_placeholder_database_url("mysql://user:pass@host:3306/database")
    assert not settings_store.is_placeholder_database_url(
        "postgresql://real_user:real_pass@localhost:5432/warehouse"
    )


def test_apply_config_defaults_ignores_placeholder_env_and_applies_saved_target(
    tmp_path, monkeypatch
):
    """Saved config should override placeholder env URLs for target/system DB keys."""
    config_dir = tmp_path / ".datachat"
    config_path = config_dir / "config.json"
    dotenv_path = tmp_path / ".env"

    config_dir.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        json.dumps(
            {
                settings_store.TARGET_DB_KEY: "postgresql://cfg:pw@localhost:5432/cfg_db",
                settings_store.SYSTEM_DB_KEY: "postgresql://cfg:pw@localhost:5432/cfg_system",
            }
        ),
        encoding="utf-8",
    )
    dotenv_path.write_text(
        "\n".join(
            [
                "DATABASE_URL=postgresql://user:pass@localhost:5432/testdb",
                "SYSTEM_DATABASE_URL=postgresql://user:pass@localhost:5432/testdb",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(settings_store, "CONFIG_DIR", config_dir)
    monkeypatch.setattr(settings_store, "CONFIG_PATH", config_path)
    monkeypatch.setattr(settings_store, "DOTENV_PATH", dotenv_path)
    monkeypatch.setenv("DATA_CHAT_ENV_SOURCE", "dotenv")

    settings_store.apply_config_defaults()

    assert os.getenv("DATABASE_URL") == "postgresql://cfg:pw@localhost:5432/cfg_db"
    assert os.getenv("SYSTEM_DATABASE_URL") == "postgresql://cfg:pw@localhost:5432/cfg_system"
