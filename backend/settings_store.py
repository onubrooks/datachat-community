"""
Settings store utilities.

Persists configuration to ~/.datachat/config.json and bridges env <-> config
for one-time setup and reuse across CLI/UI.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

CONFIG_DIR = Path.home() / ".datachat"
CONFIG_PATH = CONFIG_DIR / "config.json"
DOTENV_PATH = Path(__file__).resolve().parents[1] / ".env"

TARGET_DB_KEY = "target_database_url"
SYSTEM_DB_KEY = "system_database_url"
DATABASE_CREDENTIALS_KEY = "database_credentials_key"
LLM_DEFAULT_PROVIDER_KEY = "llm_default_provider"
LLM_OPENAI_API_KEY = "llm_openai_api_key"
LLM_ANTHROPIC_API_KEY = "llm_anthropic_api_key"
LLM_GOOGLE_API_KEY = "llm_google_api_key"
LLM_OPENAI_MODEL = "llm_openai_model"
LLM_OPENAI_MODEL_MINI = "llm_openai_model_mini"
LLM_ANTHROPIC_MODEL = "llm_anthropic_model"
LLM_ANTHROPIC_MODEL_MINI = "llm_anthropic_model_mini"
LLM_GOOGLE_MODEL = "llm_google_model"
LLM_GOOGLE_MODEL_MINI = "llm_google_model_mini"
LLM_LOCAL_MODEL = "llm_local_model"
LLM_TEMPERATURE = "llm_temperature"

_CONFIG_TO_ENV = {
    TARGET_DB_KEY: "DATABASE_URL",
    SYSTEM_DB_KEY: "SYSTEM_DATABASE_URL",
    DATABASE_CREDENTIALS_KEY: "DATABASE_CREDENTIALS_KEY",
    LLM_DEFAULT_PROVIDER_KEY: "LLM_DEFAULT_PROVIDER",
    LLM_OPENAI_API_KEY: "LLM_OPENAI_API_KEY",
    LLM_ANTHROPIC_API_KEY: "LLM_ANTHROPIC_API_KEY",
    LLM_GOOGLE_API_KEY: "LLM_GOOGLE_API_KEY",
    LLM_OPENAI_MODEL: "LLM_OPENAI_MODEL",
    LLM_OPENAI_MODEL_MINI: "LLM_OPENAI_MODEL_MINI",
    LLM_ANTHROPIC_MODEL: "LLM_ANTHROPIC_MODEL",
    LLM_ANTHROPIC_MODEL_MINI: "LLM_ANTHROPIC_MODEL_MINI",
    LLM_GOOGLE_MODEL: "LLM_GOOGLE_MODEL",
    LLM_GOOGLE_MODEL_MINI: "LLM_GOOGLE_MODEL_MINI",
    LLM_LOCAL_MODEL: "LLM_LOCAL_MODEL",
    LLM_TEMPERATURE: "LLM_TEMPERATURE",
}


def _ensure_dir() -> None:
    CONFIG_DIR.mkdir(exist_ok=True, mode=0o700)


def load_config() -> dict[str, Any]:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, encoding="utf-8") as handle:
            return json.load(handle)
    return {}


def save_config(config: dict[str, Any]) -> None:
    _ensure_dir()
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as handle:
            json.dump(config, handle, indent=2)
    except PermissionError:
        return


def set_value(key: str, value: str | None) -> None:
    config = load_config()
    if value is None or str(value).strip() == "":
        config.pop(key, None)
    else:
        config[key] = value
    save_config(config)


def set_values(values: dict[str, str | None]) -> None:
    config = load_config()
    for key, value in values.items():
        if value is None or str(value).strip() == "":
            config.pop(key, None)
        else:
            config[key] = value
    save_config(config)


def get_value(key: str) -> str | None:
    return load_config().get(key)


def apply_config_defaults() -> None:
    """
    Persist env values into config (one-time) and apply config to env when missing.
    """
    env_source = os.getenv("DATA_CHAT_ENV_SOURCE", "dotenv").lower()
    if env_source in {"dotenv", "envfile", "file"} and DOTENV_PATH.exists():
        # Project-local .env should be authoritative for CLI defaults.
        load_dotenv(DOTENV_PATH, override=True)

    config = load_config()

    for config_key, env_key in _CONFIG_TO_ENV.items():
        env_value = os.getenv(env_key)
        if env_value and config.get(config_key) != env_value:
            config[config_key] = env_value

    if config:
        save_config(config)

    for config_key, env_key in _CONFIG_TO_ENV.items():
        if not os.getenv(env_key) and config.get(config_key):
            os.environ[env_key] = str(config[config_key])


def clear_config() -> None:
    """Remove persisted config file."""
    try:
        if CONFIG_PATH.exists():
            CONFIG_PATH.unlink()
    except OSError:
        return
