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
    if not value:
        return
    config = load_config()
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

    env_target = os.getenv("DATABASE_URL")
    env_system = os.getenv("SYSTEM_DATABASE_URL")

    if env_target and config.get(TARGET_DB_KEY) != env_target:
        config[TARGET_DB_KEY] = env_target
    if env_system and config.get(SYSTEM_DB_KEY) != env_system:
        config[SYSTEM_DB_KEY] = env_system

    if config:
        save_config(config)

    if not os.getenv("DATABASE_URL") and config.get(TARGET_DB_KEY):
        os.environ["DATABASE_URL"] = str(config[TARGET_DB_KEY])
    if not os.getenv("SYSTEM_DATABASE_URL") and config.get(SYSTEM_DB_KEY):
        os.environ["SYSTEM_DATABASE_URL"] = str(config[SYSTEM_DB_KEY])


def clear_config() -> None:
    """Remove persisted config file."""
    try:
        if CONFIG_PATH.exists():
            CONFIG_PATH.unlink()
    except OSError:
        return
