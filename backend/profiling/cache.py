"""Local cache helpers for lightweight profiling summaries."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from urllib.parse import urlparse

DEFAULT_PROFILE_CACHE_DIR = Path.home() / ".datachat" / "cache" / "profiles"


def _normalize_database_type(database_type: str | None) -> str:
    value = (database_type or "").strip().lower()
    if value in {"postgres", "postgresql"}:
        return "postgresql"
    return value


def build_profile_signature(database_type: str | None, database_url: str | None) -> str | None:
    """Build a stable cache key from database identity (not credentials)."""
    if not database_url:
        return None
    normalized_type = _normalize_database_type(database_type)
    parsed = urlparse(database_url.replace("postgresql+asyncpg://", "postgresql://"))
    if not parsed.hostname:
        return None
    identity = "|".join(
        [
            normalized_type,
            parsed.hostname or "",
            str(parsed.port or ""),
            parsed.path.lstrip("/") if parsed.path else "",
        ]
    )
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return digest[:24]


def get_profile_cache_path(
    database_type: str | None,
    database_url: str | None,
    cache_dir: Path | None = None,
) -> Path | None:
    signature = build_profile_signature(database_type, database_url)
    if not signature:
        return None
    root = Path(cache_dir) if cache_dir else DEFAULT_PROFILE_CACHE_DIR
    return root / f"{signature}.json"


def write_profile_cache(
    *,
    database_type: str | None,
    database_url: str | None,
    payload: dict,
    cache_dir: Path | None = None,
) -> Path | None:
    """Persist lightweight profile snapshot to a local cache file."""
    path = get_profile_cache_path(
        database_type=database_type,
        database_url=database_url,
        cache_dir=cache_dir,
    )
    if path is None:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, default=str, indent=2))
    return path


def load_profile_cache(
    *,
    database_type: str | None,
    database_url: str | None,
    cache_dir: Path | None = None,
) -> dict | None:
    """Load lightweight profile snapshot if available."""
    path = get_profile_cache_path(
        database_type=database_type,
        database_url=database_url,
        cache_dir=cache_dir,
    )
    if path is None or not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None
