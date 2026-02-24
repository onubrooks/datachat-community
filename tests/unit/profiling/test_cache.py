"""Unit tests for profiling cache helpers."""

from __future__ import annotations

from backend.profiling.cache import (
    build_profile_signature,
    load_profile_cache,
    write_profile_cache,
)


def test_build_profile_signature_ignores_credentials():
    one = build_profile_signature(
        "postgresql",
        "postgresql://user1:pass1@localhost:5432/warehouse",
    )
    two = build_profile_signature(
        "postgresql",
        "postgresql://user2:pass2@localhost:5432/warehouse",
    )
    assert one == two


def test_write_and_load_profile_cache(tmp_path):
    payload = {"tables": [{"name": "public.orders"}], "tables_profiled": 1}
    path = write_profile_cache(
        database_type="postgresql",
        database_url="postgresql://u:p@localhost:5432/warehouse",
        payload=payload,
        cache_dir=tmp_path,
    )
    assert path is not None
    loaded = load_profile_cache(
        database_type="postgresql",
        database_url="postgresql://u:p@localhost:5432/warehouse",
        cache_dir=tmp_path,
    )
    assert loaded == payload
