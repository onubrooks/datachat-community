"""Lifecycle metadata helpers for DataPoint authoring and updates."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any, Literal

from backend.models.datapoint import DataPoint

LifecycleAction = Literal["create", "update"]
LIFECYCLE_KEY = "lifecycle"
DEFAULT_REVIEWER = "pending-review"
DEFAULT_CREATED_REASON = "created"
DEFAULT_UPDATED_REASON = "updated"
DEFAULT_CHANGED_BY = "api"
DEFAULT_VERSION = "1.0.0"
_SEMVER_PATTERN = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    if isinstance(metadata, dict):
        return dict(metadata)
    return {}


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = _clean_text(value)
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_semver(version: str | None) -> tuple[int, int, int]:
    text = _clean_text(version)
    if not text:
        return (0, 0, 0)
    match = _SEMVER_PATTERN.match(text)
    if not match:
        return (0, 0, 0)
    return tuple(int(part) for part in match.groups())


def bump_patch_version(version: str | None) -> str:
    major, minor, patch = parse_semver(version)
    if (major, minor, patch) == (0, 0, 0):
        return DEFAULT_VERSION
    return f"{major}.{minor}.{patch + 1}"


def extract_lifecycle(datapoint: DataPoint) -> dict[str, str]:
    """Return normalized lifecycle metadata from a DataPoint."""
    metadata = _coerce_metadata(datapoint.metadata)
    raw = metadata.get(LIFECYCLE_KEY)
    if not isinstance(raw, dict):
        return {}
    lifecycle: dict[str, str] = {}
    for field_name in (
        "owner",
        "reviewer",
        "version",
        "changed_by",
        "changed_reason",
        "changed_at",
    ):
        cleaned = _clean_text(raw.get(field_name))
        if cleaned:
            lifecycle[field_name] = cleaned
    return lifecycle


def lifecycle_sort_key(datapoint: DataPoint) -> tuple[tuple[int, int, int], float]:
    """
    Return comparable sort key for "latest lifecycle wins".

    Sort order: semver version, then changed_at timestamp.
    """
    lifecycle = extract_lifecycle(datapoint)
    version_key = parse_semver(lifecycle.get("version"))
    changed_at = _parse_iso_datetime(lifecycle.get("changed_at"))
    timestamp = changed_at.timestamp() if changed_at else 0.0
    return (version_key, timestamp)


def apply_lifecycle_metadata(
    datapoint: DataPoint,
    *,
    action: LifecycleAction,
    changed_by: str = DEFAULT_CHANGED_BY,
    changed_reason: str | None = None,
    reviewer: str | None = None,
    previous_datapoint: DataPoint | None = None,
    timestamp: datetime | None = None,
) -> dict[str, str]:
    """
    Upsert lifecycle metadata on a DataPoint for API/ingestion update flows.

    For create, existing lifecycle values are honored when present.
    For update, server-side metadata is authoritative for audit fields to prevent
    stale client payloads from suppressing lifecycle revisions.
    """
    metadata = _coerce_metadata(datapoint.metadata)
    existing_lifecycle = extract_lifecycle(datapoint)
    previous_lifecycle = extract_lifecycle(previous_datapoint) if previous_datapoint else {}

    now = (timestamp or datetime.now(UTC)).astimezone(UTC).isoformat()
    reason_default = DEFAULT_CREATED_REASON if action == "create" else DEFAULT_UPDATED_REASON
    previous_version = previous_lifecycle.get("version")
    next_version = (
        bump_patch_version(previous_version) if action == "update" else DEFAULT_VERSION
    )

    reviewer_value = (
        _clean_text(reviewer)
        or _clean_text(existing_lifecycle.get("reviewer"))
        or _clean_text(previous_lifecycle.get("reviewer"))
        or DEFAULT_REVIEWER
    )
    if action == "update":
        lifecycle = {
            "owner": _clean_text(datapoint.owner) or datapoint.owner,
            "reviewer": reviewer_value,
            "version": next_version,
            "changed_by": _clean_text(changed_by) or DEFAULT_CHANGED_BY,
            "changed_reason": _clean_text(changed_reason) or reason_default,
            "changed_at": now,
        }
    else:
        lifecycle = {
            "owner": _clean_text(datapoint.owner) or datapoint.owner,
            "reviewer": reviewer_value,
            "version": _clean_text(existing_lifecycle.get("version")) or next_version,
            "changed_by": _clean_text(existing_lifecycle.get("changed_by"))
            or _clean_text(changed_by)
            or DEFAULT_CHANGED_BY,
            "changed_reason": _clean_text(existing_lifecycle.get("changed_reason"))
            or _clean_text(changed_reason)
            or reason_default,
            "changed_at": now,
        }

    metadata[LIFECYCLE_KEY] = lifecycle
    datapoint.metadata = metadata
    return lifecycle
