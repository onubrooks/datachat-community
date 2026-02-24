"""Conflict detection and resolution for DataPoint sync."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Literal

from backend.knowledge.lifecycle import lifecycle_sort_key
from backend.models.datapoint import BusinessDataPoint, DataPoint, QueryDataPoint, SchemaDataPoint

ConflictMode = Literal["error", "prefer_user", "prefer_managed", "prefer_latest"]
_CONFLICT_ENFORCED_TIERS = {"managed", "user", "custom", "unknown"}
_DEFAULT_SOURCE_PRIORITY = {
    "user": 5,
    "managed": 4,
    "custom": 3,
    "unknown": 2,
    "demo": 1,
    "example": 1,
}


@dataclass
class DataPointConflict:
    """Represents one semantic conflict set."""

    key: str
    datapoint_ids: list[str]
    source_tiers: list[str]
    mode: ConflictMode
    resolved_datapoint_id: str | None = None


@dataclass
class ConflictResolutionResult:
    """Resolved datapoints and conflict decisions."""

    datapoints: list[DataPoint]
    conflicts: list[DataPointConflict] = field(default_factory=list)


class DataPointConflictError(RuntimeError):
    """Raised when sync conflict policy is `error` and duplicates are found."""

    def __init__(self, conflicts: list[DataPointConflict]):
        self.conflicts = conflicts
        sample = ", ".join(
            f"{conflict.key} -> {conflict.datapoint_ids}" for conflict in conflicts[:5]
        )
        suffix = "" if len(conflicts) <= 5 else f", ... +{len(conflicts) - 5} more"
        super().__init__(
            "Conflicting DataPoint semantics detected. "
            "Re-run sync with conflict_mode set to prefer_user, prefer_managed, or prefer_latest. "
            f"Conflicts: {sample}{suffix}"
        )


def source_tier(datapoint: DataPoint) -> str:
    metadata = datapoint.metadata if isinstance(datapoint.metadata, dict) else {}
    tier = str(metadata.get("source_tier", "")).strip().lower()
    return tier or "unknown"


def build_conflict_key(datapoint: DataPoint) -> str | None:
    """Build semantic conflict key aligned to retrieval behavior."""
    if isinstance(datapoint, SchemaDataPoint):
        table_key = datapoint.table_name.strip().lower()
        schema = datapoint.schema_name.strip().lower()
        if table_key and "." not in table_key and schema:
            table_key = f"{schema}.{table_key}"
        return f"table::{table_key}" if table_key else None

    name_key = datapoint.name.strip().lower()
    if isinstance(datapoint, BusinessDataPoint):
        related_tables = sorted(table.strip().lower() for table in datapoint.related_tables if table)
        if related_tables:
            return f"metric::{name_key}::{'|'.join(related_tables)}"
        return f"metric::{name_key}"

    if isinstance(datapoint, QueryDataPoint):
        related_tables = sorted(table.strip().lower() for table in datapoint.related_tables if table)
        if related_tables:
            return f"query::{name_key}::{'|'.join(related_tables)}"
        return f"query::{name_key}"

    return f"{datapoint.type.lower()}::{name_key}" if name_key else None


def resolve_datapoint_conflicts(
    datapoints: list[DataPoint], *, mode: ConflictMode = "error"
) -> ConflictResolutionResult:
    """Resolve duplicate semantic definitions according to conflict mode."""
    deduped = _dedupe_by_datapoint_id(datapoints)
    groups: dict[str, list[DataPoint]] = defaultdict(list)
    passthrough_ids: set[str] = set()

    for datapoint in deduped:
        key = build_conflict_key(datapoint)
        if not key:
            passthrough_ids.add(datapoint.datapoint_id)
            continue
        groups[key].append(datapoint)

    resolved_ids: set[str] = set(passthrough_ids)
    conflicts: list[DataPointConflict] = []

    for key, members in groups.items():
        enforced_members = [
            datapoint for datapoint in members if source_tier(datapoint) in _CONFLICT_ENFORCED_TIERS
        ]
        if len(enforced_members) <= 1:
            resolved_ids.update(datapoint.datapoint_id for datapoint in members)
            continue

        conflict = DataPointConflict(
            key=key,
            datapoint_ids=[datapoint.datapoint_id for datapoint in enforced_members],
            source_tiers=[source_tier(datapoint) for datapoint in enforced_members],
            mode=mode,
        )

        if mode == "error":
            conflicts.append(conflict)
            continue

        winner = max(enforced_members, key=lambda datapoint: _selection_key(datapoint, mode=mode))
        conflict.resolved_datapoint_id = winner.datapoint_id
        conflicts.append(conflict)
        resolved_ids.add(winner.datapoint_id)

    if mode == "error" and conflicts:
        raise DataPointConflictError(conflicts)

    resolved_datapoints = [
        datapoint for datapoint in deduped if datapoint.datapoint_id in resolved_ids
    ]
    return ConflictResolutionResult(datapoints=resolved_datapoints, conflicts=conflicts)


def _dedupe_by_datapoint_id(datapoints: list[DataPoint]) -> list[DataPoint]:
    selected: dict[str, DataPoint] = {}
    for datapoint in datapoints:
        existing = selected.get(datapoint.datapoint_id)
        if existing is None:
            selected[datapoint.datapoint_id] = datapoint
            continue
        if _selection_key(datapoint, mode="prefer_user") > _selection_key(
            existing, mode="prefer_user"
        ):
            selected[datapoint.datapoint_id] = datapoint
    ordered: list[DataPoint] = []
    seen: set[str] = set()
    for datapoint in datapoints:
        datapoint_id = datapoint.datapoint_id
        if datapoint_id in seen:
            continue
        seen.add(datapoint_id)
        ordered.append(selected[datapoint_id])
    return ordered


def _selection_key(datapoint: DataPoint, *, mode: ConflictMode) -> tuple:
    tier = source_tier(datapoint)
    if mode == "prefer_managed":
        tier_priority = {
            "managed": 5,
            "user": 4,
            "custom": 3,
            "unknown": 2,
            "demo": 1,
            "example": 1,
        }.get(tier, 0)
    else:
        tier_priority = _DEFAULT_SOURCE_PRIORITY.get(tier, 0)

    lifecycle_key = lifecycle_sort_key(datapoint)
    path = ""
    if isinstance(datapoint.metadata, dict):
        path = str(datapoint.metadata.get("source_path", "")).strip()
    if mode == "prefer_latest":
        return (
            lifecycle_key[0],
            lifecycle_key[1],
            tier_priority,
            datapoint.datapoint_id,
            path,
        )
    return (
        tier_priority,
        lifecycle_key[0],
        lifecycle_key[1],
        datapoint.datapoint_id,
        path,
    )
