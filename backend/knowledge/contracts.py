"""DataPoint metadata contract validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from backend.models.datapoint import (
    BusinessDataPoint,
    DataPoint,
    ProcessDataPoint,
    QueryDataPoint,
    SchemaDataPoint,
)

Severity = Literal["error", "warning"]


@dataclass
class ContractIssue:
    """A single metadata contract violation."""

    code: str
    message: str
    severity: Severity
    field: str | None = None


@dataclass
class DataPointContractReport:
    """Contract validation result for one DataPoint."""

    datapoint_id: str
    issues: list[ContractIssue] = field(default_factory=list)

    @property
    def errors(self) -> list[ContractIssue]:
        return [issue for issue in self.issues if issue.severity == "error"]

    @property
    def warnings(self) -> list[ContractIssue]:
        return [issue for issue in self.issues if issue.severity == "warning"]

    @property
    def is_valid(self) -> bool:
        return not self.errors


def _metadata_value(datapoint: DataPoint, key: str) -> str | list[str] | None:
    value = (datapoint.metadata or {}).get(key)
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    if isinstance(value, list):
        cleaned = [str(item).strip() for item in value if str(item).strip()]
        return cleaned or None
    return None


def _lifecycle_value(datapoint: DataPoint, key: str) -> str | None:
    metadata = datapoint.metadata if isinstance(datapoint.metadata, dict) else {}
    lifecycle = metadata.get("lifecycle")
    if not isinstance(lifecycle, dict):
        return None
    value = lifecycle.get(key)
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _source_tier(datapoint: DataPoint) -> str:
    metadata = datapoint.metadata if isinstance(datapoint.metadata, dict) else {}
    raw = metadata.get("source_tier")
    if raw is None:
        return "unknown"
    tier = str(raw).strip().lower()
    return tier or "unknown"


def _has_lifecycle_block(datapoint: DataPoint) -> bool:
    metadata = datapoint.metadata if isinstance(datapoint.metadata, dict) else {}
    return isinstance(metadata.get("lifecycle"), dict)


def _missing_issue(*, code: str, message: str, field: str, severity: Severity) -> ContractIssue:
    return ContractIssue(code=code, message=message, field=field, severity=severity)


def validate_datapoint_contract(
    datapoint: DataPoint,
    *,
    strict: bool = False,
) -> DataPointContractReport:
    """
    Validate DataPoint contract expectations used by retrieval/runtime.

    Contract focus:
    - owner/freshness/unit-like attributes for runtime correctness
    - metadata quality signals for trust and governance
    """
    report = DataPointContractReport(datapoint_id=datapoint.datapoint_id)
    warning_level: Severity = "error" if strict else "warning"

    # Cross-type metadata quality fields (non-blocking unless strict=True).
    if not _metadata_value(datapoint, "grain"):
        report.issues.append(
            _missing_issue(
                code="missing_grain",
                message="Metadata should define analytic grain (e.g., row-level, daily_store).",
                field="metadata.grain",
                severity=warning_level,
            )
        )
    if not _metadata_value(datapoint, "exclusions"):
        report.issues.append(
            _missing_issue(
                code="missing_exclusions",
                message="Metadata should document exclusions/known omissions.",
                field="metadata.exclusions",
                severity=warning_level,
            )
        )
    if not _metadata_value(datapoint, "confidence_notes"):
        report.issues.append(
            _missing_issue(
                code="missing_confidence_notes",
                message="Metadata should include confidence notes or validation caveats.",
                field="metadata.confidence_notes",
                severity=warning_level,
            )
        )

    should_validate_lifecycle = _has_lifecycle_block(datapoint) or _source_tier(datapoint) in {
        "managed",
        "user",
    }
    if should_validate_lifecycle:
        if not _lifecycle_value(datapoint, "version"):
            report.issues.append(
                _missing_issue(
                    code="missing_lifecycle_version",
                    message="Metadata lifecycle should include semantic version metadata.lifecycle.version.",
                    field="metadata.lifecycle.version",
                    severity="warning",
                )
            )
        if not _lifecycle_value(datapoint, "changed_by"):
            report.issues.append(
                _missing_issue(
                    code="missing_lifecycle_changed_by",
                    message="Metadata lifecycle should include metadata.lifecycle.changed_by.",
                    field="metadata.lifecycle.changed_by",
                    severity="warning",
                )
            )
        if not _lifecycle_value(datapoint, "changed_reason"):
            report.issues.append(
                _missing_issue(
                    code="missing_lifecycle_changed_reason",
                    message="Metadata lifecycle should include metadata.lifecycle.changed_reason.",
                    field="metadata.lifecycle.changed_reason",
                    severity="warning",
                )
            )
        if not _lifecycle_value(datapoint, "reviewer"):
            report.issues.append(
                _missing_issue(
                    code="missing_lifecycle_reviewer",
                    message="Metadata lifecycle should include metadata.lifecycle.reviewer.",
                    field="metadata.lifecycle.reviewer",
                    severity="warning",
                )
            )

    if isinstance(datapoint, SchemaDataPoint):
        freshness = datapoint.freshness or _metadata_value(datapoint, "freshness")
        if not freshness:
            report.issues.append(
                _missing_issue(
                    code="missing_freshness",
                    message="Schema DataPoint must define freshness (top-level freshness or metadata.freshness).",
                    field="freshness",
                    severity="error",
                )
            )

    elif isinstance(datapoint, BusinessDataPoint):
        unit = (
            datapoint.unit
            or _metadata_value(datapoint, "unit")
            or _metadata_value(datapoint, "units")
        )
        if not unit:
            report.issues.append(
                _missing_issue(
                    code="missing_units",
                    message="Business DataPoint must define units (unit or metadata.unit/units).",
                    field="unit",
                    severity="error",
                )
            )
        if not _metadata_value(datapoint, "freshness"):
            report.issues.append(
                _missing_issue(
                    code="missing_freshness",
                    message="Business DataPoint should define freshness in metadata.freshness.",
                    field="metadata.freshness",
                    severity=warning_level,
                )
            )

    elif isinstance(datapoint, ProcessDataPoint):
        freshness = datapoint.data_freshness or _metadata_value(datapoint, "freshness")
        if not freshness:
            report.issues.append(
                _missing_issue(
                    code="missing_freshness",
                    message="Process DataPoint must define data freshness (data_freshness or metadata.freshness).",
                    field="data_freshness",
                    severity="error",
                )
            )

    elif isinstance(datapoint, QueryDataPoint):
        if not datapoint.related_tables:
            report.issues.append(
                _missing_issue(
                    code="missing_related_tables",
                    message="Query DataPoint should list related_tables for retrieval.",
                    field="related_tables",
                    severity=warning_level,
                )
            )

    return report


def validate_contracts(
    datapoints: list[DataPoint],
    *,
    strict: bool = False,
) -> list[DataPointContractReport]:
    """Validate contracts for a batch of DataPoints."""
    return [validate_datapoint_contract(datapoint, strict=strict) for datapoint in datapoints]
