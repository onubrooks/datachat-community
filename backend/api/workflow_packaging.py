"""Workflow-style response packaging helpers (finance-focused v1)."""

from __future__ import annotations

import json
import math
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

from backend.models.api import (
    DataSource,
    WorkflowArtifacts,
    WorkflowDriver,
    WorkflowMetric,
    WorkflowSource,
)

FINANCE_SIGNAL_KEYWORDS = (
    "revenue",
    "deposit",
    "withdrawal",
    "net flow",
    "liquidity",
    "loan",
    "default",
    "delinquency",
    "interest",
    "fee",
    "balance",
    "bank_",
    "fx",
    "treasury",
    "risk",
)


def build_workflow_artifacts(
    *,
    query: str,
    answer: str,
    answer_source: str,
    data: dict[str, list] | None,
    sources: list[DataSource],
    validation_warnings: list[Any],
    clarifying_questions: list[str],
    has_datapoints: bool,
    workflow_mode: str | None = "auto",
    sql: str | None = None,
    retrieved_datapoints: list[dict[str, Any]] | None = None,
    used_datapoints: list[Any] | None = None,
) -> WorkflowArtifacts | None:
    """Build optional finance workflow package for decision-ready responses."""
    if answer_source in {"error", "clarification", "approval", "system"}:
        return None
    # Finance brief is intentionally finance-scoped even when finance workflow mode is enabled.
    # This prevents non-finance exploration/system answers from being mislabeled as finance briefs.
    if not _looks_finance_like(query=query, answer=answer, sources=sources):
        return None

    metrics = _extract_workflow_metrics(data)
    drivers = _extract_workflow_drivers(data)
    caveats = _build_workflow_caveats(
        query=query,
        sql=sql,
        answer_source=answer_source,
        data=data,
        sources=sources,
        validation_warnings=validation_warnings,
        clarifying_questions=clarifying_questions,
        has_datapoints=has_datapoints,
        retrieved_datapoints=retrieved_datapoints or [],
        used_datapoints=used_datapoints or [],
    )
    follow_ups = _build_workflow_follow_ups(query=query, data=data, drivers=drivers)
    workflow_sources = [
        WorkflowSource(
            datapoint_id=source.datapoint_id,
            name=source.name,
            source_type=source.type,
        )
        for source in sources[:5]
    ]

    return WorkflowArtifacts(
        package_version="1.0",
        domain="finance",
        summary=_summarize_answer(answer),
        metrics=metrics,
        drivers=drivers,
        caveats=caveats,
        sources=workflow_sources,
        follow_ups=follow_ups,
    )


def _looks_finance_like(*, query: str, answer: str, sources: list[DataSource]) -> bool:
    combined = f"{query} {answer}".lower()
    if any(keyword in combined for keyword in FINANCE_SIGNAL_KEYWORDS):
        return True
    for source in sources:
        source_text = f"{source.datapoint_id} {source.name} {source.type}".lower()
        if "bank" in source_text or "loan" in source_text or "deposit" in source_text:
            return True
    return False


def _summarize_answer(answer: str) -> str:
    cleaned = re.sub(r"\s+", " ", answer or "").strip()
    if not cleaned:
        return "No summary available."
    first_sentence = re.split(r"(?<=[.!?])\s", cleaned, maxsplit=1)[0]
    return first_sentence[:280]


def _extract_workflow_metrics(data: dict[str, list] | None) -> list[WorkflowMetric]:
    if not data:
        return []
    columns = list(data.keys())
    if not columns:
        return []
    row_count = max((len(data[column]) for column in columns), default=0)
    if row_count == 0:
        return []

    metrics: list[WorkflowMetric] = []
    if row_count == 1:
        for column in columns:
            value = data[column][0] if data[column] else None
            if value is None:
                continue
            metrics.append(WorkflowMetric(label=_pretty_label(column), value=_format_value(value)))
            if len(metrics) >= 5:
                break
        return metrics

    for column in columns:
        series = data.get(column, [])
        numeric_values = [
            float(value)
            for value in series
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        ]
        if not numeric_values:
            continue
        avg_value = sum(numeric_values) / len(numeric_values)
        metrics.append(
            WorkflowMetric(label=f"Average {_pretty_label(column)}", value=_format_value(avg_value))
        )
        if len(metrics) >= 4:
            break
    return metrics


def _extract_workflow_drivers(data: dict[str, list] | None) -> list[WorkflowDriver]:
    if not data:
        return []
    columns = list(data.keys())
    if len(columns) < 2:
        return []

    row_count = max((len(data[column]) for column in columns), default=0)
    if row_count <= 1:
        return []

    dimension_col = next(
        (
            column
            for column in columns
            if any(
                isinstance(value, str) and value.strip()
                for value in data.get(column, [])[: min(row_count, 30)]
            )
        ),
        None,
    )
    measure_col = next(
        (
            column
            for column in columns
            if any(
                isinstance(value, (int, float)) and not isinstance(value, bool)
                for value in data.get(column, [])[: min(row_count, 30)]
            )
        ),
        None,
    )
    if not dimension_col or not measure_col:
        return []

    ranked_rows: list[tuple[float, str]] = []
    for index in range(row_count):
        dimension_value = data.get(dimension_col, [None] * row_count)[index]
        measure_value = data.get(measure_col, [None] * row_count)[index]
        if not isinstance(dimension_value, str) or not dimension_value.strip():
            continue
        if not isinstance(measure_value, (int, float)) or isinstance(measure_value, bool):
            continue
        numeric_measure = float(measure_value)
        if math.isfinite(numeric_measure):
            ranked_rows.append((numeric_measure, dimension_value.strip()))
    ranked_rows.sort(reverse=True, key=lambda item: item[0])

    drivers: list[WorkflowDriver] = []
    for measure_value, dimension_value in ranked_rows[:3]:
        drivers.append(
            WorkflowDriver(
                dimension=_pretty_label(dimension_col),
                value=dimension_value,
                contribution=f"{_pretty_label(measure_col)}: {_format_value(measure_value)}",
            )
        )
    return drivers


def _build_workflow_caveats(
    *,
    query: str,
    sql: str | None,
    answer_source: str,
    data: dict[str, list] | None,
    sources: list[DataSource],
    validation_warnings: list[Any],
    clarifying_questions: list[str],
    has_datapoints: bool,
    retrieved_datapoints: list[dict[str, Any]],
    used_datapoints: list[Any],
) -> list[str]:
    caveats: list[str] = []
    caveats.extend(
        _build_specific_caveats(
            query=query,
            sql=sql,
            data=data,
            sources=sources,
            retrieved_datapoints=retrieved_datapoints,
            used_datapoints=used_datapoints,
        )
    )
    if answer_source == "context":
        caveats.append("Answer derived from context and metadata; verify with SQL for final reporting.")
    if not sources:
        caveats.append("No explicit DataPoint sources were attached to this answer.")
    if not has_datapoints:
        caveats.append("Running in live schema mode without DataPoints may reduce business-definition precision.")
    if clarifying_questions:
        caveats.append("Further clarification could improve answer precision.")
    for warning in validation_warnings[:2]:
        if isinstance(warning, dict):
            message = str(warning.get("message", "")).strip()
            if message:
                caveats.append(message)
    caveats = _dedupe_caveats(caveats)
    if not caveats:
        caveats.append("Review source assumptions before sharing externally.")
    return caveats[:4]


def _build_specific_caveats(
    *,
    query: str,
    sql: str | None,
    data: dict[str, list] | None,
    sources: list[DataSource],
    retrieved_datapoints: list[dict[str, Any]],
    used_datapoints: list[Any],
) -> list[str]:
    caveats: list[str] = []
    metadata_by_id = _index_datapoint_metadata(retrieved_datapoints)
    selected_ids = _normalize_used_datapoint_ids(used_datapoints)
    if not selected_ids:
        selected_ids = [source.datapoint_id for source in sources if source.datapoint_id]

    for datapoint_id in selected_ids[:5]:
        metadata = metadata_by_id.get(datapoint_id) or _load_datapoint_metadata_from_disk(datapoint_id)
        if metadata:
            caveats.extend(_extract_metadata_caveats(metadata))

    caveats.extend(_derive_query_shape_caveats(query=query, sql=sql, data=data))
    return _dedupe_caveats(caveats)


def _index_datapoint_metadata(
    retrieved_datapoints: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for item in retrieved_datapoints:
        if not isinstance(item, dict):
            continue
        datapoint_id = str(item.get("datapoint_id") or "").strip()
        metadata = item.get("metadata")
        if datapoint_id and isinstance(metadata, dict):
            indexed[datapoint_id] = metadata
    return indexed


def _normalize_used_datapoint_ids(used_datapoints: list[Any]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in used_datapoints:
        if isinstance(item, str):
            datapoint_id = item.strip()
        elif isinstance(item, dict):
            datapoint_id = str(item.get("datapoint_id", "")).strip()
        else:
            datapoint_id = str(getattr(item, "datapoint_id", "")).strip()
        if not datapoint_id or datapoint_id in seen:
            continue
        seen.add(datapoint_id)
        normalized.append(datapoint_id)
    return normalized


@lru_cache(maxsize=512)
def _load_datapoint_metadata_from_disk(datapoint_id: str) -> dict[str, Any] | None:
    if not datapoint_id:
        return None
    datapoints_root = Path("datapoints")
    if not datapoints_root.exists():
        return None
    for candidate in datapoints_root.rglob(f"{datapoint_id}.json"):
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        metadata = payload.get("metadata")
        if isinstance(metadata, dict):
            return metadata
    return None


def _extract_metadata_caveats(metadata: dict[str, Any]) -> list[str]:
    caveats: list[str] = []
    caveat_templates = _coerce_list(metadata.get("caveat_templates"))
    caveats.extend(caveat_templates[:2])

    exclusions = _normalize_text(metadata.get("exclusions"))
    if exclusions:
        caveats.append(f"Scope exclusions: {exclusions}")

    assumptions = _normalize_text(metadata.get("assumptions"))
    if assumptions:
        caveats.append(f"Assumptions: {assumptions}")

    methodology = _normalize_text(metadata.get("methodology"))
    if methodology:
        caveats.append(f"Methodology: {methodology}")

    confidence_notes = _normalize_text(metadata.get("confidence_notes"))
    if confidence_notes:
        caveats.append(f"Data quality note: {confidence_notes}")

    filtered = []
    for caveat in caveats:
        normalized = caveat.strip()
        if not normalized:
            continue
        if _is_low_information_caveat(normalized):
            continue
        filtered.append(_trim_caveat_text(normalized))
    return filtered


def _derive_query_shape_caveats(
    *,
    query: str,
    sql: str | None,
    data: dict[str, list] | None,
) -> list[str]:
    caveats: list[str] = []
    query_lower = query.lower()
    sql_lower = (sql or "").lower()

    if sql_lower and re.search(r"\blimit\s+\d+", sql_lower):
        if any(token in query_lower for token in ("top", "rank", "highest", "lowest")):
            caveats.append("Top-N output shows only the highest-ranked entities; tail exposure is not shown.")

    concentration_like = "concentration" in query_lower or "share of total" in query_lower
    if concentration_like and sql_lower:
        has_time_window = any(
            marker in sql_lower
            for marker in ("interval", "date_trunc", "posted_at", "business_date", "created_at")
        )
        if not has_time_window:
            caveats.append(
                "Concentration risk here is a balance-share snapshot and does not capture outflow velocity."
            )

    if data and "balance_share_pct" in data and "total_balance" in data:
        caveats.append(
            "Balance share is relative to included account statuses in this query, not all possible balances."
        )

    return [_trim_caveat_text(caveat) for caveat in caveats if caveat.strip()]


def _coerce_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        if stripped.startswith("[") and stripped.endswith("]"):
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                return [stripped]
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        return [stripped]
    return []


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text


def _trim_caveat_text(text: str, max_length: int = 220) -> str:
    if len(text) <= max_length:
        return text
    return text[: max_length - 3].rstrip() + "..."


def _dedupe_caveats(items: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in items:
        normalized = item.strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)
    return deduped


def _is_low_information_caveat(text: str) -> bool:
    normalized = text.lower()
    low_information_markers = (
        "no known exclusions",
        "validated against seeded demo dataset",
        "verify against production policies",
        "documented table/model constraints",
    )
    return any(marker in normalized for marker in low_information_markers)


def _build_workflow_follow_ups(
    *,
    query: str,
    data: dict[str, list] | None,
    drivers: list[WorkflowDriver],
) -> list[str]:
    follow_ups: list[str] = []
    lower_query = query.lower()
    if "last" in lower_query or "trend" in lower_query or "week" in lower_query:
        follow_ups.append("Compare this result against the previous equivalent period.")
    else:
        follow_ups.append("Show the same metric as a weekly trend for the last 8 weeks.")

    if drivers:
        primary_dimension = drivers[0].dimension
        follow_ups.append(f"Break down this result further by {primary_dimension}.")
    elif data and len(data.keys()) > 1:
        first_column = _pretty_label(list(data.keys())[0])
        follow_ups.append(f"Show the top contributors by {first_column}.")
    else:
        follow_ups.append("Show top drivers of change by segment and country.")

    follow_ups.append("List caveats and data quality checks for this answer.")
    return follow_ups[:3]


def _pretty_label(value: str) -> str:
    return value.replace("_", " ").strip().title()


def _format_value(value: Any) -> str:
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        if abs(value) >= 1_000:
            return f"{value:,.2f}"
        return f"{value:.4g}"
    if value is None:
        return "-"
    return str(value)
