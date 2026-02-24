"""Visualization inference helpers for API responses."""

from __future__ import annotations

from typing import Any


def infer_direct_sql_visualization(
    data: dict[str, list[Any]] | None,
) -> tuple[str, dict[str, str]]:
    """Infer a deterministic visualization hint for direct SQL responses."""
    hint = _infer_visualization_hint(data)
    return (
        hint,
        {
            "requested": "direct_sql",
            "deterministic": hint,
            "final": hint,
            "resolution_reason": "direct_sql_result_shape",
        },
    )


def _infer_visualization_hint(data: dict[str, list[Any]] | None) -> str:
    if not data:
        return "none"

    column_names = list(data.keys())
    if not column_names:
        return "none"

    row_count = max(
        (
            len(values)
            for values in data.values()
            if isinstance(values, list)
        ),
        default=0,
    )
    if row_count <= 0:
        return "none"

    if len(column_names) == 1:
        return "table"

    numeric_columns = [
        column for column in column_names if _column_has_numeric_value(data.get(column))
    ]
    date_like_columns = [column for column in column_names if _is_date_like_column(column)]
    categorical_columns = [column for column in column_names if column not in numeric_columns]

    if len(numeric_columns) >= 2 and row_count <= 400:
        return "scatter"
    if numeric_columns and date_like_columns:
        return "line_chart"
    if numeric_columns and categorical_columns and row_count <= 40:
        return "bar_chart"
    return "table"


def _column_has_numeric_value(values: list[Any] | Any) -> bool:
    if not isinstance(values, list):
        return False
    return any(_to_float(value) is not None for value in values)


def _to_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return float(raw.replace(",", ""))
        except ValueError:
            return None
    return None


def _is_date_like_column(column_name: str) -> bool:
    lowered = column_name.lower()
    tokens = [token for token in lowered.replace("-", "_").split("_") if token]
    date_markers = {"date", "time", "timestamp", "datetime", "day", "week", "month", "year"}
    return any(token in date_markers for token in tokens)
