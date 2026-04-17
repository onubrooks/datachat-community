"""LLM-backed DataPoint generation from profiles."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable

from backend.llm.factory import LLMProviderFactory
from backend.llm.models import LLMMessage, LLMRequest
from backend.models.datapoint import (
    BusinessDataPoint,
    ColumnMetadata,
    QueryDataPoint,
    QueryParameter,
    SchemaDataPoint,
)
from backend.profiling.models import (
    ColumnProfile,
    DatabaseProfile,
    GeneratedDataPoint,
    GeneratedDataPoints,
    TableProfile,
)

_DEFAULT_OWNER = "auto-profiler@datachat.ai"
_DEFAULT_EXCLUSIONS = "No explicit exclusions documented by auto-profiler."
_DEFAULT_CONFIDENCE_NOTES = (
    "Auto-generated datapoint. Human validation is recommended before "
    "production-critical use."
)
_NUMERIC_TYPE_TOKENS = ("int", "numeric", "decimal", "float", "double", "real", "number")


class DataPointGenerator:
    """Generate DataPoints from a schema profile using LLM assistance."""

    def __init__(self, llm_provider=None) -> None:
        if llm_provider is None:
            from backend.config import get_settings

            settings = get_settings()
            self._llm = LLMProviderFactory.create_default_provider(
                settings.llm, model_type="mini"
            )
        else:
            self._llm = llm_provider

    async def generate_from_profile(
        self,
        profile: DatabaseProfile,
        tables: list[str] | None = None,
        depth: str = "metrics_basic",
        batch_size: int = 10,
        max_tables: int | None = None,
        max_metrics_per_table: int = 3,
        progress_callback=None,
    ) -> GeneratedDataPoints:
        selected_tables = self._select_tables(profile.tables, tables, max_tables)
        schema_points: list[GeneratedDataPoint] = []
        query_points: list[GeneratedDataPoint] = []

        use_llm_schema = depth == "metrics_full"
        for idx, table in enumerate(selected_tables, start=1):
            if use_llm_schema:
                schema_points.append(await self._generate_schema_datapoint(table, idx))
            else:
                schema_points.append(self._generate_schema_datapoint_deterministic(table, idx))
            if depth != "schema_only":
                query_points.extend(self._generate_query_datapoints(table))

        business_points: list[GeneratedDataPoint] = []
        if depth == "schema_only":
            result = self._dedupe_generated(
                GeneratedDataPoints(
                    profile_id=profile.profile_id,
                    schema_datapoints=schema_points,
                    business_datapoints=business_points,
                    query_datapoints=query_points,
                )
            )
            self._attach_connection_metadata(result, str(profile.connection_id))
            return result

        if depth == "metrics_basic":
            for idx, table in enumerate(selected_tables, start=1):
                business_points.extend(
                    self._generate_basic_metrics(table, idx, max_metrics_per_table)
                )
                if progress_callback:
                    await progress_callback(len(selected_tables), idx)
        else:
            business_points = await self._generate_business_datapoints_batched(
                selected_tables,
                batch_size=batch_size,
                max_metrics_per_table=max_metrics_per_table,
                progress_callback=progress_callback,
            )

        result = self._dedupe_generated(
            GeneratedDataPoints(
                profile_id=profile.profile_id,
                schema_datapoints=schema_points,
                business_datapoints=business_points,
                query_datapoints=query_points,
            )
        )
        self._attach_connection_metadata(result, str(profile.connection_id))
        return result

    @staticmethod
    def _attach_connection_metadata(
        generated: GeneratedDataPoints, connection_id: str
    ) -> None:
        for item in generated.all_items():
            payload = item.datapoint
            if not isinstance(payload, dict):
                continue
            metadata = payload.get("metadata")
            if not isinstance(metadata, dict):
                metadata = {}
                payload["metadata"] = metadata
            metadata["connection_id"] = connection_id
            DataPointGenerator._ensure_contract_metadata(payload)

    @staticmethod
    def _ensure_contract_metadata(payload: dict[str, object]) -> None:
        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
            payload["metadata"] = metadata

        datapoint_type = str(payload.get("type") or "").strip().lower()
        default_grain = {
            "schema": "row-level",
            "business": "table-level",
            "process": "workflow-level",
            "query": "query-level",
        }.get(datapoint_type, "unknown")

        grain = metadata.get("grain")
        if not isinstance(grain, str) or not grain.strip():
            metadata["grain"] = default_grain

        exclusions = metadata.get("exclusions")
        if not isinstance(exclusions, str) or not exclusions.strip():
            metadata["exclusions"] = _DEFAULT_EXCLUSIONS

        confidence_notes = metadata.get("confidence_notes")
        if not isinstance(confidence_notes, str) or not confidence_notes.strip():
            metadata["confidence_notes"] = _DEFAULT_CONFIDENCE_NOTES

        freshness = metadata.get("freshness")
        if not isinstance(freshness, str) or not freshness.strip():
            metadata["freshness"] = "unknown"

    @staticmethod
    def _sanitize_row_count(value: int | None) -> int | None:
        if value is None:
            return None
        return value if value >= 0 else None

    @staticmethod
    def _contract_metadata(
        table: TableProfile,
        *,
        source: str,
        grain: str,
        freshness: str = "unknown",
    ) -> dict[str, str]:
        table_key = f"{table.schema_name}.{table.name}"
        return {
            "source": source,
            "table_key": table_key,
            "grain": grain,
            "freshness": freshness,
            "exclusions": _DEFAULT_EXCLUSIONS,
            "confidence_notes": _DEFAULT_CONFIDENCE_NOTES,
        }

    @staticmethod
    def _is_numeric_data_type(data_type: str | None) -> bool:
        if not data_type:
            return False
        lowered = data_type.lower()
        return any(token in lowered for token in _NUMERIC_TYPE_TOKENS)

    @staticmethod
    def _is_temporal_data_type(data_type: str | None) -> bool:
        if not data_type:
            return False
        lowered = data_type.lower()
        return any(token in lowered for token in ("date", "time", "timestamp"))

    @staticmethod
    def _infer_table_role(
        *,
        has_temporal: bool,
        measure_count: int,
        dimension_count: int,
    ) -> str:
        if has_temporal and measure_count > 0:
            return "fact_event"
        if dimension_count >= measure_count and measure_count <= 1:
            return "dimension_reference"
        if measure_count > 0:
            return "fact_snapshot"
        return "lookup"

    def _infer_display_hints(
        self,
        *,
        has_temporal: bool,
        measure_count: int,
        dimension_count: int,
    ) -> list[str]:
        if has_temporal and measure_count > 0:
            return ["line", "area", "table"]
        if dimension_count > 0 and measure_count > 0:
            return ["bar", "table", "pie"]
        if measure_count > 0:
            return ["kpi", "table"]
        return ["table"]

    def _build_table_semantic_metadata(self, table: TableProfile) -> dict[str, object]:
        dimensions: list[str] = []
        measures: list[str] = []
        time_columns: list[str] = []
        for col in table.columns:
            name = col.name
            if self._is_temporal_data_type(col.data_type):
                time_columns.append(name)
                continue
            if self._is_numeric_data_type(col.data_type):
                measures.append(name)
                continue
            dimensions.append(name)

        role = self._infer_table_role(
            has_temporal=bool(time_columns),
            measure_count=len(measures),
            dimension_count=len(dimensions),
        )
        display_hints = self._infer_display_hints(
            has_temporal=bool(time_columns),
            measure_count=len(measures),
            dimension_count=len(dimensions),
        )
        return {
            "semantic_role": role,
            "dimension_columns": dimensions[:12],
            "measure_columns": measures[:12],
            "time_columns": time_columns[:8],
            "display_hints": display_hints,
            "profiled_columns": self._build_profile_column_snapshot(table.columns),
        }

    @staticmethod
    def _build_profile_column_snapshot(columns: list[ColumnProfile]) -> list[dict[str, object]]:
        snapshot: list[dict[str, object]] = []
        for column in columns[:25]:
            snapshot.append(
                {
                    "name": column.name,
                    "type": column.data_type,
                    "distinct_count": column.distinct_count,
                    "sample_values": [str(value) for value in (column.sample_values or [])[:5]],
                    "null_count": column.null_count,
                }
            )
        return snapshot

    def _build_metric_semantic_metadata(
        self, table: TableProfile, aggregation: str | None
    ) -> dict[str, object]:
        normalized = (aggregation or "CUSTOM").upper()
        metric_kind = {
            "SUM": "additive_measure",
            "AVG": "average_measure",
            "COUNT": "volume_measure",
            "MIN": "extrema_measure",
            "MAX": "extrema_measure",
        }.get(normalized, "derived_measure")
        table_semantics = self._build_table_semantic_metadata(table)
        default_viz = "kpi"
        hints = table_semantics.get("display_hints")
        if isinstance(hints, list) and hints:
            default_viz = str(hints[0])
        return {
            "metric_kind": metric_kind,
            "default_visualization": default_viz,
            "table_semantic_role": table_semantics.get("semantic_role"),
        }

    @staticmethod
    def _default_metric_unit(aggregation: str | None) -> str:
        if aggregation and aggregation.upper() == "COUNT":
            return "count"
        return "unknown"

    async def _generate_schema_datapoint(
        self, table: TableProfile, index: int
    ) -> GeneratedDataPoint:
        system_prompt = (
            "You are a data analyst helping document a database table. "
            "Use domain-neutral language suitable for finance, operations, product, "
            "support, and growth teams. "
            "Return ONLY valid JSON."
        )
        user_prompt = self._build_schema_prompt(table)
        response = await self._llm.generate(
            LLMRequest(messages=[
                LLMMessage(role="system", content=system_prompt),
                LLMMessage(role="user", content=user_prompt),
            ])
        )
        payload = self._parse_json_response(response.content)

        business_purpose = payload.get(
            "business_purpose",
            f"Auto-profiled table {table.schema_name}.{table.name} for analytics.",
        )
        column_meanings = payload.get("columns", {}) if isinstance(payload, dict) else {}

        key_columns: list[ColumnMetadata] = []
        for col in table.columns:
            meaning = column_meanings.get(col.name)
            if not meaning:
                meaning = self._fallback_column_meaning(col.name, col.sample_values)
            key_columns.append(
                ColumnMetadata(
                    name=col.name,
                    type=col.data_type,
                    business_meaning=meaning,
                    nullable=col.nullable,
                    default_value=col.default_value,
                    sample_values=col.sample_values[:5],
                    distinct_count=col.distinct_count,
                )
            )

        common_queries = self._normalize_list(payload.get("common_queries"))
        gotchas = self._normalize_list(payload.get("gotchas"))
        freshness = payload.get("freshness") if isinstance(payload, dict) else None
        if not isinstance(freshness, str) or not freshness.strip():
            freshness = "unknown"

        if self._has_time_series(table) and not any("DATE_TRUNC" in q for q in common_queries):
            common_queries.append(
                "SELECT DATE_TRUNC('day', <timestamp_column>), COUNT(*) FROM "
                f"{table.schema_name}.{table.name} GROUP BY 1 ORDER BY 1;"
            )

        row_count = self._sanitize_row_count(table.row_count)
        metadata = self._contract_metadata(
            table,
            source="auto-profiler-llm",
            grain="row-level",
            freshness=freshness,
        )
        metadata.update(self._build_table_semantic_metadata(table))
        schema_datapoint = SchemaDataPoint(
            datapoint_id=self._make_table_id(table.schema_name, table.name),
            name=self._title_case(table.name),
            table_name=f"{table.schema_name}.{table.name}",
            schema=table.schema_name,
            business_purpose=self._ensure_min_length(business_purpose, 10),
            key_columns=key_columns,
            relationships=[
                self._relationship_to_model(rel) for rel in table.relationships
            ],
            common_queries=common_queries,
            gotchas=gotchas,
            freshness=freshness,
            row_count=row_count,
            owner=_DEFAULT_OWNER,
            tags=["auto-profiled"],
            metadata=metadata,
        )

        return GeneratedDataPoint(
            datapoint=schema_datapoint.model_dump(mode="json", by_alias=True),
            confidence=float(payload.get("confidence", 0.7))
            if isinstance(payload, dict)
            else 0.7,
            explanation=payload.get("explanation") if isinstance(payload, dict) else None,
        )

    def _generate_schema_datapoint_deterministic(
        self, table: TableProfile, index: int
    ) -> GeneratedDataPoint:
        key_columns = [
            ColumnMetadata(
                name=col.name,
                type=col.data_type,
                business_meaning=self._fallback_column_meaning(
                    col.name, col.sample_values
                ),
                nullable=col.nullable,
                default_value=col.default_value,
                sample_values=col.sample_values[:5],
                distinct_count=col.distinct_count,
            )
            for col in table.columns
        ]
        metadata = self._contract_metadata(
            table,
            source="auto-profiler-basic",
            grain="row-level",
            freshness="unknown",
        )
        metadata.update(self._build_table_semantic_metadata(table))
        schema_datapoint = SchemaDataPoint(
            datapoint_id=self._make_table_id(table.schema_name, table.name),
            name=self._title_case(table.name),
            table_name=f"{table.schema_name}.{table.name}",
            schema=table.schema_name,
            business_purpose=self._derive_table_purpose(table),
            key_columns=key_columns,
            relationships=[
                self._relationship_to_model(rel) for rel in table.relationships
            ],
            common_queries=[],
            gotchas=[],
            freshness="unknown",
            row_count=self._sanitize_row_count(table.row_count),
            owner=_DEFAULT_OWNER,
            tags=["auto-profiled"],
            metadata=metadata,
        )
        return GeneratedDataPoint(
            datapoint=schema_datapoint.model_dump(mode="json", by_alias=True),
            confidence=0.5,
            explanation="Deterministic schema summary",
        )

    async def _generate_business_datapoints(
        self, table: TableProfile, index: int
    ) -> list[GeneratedDataPoint]:
        numeric_columns = [
            col for col in table.columns if self._is_numeric_type(col.data_type)
        ]
        if not numeric_columns:
            return []

        system_prompt = (
            "You are a data analyst defining KPIs from numeric columns. "
            "Prefer practical metrics that fit any domain (finance, product, support, operations), "
            "not finance-only assumptions. "
            "Return ONLY valid JSON."
        )
        user_prompt = self._build_metric_prompt(table, numeric_columns)
        response = await self._llm.generate(
            LLMRequest(messages=[
                LLMMessage(role="system", content=system_prompt),
                LLMMessage(role="user", content=user_prompt),
            ])
        )
        payload = self._parse_json_response(response.content)
        metrics = payload.get("metrics", []) if isinstance(payload, dict) else []

        generated: list[GeneratedDataPoint] = []
        for _metric_index, metric in enumerate(metrics, start=1):
            name = metric.get("name") or f"{table.name} metric"
            calculation = metric.get("calculation") or f"SUM({numeric_columns[0].name})"
            business_rules = self._normalize_list(metric.get("business_rules"))
            synonyms = self._normalize_list(metric.get("synonyms"))

            business_datapoint = BusinessDataPoint(
                datapoint_id=self._make_metric_id(f"{table.schema_name}.{table.name}", name),
                name=name,
                calculation=calculation,
                synonyms=synonyms,
                business_rules=business_rules,
                related_tables=[f"{table.schema_name}.{table.name}"],
                unit=metric.get("unit") or self._default_metric_unit(
                    self._normalize_aggregation(metric.get("aggregation"))
                ),
                aggregation=self._normalize_aggregation(metric.get("aggregation")),
                owner=_DEFAULT_OWNER,
                tags=["auto-profiled"],
                metadata={
                    **self._contract_metadata(
                        table,
                        source="auto-profiler-llm",
                        grain="table-level",
                        freshness="unknown",
                    ),
                    **self._build_metric_semantic_metadata(
                        table,
                        self._normalize_aggregation(metric.get("aggregation")),
                    ),
                },
            )
            generated.append(
                GeneratedDataPoint(
                    datapoint=business_datapoint.model_dump(mode="json", by_alias=True),
                    confidence=self._normalize_confidence(metric.get("confidence", 0.6)),
                    explanation=metric.get("explanation"),
                )
            )

        return generated

    @staticmethod
    def _parse_json_response(content: str) -> dict:
        response_text = content.strip()
        start_idx = response_text.find("{")
        end_idx = response_text.rfind("}") + 1
        if start_idx == -1 or end_idx == 0:
            return {}
        json_str = response_text[start_idx:end_idx]
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            return {}

    @staticmethod
    def _normalize_list(value: object) -> list[str]:
        if isinstance(value, list):
            return [str(item) for item in value if item]
        if isinstance(value, str):
            return [value]
        return []

    @staticmethod
    def _select_tables(
        tables: list[TableProfile],
        requested: list[str] | None,
        max_tables: int | None,
    ) -> list[TableProfile]:
        selection = [
            table
            for table in tables
            if table.status != "failed" and table.columns
        ]
        if requested:
            requested_set = {name.lower() for name in requested}
            selection = [table for table in tables if table.name.lower() in requested_set]
            selection = [
                table
                for table in selection
                if table.status != "failed" and table.columns
            ]
        selection = sorted(selection, key=lambda t: t.row_count or 0, reverse=True)
        if max_tables is not None:
            return selection[:max_tables]
        return selection

    def _generate_basic_metrics(
        self, table: TableProfile, index: int, max_metrics_per_table: int
    ) -> list[GeneratedDataPoint]:
        numeric_columns = [
            col for col in table.columns if self._is_numeric_type(col.data_type)
        ]
        if not numeric_columns:
            return []

        metric_defs: list[tuple[str, str, str | None]] = [
            ("Row Count", "COUNT(*)", "COUNT"),
        ]
        col = self._select_primary_numeric_column(numeric_columns)
        metric_defs.append((f"Total {col}", f"SUM({col})", "SUM"))
        metric_defs.append((f"Average {col}", f"AVG({col})", "AVG"))

        generated: list[GeneratedDataPoint] = []
        for _metric_index, (name, calculation, aggregation) in enumerate(
            metric_defs[:max_metrics_per_table], start=1
        ):
            display_name = f"{self._title_case(table.name)} {name}"
            business_datapoint = BusinessDataPoint(
                datapoint_id=self._make_metric_id(f"{table.schema_name}.{table.name}", name),
                name=display_name,
                calculation=calculation,
                synonyms=[],
                business_rules=[],
                related_tables=[f"{table.schema_name}.{table.name}"],
                unit=self._default_metric_unit(aggregation),
                aggregation=aggregation,
                owner=_DEFAULT_OWNER,
                tags=["auto-profiled", "basic"],
                metadata={
                    **self._contract_metadata(
                        table,
                        source="auto-profiler-basic",
                        grain="table-level",
                        freshness="unknown",
                    ),
                    **self._build_metric_semantic_metadata(table, aggregation),
                },
            )
            generated.append(
                GeneratedDataPoint(
                    datapoint=business_datapoint.model_dump(
                        mode="json", by_alias=True
                    ),
                    confidence=0.6,
                    explanation="Deterministic metric template",
                )
            )
        return generated

    async def _generate_business_datapoints_batched(
        self,
        tables: list[TableProfile],
        batch_size: int,
        max_metrics_per_table: int,
        progress_callback=None,
    ) -> list[GeneratedDataPoint]:
        generated: list[GeneratedDataPoint] = []
        eligible_tables = [
            table
            for table in tables
            if any(self._is_numeric_type(col.data_type) for col in table.columns)
        ]
        for batch_start in range(0, len(eligible_tables), batch_size):
            batch = eligible_tables[batch_start : batch_start + batch_size]
            payload = await self._generate_metrics_batch(batch, max_metrics_per_table)
            for _idx, table in enumerate(batch):
                table_payload = payload.get(f"{table.schema_name}.{table.name}") or payload.get(
                    table.name, {}
                )
                metrics = table_payload.get("metrics", [])
                numeric_cols = [
                    col.name
                    for col in table.columns
                    if self._is_numeric_type(col.data_type)
                ]
                if not numeric_cols:
                    continue
                for _metric_index, metric in enumerate(metrics, start=1):
                    name = metric.get("name") or f"{table.name} metric"
                    calculation = metric.get("calculation") or f"SUM({numeric_cols[0]})"
                    business_rules = self._normalize_list(metric.get("business_rules"))
                    synonyms = self._normalize_list(metric.get("synonyms"))
                    business_datapoint = BusinessDataPoint(
                        datapoint_id=self._make_metric_id(
                            f"{table.schema_name}.{table.name}", name
                        ),
                        name=name,
                        calculation=calculation,
                        synonyms=synonyms,
                        business_rules=business_rules,
                        related_tables=[f"{table.schema_name}.{table.name}"],
                        unit=metric.get("unit") or self._default_metric_unit(
                            self._normalize_aggregation(metric.get("aggregation"))
                        ),
                        aggregation=self._normalize_aggregation(metric.get("aggregation")),
                        owner=_DEFAULT_OWNER,
                        tags=["auto-profiled"],
                        metadata={
                            **self._contract_metadata(
                                table,
                                source="auto-profiler-llm",
                                grain="table-level",
                                freshness="unknown",
                            ),
                            **self._build_metric_semantic_metadata(
                                table,
                                self._normalize_aggregation(metric.get("aggregation")),
                            ),
                        },
                    )
                    generated.append(
                        GeneratedDataPoint(
                            datapoint=business_datapoint.model_dump(
                                mode="json", by_alias=True
                            ),
                            confidence=self._normalize_confidence(
                                metric.get("confidence", 0.6)
                            ),
                            explanation=metric.get("explanation"),
                        )
                    )
            if progress_callback:
                await progress_callback(
                    len(eligible_tables),
                    min(batch_start + len(batch), len(eligible_tables)),
                )
        return generated

    async def _generate_metrics_batch(
        self, tables: list[TableProfile], max_metrics_per_table: int
    ) -> dict:
        system_prompt = (
            "You are a data analyst defining KPIs from numeric columns. "
            "Prefer practical metrics that fit any domain (finance, product, support, operations), "
            "not finance-only assumptions. "
            "Return ONLY valid JSON."
        )
        batch_payload = {
            "tables": [
                {
                    "table": f"{table.schema_name}.{table.name}",
                    "row_count_estimate": self._sanitize_row_count(table.row_count),
                    "numeric_columns": [
                        {
                            "name": col.name,
                            "type": col.data_type,
                            "distinct_count": col.distinct_count,
                            "sample_values": [str(v) for v in col.sample_values[:5]],
                            "min_value": col.min_value,
                            "max_value": col.max_value,
                        }
                        for col in table.columns
                        if self._is_numeric_type(col.data_type)
                    ],
                    "categorical_context": [
                        {
                            "name": col.name,
                            "type": col.data_type,
                            "distinct_count": col.distinct_count,
                            "sample_values": [str(v) for v in col.sample_values[:5]],
                        }
                        for col in table.columns
                        if not self._is_numeric_type(col.data_type)
                    ][:8],
                }
                for table in tables
            ],
            "max_metrics_per_table": max_metrics_per_table,
        }
        user_prompt = (
            "For each table, return JSON keyed by fully qualified table name (schema.table) with "
            "a list of metric objects under 'metrics'. "
            "Each metric must include name, calculation, aggregation, "
            "synonyms, business_rules, unit, confidence.\n\n"
            f"{json.dumps(batch_payload, indent=2)}"
        )
        response = await self._llm.generate(
            LLMRequest(
                messages=[
                    LLMMessage(role="system", content=system_prompt),
                    LLMMessage(role="user", content=user_prompt),
                ]
            )
        )
        payload = self._parse_json_response(response.content)
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _normalize_aggregation(value: object) -> str | None:
        if not isinstance(value, str):
            return None
        normalized = value.strip().upper()
        allowed = {"SUM", "AVG", "COUNT", "MIN", "MAX", "CUSTOM"}
        return normalized if normalized in allowed else None

    @staticmethod
    def _is_numeric_type(data_type: str) -> bool:
        return any(token in data_type.lower() for token in _NUMERIC_TYPE_TOKENS)

    @staticmethod
    def _has_time_series(table: TableProfile) -> bool:
        for col in table.columns:
            if any(
                keyword in col.name.lower()
                for keyword in ["date", "time", "timestamp", "created", "updated"]
            ):
                return True
        return False

    @staticmethod
    def _normalize_confidence(value: object) -> float:
        if isinstance(value, (int, float)):
            return max(0.0, min(float(value), 1.0))
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"high", "high confidence"}:
                return 0.8
            if lowered in {"medium", "medium confidence"}:
                return 0.6
            if lowered in {"low", "low confidence"}:
                return 0.4
            try:
                return max(0.0, min(float(lowered), 1.0))
            except ValueError:
                return 0.6
        return 0.6

    @staticmethod
    def _fallback_column_meaning(name: str, samples: Iterable[str]) -> str:
        normalized = name.lower()
        sample_preview = ", ".join(list(samples)[:3])
        base = f"Auto-profiled column `{name}`."
        if normalized == "id" or normalized.endswith("_id"):
            base = f"Identifier column for `{name}`."
        elif any(token in normalized for token in ["created", "updated", "timestamp", "date", "time"]):
            base = f"Timestamp/date column `{name}` used for time-based analysis."
        elif any(token in normalized for token in ["status", "state"]):
            base = f"Lifecycle status column `{name}`."
        elif any(token in normalized for token in ["type", "category", "segment"]):
            base = f"Classification column `{name}`."
        elif any(token in normalized for token in ["amount", "price", "cost", "revenue"]):
            base = f"Monetary value column `{name}`."
        elif any(token in normalized for token in ["qty", "quantity", "count", "volume"]):
            base = f"Volume/count column `{name}`."
        elif any(token in normalized for token in ["email", "phone", "contact"]):
            base = f"Contact information column `{name}`."
        elif any(token in normalized for token in ["country", "region", "city", "state_code"]):
            base = f"Geographic attribute column `{name}`."
        elif normalized.startswith("is_") or normalized.startswith("has_") or normalized.endswith("_flag"):
            base = f"Boolean flag column `{name}`."

        if sample_preview:
            return f"{base} Example values: {sample_preview}."
        return base

    @staticmethod
    def _ensure_min_length(value: str, length: int) -> str:
        if len(value) >= length:
            return value
        return value + " (auto-profiled)"

    def _select_primary_numeric_column(self, columns: list[ColumnProfile]) -> str:
        priority_tokens = [
            "amount",
            "total",
            "price",
            "cost",
            "revenue",
            "qty",
            "quantity",
            "count",
            "number",
            "balance",
            "score",
        ]
        scored = []
        for col in columns:
            score = 0
            name = col.name.lower()
            for token in priority_tokens:
                if token in name:
                    score += 1
            scored.append((score, col.name))
        scored.sort(reverse=True)
        return scored[0][1]

    @staticmethod
    def _is_identifier_like(column_name: str) -> bool:
        normalized = column_name.lower()
        return (
            normalized == "id"
            or normalized.endswith("_id")
            or normalized.endswith("uuid")
            or normalized.endswith("_key")
        )

    @staticmethod
    def _dimension_priority(column: ColumnProfile) -> tuple[int, int]:
        name = column.name.lower()
        score = 0
        if any(token in name for token in ("segment", "category", "region", "store", "channel", "status", "type")):
            score += 5
        if any(token in name for token in ("customer", "account", "product", "supplier", "team")):
            score += 3
        distinct = column.distinct_count if isinstance(column.distinct_count, int) else 0
        return (score, -distinct)

    def _select_dimension_column(self, columns: list[ColumnProfile]) -> ColumnProfile | None:
        candidates = [
            col
            for col in columns
            if not self._is_numeric_data_type(col.data_type)
            and not self._is_temporal_data_type(col.data_type)
            and not self._is_identifier_like(col.name)
        ]
        if not candidates:
            return None
        return sorted(candidates, key=self._dimension_priority, reverse=True)[0]

    @staticmethod
    def _time_priority(column: ColumnProfile) -> tuple[int, int]:
        name = column.name.lower()
        score = 0
        if "created" in name:
            score += 5
        if "date" in name:
            score += 4
        if "time" in name or "timestamp" in name:
            score += 3
        distinct = column.distinct_count if isinstance(column.distinct_count, int) else 0
        return (score, distinct)

    def _select_time_column(self, columns: list[ColumnProfile]) -> ColumnProfile | None:
        candidates = [col for col in columns if self._is_temporal_data_type(col.data_type)]
        if not candidates:
            return None
        return sorted(candidates, key=self._time_priority, reverse=True)[0]

    def _generate_query_datapoints(self, table: TableProfile) -> list[GeneratedDataPoint]:
        table_key = f"{table.schema_name}.{table.name}"
        numeric_columns = self._select_measure_columns(table.columns, limit=2)
        time_column = self._select_time_column(table.columns)
        dimension_column = self._select_dimension_column(table.columns)

        query_points: list[GeneratedDataPoint] = []
        query_points.extend(self._build_transaction_flow_query_datapoints(table=table))
        primary_measure = numeric_columns[0] if numeric_columns else None
        if primary_measure and dimension_column:
            query_points.extend(
                [
                    self._build_top_n_query_datapoint(
                        table=table,
                        dimension_column=dimension_column,
                        measure_column=primary_measure,
                    ),
                    self._build_average_by_dimension_query_datapoint(
                        table=table,
                        dimension_column=dimension_column,
                        measure_column=primary_measure,
                    ),
                    self._build_share_of_total_query_datapoint(
                        table=table,
                        dimension_column=dimension_column,
                        measure_column=primary_measure,
                    ),
                ]
            )
        if time_column:
            for measure_column in numeric_columns[:2]:
                query_points.extend(
                    [
                        self._build_weekly_trend_query_datapoint(
                            table=table,
                            time_column=time_column,
                            measure_column=measure_column,
                            dimension_column=dimension_column,
                        ),
                        self._build_monthly_trend_query_datapoint(
                            table=table,
                            time_column=time_column,
                            measure_column=measure_column,
                            dimension_column=dimension_column,
                        ),
                    ]
                )

        flow_query = self._build_net_flow_query_datapoint(
            table=table,
            time_column=time_column,
            dimension_column=dimension_column,
        )
        if flow_query is not None:
            query_points.append(flow_query)

        deduped: dict[str, GeneratedDataPoint] = {}
        for item in query_points:
            datapoint_id = str(item.datapoint.get("datapoint_id") or "")
            if datapoint_id and datapoint_id not in deduped:
                deduped[datapoint_id] = item
        return list(deduped.values())

    def _build_query_metadata(
        self,
        table: TableProfile,
        *,
        query_family: str,
        primary_dimension: str | None,
        primary_measure: str | None,
        time_grain: str | None = None,
    ) -> dict[str, object]:
        metadata = self._contract_metadata(
            table,
            source="auto-profiler-query-template",
            grain="query-level",
            freshness="unknown",
        )
        metadata.update(
            {
                "query_family": query_family,
                "primary_dimension": primary_dimension,
                "primary_measure": primary_measure,
                "time_grain": time_grain,
            }
        )
        return metadata

    @staticmethod
    def _anchor_ref_expr(
        *,
        dialect: str,
        table_key: str,
        time_column_name: str,
        filter_clause: str | None = None,
    ) -> tuple[str, str]:
        where_clause = f" WHERE {filter_clause}" if filter_clause else ""
        if dialect == "clickhouse":
            prefix = (
                f"WITH (SELECT max({time_column_name}) FROM {table_key}{where_clause}) AS anchor_date "
            )
            return prefix, "coalesce(anchor_date, today())"

        prefix = (
            f"WITH anchor AS (SELECT MAX({time_column_name}) AS anchor_date FROM "
            f"{table_key}{where_clause}) "
        )
        return prefix, "COALESCE((SELECT anchor_date FROM anchor), CURRENT_DATE)"

    def _build_transaction_flow_query_datapoints(
        self, *, table: TableProfile
    ) -> list[GeneratedDataPoint]:
        time_column = self._select_time_column(table.columns)
        if time_column is None:
            return []

        amount_column = next(
            (
                column
                for column in self._select_measure_columns(table.columns, limit=3)
                if "amount" in column.name.lower()
            ),
            None,
        )
        if amount_column is None:
            return []

        direction_column = next(
            (
                column
                for column in table.columns
                if not self._is_numeric_data_type(column.data_type)
                and any(token in column.name.lower() for token in ("direction", "flow_direction"))
                and any(
                    value.lower() in {"credit", "debit"}
                    for value in column.sample_values
                    if isinstance(value, str)
                )
            ),
            None,
        )
        if direction_column is None:
            return []

        query_points = [
            self._build_filtered_flow_trend_query_datapoint(
                table=table,
                time_column=time_column,
                amount_column=amount_column,
                direction_column=direction_column,
                event_name="Deposit",
                direction_value="credit",
                time_grain="week",
            ),
            self._build_filtered_flow_trend_query_datapoint(
                table=table,
                time_column=time_column,
                amount_column=amount_column,
                direction_column=direction_column,
                event_name="Deposit",
                direction_value="credit",
                time_grain="month",
            ),
            self._build_filtered_flow_trend_query_datapoint(
                table=table,
                time_column=time_column,
                amount_column=amount_column,
                direction_column=direction_column,
                event_name="Withdrawal",
                direction_value="debit",
                time_grain="week",
            ),
            self._build_filtered_flow_trend_query_datapoint(
                table=table,
                time_column=time_column,
                amount_column=amount_column,
                direction_column=direction_column,
                event_name="Withdrawal",
                direction_value="debit",
                time_grain="month",
            ),
        ]
        return query_points

    @staticmethod
    def _measure_priority(column: ColumnProfile) -> tuple[int, int]:
        name = column.name.lower()
        score = 0
        if any(token in name for token in ("revenue", "deposit", "withdraw", "amount", "balance")):
            score += 6
        if any(token in name for token in ("cost", "margin", "fee", "value", "sales")):
            score += 4
        if any(token in name for token in ("qty", "quantity", "count", "total", "score")):
            score += 2
        distinct = column.distinct_count if isinstance(column.distinct_count, int) else 0
        return (score, distinct)

    def _select_measure_columns(
        self, columns: list[ColumnProfile], limit: int = 2
    ) -> list[ColumnProfile]:
        candidates = [
            col
            for col in columns
            if self._is_numeric_type(col.data_type) and not self._is_identifier_like(col.name)
        ]
        if not candidates:
            return []
        ranked = sorted(candidates, key=self._measure_priority, reverse=True)
        return ranked[:limit]

    def _build_top_n_query_datapoint(
        self,
        *,
        table: TableProfile,
        dimension_column: ColumnProfile,
        measure_column: ColumnProfile,
    ) -> GeneratedDataPoint:
        table_key = f"{table.schema_name}.{table.name}"
        measure_name = self._title_case(measure_column.name)
        dimension_name = self._title_case(dimension_column.name)
        measure_alias = f"total_{self._slugify(measure_column.name)}"
        sql_template = (
            f"SELECT {dimension_column.name}, SUM({measure_column.name}) AS {measure_alias} "
            f"FROM {table_key} "
            f"GROUP BY 1 "
            f"ORDER BY {measure_alias} DESC "
            f"LIMIT {{top_n}}"
        )
        datapoint = QueryDataPoint(
            datapoint_id=self._make_query_id(table_key, f"top_{dimension_column.name}_{measure_column.name}"),
            name=f"Top {dimension_name} by {measure_name}",
            sql_template=sql_template,
            parameters={
                "top_n": QueryParameter(
                    type="integer",
                    required=False,
                    default=10,
                    description="Number of ranked rows to return.",
                )
            },
            description=(
                f"Ranks {dimension_name.lower()} values by summed {measure_name.lower()} in "
                f"{table_key}."
            ),
            related_tables=[table_key],
            owner=_DEFAULT_OWNER,
            tags=[
                "auto-profiled",
                "query-template",
                "ranking",
                "top_n",
                self._slugify(dimension_column.name),
                self._slugify(measure_column.name),
            ],
            metadata=self._build_query_metadata(
                table,
                query_family="top_n_ranking",
                primary_dimension=dimension_column.name,
                primary_measure=measure_column.name,
            ),
        )
        return GeneratedDataPoint(
            datapoint=datapoint.model_dump(mode="json", by_alias=True),
            confidence=0.72,
            explanation="Deterministic top-N query template from profiled dimension and measure columns.",
        )

    def _build_average_by_dimension_query_datapoint(
        self,
        *,
        table: TableProfile,
        dimension_column: ColumnProfile,
        measure_column: ColumnProfile,
    ) -> GeneratedDataPoint:
        table_key = f"{table.schema_name}.{table.name}"
        measure_slug = self._slugify(measure_column.name)
        measure_name = self._title_case(measure_column.name)
        dimension_name = self._title_case(dimension_column.name)
        sql_template = (
            f"SELECT {dimension_column.name}, AVG({measure_column.name}) AS avg_{measure_slug} "
            f"FROM {table_key} "
            f"GROUP BY 1 "
            f"ORDER BY avg_{measure_slug} DESC "
            f"LIMIT {{top_n}}"
        )
        datapoint = QueryDataPoint(
            datapoint_id=self._make_query_id(table_key, f"avg_{dimension_column.name}_{measure_column.name}"),
            name=f"Average {measure_name} by {dimension_name}",
            sql_template=sql_template,
            parameters={
                "top_n": QueryParameter(
                    type="integer",
                    required=False,
                    default=10,
                    description="Number of grouped rows to return.",
                )
            },
            description=(
                f"Compares average {measure_name.lower()} across "
                f"{dimension_name.lower()} values in {table_key}."
            ),
            related_tables=[table_key],
            owner=_DEFAULT_OWNER,
            tags=[
                "auto-profiled",
                "query-template",
                "average",
                "grouped",
                self._slugify(dimension_column.name),
                self._slugify(measure_column.name),
            ],
            metadata=self._build_query_metadata(
                table,
                query_family="average_by_dimension",
                primary_dimension=dimension_column.name,
                primary_measure=measure_column.name,
            ),
        )
        return GeneratedDataPoint(
            datapoint=datapoint.model_dump(mode="json", by_alias=True),
            confidence=0.7,
            explanation="Deterministic grouped average template from profiled dimension and measure columns.",
        )

    def _build_share_of_total_query_datapoint(
        self,
        *,
        table: TableProfile,
        dimension_column: ColumnProfile,
        measure_column: ColumnProfile,
    ) -> GeneratedDataPoint:
        table_key = f"{table.schema_name}.{table.name}"
        measure_slug = self._slugify(measure_column.name)
        measure_name = self._title_case(measure_column.name)
        dimension_name = self._title_case(dimension_column.name)
        postgres_template = (
            f"WITH grouped AS ("
            f"SELECT {dimension_column.name}, SUM({measure_column.name}) AS total_{measure_slug} "
            f"FROM {table_key} "
            f"GROUP BY 1"
            f") "
            f"SELECT {dimension_column.name}, total_{measure_slug}, "
            f"total_{measure_slug} / NULLIF(SUM(total_{measure_slug}) OVER (), 0) AS share_of_total "
            f"FROM grouped "
            f"ORDER BY total_{measure_slug} DESC "
            f"LIMIT {{top_n}}"
        )
        clickhouse_variant = (
            f"WITH grouped AS ("
            f"SELECT {dimension_column.name}, sum({measure_column.name}) AS total_{measure_slug} "
            f"FROM {table_key} "
            f"GROUP BY 1"
            f") "
            f"SELECT {dimension_column.name}, total_{measure_slug}, "
            f"total_{measure_slug} / nullIf(sum(total_{measure_slug}) OVER (), 0) AS share_of_total "
            f"FROM grouped "
            f"ORDER BY total_{measure_slug} DESC "
            f"LIMIT {{top_n}}"
        )
        datapoint = QueryDataPoint(
            datapoint_id=self._make_query_id(table_key, f"share_{dimension_column.name}_{measure_column.name}"),
            name=f"{measure_name} Share by {dimension_name}",
            sql_template=postgres_template,
            parameters={
                "top_n": QueryParameter(
                    type="integer",
                    required=False,
                    default=10,
                    description="Number of grouped rows to return.",
                )
            },
            description=(
                f"Shows each {dimension_name.lower()} value's share of total "
                f"{measure_name.lower()} in {table_key}."
            ),
            backend_variants={
                "mysql": postgres_template,
                "clickhouse": clickhouse_variant,
            },
            related_tables=[table_key],
            owner=_DEFAULT_OWNER,
            tags=[
                "auto-profiled",
                "query-template",
                "share_of_total",
                "composition",
                self._slugify(dimension_column.name),
                self._slugify(measure_column.name),
            ],
            metadata=self._build_query_metadata(
                table,
                query_family="share_of_total",
                primary_dimension=dimension_column.name,
                primary_measure=measure_column.name,
            ),
        )
        return GeneratedDataPoint(
            datapoint=datapoint.model_dump(mode="json", by_alias=True),
            confidence=0.69,
            explanation="Deterministic share-of-total template from profiled dimension and measure columns.",
        )

    def _build_weekly_trend_query_datapoint(
        self,
        *,
        table: TableProfile,
        time_column: ColumnProfile,
        measure_column: ColumnProfile,
        dimension_column: ColumnProfile | None,
    ) -> GeneratedDataPoint:
        table_key = f"{table.schema_name}.{table.name}"
        measure_slug = self._slugify(measure_column.name)
        measure_name = self._title_case(measure_column.name)
        group_dimension = ""
        description_suffix = ""
        if dimension_column is not None:
            group_dimension = f", {dimension_column.name}"
            description_suffix = f" by {self._title_case(dimension_column.name).lower()}"

        postgres_prefix, postgres_anchor_ref = self._anchor_ref_expr(
            dialect="postgresql",
            table_key=table_key,
            time_column_name=time_column.name,
        )
        postgres_template = (
            f"{postgres_prefix}"
            f"SELECT DATE_TRUNC('week', {time_column.name}) AS week_start{group_dimension}, "
            f"SUM({measure_column.name}) AS total_{measure_slug} "
            f"FROM {table_key} "
            f"WHERE {time_column.name} >= {postgres_anchor_ref} - INTERVAL '{{lookback_weeks}} weeks' "
            f"GROUP BY 1{', 2' if dimension_column is not None else ''} "
            f"ORDER BY 1 DESC"
        )
        mysql_prefix, mysql_anchor_ref = self._anchor_ref_expr(
            dialect="mysql",
            table_key=table_key,
            time_column_name=time_column.name,
        )
        mysql_variant = (
            f"{mysql_prefix}"
            f"SELECT DATE_SUB(DATE({time_column.name}), INTERVAL WEEKDAY({time_column.name}) DAY) AS week_start{group_dimension}, "
            f"SUM({measure_column.name}) AS total_{measure_slug} "
            f"FROM {table_key} "
            f"WHERE {time_column.name} >= DATE_SUB({mysql_anchor_ref}, INTERVAL {{lookback_weeks}} WEEK) "
            f"GROUP BY 1{', 2' if dimension_column is not None else ''} "
            f"ORDER BY 1 DESC"
        )
        clickhouse_prefix, clickhouse_anchor_ref = self._anchor_ref_expr(
            dialect="clickhouse",
            table_key=table_key,
            time_column_name=time_column.name,
        )
        clickhouse_variant = (
            f"{clickhouse_prefix}"
            f"SELECT toStartOfWeek({time_column.name}) AS week_start{group_dimension}, "
            f"sum({measure_column.name}) AS total_{measure_slug} "
            f"FROM {table_key} "
            f"WHERE {time_column.name} >= {clickhouse_anchor_ref} - INTERVAL {{lookback_weeks}} WEEK "
            f"GROUP BY 1{', 2' if dimension_column is not None else ''} "
            f"ORDER BY 1 DESC"
        )

        datapoint = QueryDataPoint(
            datapoint_id=self._make_query_id(table_key, f"weekly_{measure_column.name}_trend"),
            name=f"Weekly {measure_name} Trend",
            sql_template=postgres_template,
            parameters={
                "lookback_weeks": QueryParameter(
                    type="integer",
                    required=False,
                    default=8,
                    description="Number of recent weeks to include.",
                )
            },
            description=(
                f"Shows weekly {measure_name.lower()} totals{description_suffix} for the last "
                f"number of weeks in {table_key}."
            ),
            backend_variants={
                "mysql": mysql_variant,
                "clickhouse": clickhouse_variant,
            },
            related_tables=[table_key],
            owner=_DEFAULT_OWNER,
            tags=[
                "auto-profiled",
                "query-template",
                "weekly",
                "trend",
                "time_series",
                measure_slug,
                *( [self._slugify(dimension_column.name)] if dimension_column is not None else [] ),
            ],
            metadata=self._build_query_metadata(
                table,
                query_family="weekly_trend",
                primary_dimension=dimension_column.name if dimension_column is not None else None,
                primary_measure=measure_column.name,
                time_grain="week",
            ),
        )
        return GeneratedDataPoint(
            datapoint=datapoint.model_dump(mode="json", by_alias=True),
            confidence=0.74,
            explanation="Deterministic weekly trend template from profiled time and measure columns.",
        )

    def _build_monthly_trend_query_datapoint(
        self,
        *,
        table: TableProfile,
        time_column: ColumnProfile,
        measure_column: ColumnProfile,
        dimension_column: ColumnProfile | None,
    ) -> GeneratedDataPoint:
        table_key = f"{table.schema_name}.{table.name}"
        measure_slug = self._slugify(measure_column.name)
        measure_name = self._title_case(measure_column.name)
        group_dimension = ""
        description_suffix = ""
        if dimension_column is not None:
            group_dimension = f", {dimension_column.name}"
            description_suffix = f" by {self._title_case(dimension_column.name).lower()}"

        postgres_prefix, postgres_anchor_ref = self._anchor_ref_expr(
            dialect="postgresql",
            table_key=table_key,
            time_column_name=time_column.name,
        )
        postgres_template = (
            f"{postgres_prefix}"
            f"SELECT DATE_TRUNC('month', {time_column.name}) AS month_start{group_dimension}, "
            f"SUM({measure_column.name}) AS total_{measure_slug} "
            f"FROM {table_key} "
            f"WHERE {time_column.name} >= {postgres_anchor_ref} - INTERVAL '{{lookback_months}} months' "
            f"GROUP BY 1{', 2' if dimension_column is not None else ''} "
            f"ORDER BY 1 DESC"
        )
        mysql_prefix, mysql_anchor_ref = self._anchor_ref_expr(
            dialect="mysql",
            table_key=table_key,
            time_column_name=time_column.name,
        )
        mysql_variant = (
            f"{mysql_prefix}"
            f"SELECT DATE_FORMAT({time_column.name}, '%Y-%m-01') AS month_start{group_dimension}, "
            f"SUM({measure_column.name}) AS total_{measure_slug} "
            f"FROM {table_key} "
            f"WHERE {time_column.name} >= DATE_SUB({mysql_anchor_ref}, INTERVAL {{lookback_months}} MONTH) "
            f"GROUP BY 1{', 2' if dimension_column is not None else ''} "
            f"ORDER BY 1 DESC"
        )
        clickhouse_prefix, clickhouse_anchor_ref = self._anchor_ref_expr(
            dialect="clickhouse",
            table_key=table_key,
            time_column_name=time_column.name,
        )
        clickhouse_variant = (
            f"{clickhouse_prefix}"
            f"SELECT toStartOfMonth({time_column.name}) AS month_start{group_dimension}, "
            f"sum({measure_column.name}) AS total_{measure_slug} "
            f"FROM {table_key} "
            f"WHERE {time_column.name} >= {clickhouse_anchor_ref} - INTERVAL {{lookback_months}} MONTH "
            f"GROUP BY 1{', 2' if dimension_column is not None else ''} "
            f"ORDER BY 1 DESC"
        )

        datapoint = QueryDataPoint(
            datapoint_id=self._make_query_id(table_key, f"monthly_{measure_column.name}_trend"),
            name=f"Monthly {measure_name} Trend",
            sql_template=postgres_template,
            parameters={
                "lookback_months": QueryParameter(
                    type="integer",
                    required=False,
                    default=6,
                    description="Number of recent months to include.",
                )
            },
            description=(
                f"Shows monthly {measure_name.lower()} totals{description_suffix} for recent months "
                f"in {table_key}."
            ),
            backend_variants={
                "mysql": mysql_variant,
                "clickhouse": clickhouse_variant,
            },
            related_tables=[table_key],
            owner=_DEFAULT_OWNER,
            tags=[
                "auto-profiled",
                "query-template",
                "monthly",
                "trend",
                "time_series",
                measure_slug,
                *([self._slugify(dimension_column.name)] if dimension_column is not None else []),
            ],
            metadata=self._build_query_metadata(
                table,
                query_family="monthly_trend",
                primary_dimension=dimension_column.name if dimension_column is not None else None,
                primary_measure=measure_column.name,
                time_grain="month",
            ),
        )
        return GeneratedDataPoint(
            datapoint=datapoint.model_dump(mode="json", by_alias=True),
            confidence=0.73,
            explanation="Deterministic monthly trend template from profiled time and measure columns.",
        )

    def _build_net_flow_query_datapoint(
        self,
        *,
        table: TableProfile,
        time_column: ColumnProfile | None,
        dimension_column: ColumnProfile | None,
    ) -> GeneratedDataPoint | None:
        if time_column is None or dimension_column is None:
            return None

        deposits_column = None
        withdrawals_column = None
        for column in table.columns:
            if not self._is_numeric_type(column.data_type):
                continue
            normalized = column.name.lower()
            if deposits_column is None and any(token in normalized for token in ("deposit", "inflow", "credit")):
                deposits_column = column
            if withdrawals_column is None and any(token in normalized for token in ("withdraw", "outflow", "debit")):
                withdrawals_column = column

        if deposits_column is None or withdrawals_column is None:
            return None

        table_key = f"{table.schema_name}.{table.name}"
        postgres_prefix, postgres_anchor_ref = self._anchor_ref_expr(
            dialect="postgresql",
            table_key=table_key,
            time_column_name=time_column.name,
        )
        postgres_template = (
            f"{postgres_prefix}"
            f"SELECT DATE_TRUNC('week', {time_column.name}) AS week_start, "
            f"{dimension_column.name}, "
            f"SUM({deposits_column.name}) AS total_deposits, "
            f"SUM({withdrawals_column.name}) AS total_withdrawals, "
            f"SUM({deposits_column.name}) - SUM({withdrawals_column.name}) AS net_flow "
            f"FROM {table_key} "
            f"WHERE {time_column.name} >= {postgres_anchor_ref} - INTERVAL '{{lookback_weeks}} weeks' "
            f"GROUP BY 1, 2 "
            f"ORDER BY 1 DESC, net_flow DESC"
        )
        mysql_prefix, mysql_anchor_ref = self._anchor_ref_expr(
            dialect="mysql",
            table_key=table_key,
            time_column_name=time_column.name,
        )
        mysql_variant = (
            f"{mysql_prefix}"
            f"SELECT DATE_SUB(DATE({time_column.name}), INTERVAL WEEKDAY({time_column.name}) DAY) AS week_start, "
            f"{dimension_column.name}, "
            f"SUM({deposits_column.name}) AS total_deposits, "
            f"SUM({withdrawals_column.name}) AS total_withdrawals, "
            f"SUM({deposits_column.name}) - SUM({withdrawals_column.name}) AS net_flow "
            f"FROM {table_key} "
            f"WHERE {time_column.name} >= DATE_SUB({mysql_anchor_ref}, INTERVAL {{lookback_weeks}} WEEK) "
            f"GROUP BY 1, 2 "
            f"ORDER BY 1 DESC, net_flow DESC"
        )
        clickhouse_prefix, clickhouse_anchor_ref = self._anchor_ref_expr(
            dialect="clickhouse",
            table_key=table_key,
            time_column_name=time_column.name,
        )
        clickhouse_variant = (
            f"{clickhouse_prefix}"
            f"SELECT toStartOfWeek({time_column.name}) AS week_start, "
            f"{dimension_column.name}, "
            f"sum({deposits_column.name}) AS total_deposits, "
            f"sum({withdrawals_column.name}) AS total_withdrawals, "
            f"sum({deposits_column.name}) - sum({withdrawals_column.name}) AS net_flow "
            f"FROM {table_key} "
            f"WHERE {time_column.name} >= {clickhouse_anchor_ref} - INTERVAL {{lookback_weeks}} WEEK "
            f"GROUP BY 1, 2 "
            f"ORDER BY 1 DESC, net_flow DESC"
        )

        datapoint = QueryDataPoint(
            datapoint_id=self._make_query_id(table_key, "weekly_net_flow_by_dimension"),
            name=f"Weekly Deposits, Withdrawals, and Net Flow by {self._title_case(dimension_column.name)}",
            sql_template=postgres_template,
            parameters={
                "lookback_weeks": QueryParameter(
                    type="integer",
                    required=False,
                    default=8,
                    description="Number of recent weeks to include.",
                )
            },
            description=(
                f"Shows total deposits, withdrawals, and net flow by "
                f"{self._title_case(dimension_column.name).lower()} for recent weeks."
            ),
            backend_variants={
                "mysql": mysql_variant,
                "clickhouse": clickhouse_variant,
            },
            related_tables=[table_key],
            owner=_DEFAULT_OWNER,
            tags=[
                "auto-profiled",
                "query-template",
                "net_flow",
                "deposits",
                "withdrawals",
                "weekly",
                self._slugify(dimension_column.name),
            ],
            metadata=self._build_query_metadata(
                table,
                query_family="net_flow",
                primary_dimension=dimension_column.name,
                primary_measure=f"{deposits_column.name},{withdrawals_column.name}",
                time_grain="week",
            ),
        )
        return GeneratedDataPoint(
            datapoint=datapoint.model_dump(mode="json", by_alias=True),
            confidence=0.8,
            explanation="Heuristic net-flow template generated from paired deposit and withdrawal measures.",
        )

    def _build_filtered_flow_trend_query_datapoint(
        self,
        *,
        table: TableProfile,
        time_column: ColumnProfile,
        amount_column: ColumnProfile,
        direction_column: ColumnProfile,
        event_name: str,
        direction_value: str,
        time_grain: str,
    ) -> GeneratedDataPoint:
        table_key = f"{table.schema_name}.{table.name}"
        amount_slug = self._slugify(amount_column.name)
        event_slug = self._slugify(event_name)
        filter_clause = f"{direction_column.name} = '{direction_value}'"
        parameter_name = "lookback_weeks" if time_grain == "week" else "lookback_months"
        parameter_unit = "weeks" if time_grain == "week" else "months"
        default_value = 8 if time_grain == "week" else 6

        postgres_prefix, postgres_anchor_ref = self._anchor_ref_expr(
            dialect="postgresql",
            table_key=table_key,
            time_column_name=time_column.name,
            filter_clause=filter_clause,
        )
        mysql_prefix, mysql_anchor_ref = self._anchor_ref_expr(
            dialect="mysql",
            table_key=table_key,
            time_column_name=time_column.name,
            filter_clause=filter_clause,
        )
        clickhouse_prefix, clickhouse_anchor_ref = self._anchor_ref_expr(
            dialect="clickhouse",
            table_key=table_key,
            time_column_name=time_column.name,
            filter_clause=filter_clause,
        )

        if time_grain == "week":
            grain_label = "weekly"
            postgres_template = (
                f"{postgres_prefix}"
                f"SELECT DATE_TRUNC('week', {time_column.name}) AS week_start, "
                f"SUM({amount_column.name}) AS total_{event_slug}_{amount_slug} "
                f"FROM {table_key} "
                f"WHERE {filter_clause} "
                f"AND {time_column.name} >= {postgres_anchor_ref} - INTERVAL '{{{parameter_name}}} weeks' "
                f"GROUP BY 1 "
                f"ORDER BY 1 DESC"
            )
            mysql_variant = (
                f"{mysql_prefix}"
                f"SELECT DATE_SUB(DATE({time_column.name}), INTERVAL WEEKDAY({time_column.name}) DAY) AS week_start, "
                f"SUM({amount_column.name}) AS total_{event_slug}_{amount_slug} "
                f"FROM {table_key} "
                f"WHERE {filter_clause} "
                f"AND {time_column.name} >= DATE_SUB({mysql_anchor_ref}, INTERVAL {{{parameter_name}}} WEEK) "
                f"GROUP BY 1 "
                f"ORDER BY 1 DESC"
            )
            clickhouse_variant = (
                f"{clickhouse_prefix}"
                f"SELECT toStartOfWeek({time_column.name}) AS week_start, "
                f"sum({amount_column.name}) AS total_{event_slug}_{amount_slug} "
                f"FROM {table_key} "
                f"WHERE {filter_clause} "
                f"AND {time_column.name} >= {clickhouse_anchor_ref} - INTERVAL {{{parameter_name}}} WEEK "
                f"GROUP BY 1 "
                f"ORDER BY 1 DESC"
            )
            query_family = f"{grain_label}_{event_slug}_trend"
            name = f"Weekly {event_name} Trend from Latest {event_name} Date"
        else:
            grain_label = "monthly"
            postgres_template = (
                f"{postgres_prefix}"
                f"SELECT DATE_TRUNC('month', {time_column.name}) AS month_start, "
                f"SUM({amount_column.name}) AS total_{event_slug}_{amount_slug} "
                f"FROM {table_key} "
                f"WHERE {filter_clause} "
                f"AND {time_column.name} >= {postgres_anchor_ref} - INTERVAL '{{{parameter_name}}} months' "
                f"GROUP BY 1 "
                f"ORDER BY 1 DESC"
            )
            mysql_variant = (
                f"{mysql_prefix}"
                f"SELECT DATE_FORMAT({time_column.name}, '%Y-%m-01') AS month_start, "
                f"SUM({amount_column.name}) AS total_{event_slug}_{amount_slug} "
                f"FROM {table_key} "
                f"WHERE {filter_clause} "
                f"AND {time_column.name} >= DATE_SUB({mysql_anchor_ref}, INTERVAL {{{parameter_name}}} MONTH) "
                f"GROUP BY 1 "
                f"ORDER BY 1 DESC"
            )
            clickhouse_variant = (
                f"{clickhouse_prefix}"
                f"SELECT toStartOfMonth({time_column.name}) AS month_start, "
                f"sum({amount_column.name}) AS total_{event_slug}_{amount_slug} "
                f"FROM {table_key} "
                f"WHERE {filter_clause} "
                f"AND {time_column.name} >= {clickhouse_anchor_ref} - INTERVAL {{{parameter_name}}} MONTH "
                f"GROUP BY 1 "
                f"ORDER BY 1 DESC"
            )
            query_family = f"{grain_label}_{event_slug}_trend"
            name = f"Monthly {event_name} Trend from Latest {event_name} Date"

        datapoint = QueryDataPoint(
            datapoint_id=self._make_query_id(table_key, f"{grain_label}_{event_slug}_trend"),
            name=name,
            sql_template=postgres_template,
            parameters={
                parameter_name: QueryParameter(
                    type="integer",
                    required=False,
                    default=default_value,
                    description=f"Number of recent {parameter_unit} to include.",
                )
            },
            description=(
                f"Shows {time_grain}ly {event_name.lower()} totals using {amount_column.name} "
                f"and anchors the lookback on the latest {event_name.lower()} date in {table_key}."
            ),
            backend_variants={
                "mysql": mysql_variant,
                "clickhouse": clickhouse_variant,
            },
            related_tables=[table_key],
            owner=_DEFAULT_OWNER,
            tags=[
                "auto-profiled",
                "query-template",
                f"{time_grain}ly",
                "trend",
                event_slug,
                direction_value,
                "latest_date_anchor",
                self._slugify(direction_column.name),
                amount_slug,
            ],
            metadata=self._build_query_metadata(
                table,
                query_family=query_family,
                primary_dimension=None,
                primary_measure=amount_column.name,
                time_grain=time_grain,
            ),
        )
        return GeneratedDataPoint(
            datapoint=datapoint.model_dump(mode="json", by_alias=True),
            confidence=0.83,
            explanation=(
                f"Transaction-flow {time_grain} trend template generated from "
                f"{direction_column.name}={direction_value} and anchored on the latest matching date."
            ),
        )

    def _derive_table_purpose(self, table: TableProfile) -> str:
        column_names = [col.name for col in table.columns[:5]]
        hints = ""
        name_lower = table.name.lower()
        if any(token in name_lower for token in ["user", "account", "customer"]):
            hints = "Stores people or account records."
        elif any(token in name_lower for token in ["order", "invoice", "payment"]):
            hints = "Captures transactional records."
        elif any(token in name_lower for token in ["ticket", "case", "incident", "support"]):
            hints = "Tracks support cases, incidents, or customer requests."
        elif any(token in name_lower for token in ["event", "log", "activity", "audit"]):
            hints = "Captures application or user activity events."
        elif any(token in name_lower for token in ["lead", "campaign", "attribution", "funnel"]):
            hints = "Stores growth or marketing funnel signals."
        elif any(token in name_lower for token in ["inventory", "stock", "warehouse", "shipment"]):
            hints = "Tracks inventory and operational movement."
        elif any(token in name_lower for token in ["subscription", "plan", "billing"]):
            hints = "Stores subscription lifecycle and billing information."
        elif any(token in name_lower for token in ["product", "item", "catalog"]):
            hints = "Holds product or catalog details."
        elif any(token in name_lower for token in ["session", "visit"]):
            hints = "Tracks session or visit details."
        columns_hint = ""
        if column_names:
            columns_hint = f"Key columns include {', '.join(column_names)}."
        parts = [f"Auto-profiled table {table.schema_name}.{table.name}.", hints, columns_hint]
        return " ".join(part for part in parts if part)

    @staticmethod
    def _slugify(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")

    def _make_table_id(self, schema: str, table: str) -> str:
        slug = self._slugify(f"{schema}_{table}")
        return f"table_{slug}"

    def _make_metric_id(self, table: str, metric_name: str) -> str:
        slug = self._slugify(f"{table}_{metric_name}")
        return f"metric_{slug}"

    def _make_query_id(self, table: str, query_name: str) -> str:
        slug = self._slugify(f"{table}_{query_name}")
        return f"query_{slug}"

    @staticmethod
    def _title_case(value: str) -> str:
        return value.replace("_", " ").title()

    @staticmethod
    def _relationship_to_model(relationship) -> dict:
        return {
            "target_table": f"{relationship.target_table}",
            "join_column": relationship.source_column,
            "cardinality": relationship.cardinality,
            "relationship_type": relationship.relationship_type,
        }

    def _build_schema_prompt(self, table: TableProfile) -> str:
        columns = [
            {
                "name": col.name,
                "type": col.data_type,
                "nullable": col.nullable,
                "sample_values": [str(value) for value in col.sample_values[:5]],
                "distinct_count": col.distinct_count,
                "null_count": col.null_count,
                "min_value": col.min_value,
                "max_value": col.max_value,
            }
            for col in table.columns
        ]
        relationships = [
            {
                "target_table": rel.target_table,
                "source_column": rel.source_column,
                "relationship_type": rel.relationship_type,
                "cardinality": rel.cardinality,
            }
            for rel in table.relationships
        ]
        return (
            "Document this table for business users. Return JSON with keys: "
            "business_purpose (string), columns (object mapping column name to business meaning), "
            "common_queries (array), gotchas (array), freshness (string or null), "
            "confidence (0-1), explanation (string).\n\n"
            f"Table: {table.schema_name}.{table.name}\n"
            f"Row count (estimate): {self._sanitize_row_count(table.row_count)}\n"
            f"Columns: {json.dumps(columns)}\n"
            f"Relationships: {json.dumps(relationships)}"
        )

    def _dedupe_generated(self, generated: GeneratedDataPoints) -> GeneratedDataPoints:
        schema_map: dict[str, GeneratedDataPoint] = {}
        for item in generated.schema_datapoints:
            datapoint = item.datapoint or {}
            table_name = datapoint.get("table_name") or datapoint.get("schema")
            if not table_name:
                table_name = datapoint.get("datapoint_id")
            existing = schema_map.get(table_name)
            if not existing or item.confidence >= existing.confidence:
                schema_map[table_name] = item

        business_map: dict[tuple[str, str], GeneratedDataPoint] = {}
        for item in generated.business_datapoints:
            datapoint = item.datapoint or {}
            related_tables = datapoint.get("related_tables") or []
            table_key = related_tables[0] if related_tables else "unknown"
            name = datapoint.get("name") or datapoint.get("datapoint_id")
            key = (table_key, str(name))
            existing = business_map.get(key)
            if not existing or item.confidence >= existing.confidence:
                business_map[key] = item

        query_map: dict[str, GeneratedDataPoint] = {}
        for item in generated.query_datapoints:
            datapoint = item.datapoint or {}
            datapoint_id = str(datapoint.get("datapoint_id") or "")
            if not datapoint_id:
                continue
            existing = query_map.get(datapoint_id)
            if not existing or item.confidence >= existing.confidence:
                query_map[datapoint_id] = item

        return GeneratedDataPoints(
            profile_id=generated.profile_id,
            schema_datapoints=list(schema_map.values()),
            business_datapoints=list(business_map.values()),
            query_datapoints=list(query_map.values()),
        )

    def _build_metric_prompt(self, table: TableProfile, numeric_columns: list) -> str:
        numeric_cols = [
            {
                "name": col.name,
                "type": col.data_type,
                "sample_values": [str(value) for value in col.sample_values[:5]],
                "distinct_count": col.distinct_count,
                "min_value": col.min_value,
                "max_value": col.max_value,
            }
            for col in numeric_columns
        ]
        return (
            "Suggest KPIs from numeric columns. Return JSON with key 'metrics', "
            "an array of objects with fields: name, calculation, aggregation, unit, "
            "synonyms, business_rules, confidence, explanation.\n\n"
            f"Table: {table.schema_name}.{table.name}\n"
            f"Row count (estimate): {self._sanitize_row_count(table.row_count)}\n"
            f"Numeric columns: {json.dumps(numeric_cols)}"
        )
