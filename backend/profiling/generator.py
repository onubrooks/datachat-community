"""LLM-backed DataPoint generation from profiles."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable

from backend.llm.factory import LLMProviderFactory
from backend.llm.models import LLMMessage, LLMRequest
from backend.models.datapoint import BusinessDataPoint, ColumnMetadata, SchemaDataPoint
from backend.profiling.models import (
    ColumnProfile,
    DatabaseProfile,
    GeneratedDataPoint,
    GeneratedDataPoints,
    TableProfile,
)

_DEFAULT_OWNER = "auto-profiler@datachat.ai"


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

        use_llm_schema = depth == "metrics_full"
        for idx, table in enumerate(selected_tables, start=1):
            if use_llm_schema:
                schema_points.append(await self._generate_schema_datapoint(table, idx))
            else:
                schema_points.append(self._generate_schema_datapoint_deterministic(table, idx))

        business_points: list[GeneratedDataPoint] = []
        if depth == "schema_only":
            result = self._dedupe_generated(
                GeneratedDataPoints(
                    profile_id=profile.profile_id,
                    schema_datapoints=schema_points,
                    business_datapoints=business_points,
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
            )
        )
        self._attach_connection_metadata(result, str(profile.connection_id))
        return result

    @staticmethod
    def _attach_connection_metadata(
        generated: GeneratedDataPoints, connection_id: str
    ) -> None:
        for item in [*generated.schema_datapoints, *generated.business_datapoints]:
            payload = item.datapoint
            if not isinstance(payload, dict):
                continue
            metadata = payload.get("metadata")
            if not isinstance(metadata, dict):
                metadata = {}
                payload["metadata"] = metadata
            metadata["connection_id"] = connection_id

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
            "exclusions": "No explicit exclusions documented by auto-profiler.",
            "confidence_notes": (
                "Auto-generated datapoint. Human validation is recommended "
                "before production-critical use."
            ),
        }

    @staticmethod
    def _is_numeric_data_type(data_type: str | None) -> bool:
        if not data_type:
            return False
        lowered = data_type.lower()
        return any(token in lowered for token in ("int", "numeric", "decimal", "float", "double"))

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
        }

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

        row_count = table.row_count if table.row_count is not None and table.row_count >= 0 else None
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
            row_count=table.row_count if table.row_count is not None else None,
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
                table_payload = payload.get(table.name) or payload.get(
                    f"{table.schema_name}.{table.name}", {}
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
            "Return ONLY valid JSON."
        )
        batch_payload = {
            "tables": [
                {
                    "table": f"{table.schema_name}.{table.name}",
                    "numeric_columns": [
                        col.name
                        for col in table.columns
                        if self._is_numeric_type(col.data_type)
                    ],
                }
                for table in tables
            ],
            "max_metrics_per_table": max_metrics_per_table,
        }
        user_prompt = (
            "For each table, return JSON keyed by table name (without schema) with "
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
        return any(token in data_type.lower() for token in ["int", "numeric", "decimal", "float"])

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
        sample_preview = ", ".join(list(samples)[:3])
        if sample_preview:
            return f"Values such as {sample_preview}."
        return f"Auto-profiled column {name}."

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

    def _derive_table_purpose(self, table: TableProfile) -> str:
        column_names = [col.name for col in table.columns[:5]]
        hints = ""
        name_lower = table.name.lower()
        if any(token in name_lower for token in ["user", "account", "customer"]):
            hints = "Stores people or account records."
        elif any(token in name_lower for token in ["order", "invoice", "payment"]):
            hints = "Captures transactional records."
        elif any(token in name_lower for token in ["event", "log", "activity"]):
            hints = "Tracks events or activity logs."
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
                "samples": col.sample_values[:3],
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
            f"Row count (estimate): {table.row_count}\n"
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

        return GeneratedDataPoints(
            profile_id=generated.profile_id,
            schema_datapoints=list(schema_map.values()),
            business_datapoints=list(business_map.values()),
        )

    def _build_metric_prompt(self, table: TableProfile, numeric_columns: list) -> str:
        numeric_cols = [
            {
                "name": col.name,
                "type": col.data_type,
                "samples": col.sample_values[:3],
            }
            for col in numeric_columns
        ]
        return (
            "Suggest KPIs from numeric columns. Return JSON with key 'metrics', "
            "an array of objects with fields: name, calculation, aggregation, unit, "
            "synonyms, business_rules, confidence, explanation.\n\n"
            f"Table: {table.schema_name}.{table.name}\n"
            f"Row count (estimate): {table.row_count}\n"
            f"Numeric columns: {json.dumps(numeric_cols)}"
        )
