"""
ContextAgent

Pure retrieval agent (NO LLM calls) that gathers relevant DataPoints for a query.
Uses the Retriever to perform semantic + structural search across the knowledge base.

The ContextAgent:
1. Takes user query (and optional extracted entities)
2. Retrieves relevant DataPoints using vector search + knowledge graph
3. Formats results into InvestigationMemory for downstream agents
4. Tracks sources for citation purposes
5. NO LLM calls - pure retrieval only
"""

import logging
import re
from typing import Any

from backend.agents.base import BaseAgent
from backend.knowledge.retriever import RetrievalMode, RetrievalResult, RetrievedItem, Retriever
from backend.models.agent import (
    AgentMetadata,
    ContextAgentInput,
    ContextAgentOutput,
    InvestigationMemory,
    RetrievalError,
    RetrievedDataPoint,
)

logger = logging.getLogger(__name__)


class ContextAgent(BaseAgent):
    """
    Context retrieval agent.

    Retrieves relevant DataPoints from the knowledge base using hybrid
    semantic + structural search. Does NOT use LLMs - pure retrieval only.

    Usage:
        retriever = Retriever(vector_store, knowledge_graph)
        agent = ContextAgent(retriever)

        input = ContextAgentInput(
            query="What were total sales last quarter?",
            entities=[...],  # optional
            retrieval_mode="hybrid",
            max_datapoints=10
        )

        output = await agent(input)
        memory = output.investigation_memory
    """

    def __init__(self, retriever: Retriever):
        """
        Initialize ContextAgent.

        Args:
            retriever: Retriever instance for knowledge base access
        """
        super().__init__(name="ContextAgent")
        self.retriever = retriever

        logger.info("ContextAgent initialized")

    async def execute(self, input: ContextAgentInput) -> ContextAgentOutput:
        """
        Execute context retrieval.

        Args:
            input: ContextAgentInput with query and optional entities

        Returns:
            ContextAgentOutput with InvestigationMemory

        Raises:
            RetrievalError: If retrieval fails
        """
        # Validate input type first
        self._validate_input(input)

        logger.info(f"Retrieving context for query: '{input.query[:100]}...'")

        metadata = AgentMetadata(agent_name=self.name)

        try:
            # Convert retrieval mode string to enum
            mode = RetrievalMode(input.retrieval_mode)
            metadata_filter = None
            if isinstance(input.context, dict):
                candidate_filter = input.context.get("retrieval_metadata_filter")
                if isinstance(candidate_filter, dict) and candidate_filter:
                    metadata_filter = candidate_filter
            entity_hints = self._extract_entity_hints(input.entities)
            retrieval_query = self._build_entity_aware_query(input.query, entity_hints)

            # Perform retrieval
            result = await self.retriever.retrieve(
                query=retrieval_query,
                mode=mode,
                top_k=input.max_datapoints,
                metadata_filter=metadata_filter,
            )
            if metadata_filter and len(result.items) < input.max_datapoints:
                unfiltered_result = await self.retriever.retrieve(
                    query=retrieval_query,
                    mode=mode,
                    top_k=input.max_datapoints,
                    metadata_filter=None,
                )
                result = self._merge_retrieval_results(
                    primary=result,
                    fallback=unfiltered_result,
                    top_k=input.max_datapoints,
                    metadata_filter=metadata_filter,
                )
            if entity_hints:
                result = self._apply_entity_boosting(result, entity_hints)

            # Convert retrieval results to RetrievedDataPoints
            datapoints = []
            sources_used = []

            for item in result.items:
                # Extract type from metadata
                # Vector store uses "type" field directly (Schema/Business/Process)
                # Knowledge graph uses "node_type" field (table/column/metric/process/glossary)
                datapoint_type = self._extract_datapoint_type(item.metadata)

                # Extract name from metadata (default to datapoint_id if not found)
                name = item.metadata.get("name", item.datapoint_id)

                retrieved_dp = RetrievedDataPoint(
                    datapoint_id=item.datapoint_id,
                    datapoint_type=datapoint_type,
                    name=name,
                    score=item.score,
                    source=item.source,
                    metadata=item.metadata,
                    content=item.content,
                )

                datapoints.append(retrieved_dp)
                sources_used.append(item.datapoint_id)

            # Create investigation memory
            investigation_memory = InvestigationMemory(
                query=input.query,
                datapoints=datapoints,
                total_retrieved=len(datapoints),
                retrieval_mode=input.retrieval_mode,
                sources_used=list(set(sources_used)),  # Deduplicate
            )
            context_confidence = self._estimate_context_confidence(
                input.query, investigation_memory
            )

            metadata.mark_complete()

            logger.info(
                f"Successfully retrieved {len(datapoints)} DataPoints "
                f"using {input.retrieval_mode} mode"
            )

            return ContextAgentOutput(
                success=True,
                data={"retrieval_trace": result.trace},
                metadata=metadata,
                next_agent="SQLAgent",
                investigation_memory=investigation_memory,
                context_confidence=context_confidence,
            )

        except Exception as e:
            metadata.error = str(e)
            metadata.mark_complete()

            logger.error(f"Context retrieval failed: {e}")

            raise RetrievalError(
                agent=self.name,
                message=f"Failed to retrieve context: {e}",
                context={"query": input.query, "mode": input.retrieval_mode},
            ) from e

    def _validate_input(self, input: Any) -> None:
        """
        Validate input is ContextAgentInput.

        Args:
            input: Input to validate

        Raises:
            ValueError: If input is not ContextAgentInput
        """
        if not isinstance(input, ContextAgentInput):
            raise ValueError(f"ContextAgent requires ContextAgentInput, got {type(input)}")

    def _extract_datapoint_type(self, metadata: dict[str, Any]) -> str:
        """
        Extract DataPoint type from metadata.

        Vector store uses "type" field directly (Schema/Business/Process/Query).
        Knowledge graph uses "node_type" field (table/column/metric/process/glossary).

        Maps node_type to DataPoint type:
        - table, column → Schema
        - metric, glossary → Business
        - process → Process
        - query → Query

        Args:
            metadata: Item metadata from retrieval

        Returns:
            DataPoint type: "Schema", "Business", "Process", or "Query"
        """
        if "type" in metadata:
            return metadata["type"]

        node_type = metadata.get("node_type", "")

        if node_type in ("table", "column"):
            return "Schema"
        elif node_type in ("metric", "glossary"):
            return "Business"
        elif node_type == "process":
            return "Process"
        elif node_type == "query":
            return "Query"
        else:
            logger.warning(f"Unknown node_type '{node_type}', defaulting to Schema")
            return "Schema"

    def _build_context_for_next(
        self, input: ContextAgentInput, output: ContextAgentOutput
    ) -> dict[str, Any]:
        """
        Build context dictionary for next agent.

        Passes InvestigationMemory in context for downstream agents to use.

        Args:
            input: Original input
            output: Agent output

        Returns:
            Context dict with investigation_memory
        """
        return {
            **input.context,
            "investigation_memory": output.investigation_memory.model_dump(),
        }

    def _estimate_context_confidence(self, query: str, memory: InvestigationMemory) -> float:
        query_lower = query.lower()
        datapoints = memory.datapoints
        if not datapoints:
            return 0.0

        has_schema = any(dp.datapoint_type == "Schema" for dp in datapoints)
        has_business = any(dp.datapoint_type == "Business" for dp in datapoints)

        if any(
            keyword in query_lower
            for keyword in (
                "how",
                "what is",
                "explain",
                "describe",
                "tell me about",
                "definition",
                "meaning",
                "schema",
                "tables",
                "columns",
                "available",
            )
        ):
            return 0.75 if (has_schema or has_business) else 0.3

        if any(
            keyword in query_lower
            for keyword in (
                "total",
                "sum",
                "count",
                "average",
                "avg",
                "min",
                "max",
                "trend",
                "by",
                "per",
                "over time",
            )
        ):
            return 0.35 if has_business else 0.2

        return 0.5 if (has_schema or has_business) else 0.2

    @staticmethod
    def _merge_retrieval_results(
        *,
        primary: RetrievalResult,
        fallback: RetrievalResult,
        top_k: int,
        metadata_filter: dict[str, Any],
    ) -> RetrievalResult:
        seen_ids: set[str] = set()
        merged_items: list[RetrievedItem] = []

        for item in [*primary.items, *fallback.items]:
            if item.datapoint_id in seen_ids:
                continue
            seen_ids.add(item.datapoint_id)
            merged_items.append(item)
            if len(merged_items) >= top_k:
                break

        merged_trace = {
            **(primary.trace or {}),
            "metadata_filter_applied": metadata_filter,
            "filtered_candidate_count": len(primary.items),
            "unfiltered_fallback_candidate_count": len(fallback.items),
            "fallback_used": True,
        }

        return RetrievalResult(
            items=merged_items,
            total_count=len(merged_items),
            mode=primary.mode,
            query=primary.query,
            trace=merged_trace,
        )

    @staticmethod
    def _extract_entity_hints(entities: list[Any]) -> list[dict[str, Any]]:
        hints: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for entity in entities or []:
            entity_type = getattr(entity, "entity_type", None)
            confidence = float(getattr(entity, "confidence", 0.0) or 0.0)
            if not entity_type or confidence < 0.55:
                continue
            candidates = [
                getattr(entity, "normalized_value", None),
                getattr(entity, "value", None),
            ]
            for candidate in candidates:
                value = str(candidate or "").strip().lower()
                if not value:
                    continue
                key = (str(entity_type), value)
                if key in seen:
                    continue
                seen.add(key)
                hints.append(
                    {
                        "entity_type": str(entity_type),
                        "value": value,
                        "confidence": confidence,
                    }
                )
                for alias in ContextAgent._finance_entity_aliases(value):
                    alias_key = (str(entity_type), alias)
                    if alias_key in seen:
                        continue
                    seen.add(alias_key)
                    hints.append(
                        {
                            "entity_type": str(entity_type),
                            "value": alias,
                            "confidence": max(0.55, confidence - 0.05),
                        }
                    )
        return hints

    @staticmethod
    def _finance_entity_aliases(value: str) -> list[str]:
        aliases: set[str] = set()
        normalized = value.strip().lower()
        if not normalized:
            return []

        if "deposit" in normalized or normalized == "credit":
            aliases.update({"deposits", "deposit", "credit", "credits", "inflow", "inflows"})
        if "withdraw" in normalized or normalized == "debit":
            aliases.update({"withdrawal", "withdrawals", "debit", "debits", "outflow", "outflows"})
        if "customer segment" in normalized or normalized == "segment":
            aliases.update({"segment", "segments", "customer segment", "customer segments"})
        if "net flow" in normalized:
            aliases.update({"net flow", "cash flow", "inflow minus outflow"})
        if "stockout" in normalized or "out of stock" in normalized:
            aliases.update(
                {
                    "stockout",
                    "out of stock",
                    "inventory risk",
                    "reorder",
                    "on hand",
                    "reserved",
                }
            )
        if "inventory" in normalized:
            aliases.update({"stock", "on hand", "reserved", "reorder", "snapshot"})
        if normalized in {"sku", "skus"}:
            aliases.update({"sku", "skus", "product", "products", "item", "items"})

        aliases.discard(normalized)
        return sorted(aliases)

    @staticmethod
    def _build_entity_aware_query(query: str, entity_hints: list[dict[str, Any]]) -> str:
        if not entity_hints:
            return query
        ranked_hints = sorted(
            entity_hints,
            key=lambda item: (item.get("confidence", 0.0), len(str(item.get("value", "")))),
            reverse=True,
        )
        hint_terms: list[str] = []
        for hint in ranked_hints:
            value = str(hint.get("value", "")).strip()
            if not value or value in query.lower():
                continue
            hint_terms.append(value)
            if len(hint_terms) >= 6:
                break
        if not hint_terms:
            return query
        return f"{query}\nEntity hints: {'; '.join(hint_terms)}"

    def _apply_entity_boosting(
        self,
        result: RetrievalResult,
        entity_hints: list[dict[str, Any]],
    ) -> RetrievalResult:
        if not result.items or not entity_hints:
            return result

        boosted_payloads: list[tuple[float, int, RetrievedItem, list[dict[str, Any]]]] = []
        for original_rank, item in enumerate(result.items):
            matched_hints = self._match_item_to_entities(item, entity_hints)
            boost = sum(match["boost"] for match in matched_hints)
            boosted_score = min(1.0, item.score + boost)
            boosted_item = item.model_copy(update={"score": boosted_score})
            boosted_payloads.append((boosted_score, -original_rank, boosted_item, matched_hints))

        boosted_payloads.sort(key=lambda row: (row[0], row[1]), reverse=True)
        boosted_items = [row[2] for row in boosted_payloads]
        trace_matches = [
            {
                "datapoint_id": row[2].datapoint_id,
                "boosted_score": row[0],
                "matched_entities": row[3],
            }
            for row in boosted_payloads
            if row[3]
        ]

        trace = dict(result.trace or {})
        trace["entity_boosting"] = {
            "entity_hints": entity_hints,
            "matched_items": trace_matches,
        }
        return RetrievalResult(
            items=boosted_items,
            total_count=len(boosted_items),
            mode=result.mode,
            query=result.query,
            trace=trace,
        )

    def _match_item_to_entities(
        self,
        item: RetrievedItem,
        entity_hints: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        metadata = item.metadata if isinstance(item.metadata, dict) else {}
        searchable_segments: list[str] = [
            item.datapoint_id,
            str(metadata.get("name", "")),
            str(metadata.get("table_name", "")),
            str(metadata.get("schema", "")),
            str(metadata.get("query_description", "")),
            str(metadata.get("business_purpose", "")),
            str(metadata.get("calculation", "")),
            item.content or "",
        ]
        for list_key in ("related_tables", "tags", "synonyms", "common_queries"):
            searchable_segments.extend(self._coerce_metadata_strings(metadata.get(list_key)))
        searchable_text = " ".join(part.lower() for part in searchable_segments if part)

        matches: list[dict[str, Any]] = []
        for hint in entity_hints:
            value = str(hint.get("value", "")).strip().lower()
            if not value:
                continue
            if not self._contains_term(searchable_text, value):
                continue
            entity_type = str(hint.get("entity_type", "other"))
            base_boost = {
                "table": 0.09,
                "metric": 0.08,
                "column": 0.06,
                "filter": 0.05,
                "time_reference": 0.04,
                "other": 0.03,
            }.get(entity_type, 0.03)
            confidence = float(hint.get("confidence", 0.0) or 0.0)
            boost = base_boost * max(0.5, min(confidence, 1.0))
            matches.append(
                {
                    "entity_type": entity_type,
                    "value": value,
                    "confidence": confidence,
                    "boost": round(boost, 4),
                }
            )
        return matches

    @staticmethod
    def _coerce_metadata_strings(value: Any) -> list[str]:
        if isinstance(value, list):
            return [str(item) for item in value if str(item).strip()]
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return []
            if stripped.startswith("["):
                try:
                    import json

                    decoded = json.loads(stripped)
                except Exception:  # noqa: BLE001
                    decoded = None
                if isinstance(decoded, list):
                    return [str(item) for item in decoded if str(item).strip()]
            if "," in stripped:
                return [part.strip() for part in stripped.split(",") if part.strip()]
            return [stripped]
        return []

    @staticmethod
    def _contains_term(searchable_text: str, value: str) -> bool:
        if value in searchable_text:
            return True
        normalized = re.escape(value)
        return bool(re.search(rf"\b{normalized}\b", searchable_text))
