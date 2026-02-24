"""Helpers for bootstrapping the knowledge graph at runtime startup."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from backend.knowledge.conflicts import ConflictMode, resolve_datapoint_conflicts
from backend.knowledge.datapoints import DataPointLoader
from backend.knowledge.graph import KnowledgeGraph

logger = logging.getLogger(__name__)


def bootstrap_knowledge_graph_from_datapoints(
    knowledge_graph: KnowledgeGraph,
    *,
    datapoints_dir: str | Path = "datapoints",
    conflict_mode: ConflictMode = "prefer_latest",
    strict_contracts: bool = False,
) -> dict[str, Any]:
    """
    Load DataPoints from disk and populate the in-memory knowledge graph.

    Startup should remain resilient even when some datapoint files are invalid,
    so load/contract errors are skipped and reported in the returned summary.
    """
    datapoints_path = Path(datapoints_dir)
    summary: dict[str, Any] = {
        "datapoints_dir": str(datapoints_path),
        "loaded_files": 0,
        "failed_files": 0,
        "graph_datapoints_added": 0,
        "graph_add_failures": 0,
        "conflicts_detected": 0,
        "conflicts_resolved": 0,
    }

    if not datapoints_path.exists():
        logger.info("Knowledge graph bootstrap skipped: datapoints directory not found")
        return summary

    loader = DataPointLoader(strict_contracts=strict_contracts)
    datapoints = loader.load_directory(
        datapoints_path,
        recursive=True,
        skip_errors=True,
    )

    load_stats = loader.get_stats()
    summary["loaded_files"] = load_stats.get("loaded_count", 0)
    summary["failed_files"] = load_stats.get("failed_count", 0)

    if not datapoints:
        logger.info("Knowledge graph bootstrap loaded no datapoints from disk")
        return summary

    resolution = resolve_datapoint_conflicts(datapoints, mode=conflict_mode)
    summary["conflicts_detected"] = len(resolution.conflicts)
    summary["conflicts_resolved"] = sum(
        1 for conflict in resolution.conflicts if conflict.resolved_datapoint_id
    )

    knowledge_graph.clear()
    for datapoint in resolution.datapoints:
        try:
            knowledge_graph.add_datapoint(datapoint)
            summary["graph_datapoints_added"] += 1
        except Exception as exc:  # noqa: BLE001
            summary["graph_add_failures"] += 1
            logger.warning(
                "Skipping datapoint during knowledge graph bootstrap: %s (%s)",
                datapoint.datapoint_id,
                exc,
            )

    logger.info(
        "Knowledge graph bootstrap complete: loaded=%s failed=%s added=%s add_failures=%s",
        summary["loaded_files"],
        summary["failed_files"],
        summary["graph_datapoints_added"],
        summary["graph_add_failures"],
    )
    return summary
