"""Tests for knowledge graph bootstrap helpers."""

from __future__ import annotations

import json

from backend.knowledge.bootstrap import bootstrap_knowledge_graph_from_datapoints
from backend.knowledge.graph import KnowledgeGraph


def test_bootstrap_populates_graph_from_datapoints(tmp_path):
    datapoints_dir = tmp_path / "datapoints"
    datapoints_dir.mkdir()

    schema_payload = {
        "datapoint_id": "table_bank_accounts_001",
        "type": "Schema",
        "name": "Bank Accounts",
        "owner": "data@bank.com",
        "table_name": "bank_accounts",
        "schema": "public",
        "business_purpose": "Stores account balances and lifecycle status per customer.",
        "key_columns": [
            {
                "name": "account_id",
                "type": "bigint",
                "business_meaning": "Unique account identifier",
                "nullable": False,
            }
        ],
        "relationships": [],
        "common_queries": [],
        "gotchas": [],
    }
    query_payload = {
        "datapoint_id": "query_top_concentration_001",
        "type": "Query",
        "name": "Top Customers by Concentration",
        "owner": "finance@bank.com",
        "sql_template": "SELECT customer_id, SUM(balance) AS total_balance FROM bank_accounts GROUP BY 1 LIMIT {limit}",
        "parameters": {
            "limit": {"type": "integer", "required": False, "default": 10},
        },
        "description": "Ranks customers by their share of total balance concentration.",
        "related_tables": ["bank_accounts"],
    }

    (datapoints_dir / "table_bank_accounts_001.json").write_text(
        json.dumps(schema_payload), encoding="utf-8"
    )
    (datapoints_dir / "query_top_concentration_001.json").write_text(
        json.dumps(query_payload), encoding="utf-8"
    )

    graph = KnowledgeGraph()
    summary = bootstrap_knowledge_graph_from_datapoints(graph, datapoints_dir=datapoints_dir)

    assert summary["loaded_files"] == 2
    assert summary["failed_files"] == 0
    assert summary["graph_datapoints_added"] == 2
    assert graph.get_node("table_bank_accounts_001") is not None
    assert graph.get_node("query_top_concentration_001") is not None


def test_bootstrap_skips_when_directory_missing(tmp_path):
    graph = KnowledgeGraph()
    summary = bootstrap_knowledge_graph_from_datapoints(
        graph,
        datapoints_dir=tmp_path / "missing",
    )

    assert summary["loaded_files"] == 0
    assert summary["graph_datapoints_added"] == 0
