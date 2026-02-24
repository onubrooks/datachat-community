"""Unit tests for scripts/eval_runner.py."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
EVAL_RUNNER_PATH = ROOT / "scripts" / "eval_runner.py"
SPEC = importlib.util.spec_from_file_location("eval_runner_module", EVAL_RUNNER_PATH)
assert SPEC is not None and SPEC.loader is not None
EVAL_RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EVAL_RUNNER)


def test_infer_answer_type_from_columnar_single_value():
    data = {"total_revenue": [1234.5]}
    assert EVAL_RUNNER._infer_answer_type(data) == "single_value"


def test_infer_answer_type_from_columnar_time_series():
    data = {
        "order_date": ["2026-01-01", "2026-01-02"],
        "revenue": [120.0, 80.0],
    }
    assert EVAL_RUNNER._infer_answer_type(data) == "time_series"


def test_infer_answer_type_from_columnar_table():
    data = {
        "store_name": ["Downtown", "Midtown"],
        "total_sales": [4200.0, 3900.0],
    }
    assert EVAL_RUNNER._infer_answer_type(data) == "table"


def test_run_qa_handles_columnar_payload(monkeypatch, capsys):
    dataset = [
        {
            "query": "How many active users are there?",
            "expected_sql_contains": ["count", "users"],
            "expected_answer_type": "single_value",
        }
    ]

    def _mock_post_chat(api_base: str, message: str, **kwargs):
        return {
            "sql": "SELECT COUNT(*) AS users FROM users",
            "data": {"users": [3]},
            "validation_errors": [],
        }

    monkeypatch.setattr(EVAL_RUNNER, "_post_chat", _mock_post_chat)

    rc = EVAL_RUNNER.run_qa("http://localhost:8000", dataset)
    output = capsys.readouterr().out

    assert rc == 0
    assert "answer_type: single_value" in output
    assert "SQL match rate: 1/1" in output


def test_run_retrieval_threshold_failure(monkeypatch):
    dataset = [
        {
            "query": "What is revenue?",
            "expected_datapoint_ids": ["metric_revenue_001"],
        }
    ]

    def _mock_post_chat(api_base: str, message: str, **kwargs):
        return {
            "sources": [{"datapoint_id": "table_orders_001"}],
        }

    monkeypatch.setattr(EVAL_RUNNER, "_post_chat", _mock_post_chat)

    rc = EVAL_RUNNER.run_retrieval(
        "http://localhost:8000",
        dataset,
        min_hit_rate=1.0,
        min_recall=1.0,
        min_mrr=1.0,
    )

    assert rc == 1


def test_run_catalog_thresholds(monkeypatch):
    dataset = [
        {
            "query": "list tables",
            "expected_sql_contains": ["information_schema.tables"],
            "expected_answer_source": "sql",
            "expect_clarification": False,
        }
    ]

    def _mock_post_chat(api_base: str, message: str, **kwargs):
        return {
            "sql": "SELECT table_schema, table_name FROM information_schema.tables",
            "answer_source": "sql",
            "clarifying_questions": [],
        }

    monkeypatch.setattr(EVAL_RUNNER, "_post_chat", _mock_post_chat)

    rc = EVAL_RUNNER.run_catalog(
        "http://localhost:8000",
        dataset,
        min_sql_match_rate=1.0,
        min_source_match_rate=1.0,
        min_clarification_match_rate=1.0,
    )

    assert rc == 0


def test_run_route_thresholds(monkeypatch):
    dataset = [
        {
            "query": "list tables",
            "expected_answer_source": "sql",
            "expected_decisions": [
                {"stage": "intent_gate", "decision": "data_query_fast_path"},
                {"stage": "continue_after_intent_gate", "decision": "sql"},
            ],
        }
    ]

    def _mock_post_chat(api_base: str, message: str, **kwargs):
        return {
            "answer_source": "sql",
            "decision_trace": [
                {"stage": "intent_gate", "decision": "data_query_fast_path"},
                {"stage": "continue_after_intent_gate", "decision": "sql"},
            ],
        }

    monkeypatch.setattr(EVAL_RUNNER, "_post_chat", _mock_post_chat)

    rc = EVAL_RUNNER.run_route(
        "http://localhost:8000",
        dataset,
        min_route_match_rate=1.0,
        min_source_match_rate=1.0,
    )

    assert rc == 0


def test_run_compiler_thresholds(monkeypatch):
    dataset = [
        {
            "query": "list stores",
            "expected_selected_tables": ["grocery_stores"],
            "expected_compiler_path": "deterministic",
            "expected_answer_source": "sql",
        }
    ]

    def _mock_post_chat(api_base: str, message: str, **kwargs):
        return {
            "answer_source": "sql",
            "decision_trace": [
                {
                    "stage": "query_compiler",
                    "decision": "deterministic",
                    "reason": "deterministic",
                    "details": {
                        "selected_tables": ["public.grocery_stores"],
                    },
                }
            ],
        }

    monkeypatch.setattr(EVAL_RUNNER, "_post_chat", _mock_post_chat)

    rc = EVAL_RUNNER.run_compiler(
        "http://localhost:8000",
        dataset,
        min_compiler_table_match_rate=1.0,
        min_compiler_path_match_rate=1.0,
        min_source_match_rate=1.0,
    )

    assert rc == 0
