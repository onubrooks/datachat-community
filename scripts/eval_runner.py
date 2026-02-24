#!/usr/bin/env python3
"""
Minimal evaluation runner for DataChat RAG checks.

Usage:
  python scripts/eval_runner.py --mode retrieval --dataset eval/retrieval.json
  python scripts/eval_runner.py --mode qa --dataset eval/qa.json
  python scripts/eval_runner.py --mode intent --dataset eval/intent_credentials.json
  python scripts/eval_runner.py --mode catalog --dataset eval/catalog/mysql_credentials.json
  python scripts/eval_runner.py --mode route --dataset eval/routes_credentials.json
  python scripts/eval_runner.py --mode compiler --dataset eval/compiler/grocery_query_compiler.json
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

import httpx


def _post_chat(
    api_base: str,
    message: str,
    conversation_id: str = "eval_run",
    conversation_history: list[dict[str, str]] | None = None,
    target_database: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "message": message,
        "conversation_id": conversation_id,
        "conversation_history": conversation_history or [],
    }
    if target_database:
        payload["target_database"] = target_database

    response = httpx.post(
        f"{api_base}/api/v1/chat",
        json=payload,
        timeout=120.0,
    )
    response.raise_for_status()
    return response.json()


def _columnar_to_rows(data: Any) -> list[dict[str, Any]]:
    """
    Normalize API response data into row-oriented dictionaries.

    `/api/v1/chat` returns columnar payloads (`{column: [values...]}`), while
    legacy callers may still provide row lists (`[{...}, {...}]`).
    """
    if not data:
        return []

    if isinstance(data, list):
        if data and all(isinstance(row, dict) for row in data):
            return data
        return []

    if isinstance(data, dict):
        columns = list(data.keys())
        if not columns:
            return []
        lengths = [
            len(values)
            for values in data.values()
            if isinstance(values, list)
        ]
        if not lengths:
            return []
        row_count = max(lengths)
        rows: list[dict[str, Any]] = []
        for idx in range(row_count):
            row = {}
            for col in columns:
                values = data.get(col)
                if isinstance(values, list):
                    row[col] = values[idx] if idx < len(values) else None
                else:
                    row[col] = None
            rows.append(row)
        return rows

    return []


def _infer_answer_type(data: Any) -> str | None:
    rows = _columnar_to_rows(data)
    if not rows:
        return None
    if len(rows) == 1 and len(rows[0]) == 1:
        return "single_value"

    cols = [str(col).lower() for col in rows[0].keys()]
    if any(token in col for col in cols for token in ("date", "time", "day")):
        return "time_series"
    return "table"


def run_retrieval(
    api_base: str,
    dataset: list[dict[str, Any]],
    min_hit_rate: float | None = None,
    min_recall: float | None = None,
    min_mrr: float | None = None,
) -> int:
    total = len(dataset)
    hits = 0
    recall_sum = 0.0
    mrr_sum = 0.0
    coverage_hits = 0

    for item in dataset:
        query = item["query"]
        expected = set(item["expected_datapoint_ids"])
        response = _post_chat(api_base, query)
        sources = response.get("sources") or []
        retrieved_ranked = [
            src.get("datapoint_id")
            for src in sources
            if src.get("datapoint_id")
        ]
        retrieved = set(retrieved_ranked)

        hit = 1 if expected & retrieved else 0
        hits += hit
        coverage_hits += hit
        recall = len(expected & retrieved) / len(expected) if expected else 0.0
        recall_sum += recall
        reciprocal_rank = 0.0
        for rank, datapoint_id in enumerate(retrieved_ranked, start=1):
            if datapoint_id in expected:
                reciprocal_rank = 1.0 / rank
                break
        mrr_sum += reciprocal_rank

        print(f"- {query}")
        print(f"  expected: {sorted(expected)}")
        print(f"  retrieved: {retrieved_ranked}")
        print(f"  hit: {hit}  recall: {recall:.2f}  rr: {reciprocal_rank:.2f}")

    hit_rate = hits / total if total else 0.0
    avg_recall = recall_sum / total if total else 0.0
    avg_mrr = mrr_sum / total if total else 0.0
    coverage = coverage_hits / total if total else 0.0
    print(
        f"\nHit rate: {hit_rate:.2f}  Avg recall@K: {avg_recall:.2f}  "
        f"MRR: {avg_mrr:.2f}  Coverage: {coverage:.2f}"
    )

    failures: list[str] = []
    if min_hit_rate is not None and hit_rate < min_hit_rate:
        failures.append(f"hit_rate {hit_rate:.2f} < {min_hit_rate:.2f}")
    if min_recall is not None and avg_recall < min_recall:
        failures.append(f"recall {avg_recall:.2f} < {min_recall:.2f}")
    if min_mrr is not None and avg_mrr < min_mrr:
        failures.append(f"mrr {avg_mrr:.2f} < {min_mrr:.2f}")

    if failures:
        print("Threshold failures:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    return 0


def run_qa(
    api_base: str,
    dataset: list[dict[str, Any]],
    min_sql_match_rate: float | None = None,
    min_answer_type_rate: float | None = None,
) -> int:
    total = len(dataset)
    sql_matches = 0
    answer_matches = 0
    validation_errors = 0

    for item in dataset:
        query = item["query"]
        expected_sql_contains = [token.lower() for token in item["expected_sql_contains"]]
        expected_answer_type = item["expected_answer_type"]

        response = _post_chat(api_base, query)
        sql = (response.get("sql") or "").lower()
        data = response.get("data")
        answer_type = _infer_answer_type(data)
        errors = response.get("validation_errors") or []

        sql_ok = all(token in sql for token in expected_sql_contains)
        answer_ok = answer_type == expected_answer_type
        if sql_ok:
            sql_matches += 1
        if answer_ok:
            answer_matches += 1
        if errors:
            validation_errors += 1

        print(f"- {query}")
        print(f"  sql_ok: {sql_ok}  answer_type: {answer_type}  expected: {expected_answer_type}")
        if errors:
            print(f"  validation_errors: {len(errors)}")

    sql_match_rate = (sql_matches / total) if total else 0.0
    answer_type_rate = (answer_matches / total) if total else 0.0
    print(f"\nSQL match rate: {sql_matches}/{total} ({sql_match_rate:.2f})")
    print(f"Answer type match: {answer_matches}/{total} ({answer_type_rate:.2f})")
    print(f"Validation errors: {validation_errors}/{total}")

    failures: list[str] = []
    if min_sql_match_rate is not None and sql_match_rate < min_sql_match_rate:
        failures.append(f"sql_match_rate {sql_match_rate:.2f} < {min_sql_match_rate:.2f}")
    if min_answer_type_rate is not None and answer_type_rate < min_answer_type_rate:
        failures.append(
            f"answer_type_rate {answer_type_rate:.2f} < {min_answer_type_rate:.2f}"
        )

    if failures:
        print("Threshold failures:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    return 0


def run_intent(api_base: str, dataset: list[dict[str, Any]]) -> int:
    total = len(dataset)
    source_matches = 0
    sql_matches = 0
    clarification_matches = 0
    total_latency = 0.0
    total_llm_calls = 0

    for idx, item in enumerate(dataset):
        query = item["query"]
        expected_source = item.get("expected_answer_source")
        expected_sql_contains = [token.lower() for token in item.get("expected_sql_contains", [])]
        expect_clarification = item.get("expect_clarification")
        history = item.get("conversation_history") or []

        response = _post_chat(
            api_base=api_base,
            message=query,
            conversation_id=f"intent_eval_{idx}",
            conversation_history=history,
            target_database=item.get("target_database"),
        )

        source = response.get("answer_source")
        sql = (response.get("sql") or "").lower()
        clarifying_questions = response.get("clarifying_questions") or []
        is_clarification = source == "clarification" or bool(clarifying_questions)
        metrics = response.get("metrics") or {}
        latency = float(metrics.get("total_latency_ms") or 0.0)
        llm_calls = int(metrics.get("llm_calls") or 0)
        total_latency += latency
        total_llm_calls += llm_calls

        source_ok = expected_source is None or source == expected_source
        if source_ok:
            source_matches += 1

        sql_ok = True
        if expected_sql_contains:
            sql_ok = all(token in sql for token in expected_sql_contains)
            if sql_ok:
                sql_matches += 1

        clarification_ok = True
        if expect_clarification is not None:
            clarification_ok = is_clarification == bool(expect_clarification)
            if clarification_ok:
                clarification_matches += 1

        print(f"- {query}")
        print(f"  source={source} expected_source={expected_source} source_ok={source_ok}")
        print(
            f"  clarification={is_clarification} "
            f"expected={expect_clarification} clarification_ok={clarification_ok}"
        )
        if expected_sql_contains:
            print(f"  sql_ok={sql_ok}")
        print(f"  latency_ms={latency:.1f} llm_calls={llm_calls}")

    print("\nIntent/Credentials Summary")
    print(f"Source accuracy: {source_matches}/{total}")
    if any(item.get("expected_sql_contains") for item in dataset):
        expected_sql_cases = sum(1 for item in dataset if item.get("expected_sql_contains"))
        print(f"SQL pattern match: {sql_matches}/{expected_sql_cases}")
    if any(item.get("expect_clarification") is not None for item in dataset):
        clarification_cases = sum(
            1 for item in dataset if item.get("expect_clarification") is not None
        )
        print(f"Clarification expectation match: {clarification_matches}/{clarification_cases}")
    print(f"Avg latency: {(total_latency / total):.1f}ms")
    print(f"Avg LLM calls: {(total_llm_calls / total):.2f}")
    return 0


def run_catalog(
    api_base: str,
    dataset: list[dict[str, Any]],
    min_sql_match_rate: float | None = None,
    min_source_match_rate: float | None = None,
    min_clarification_match_rate: float | None = None,
) -> int:
    """Evaluate deterministic catalog behaviors (credentials-only flows)."""
    total = len(dataset)
    sql_matches = 0
    source_matches = 0
    clarification_matches = 0

    for idx, item in enumerate(dataset):
        query = item["query"]
        expected_sql_contains = [token.lower() for token in item.get("expected_sql_contains", [])]
        expected_source = item.get("expected_answer_source")
        expect_clarification = item.get("expect_clarification")

        response = _post_chat(
            api_base=api_base,
            message=query,
            conversation_id=f"catalog_eval_{idx}",
            target_database=item.get("target_database"),
        )
        sql = (response.get("sql") or "").lower()
        source = response.get("answer_source")
        clarifying_questions = response.get("clarifying_questions") or []
        is_clarification = source == "clarification" or bool(clarifying_questions)

        sql_ok = all(token in sql for token in expected_sql_contains)
        source_ok = expected_source is None or source == expected_source
        clarification_ok = (
            expect_clarification is None or bool(expect_clarification) == is_clarification
        )

        if sql_ok:
            sql_matches += 1
        if source_ok:
            source_matches += 1
        if clarification_ok:
            clarification_matches += 1

        print(f"- {query}")
        print(
            "  sql_ok="
            f"{sql_ok} source_ok={source_ok} clarification_ok={clarification_ok} "
            f"(source={source}, clarification={is_clarification})"
        )

    sql_match_rate = (sql_matches / total) if total else 0.0
    source_match_rate = (source_matches / total) if total else 0.0
    clarification_match_rate = (clarification_matches / total) if total else 0.0

    print(f"\nCatalog SQL match rate: {sql_matches}/{total} ({sql_match_rate:.2f})")
    print(f"Catalog source match rate: {source_matches}/{total} ({source_match_rate:.2f})")
    print(
        "Catalog clarification match rate: "
        f"{clarification_matches}/{total} ({clarification_match_rate:.2f})"
    )

    failures: list[str] = []
    if min_sql_match_rate is not None and sql_match_rate < min_sql_match_rate:
        failures.append(f"sql_match_rate {sql_match_rate:.2f} < {min_sql_match_rate:.2f}")
    if min_source_match_rate is not None and source_match_rate < min_source_match_rate:
        failures.append(f"source_match_rate {source_match_rate:.2f} < {min_source_match_rate:.2f}")
    if (
        min_clarification_match_rate is not None
        and clarification_match_rate < min_clarification_match_rate
    ):
        failures.append(
            "clarification_match_rate "
            f"{clarification_match_rate:.2f} < {min_clarification_match_rate:.2f}"
        )

    if failures:
        print("Threshold failures:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    return 0


def run_route(
    api_base: str,
    dataset: list[dict[str, Any]],
    min_route_match_rate: float | None = None,
    min_source_match_rate: float | None = None,
) -> int:
    """Evaluate deterministic routing behavior via response decision traces."""
    total = len(dataset)
    route_matches = 0
    source_matches = 0

    for idx, item in enumerate(dataset):
        query = item["query"]
        expected_source = item.get("expected_answer_source")
        expected_decisions = item.get("expected_decisions") or []

        response = _post_chat(
            api_base=api_base,
            message=query,
            conversation_id=f"route_eval_{idx}",
            conversation_history=item.get("conversation_history") or [],
            target_database=item.get("target_database"),
        )
        source = response.get("answer_source")
        decision_trace = response.get("decision_trace") or []

        source_ok = expected_source is None or source == expected_source
        if source_ok:
            source_matches += 1

        route_ok = True
        for expected in expected_decisions:
            stage = expected.get("stage")
            decision = expected.get("decision")
            matched = any(
                isinstance(entry, dict)
                and entry.get("stage") == stage
                and entry.get("decision") == decision
                for entry in decision_trace
            )
            if not matched:
                route_ok = False
                break
        if route_ok:
            route_matches += 1

        print(f"- {query}")
        print(
            f"  source={source} expected_source={expected_source} "
            f"source_ok={source_ok} route_ok={route_ok}"
        )

    route_match_rate = (route_matches / total) if total else 0.0
    source_match_rate = (source_matches / total) if total else 0.0
    print(f"\nRoute match rate: {route_matches}/{total} ({route_match_rate:.2f})")
    print(f"Route source match rate: {source_matches}/{total} ({source_match_rate:.2f})")

    failures: list[str] = []
    if min_route_match_rate is not None and route_match_rate < min_route_match_rate:
        failures.append(f"route_match_rate {route_match_rate:.2f} < {min_route_match_rate:.2f}")
    if min_source_match_rate is not None and source_match_rate < min_source_match_rate:
        failures.append(
            f"route_source_match_rate {source_match_rate:.2f} < {min_source_match_rate:.2f}"
        )

    if failures:
        print("Threshold failures:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    return 0


def _table_name_matches_expected(selected: str, expected: str) -> bool:
    selected_lower = selected.lower().strip()
    expected_lower = expected.lower().strip()
    if not selected_lower or not expected_lower:
        return False
    if selected_lower == expected_lower:
        return True
    if selected_lower.endswith(f".{expected_lower}"):
        return True
    if expected_lower.endswith(f".{selected_lower}"):
        return True
    return False


def run_compiler(
    api_base: str,
    dataset: list[dict[str, Any]],
    min_compiler_table_match_rate: float | None = None,
    min_compiler_path_match_rate: float | None = None,
    min_source_match_rate: float | None = None,
) -> int:
    """Evaluate query-compiler plan quality from decision traces."""
    total = len(dataset)
    table_matches = 0
    path_matches = 0
    source_matches = 0

    for idx, item in enumerate(dataset):
        query = item["query"]
        expected_tables = [str(v).lower() for v in item.get("expected_selected_tables", [])]
        expected_path = item.get("expected_compiler_path")
        expected_source = item.get("expected_answer_source")

        response = _post_chat(
            api_base=api_base,
            message=query,
            conversation_id=f"compiler_eval_{idx}",
            conversation_history=item.get("conversation_history") or [],
            target_database=item.get("target_database"),
        )
        source = response.get("answer_source")
        decision_trace = response.get("decision_trace") or []
        compiler_entry = next(
            (
                entry
                for entry in decision_trace
                if isinstance(entry, dict) and entry.get("stage") == "query_compiler"
            ),
            None,
        )
        details = compiler_entry.get("details", {}) if isinstance(compiler_entry, dict) else {}
        selected_tables = [
            str(v).lower()
            for v in (
                details.get("selected_tables", [])
                if isinstance(details, dict)
                else []
            )
        ]
        compiler_path = (
            str(compiler_entry.get("decision"))
            if isinstance(compiler_entry, dict) and compiler_entry.get("decision") is not None
            else None
        )

        table_ok = True
        if expected_tables:
            table_ok = all(
                any(_table_name_matches_expected(selected, expected) for selected in selected_tables)
                for expected in expected_tables
            )
            if table_ok:
                table_matches += 1
        else:
            table_matches += 1

        path_ok = expected_path is None or compiler_path == expected_path
        if path_ok:
            path_matches += 1

        source_ok = expected_source is None or source == expected_source
        if source_ok:
            source_matches += 1

        print(f"- {query}")
        print(
            f"  compiler_path={compiler_path} expected_path={expected_path} path_ok={path_ok}"
        )
        print(
            f"  selected_tables={selected_tables} expected_tables={expected_tables} table_ok={table_ok}"
        )
        print(f"  source={source} expected_source={expected_source} source_ok={source_ok}")

    table_match_rate = (table_matches / total) if total else 0.0
    path_match_rate = (path_matches / total) if total else 0.0
    source_match_rate = (source_matches / total) if total else 0.0
    print(
        f"\nCompiler table match rate: {table_matches}/{total} ({table_match_rate:.2f})"
    )
    print(
        f"Compiler path match rate: {path_matches}/{total} ({path_match_rate:.2f})"
    )
    print(
        f"Compiler source match rate: {source_matches}/{total} ({source_match_rate:.2f})"
    )

    failures: list[str] = []
    if (
        min_compiler_table_match_rate is not None
        and table_match_rate < min_compiler_table_match_rate
    ):
        failures.append(
            "compiler_table_match_rate "
            f"{table_match_rate:.2f} < {min_compiler_table_match_rate:.2f}"
        )
    if (
        min_compiler_path_match_rate is not None
        and path_match_rate < min_compiler_path_match_rate
    ):
        failures.append(
            "compiler_path_match_rate "
            f"{path_match_rate:.2f} < {min_compiler_path_match_rate:.2f}"
        )
    if min_source_match_rate is not None and source_match_rate < min_source_match_rate:
        failures.append(
            f"compiler_source_match_rate {source_match_rate:.2f} < {min_source_match_rate:.2f}"
        )

    if failures:
        print("Threshold failures:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="DataChat eval runner")
    parser.add_argument(
        "--mode",
        choices=["retrieval", "qa", "intent", "catalog", "route", "compiler"],
        required=True,
    )
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--api-base", default="http://localhost:8000")
    parser.add_argument(
        "--min-hit-rate",
        type=float,
        default=None,
        help="Optional minimum hit-rate threshold for retrieval mode.",
    )
    parser.add_argument(
        "--min-recall",
        type=float,
        default=None,
        help="Optional minimum average recall@K threshold for retrieval mode.",
    )
    parser.add_argument(
        "--min-mrr",
        type=float,
        default=None,
        help="Optional minimum MRR threshold for retrieval mode.",
    )
    parser.add_argument(
        "--min-sql-match-rate",
        type=float,
        default=None,
        help="Optional minimum SQL match rate threshold for QA mode.",
    )
    parser.add_argument(
        "--min-answer-type-rate",
        type=float,
        default=None,
        help="Optional minimum answer-type match threshold for QA mode.",
    )
    parser.add_argument(
        "--min-source-match-rate",
        type=float,
        default=None,
        help="Optional minimum source match threshold for catalog/route modes.",
    )
    parser.add_argument(
        "--min-clarification-match-rate",
        type=float,
        default=None,
        help="Optional minimum clarification match threshold for catalog mode.",
    )
    parser.add_argument(
        "--min-route-match-rate",
        type=float,
        default=None,
        help="Optional minimum route match threshold for route mode.",
    )
    parser.add_argument(
        "--min-compiler-table-match-rate",
        type=float,
        default=None,
        help="Optional minimum selected-table match threshold for compiler mode.",
    )
    parser.add_argument(
        "--min-compiler-path-match-rate",
        type=float,
        default=None,
        help="Optional minimum compiler path match threshold for compiler mode.",
    )
    args = parser.parse_args()

    try:
        with open(args.dataset, encoding="utf-8") as handle:
            dataset = json.load(handle)
    except OSError as exc:
        print(f"Failed to load dataset: {exc}")
        return 1

    if args.mode == "retrieval":
        return run_retrieval(
            args.api_base,
            dataset,
            min_hit_rate=args.min_hit_rate,
            min_recall=args.min_recall,
            min_mrr=args.min_mrr,
        )
    if args.mode == "qa":
        return run_qa(
            args.api_base,
            dataset,
            min_sql_match_rate=args.min_sql_match_rate,
            min_answer_type_rate=args.min_answer_type_rate,
        )
    if args.mode == "intent":
        return run_intent(args.api_base, dataset)
    if args.mode == "catalog":
        return run_catalog(
            args.api_base,
            dataset,
            min_sql_match_rate=args.min_sql_match_rate,
            min_source_match_rate=args.min_source_match_rate,
            min_clarification_match_rate=args.min_clarification_match_rate,
        )
    if args.mode == "route":
        return run_route(
            args.api_base,
            dataset,
            min_route_match_rate=args.min_route_match_rate,
            min_source_match_rate=args.min_source_match_rate,
        )
    if args.mode == "compiler":
        return run_compiler(
            args.api_base,
            dataset,
            min_compiler_table_match_rate=args.min_compiler_table_match_rate,
            min_compiler_path_match_rate=args.min_compiler_path_match_rate,
            min_source_match_rate=args.min_source_match_rate,
        )
    return 1


if __name__ == "__main__":
    sys.exit(main())
