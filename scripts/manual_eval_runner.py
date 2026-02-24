#!/usr/bin/env python3
"""
Interactive manual evaluation runner for UI/CLI parity testing.

This script:
1. Loads question rows from docs/DOMAIN_QUESTION_BANK.md.
2. Sends each question to /api/v1/chat.
3. Prompts you to score each result with the manual rubric.
4. Writes detailed JSON and CSV artifacts for comparison.

Usage examples:
  python scripts/manual_eval_runner.py --domain grocery --mode-label without_dp
  python scripts/manual_eval_runner.py --domain fintech --mode-label with_dp --target-database <uuid>
  python scripts/manual_eval_runner.py --domain all --no-score-prompt
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_QUESTION_DOC = ROOT / "docs" / "DOMAIN_QUESTION_BANK.md"

RUBRIC_FIELDS = [
    ("sql_correctness", "SQL correctness"),
    ("answer_usefulness", "Answer usefulness"),
    ("clarification_overhead", "Clarification overhead"),
    ("source_quality", "Source quality"),
    ("latency", "Latency"),
]


class StopRun(Exception):
    """Raised when the operator ends the evaluation early."""


@dataclass
class QuestionItem:
    domain: str
    index: int
    question: str
    expected_signal: str
    possible_answer_hint: str


def _utc_now() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _parse_table_row(line: str) -> list[str]:
    parts = [part.strip() for part in line.strip().split("|")]
    # Leading/trailing pipes produce empty entries.
    return [part for part in parts if part]


def load_question_bank(doc_path: Path, domain: str) -> list[QuestionItem]:
    if not doc_path.exists():
        raise FileNotFoundError(f"Question bank file not found: {doc_path}")

    current_domain: str | None = None
    rows: list[QuestionItem] = []

    with doc_path.open(encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            low = line.lower()

            if low.startswith("## grocery"):
                current_domain = "grocery"
                continue
            if low.startswith("## fintech"):
                current_domain = "fintech"
                continue

            if not current_domain:
                continue
            if not line.strip().startswith("|"):
                continue

            cols = _parse_table_row(line)
            if len(cols) != 4:
                continue
            if cols[0] == "#" or cols[0].startswith("---"):
                continue
            try:
                idx = int(cols[0])
            except ValueError:
                continue
            rows.append(
                QuestionItem(
                    domain=current_domain,
                    index=idx,
                    question=cols[1],
                    expected_signal=cols[2],
                    possible_answer_hint=cols[3],
                )
            )

    if domain != "all":
        rows = [row for row in rows if row.domain == domain]

    rows.sort(key=lambda item: (item.domain, item.index))
    return rows


def post_chat(
    *,
    api_base: str,
    message: str,
    target_database: str | None,
    conversation_id: str,
    timeout_s: float,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "message": message,
        "conversation_id": conversation_id,
        "conversation_history": [],
    }
    if target_database:
        payload["target_database"] = target_database
    response = httpx.post(
        f"{api_base.rstrip('/')}/api/v1/chat",
        json=payload,
        timeout=timeout_s,
    )
    response.raise_for_status()
    return response.json()


def _prompt_score(label: str) -> int | None:
    while True:
        raw = input(f"Score {label} [0/1/2, skip, quit]: ").strip().lower()
        if raw in {"quit", "q", "exit"}:
            raise StopRun
        if raw in {"skip", "s", ""}:
            return None
        if raw in {"0", "1", "2"}:
            return int(raw)
        print("Invalid input. Enter 0, 1, 2, skip, or quit.")


def _score_record_interactive(record: dict[str, Any]) -> None:
    print("\nScoring rubric (0-2 each):")
    scores: dict[str, int | None] = {}
    for key, label in RUBRIC_FIELDS:
        scores[key] = _prompt_score(label)
    note = input("Notes (optional): ").strip()
    record["scores"] = scores
    record["notes"] = note

    scored = [value for value in scores.values() if isinstance(value, int)]
    record["total_score"] = int(sum(scored)) if len(scored) == len(RUBRIC_FIELDS) else None


def _print_response_summary(response: dict[str, Any]) -> None:
    answer = (response.get("answer") or "").strip()
    preview = answer if len(answer) <= 900 else f"{answer[:900]}..."
    metrics = response.get("metrics") or {}
    clarifications = response.get("clarifying_questions") or []
    has_sql = bool((response.get("sql") or "").strip())
    print("\n--- Response summary ---")
    print(f"Source: {response.get('answer_source')}")
    print(f"Confidence: {response.get('answer_confidence')}")
    print(f"Latency (ms): {metrics.get('total_latency_ms')}")
    print(f"LLM calls: {metrics.get('llm_calls')}")
    print(f"SQL present: {has_sql}")
    print(f"Clarifying questions: {len(clarifications)}")
    print(f"Answer preview:\n{preview}\n")


def _flatten_record(record: dict[str, Any]) -> dict[str, Any]:
    response = record.get("response", {})
    metrics = response.get("metrics") or {}
    scores = record.get("scores") or {}
    clarifications = response.get("clarifying_questions") or []
    return {
        "run_id": record.get("run_id"),
        "mode_label": record.get("mode_label"),
        "domain": record.get("domain"),
        "question_index": record.get("question_index"),
        "question": record.get("question"),
        "expected_signal": record.get("expected_signal"),
        "possible_answer_hint": record.get("possible_answer_hint"),
        "answer_source": response.get("answer_source"),
        "answer_confidence": response.get("answer_confidence"),
        "latency_ms": metrics.get("total_latency_ms"),
        "llm_calls": metrics.get("llm_calls"),
        "sql_present": bool((response.get("sql") or "").strip()),
        "clarification_count": len(clarifications),
        "score_sql_correctness": scores.get("sql_correctness"),
        "score_answer_usefulness": scores.get("answer_usefulness"),
        "score_clarification_overhead": scores.get("clarification_overhead"),
        "score_source_quality": scores.get("source_quality"),
        "score_latency": scores.get("latency"),
        "total_score": record.get("total_score"),
        "notes": record.get("notes") or "",
    }


def _write_outputs(output_dir: Path, run_id: str, records: list[dict[str, Any]]) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"manual_eval_{run_id}.json"
    csv_path = output_dir / f"manual_eval_{run_id}.csv"

    payload = {
        "run_id": run_id,
        "generated_at": _utc_now(),
        "records": records,
    }
    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")

    rows = [_flatten_record(record) for record in records]
    fieldnames = list(rows[0].keys()) if rows else []
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()
            writer.writerows(rows)

    return json_path, csv_path


def _print_run_summary(records: list[dict[str, Any]]) -> None:
    scored = [record for record in records if isinstance(record.get("total_score"), int)]
    total = len(records)
    print("\n=== Run Summary ===")
    print(f"Questions attempted: {total}")
    print(f"Questions scored: {len(scored)}")
    if scored:
        avg_score = sum(int(record["total_score"]) for record in scored) / len(scored)
        avg_latency = (
            sum(float((record.get("response", {}).get("metrics", {}) or {}).get("total_latency_ms") or 0.0) for record in scored)
            / len(scored)
        )
        avg_clarifications = (
            sum(len((record.get("response", {}) or {}).get("clarifying_questions") or []) for record in scored)
            / len(scored)
        )
        print(f"Average score (/10): {avg_score:.2f}")
        print(f"Average latency (ms): {avg_latency:.1f}")
        print(f"Average clarification count: {avg_clarifications:.2f}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Interactive manual evaluation runner")
    parser.add_argument("--api-base", default="http://localhost:8000")
    parser.add_argument("--domain", choices=["grocery", "fintech", "all"], default="all")
    parser.add_argument("--mode-label", default="manual")
    parser.add_argument("--target-database", default=None)
    parser.add_argument("--question-doc", default=str(DEFAULT_QUESTION_DOC))
    parser.add_argument("--output-dir", default="reports/manual_eval")
    parser.add_argument("--timeout-s", type=float, default=120.0)
    parser.add_argument(
        "--no-score-prompt",
        action="store_true",
        help="Do not prompt for manual rubric scores; collect responses only.",
    )
    args = parser.parse_args()

    question_doc = Path(args.question_doc)
    output_dir = Path(args.output_dir)
    questions = load_question_bank(question_doc, args.domain)
    if not questions:
        print(f"No questions found for domain={args.domain} in {question_doc}")
        return 1

    timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"{args.mode_label}_{args.domain}_{timestamp}"
    print(f"Loaded {len(questions)} questions. run_id={run_id}")

    records: list[dict[str, Any]] = []
    try:
        for item in questions:
            print("\n" + "=" * 110)
            print(f"[{item.domain.upper()} #{item.index}] {item.question}")
            print(f"Expected signal: {item.expected_signal}")
            print(f"Possible answer hint: {item.possible_answer_hint}")
            response = post_chat(
                api_base=args.api_base,
                message=item.question,
                target_database=args.target_database,
                conversation_id=f"manual_eval_{uuid4().hex[:12]}",
                timeout_s=args.timeout_s,
            )
            _print_response_summary(response)

            record: dict[str, Any] = {
                "run_id": run_id,
                "mode_label": args.mode_label,
                "domain": item.domain,
                "question_index": item.index,
                "question": item.question,
                "expected_signal": item.expected_signal,
                "possible_answer_hint": item.possible_answer_hint,
                "response": response,
                "scores": {},
                "notes": "",
                "total_score": None,
            }
            if not args.no_score_prompt:
                _score_record_interactive(record)
            records.append(record)
    except KeyboardInterrupt:
        print("\nInterrupted by user. Writing partial results.")
    except StopRun:
        print("\nRun stopped by user command. Writing partial results.")

    json_path, csv_path = _write_outputs(output_dir, run_id, records)
    _print_run_summary(records)
    print(f"Saved JSON report: {json_path}")
    print(f"Saved CSV report: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
