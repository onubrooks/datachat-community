#!/usr/bin/env python3
"""
Finance Workflow v1 quality gate runner.

Usage:
  python scripts/finance_workflow_gate.py --scorecard docs/templates/finance_workflow_scorecard.csv
  python scripts/finance_workflow_gate.py --scorecard reports/finance_scorecard.csv --report-json reports/finance_gate.json
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ScorecardRow:
    prompt_id: str
    has_source_attribution: bool
    source_count: int
    clarification_count: float
    driver_quality_pass: bool
    consistency_applicable: bool
    consistency_pass: bool
    reproducibility_pass: bool


@dataclass(frozen=True)
class GateThresholds:
    min_prompts: int
    min_source_coverage: float
    max_avg_clarifications: float
    min_driver_quality_pass_rate: float
    min_consistency_pass_rate: float
    min_reproducibility_pass_rate: float


def _parse_bool(value: str) -> bool:
    text = (value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "pass", "passed"}


def _parse_int(value: str, *, default: int = 0) -> int:
    text = (value or "").strip()
    if not text:
        return default
    return int(text)


def _parse_float(value: str, *, default: float = 0.0) -> float:
    text = (value or "").strip()
    if not text:
        return default
    return float(text)


def load_scorecard(path: Path) -> list[ScorecardRow]:
    required_columns = {
        "prompt_id",
        "has_source_attribution",
        "source_count",
        "clarification_count",
        "driver_quality_pass",
        "consistency_applicable",
        "consistency_pass",
        "reproducibility_pass",
    }
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        field_names = set(reader.fieldnames or [])
        missing_columns = sorted(required_columns - field_names)
        if missing_columns:
            raise ValueError(
                "Scorecard is missing required columns: " + ", ".join(missing_columns)
            )

        rows: list[ScorecardRow] = []
        for index, raw in enumerate(reader, start=2):
            try:
                row = ScorecardRow(
                    prompt_id=(raw.get("prompt_id") or f"row_{index}").strip() or f"row_{index}",
                    has_source_attribution=_parse_bool(raw.get("has_source_attribution", "")),
                    source_count=_parse_int(raw.get("source_count", ""), default=0),
                    clarification_count=_parse_float(raw.get("clarification_count", ""), default=0.0),
                    driver_quality_pass=_parse_bool(raw.get("driver_quality_pass", "")),
                    consistency_applicable=_parse_bool(raw.get("consistency_applicable", "")),
                    consistency_pass=_parse_bool(raw.get("consistency_pass", "")),
                    reproducibility_pass=_parse_bool(raw.get("reproducibility_pass", "")),
                )
            except ValueError as exc:
                raise ValueError(f"Invalid scorecard value on line {index}: {exc}") from exc
            rows.append(row)
    return rows


def evaluate_quality(
    rows: list[ScorecardRow], thresholds: GateThresholds
) -> tuple[dict[str, float | int | bool], list[str]]:
    total = len(rows)
    failures: list[str] = []
    if total == 0:
        return {
            "prompt_count": 0,
            "source_coverage": 0.0,
            "avg_clarifications": 0.0,
            "driver_quality_pass_rate": 0.0,
            "consistency_pass_rate": 0.0,
            "reproducibility_pass_rate": 0.0,
            "passed": False,
        }, ["Scorecard has no rows."]

    source_coverage_hits = sum(
        1 for row in rows if row.has_source_attribution and row.source_count >= 2
    )
    source_coverage = source_coverage_hits / total
    avg_clarifications = sum(row.clarification_count for row in rows) / total
    driver_quality_hits = sum(1 for row in rows if row.driver_quality_pass)
    driver_quality_pass_rate = driver_quality_hits / total
    reproducibility_hits = sum(1 for row in rows if row.reproducibility_pass)
    reproducibility_pass_rate = reproducibility_hits / total

    consistency_rows = [row for row in rows if row.consistency_applicable]
    if consistency_rows:
        consistency_hits = sum(1 for row in consistency_rows if row.consistency_pass)
        consistency_pass_rate = consistency_hits / len(consistency_rows)
    else:
        consistency_pass_rate = 0.0
        failures.append(
            "No rows marked consistency_applicable=yes; cannot verify arithmetic consistency gate."
        )

    if total < thresholds.min_prompts:
        failures.append(
            f"Prompt count {total} < minimum required {thresholds.min_prompts}."
        )
    if source_coverage < thresholds.min_source_coverage:
        failures.append(
            f"Source coverage {source_coverage:.2%} < {thresholds.min_source_coverage:.2%}."
        )
    if avg_clarifications > thresholds.max_avg_clarifications:
        failures.append(
            f"Average clarifications {avg_clarifications:.2f} > {thresholds.max_avg_clarifications:.2f}."
        )
    if driver_quality_pass_rate < thresholds.min_driver_quality_pass_rate:
        failures.append(
            "Driver quality pass rate "
            f"{driver_quality_pass_rate:.2%} < {thresholds.min_driver_quality_pass_rate:.2%}."
        )
    if consistency_pass_rate < thresholds.min_consistency_pass_rate:
        failures.append(
            f"Consistency pass rate {consistency_pass_rate:.2%} < {thresholds.min_consistency_pass_rate:.2%}."
        )
    if reproducibility_pass_rate < thresholds.min_reproducibility_pass_rate:
        failures.append(
            "Reproducibility pass rate "
            f"{reproducibility_pass_rate:.2%} < {thresholds.min_reproducibility_pass_rate:.2%}."
        )

    summary = {
        "prompt_count": total,
        "source_coverage": source_coverage,
        "avg_clarifications": avg_clarifications,
        "driver_quality_pass_rate": driver_quality_pass_rate,
        "consistency_pass_rate": consistency_pass_rate,
        "reproducibility_pass_rate": reproducibility_pass_rate,
        "passed": len(failures) == 0,
    }
    return summary, failures


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Finance Workflow v1 quality gate")
    parser.add_argument("--scorecard", required=True, type=Path, help="Path to scorecard CSV")
    parser.add_argument("--report-json", type=Path, help="Optional path to write JSON gate report")
    parser.add_argument("--min-prompts", type=int, default=10)
    parser.add_argument("--min-source-coverage", type=float, default=0.95)
    parser.add_argument("--max-avg-clarifications", type=float, default=0.5)
    parser.add_argument("--min-driver-pass-rate", type=float, default=0.8)
    parser.add_argument("--min-consistency-pass-rate", type=float, default=0.95)
    parser.add_argument("--min-repro-pass-rate", type=float, default=0.9)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    rows = load_scorecard(args.scorecard)
    thresholds = GateThresholds(
        min_prompts=max(1, int(args.min_prompts)),
        min_source_coverage=float(args.min_source_coverage),
        max_avg_clarifications=float(args.max_avg_clarifications),
        min_driver_quality_pass_rate=float(args.min_driver_pass_rate),
        min_consistency_pass_rate=float(args.min_consistency_pass_rate),
        min_reproducibility_pass_rate=float(args.min_repro_pass_rate),
    )
    summary, failures = evaluate_quality(rows, thresholds)

    print("Finance Workflow v1 Gate Summary")
    print(f"- prompt_count: {summary['prompt_count']}")
    print(f"- source_coverage: {summary['source_coverage']:.2%}")
    print(f"- avg_clarifications: {summary['avg_clarifications']:.2f}")
    print(f"- driver_quality_pass_rate: {summary['driver_quality_pass_rate']:.2%}")
    print(f"- consistency_pass_rate: {summary['consistency_pass_rate']:.2%}")
    print(f"- reproducibility_pass_rate: {summary['reproducibility_pass_rate']:.2%}")

    if failures:
        print("Gate result: FAIL")
        for failure in failures:
            print(f"  - {failure}")
    else:
        print("Gate result: PASS")

    if args.report_json:
        report = {
            "scorecard": str(args.scorecard),
            "thresholds": thresholds.__dict__,
            "summary": summary,
            "failures": failures,
        }
        args.report_json.parent.mkdir(parents=True, exist_ok=True)
        args.report_json.write_text(json.dumps(report, indent=2), encoding="utf-8")

    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
