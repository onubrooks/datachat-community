#!/usr/bin/env python3
"""
Phase 1 KPI gate runner.

Usage:
  python scripts/phase1_kpi_gate.py --mode ci
  python scripts/phase1_kpi_gate.py --mode release --api-base http://localhost:8000
  python scripts/phase1_kpi_gate.py --mode ci --report-json reports/phase1_ci_gate.json
"""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
EVAL_RUNNER = ROOT / "scripts" / "eval_runner.py"
DEFAULT_CONFIG = ROOT / "config" / "phase1_kpi.json"


@dataclass(frozen=True)
class CommandResult:
    name: str
    command: str
    return_code: int
    stdout: str
    stderr: str
    duration_ms: float


@dataclass(frozen=True)
class SmokeCheckResult:
    name: str
    method: str
    url: str
    expected_status: int
    actual_status: int | None
    passed: bool
    error: str | None
    missing_keys: list[str]
    duration_ms: float


@dataclass(frozen=True)
class RunPreconditionResult:
    available_database_types: set[str]
    error: str | None = None


def _load_config(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _run_command(name: str, command: str) -> CommandResult:
    started = time.perf_counter()
    completed = subprocess.run(
        shlex.split(command),
        capture_output=True,
        text=True,
        check=False,
    )
    duration_ms = (time.perf_counter() - started) * 1000.0
    return CommandResult(
        name=name,
        command=command,
        return_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        duration_ms=duration_ms,
    )


def _print_command_result(result: CommandResult) -> None:
    status = "PASS" if result.return_code == 0 else "FAIL"
    print(f"[{status}] {result.name} ({result.duration_ms:.1f}ms)")
    print(f"  $ {result.command}")
    if result.stdout.strip():
        print(result.stdout.rstrip())
    if result.stderr.strip():
        print(result.stderr.rstrip())


def _utc_now() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _append_report_check(report: dict[str, Any] | None, entry: dict[str, Any]) -> None:
    if report is None:
        return
    report.setdefault("checks", []).append(entry)


def _run_smoke_check(api_base: str, item: dict[str, Any]) -> SmokeCheckResult:
    name = item.get("name", "unnamed-smoke-check")
    method = str(item.get("method", "GET")).upper()
    path = item.get("path")
    expect_status = int(item.get("expect_status", 200))
    timeout_s = float(item.get("timeout_s", 10.0))
    require_json_keys = [str(key) for key in item.get("require_json_keys", [])]

    if not path:
        return SmokeCheckResult(
            name=name,
            method=method,
            url="",
            expected_status=expect_status,
            actual_status=None,
            passed=False,
            error="missing path in smoke check config",
            missing_keys=[],
            duration_ms=0.0,
        )

    url = path if str(path).startswith("http") else f"{api_base.rstrip('/')}{path}"
    started = time.perf_counter()
    try:
        response = httpx.request(method, url, timeout=timeout_s)
        duration_ms = (time.perf_counter() - started) * 1000.0
        missing_keys: list[str] = []
        if require_json_keys:
            try:
                payload = response.json()
            except ValueError:
                return SmokeCheckResult(
                    name=name,
                    method=method,
                    url=url,
                    expected_status=expect_status,
                    actual_status=response.status_code,
                    passed=False,
                    error="response is not valid JSON",
                    missing_keys=require_json_keys,
                    duration_ms=duration_ms,
                )
            if isinstance(payload, dict):
                missing_keys = [key for key in require_json_keys if key not in payload]
            else:
                missing_keys = list(require_json_keys)

        status_ok = response.status_code == expect_status
        passed = status_ok and not missing_keys
        return SmokeCheckResult(
            name=name,
            method=method,
            url=url,
            expected_status=expect_status,
            actual_status=response.status_code,
            passed=passed,
            error=None if passed else "status or payload checks failed",
            missing_keys=missing_keys,
            duration_ms=duration_ms,
        )
    except Exception as exc:  # pragma: no cover - network failure path
        duration_ms = (time.perf_counter() - started) * 1000.0
        return SmokeCheckResult(
            name=name,
            method=method,
            url=url,
            expected_status=expect_status,
            actual_status=None,
            passed=False,
            error=str(exc),
            missing_keys=require_json_keys,
            duration_ms=duration_ms,
        )


def _fetch_available_database_types(api_base: str) -> RunPreconditionResult:
    url = f"{api_base.rstrip('/')}/api/v1/databases"
    try:
        response = httpx.get(url, timeout=10.0)
    except Exception as exc:  # pragma: no cover - network failure path
        return RunPreconditionResult(available_database_types=set(), error=str(exc))

    if response.status_code != 200:
        return RunPreconditionResult(
            available_database_types=set(),
            error=f"GET {url} returned {response.status_code}",
        )

    try:
        payload = response.json()
    except ValueError:
        return RunPreconditionResult(
            available_database_types=set(),
            error=f"GET {url} returned invalid JSON",
        )

    if not isinstance(payload, list):
        return RunPreconditionResult(
            available_database_types=set(),
            error=f"GET {url} returned unexpected payload",
        )

    db_types: set[str] = set()
    for item in payload:
        if isinstance(item, dict):
            db_type = item.get("database_type")
            if isinstance(db_type, str) and db_type:
                db_types.add(db_type.strip().lower())

    return RunPreconditionResult(available_database_types=db_types, error=None)


def _print_smoke_result(result: SmokeCheckResult) -> None:
    status = "PASS" if result.passed else "FAIL"
    actual_status = (
        str(result.actual_status) if result.actual_status is not None else "n/a"
    )
    print(
        f"[{status}] smoke::{result.name} ({result.duration_ms:.1f}ms) "
        f"{result.method} {result.url} expected={result.expected_status} actual={actual_status}"
    )
    if result.missing_keys:
        print(f"  missing_json_keys={result.missing_keys}")
    if result.error and not result.passed:
        print(f"  error={result.error}")


def _print_skip_result(name: str, reason: str) -> None:
    print(f"[SKIP] {name}")
    print(f"  reason={reason}")


def _parse_fraction_rate(output: str, label: str) -> float | None:
    pattern = rf"{re.escape(label)}:\s*([0-9]+)\s*/\s*([0-9]+)"
    match = re.search(pattern, output)
    if not match:
        return None
    numerator = int(match.group(1))
    denominator = int(match.group(2))
    if denominator <= 0:
        return None
    return numerator / denominator


def run_ci_gate(config: dict[str, Any], report: dict[str, Any] | None = None) -> int:
    checks: list[dict[str, Any]] = config.get("ci_checks", [])
    if not checks:
        print("No ci_checks configured.")
        if report is not None:
            report["summary"] = {"total_checks": 0, "failures": 1, "passed": False}
        return 1

    failures = 0
    for item in checks:
        result = _run_command(item["name"], item["command"])
        _print_command_result(result)
        _append_report_check(
            report,
            {
                "kind": "ci_command",
                "name": result.name,
                "command": result.command,
                "return_code": result.return_code,
                "passed": result.return_code == 0,
                "duration_ms": round(result.duration_ms, 2),
                "stdout": result.stdout,
                "stderr": result.stderr,
            },
        )
        if result.return_code != 0:
            failures += 1

    if failures:
        print(f"\nCI KPI gate failed: {failures}/{len(checks)} checks failed.")
        if report is not None:
            report["summary"] = {
                "total_checks": len(checks),
                "failures": failures,
                "passed": False,
            }
        return 1

    print(f"\nCI KPI gate passed: {len(checks)} checks passed.")
    if report is not None:
        report["summary"] = {
            "total_checks": len(checks),
            "failures": 0,
            "passed": True,
        }
    return 0


def _build_eval_command(
    *,
    api_base: str,
    mode: str,
    dataset: str,
    thresholds: dict[str, Any] | None = None,
) -> str:
    command_parts = [
        sys.executable,
        str(EVAL_RUNNER),
        "--mode",
        mode,
        "--dataset",
        dataset,
        "--api-base",
        api_base,
    ]
    threshold_mapping = {
        "min_sql_match_rate": "--min-sql-match-rate",
        "min_source_match_rate": "--min-source-match-rate",
        "min_clarification_match_rate": "--min-clarification-match-rate",
        "min_route_match_rate": "--min-route-match-rate",
        "min_compiler_table_match_rate": "--min-compiler-table-match-rate",
        "min_compiler_path_match_rate": "--min-compiler-path-match-rate",
        "min_hit_rate": "--min-hit-rate",
        "min_recall": "--min-recall",
        "min_mrr": "--min-mrr",
        "min_answer_type_rate": "--min-answer-type-rate",
    }
    for key, value in (thresholds or {}).items():
        flag = threshold_mapping.get(key)
        if flag is None:
            continue
        command_parts.extend([flag, str(value)])
    return " ".join(shlex.quote(part) for part in command_parts)


def _parse_metric(output: str, pattern: str) -> float | None:
    match = re.search(pattern, output)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def run_release_gate(
    config: dict[str, Any],
    api_base: str,
    report: dict[str, Any] | None = None,
) -> int:
    release_config = config.get("release_checks", {})
    eval_runs: list[dict[str, Any]] = release_config.get("eval_runs", [])
    if not eval_runs:
        print("No release eval_runs configured.")
        if report is not None:
            report["summary"] = {"total_checks": 0, "failures": 1, "passed": False}
        return 1

    failures = 0
    total_checks = 0
    intent_latency_values: list[float] = []
    intent_llm_values: list[float] = []
    intent_source_accuracy_values: list[float] = []
    intent_clarification_accuracy_values: list[float] = []
    intent_eval_runs = 0

    precondition_state = _fetch_available_database_types(api_base=api_base)
    if precondition_state.error:
        print(
            "Warning: unable to fetch available database types for release-run preconditions: "
            f"{precondition_state.error}"
        )

    smoke_checks: list[dict[str, Any]] = release_config.get("smoke_checks", [])
    for smoke in smoke_checks:
        total_checks += 1
        smoke_result = _run_smoke_check(api_base=api_base, item=smoke)
        _print_smoke_result(smoke_result)
        _append_report_check(
            report,
            {
                "kind": "smoke",
                "name": smoke_result.name,
                "method": smoke_result.method,
                "url": smoke_result.url,
                "expect_status": smoke_result.expected_status,
                "actual_status": smoke_result.actual_status,
                "passed": smoke_result.passed,
                "error": smoke_result.error,
                "missing_keys": smoke_result.missing_keys,
                "duration_ms": round(smoke_result.duration_ms, 2),
            },
        )
        if not smoke_result.passed:
            failures += 1

    for run in eval_runs:
        total_checks += 1
        required_database_type = str(run.get("required_database_type") or "").strip().lower()
        missing_policy = str(run.get("on_missing", "fail")).strip().lower()
        if required_database_type:
            if precondition_state.error and missing_policy == "skip":
                _print_skip_result(
                    run["name"],
                    "precondition check unavailable while on_missing=skip",
                )
                _append_report_check(
                    report,
                    {
                        "kind": "eval",
                        "name": run["name"],
                        "mode": run["mode"],
                        "dataset": run["dataset"],
                        "passed": True,
                        "skipped": True,
                        "skip_reason": (
                            "could not verify required_database_type due precondition fetch error"
                        ),
                    },
                )
                continue
            if required_database_type not in precondition_state.available_database_types:
                reason = (
                    f"required database type '{required_database_type}' not available "
                    f"(available={sorted(precondition_state.available_database_types)})"
                )
                if missing_policy == "skip":
                    _print_skip_result(run["name"], reason)
                    _append_report_check(
                        report,
                        {
                            "kind": "eval",
                            "name": run["name"],
                            "mode": run["mode"],
                            "dataset": run["dataset"],
                            "passed": True,
                            "skipped": True,
                            "skip_reason": reason,
                        },
                    )
                    continue
                failures += 1
                print(f"[FAIL] {run['name']}")
                print(f"  reason={reason}")
                _append_report_check(
                    report,
                    {
                        "kind": "eval",
                        "name": run["name"],
                        "mode": run["mode"],
                        "dataset": run["dataset"],
                        "passed": False,
                        "skipped": False,
                        "error": reason,
                    },
                )
                continue

        command = _build_eval_command(
            api_base=api_base,
            mode=run["mode"],
            dataset=run["dataset"],
            thresholds=run.get("thresholds"),
        )
        result = _run_command(run["name"], command)
        _print_command_result(result)
        _append_report_check(
            report,
            {
                "kind": "eval",
                "name": result.name,
                "mode": run["mode"],
                "dataset": run["dataset"],
                "command": result.command,
                "return_code": result.return_code,
                "passed": result.return_code == 0,
                "duration_ms": round(result.duration_ms, 2),
                "stdout": result.stdout,
                "stderr": result.stderr,
            },
        )
        if result.return_code != 0:
            failures += 1
            continue

        if run["mode"] == "intent":
            intent_eval_runs += 1
            latency = _parse_metric(result.stdout, r"Avg latency:\s*([0-9.]+)ms")
            llm_calls = _parse_metric(result.stdout, r"Avg LLM calls:\s*([0-9.]+)")
            source_accuracy = _parse_fraction_rate(result.stdout, "Source accuracy")
            clarification_accuracy = _parse_fraction_rate(
                result.stdout,
                "Clarification expectation match",
            )
            if latency is not None:
                intent_latency_values.append(latency)
            elif release_config.get("intent_avg_latency_ms_max") is not None:
                failures += 1
                print(
                    "Intent latency metric missing from eval output. "
                    "Cannot verify intent_avg_latency_ms_max."
                )
            if llm_calls is not None:
                intent_llm_values.append(llm_calls)
            elif release_config.get("intent_avg_llm_calls_max") is not None:
                failures += 1
                print(
                    "Intent LLM-call metric missing from eval output. "
                    "Cannot verify intent_avg_llm_calls_max."
                )
            if source_accuracy is not None:
                intent_source_accuracy_values.append(source_accuracy)
            elif release_config.get("intent_source_accuracy_min") is not None:
                failures += 1
                print(
                    "Intent source-accuracy metric missing from eval output. "
                    "Cannot verify intent_source_accuracy_min."
                )
            if clarification_accuracy is not None:
                intent_clarification_accuracy_values.append(clarification_accuracy)
            elif release_config.get("intent_clarification_match_min") is not None:
                failures += 1
                print(
                    "Intent clarification metric missing from eval output. "
                    "Cannot verify intent_clarification_match_min."
                )

    max_latency = release_config.get("intent_avg_latency_ms_max")
    if max_latency is not None:
        if intent_eval_runs == 0:
            failures += 1
            print(
                "Intent latency threshold configured but no intent eval run was executed."
            )
        elif not intent_latency_values:
            failures += 1
            print("Intent latency threshold configured but no parseable latency metric was found.")
        else:
            measured = max(intent_latency_values)
            if measured > float(max_latency):
                failures += 1
                print(
                    "Intent latency threshold failed: "
                    f"{measured:.1f}ms > {float(max_latency):.1f}ms"
                )

    max_llm_calls = release_config.get("intent_avg_llm_calls_max")
    if max_llm_calls is not None:
        if intent_eval_runs == 0:
            failures += 1
            print(
                "Intent LLM-call threshold configured but no intent eval run was executed."
            )
        elif not intent_llm_values:
            failures += 1
            print("Intent LLM-call threshold configured but no parseable LLM metric was found.")
        else:
            measured = max(intent_llm_values)
            if measured > float(max_llm_calls):
                failures += 1
                print(
                    "Intent LLM-call threshold failed: "
                    f"{measured:.2f} > {float(max_llm_calls):.2f}"
                )

    min_source_accuracy = release_config.get("intent_source_accuracy_min")
    if min_source_accuracy is not None:
        if intent_eval_runs == 0:
            failures += 1
            print(
                "Intent source-accuracy threshold configured but no intent eval run was executed."
            )
        elif not intent_source_accuracy_values:
            failures += 1
            print(
                "Intent source-accuracy threshold configured but no parseable source metric "
                "was found."
            )
        else:
            measured = min(intent_source_accuracy_values)
            if measured < float(min_source_accuracy):
                failures += 1
                print(
                    "Intent source-accuracy threshold failed: "
                    f"{measured:.2f} < {float(min_source_accuracy):.2f}"
                )

    min_clarification_accuracy = release_config.get("intent_clarification_match_min")
    if min_clarification_accuracy is not None:
        if intent_eval_runs == 0:
            failures += 1
            print(
                "Intent clarification threshold configured but no intent eval run was executed."
            )
        elif not intent_clarification_accuracy_values:
            failures += 1
            print(
                "Intent clarification threshold configured but no parseable clarification "
                "metric was found."
            )
        else:
            measured = min(intent_clarification_accuracy_values)
            if measured < float(min_clarification_accuracy):
                failures += 1
                print(
                    "Intent clarification threshold failed: "
                    f"{measured:.2f} < {float(min_clarification_accuracy):.2f}"
                )

    if report is not None:
        checks = report.get("checks", [])
        skipped = (
            sum(1 for check in checks if isinstance(check, dict) and check.get("skipped"))
            if isinstance(checks, list)
            else 0
        )
        report["summary"] = {
            "total_checks": total_checks,
            "skipped": skipped,
            "failures": failures,
            "passed": failures == 0,
            "intent_eval_runs": intent_eval_runs,
            "metrics": {
                "intent_avg_latency_ms": intent_latency_values,
                "intent_avg_llm_calls": intent_llm_values,
                "intent_source_accuracy": intent_source_accuracy_values,
                "intent_clarification_match": intent_clarification_accuracy_values,
            },
        }

    if failures:
        print(f"\nRelease KPI gate failed: {failures} checks failed.")
        return 1

    print("\nRelease KPI gate passed.")
    return 0


def _write_json_report(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _render_markdown_report(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines: list[str] = [
        "# Phase 1 KPI Gate Report",
        "",
        f"- Generated at: `{report.get('generated_at', 'unknown')}`",
        f"- Mode: `{report.get('mode', 'unknown')}`",
        f"- Config: `{report.get('config', 'unknown')}`",
        f"- Passed: `{summary.get('passed', False)}`",
        f"- Failures: `{summary.get('failures', 0)}`",
        f"- Total checks: `{summary.get('total_checks', 0)}`",
        "",
        "## Checks",
        "",
    ]

    for check in report.get("checks", []):
        kind = check.get("kind", "check")
        name = check.get("name", "unnamed")
        passed = check.get("passed", False)
        badge = "PASS" if passed else "FAIL"
        duration = check.get("duration_ms")
        duration_text = f" ({duration:.1f}ms)" if isinstance(duration, (int, float)) else ""
        lines.append(f"- [{badge}] `{kind}` `{name}`{duration_text}")

    lines.append("")
    return "\n".join(lines)


def _write_markdown_report(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_render_markdown_report(report), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 1 KPI gate runner")
    parser.add_argument("--mode", choices=["ci", "release"], required=True)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--api-base", default="http://localhost:8000")
    parser.add_argument(
        "--report-json",
        default=None,
        help="Optional path to write JSON report output.",
    )
    parser.add_argument(
        "--report-md",
        default=None,
        help="Optional path to write Markdown report output.",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"KPI config not found: {config_path}")
        return 1

    config = _load_config(config_path)
    report: dict[str, Any] = {
        "generated_at": _utc_now(),
        "mode": args.mode,
        "config": str(config_path),
        "checks": [],
    }

    if args.mode == "ci":
        code = run_ci_gate(config, report=report)
    else:
        code = run_release_gate(config, api_base=args.api_base, report=report)

    report.setdefault("summary", {})
    report["summary"]["exit_code"] = code

    if args.report_json:
        _write_json_report(report, Path(args.report_json))
        print(f"Wrote JSON report to {args.report_json}")
    if args.report_md:
        _write_markdown_report(report, Path(args.report_md))
        print(f"Wrote Markdown report to {args.report_md}")

    return code


if __name__ == "__main__":
    sys.exit(main())
