"""Unit tests for scripts/phase1_kpi_gate.py."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
GATE_PATH = ROOT / "scripts" / "phase1_kpi_gate.py"
SPEC = importlib.util.spec_from_file_location("phase1_kpi_gate_module", GATE_PATH)
assert SPEC is not None and SPEC.loader is not None
GATE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = GATE
SPEC.loader.exec_module(GATE)


def _command_result(name: str, stdout: str, return_code: int = 0):
    return GATE.CommandResult(
        name=name,
        command="python scripts/eval_runner.py",
        return_code=return_code,
        stdout=stdout,
        stderr="",
        duration_ms=5.0,
    )


def test_release_gate_fails_when_intent_metrics_missing(monkeypatch):
    config = {
        "release_checks": {
            "eval_runs": [
                {"name": "intent-check", "mode": "intent", "dataset": "eval/intent.json"}
            ],
            "intent_avg_latency_ms_max": 5000,
            "intent_avg_llm_calls_max": 3.0,
        }
    }

    monkeypatch.setattr(
        GATE,
        "_run_command",
        lambda name, command: _command_result(name, "Intent accuracy: 1.00\n"),
    )

    assert GATE.run_release_gate(config, api_base="http://localhost:8000") == 1


def test_release_gate_fails_when_intent_quality_metrics_missing(monkeypatch):
    config = {
        "release_checks": {
            "eval_runs": [
                {"name": "intent-check", "mode": "intent", "dataset": "eval/intent.json"}
            ],
            "intent_source_accuracy_min": 0.8,
            "intent_clarification_match_min": 0.8,
        }
    }

    monkeypatch.setattr(
        GATE,
        "_run_command",
        lambda name, command: _command_result(
            name,
            "Avg latency: 1000.0ms\nAvg LLM calls: 1.0\n",
        ),
    )

    assert GATE.run_release_gate(config, api_base="http://localhost:8000") == 1


def test_release_gate_fails_when_threshold_set_but_no_intent_runs(monkeypatch):
    config = {
        "release_checks": {
            "eval_runs": [
                {"name": "catalog-check", "mode": "catalog", "dataset": "eval/catalog.json"}
            ],
            "intent_avg_latency_ms_max": 5000,
            "intent_avg_llm_calls_max": 3.0,
        }
    }

    monkeypatch.setattr(
        GATE,
        "_run_command",
        lambda name, command: _command_result(name, "Catalog match: 1.00\n"),
    )

    assert GATE.run_release_gate(config, api_base="http://localhost:8000") == 1


def test_release_gate_passes_when_intent_metrics_present_and_within_threshold(monkeypatch):
    config = {
        "release_checks": {
            "eval_runs": [
                {"name": "intent-check", "mode": "intent", "dataset": "eval/intent.json"}
            ],
            "intent_avg_latency_ms_max": 5000,
            "intent_avg_llm_calls_max": 3.0,
            "intent_source_accuracy_min": 0.7,
            "intent_clarification_match_min": 0.7,
        }
    }

    stdout = (
        "Source accuracy: 8/10\n"
        "Clarification expectation match: 9/10\n"
        "Avg latency: 1200.5ms\n"
        "Avg LLM calls: 1.8\n"
    )
    monkeypatch.setattr(
        GATE,
        "_run_command",
        lambda name, command: _command_result(name, stdout),
    )

    assert GATE.run_release_gate(config, api_base="http://localhost:8000") == 0


def test_release_gate_fails_when_smoke_check_fails(monkeypatch):
    config = {
        "release_checks": {
            "smoke_checks": [{"name": "health", "path": "/api/v1/health", "expect_status": 200}],
            "eval_runs": [
                {"name": "intent-check", "mode": "intent", "dataset": "eval/intent.json"}
            ],
        }
    }

    monkeypatch.setattr(
        GATE,
        "_run_smoke_check",
        lambda api_base, item: GATE.SmokeCheckResult(
            name=item["name"],
            method="GET",
            url=f"{api_base}{item['path']}",
            expected_status=item["expect_status"],
            actual_status=503,
            passed=False,
            error="service unavailable",
            missing_keys=[],
            duration_ms=5.0,
        ),
    )
    monkeypatch.setattr(
        GATE,
        "_run_command",
        lambda name, command: _command_result(
            name,
            "Source accuracy: 10/10\nClarification expectation match: 10/10\n"
            "Avg latency: 500.0ms\nAvg LLM calls: 1.0\n",
        ),
    )

    assert GATE.run_release_gate(config, api_base="http://localhost:8000") == 1


def test_ci_gate_collects_report(tmp_path):
    report: dict[str, object] = {"checks": []}
    config = {
        "ci_checks": [
            {"name": "one", "command": "python -c 'print(1)'"},
            {"name": "two", "command": "python -c 'print(2)'"},
        ]
    }

    rc = GATE.run_ci_gate(config, report=report)
    assert rc == 0
    summary = report.get("summary")
    assert isinstance(summary, dict)
    assert summary["passed"] is True
    checks = report.get("checks")
    assert isinstance(checks, list)
    assert len(checks) == 2

    json_path = tmp_path / "report.json"
    md_path = tmp_path / "report.md"
    GATE._write_json_report(report, json_path)
    GATE._write_markdown_report(report, md_path)
    assert json_path.exists()
    assert md_path.exists()


def test_parse_fraction_rate():
    assert GATE._parse_fraction_rate("Source accuracy: 7/10", "Source accuracy") == pytest.approx(
        0.7
    )
    assert GATE._parse_fraction_rate("Source accuracy: n/a", "Source accuracy") is None


def test_run_smoke_check_passes_with_required_keys(monkeypatch):
    class _Response:
        status_code = 200

        @staticmethod
        def json():
            return {"status": "healthy", "version": "1.0.0"}

    monkeypatch.setattr(GATE.httpx, "request", lambda method, url, timeout: _Response())

    result = GATE._run_smoke_check(
        "http://localhost:8000",
        {
            "name": "health",
            "path": "/api/v1/health",
            "expect_status": 200,
            "require_json_keys": ["status", "version"],
        },
    )
    assert result.passed is True
    assert result.missing_keys == []


def test_run_smoke_check_fails_when_required_key_missing(monkeypatch):
    class _Response:
        status_code = 200

        @staticmethod
        def json():
            return {"status": "healthy"}

    monkeypatch.setattr(GATE.httpx, "request", lambda method, url, timeout: _Response())

    result = GATE._run_smoke_check(
        "http://localhost:8000",
        {
            "name": "health",
            "path": "/api/v1/health",
            "expect_status": 200,
            "require_json_keys": ["status", "version"],
        },
    )
    assert result.passed is False
    assert result.missing_keys == ["version"]


def test_release_gate_skips_eval_when_required_database_type_missing(monkeypatch):
    config = {
        "release_checks": {
            "eval_runs": [
                {
                    "name": "catalog-mysql",
                    "mode": "catalog",
                    "dataset": "eval/catalog/mysql.json",
                    "required_database_type": "mysql",
                    "on_missing": "skip",
                }
            ]
        }
    }

    monkeypatch.setattr(
        GATE,
        "_fetch_available_database_types",
        lambda api_base: GATE.RunPreconditionResult(available_database_types={"postgresql"}),
    )
    monkeypatch.setattr(
        GATE,
        "_run_command",
        lambda name, command: pytest.fail("run should be skipped before executing command"),
    )

    assert GATE.run_release_gate(config, api_base="http://localhost:8000") == 0


def test_release_gate_fails_when_required_database_type_missing_without_skip(monkeypatch):
    config = {
        "release_checks": {
            "eval_runs": [
                {
                    "name": "catalog-mysql",
                    "mode": "catalog",
                    "dataset": "eval/catalog/mysql.json",
                    "required_database_type": "mysql",
                    "on_missing": "fail",
                }
            ]
        }
    }

    monkeypatch.setattr(
        GATE,
        "_fetch_available_database_types",
        lambda api_base: GATE.RunPreconditionResult(available_database_types={"postgresql"}),
    )

    assert GATE.run_release_gate(config, api_base="http://localhost:8000") == 1


def test_build_eval_command_maps_compiler_thresholds():
    command = GATE._build_eval_command(
        api_base="http://localhost:8000",
        mode="compiler",
        dataset="eval/compiler/grocery_query_compiler.json",
        thresholds={
            "min_compiler_table_match_rate": 0.8,
            "min_compiler_path_match_rate": 0.75,
            "min_source_match_rate": 0.6,
        },
    )

    assert "--mode compiler" in command
    assert "--min-compiler-table-match-rate 0.8" in command
    assert "--min-compiler-path-match-rate 0.75" in command
    assert "--min-source-match-rate 0.6" in command
