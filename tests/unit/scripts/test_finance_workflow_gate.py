"""Unit tests for scripts/finance_workflow_gate.py."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
GATE_PATH = ROOT / "scripts" / "finance_workflow_gate.py"
SPEC = importlib.util.spec_from_file_location("finance_workflow_gate_module", GATE_PATH)
assert SPEC is not None and SPEC.loader is not None
GATE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = GATE
SPEC.loader.exec_module(GATE)


def _write_scorecard(path: Path, rows: list[dict[str, str]]) -> None:
    headers = [
        "prompt_id",
        "has_source_attribution",
        "source_count",
        "clarification_count",
        "driver_quality_pass",
        "consistency_applicable",
        "consistency_pass",
        "reproducibility_pass",
    ]
    lines = [",".join(headers)]
    for row in rows:
        lines.append(",".join(row.get(header, "") for header in headers))
    path.write_text("\n".join(lines), encoding="utf-8")


def test_gate_passes_when_scorecard_meets_thresholds(tmp_path: Path) -> None:
    rows = []
    for index in range(10):
        rows.append(
            {
                "prompt_id": f"p{index+1}",
                "has_source_attribution": "yes",
                "source_count": "2",
                "clarification_count": "0",
                "driver_quality_pass": "yes",
                "consistency_applicable": "yes",
                "consistency_pass": "yes",
                "reproducibility_pass": "yes",
            }
        )
    scorecard = tmp_path / "scorecard.csv"
    _write_scorecard(scorecard, rows)

    rc = GATE.main(["--scorecard", str(scorecard)])
    assert rc == 0


def test_gate_fails_when_source_coverage_below_threshold(tmp_path: Path) -> None:
    rows = []
    for index in range(10):
        rows.append(
            {
                "prompt_id": f"p{index+1}",
                "has_source_attribution": "yes" if index < 8 else "no",
                "source_count": "2" if index < 8 else "1",
                "clarification_count": "0",
                "driver_quality_pass": "yes",
                "consistency_applicable": "yes",
                "consistency_pass": "yes",
                "reproducibility_pass": "yes",
            }
        )
    scorecard = tmp_path / "scorecard.csv"
    _write_scorecard(scorecard, rows)

    rc = GATE.main(["--scorecard", str(scorecard)])
    assert rc == 1


def test_gate_fails_when_no_consistency_rows_exist(tmp_path: Path) -> None:
    rows = []
    for index in range(10):
        rows.append(
            {
                "prompt_id": f"p{index+1}",
                "has_source_attribution": "yes",
                "source_count": "2",
                "clarification_count": "0",
                "driver_quality_pass": "yes",
                "consistency_applicable": "no",
                "consistency_pass": "no",
                "reproducibility_pass": "yes",
            }
        )
    scorecard = tmp_path / "scorecard.csv"
    _write_scorecard(scorecard, rows)

    rc = GATE.main(["--scorecard", str(scorecard)])
    assert rc == 1
