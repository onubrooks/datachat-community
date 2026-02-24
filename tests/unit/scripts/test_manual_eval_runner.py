"""Unit tests for scripts/manual_eval_runner.py."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
RUNNER_PATH = ROOT / "scripts" / "manual_eval_runner.py"
SPEC = importlib.util.spec_from_file_location("manual_eval_runner_module", RUNNER_PATH)
assert SPEC is not None and SPEC.loader is not None
RUNNER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = RUNNER
SPEC.loader.exec_module(RUNNER)


def test_load_question_bank_all_domains_has_expected_counts():
    rows = RUNNER.load_question_bank(
        ROOT / "docs" / "DOMAIN_QUESTION_BANK.md",
        domain="all",
    )
    assert len(rows) == 40
    grocery = [row for row in rows if row.domain == "grocery"]
    fintech = [row for row in rows if row.domain == "fintech"]
    assert len(grocery) == 20
    assert len(fintech) == 20
    assert grocery[0].index == 1
    assert grocery[-1].index == 20
    assert fintech[0].index == 1
    assert fintech[-1].index == 20


def test_load_question_bank_domain_filter():
    rows = RUNNER.load_question_bank(
        ROOT / "docs" / "DOMAIN_QUESTION_BANK.md",
        domain="grocery",
    )
    assert len(rows) == 20
    assert all(row.domain == "grocery" for row in rows)


def test_parse_table_row():
    line = "| 3 | Example question | Signal | Hint |"
    assert RUNNER._parse_table_row(line) == ["3", "Example question", "Signal", "Hint"]
