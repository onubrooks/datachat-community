# Phase 1 KPI Gates

This page defines the operational hardening gates for Phase 1 (core runtime).

## Goals

- enforce stability before merge
- enforce credentials-only intent/catalog quality before release
- track simple SLOs (latency + model-call budget) for release sign-off

## Configuration

All thresholds and checks are defined in:

- `config/phase1_kpi.json`

## Runner

Use:

```bash
python scripts/phase1_kpi_gate.py --mode ci
```

CI mode executes deterministic local checks configured in `ci_checks`.
You can emit machine-readable evidence for release notes:

```bash
python scripts/phase1_kpi_gate.py \
  --mode ci \
  --report-json reports/phase1_ci_gate.json \
  --report-md reports/phase1_ci_gate.md
```

Release mode (requires running API and representative environment):

```bash
python scripts/phase1_kpi_gate.py \
  --mode release \
  --api-base http://localhost:8000 \
  --report-json reports/phase1_release_gate.json \
  --report-md reports/phase1_release_gate.md
```

Release mode runs configured eval suites and enforces:

- smoke checks against health/readiness/system status endpoints
- catalog thresholds (`sql/source/clarification` match rates)
- routing thresholds (`decision_trace` route/source match rates)
- query-compiler thresholds (`selected_tables` + compiler path match rates)
- intent average latency ceiling
- intent average LLM-call ceiling
- intent source-accuracy minimum
- intent clarification-match minimum

For connector-specific eval datasets, each `eval_runs` item can define:

- `required_database_type` (for example `mysql`)
- `on_missing` (`skip` or `fail`)

This prevents false negatives when a staging environment does not include every connector type.

## Recommended Release Sign-off

1. Run `python scripts/phase1_kpi_gate.py --mode ci`.
2. Run `python scripts/phase1_kpi_gate.py --mode release --api-base ...` against staging.
3. Confirm no threshold failures and attach JSON/Markdown reports to release notes.
