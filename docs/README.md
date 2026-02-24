# DataChat Docs

This directory contains product and engineering documentation for DataChat.

Strategy alignment:

- current wedge: decision workflow system for finance teams
- long-term direction: AI platform for business decision makers
- prioritization rule: finance workflow outcomes pull platform capability sequencing

## Implementation Snapshot (February 2026)

Implemented now:

- credentials-only querying with deterministic catalog intelligence
- onboarding wizard (`datachat onboarding wizard`) for one-command connection + deep metadata generation
- DataPoint-enhanced retrieval and answer synthesis
- multi-database registry with `target_database` routing
- tool registry/execution with policy checks and typed parameter schemas
- profiling pipeline that generates pending DataPoints for review

Planned (not yet implemented as full product features):

- workspace/folder indexing and codebase understanding workflows
- governed finance authority knowledge pack (canonical finance-global metric definitions)
- runtime connectors for BigQuery/Redshift
- Levels 3-5 automation features

## Document Map

Planning hierarchy:

- `PRD.md` = what/why
- `ARCHITECTURE.md` = how
- `LEVELS.md` = maturity definitions
- `ROADMAP.md` = sequencing/status/dependencies
- `specs/` = per-initiative implementation details (execution contract)

- `../GETTING_STARTED.md` - setup paths and first-run flow.
- `CLI_QUICKSTART.md` - terminal how-to for ask/chat/templates/schema/session commands.
- `ARCHITECTURE.md` - system architecture and engineering design guide.
- `ARCHITECTURE_DYNAMIC_DATA_AGENT.md` - accepted target architecture for dynamic data-agent harness (data + business logic + organizational knowledge).
- `ROADMAP.md` - unified initiative tracker (single source of truth for status/timing/dependencies).
- `specs/README.md` - spec authoring workflow and required implementation-spec sections.
- `API.md` - API endpoints and payloads.
- `UI_HOWTO.md` - step-by-step user guide for chat UI workflows (ask mode, SQL mode, sidebars, charts, pagination, shortcuts).
- `CREDENTIALS_ONLY_MODE.md` - capabilities/limits for credentials-only mode.
- `MULTI_DATABASE.md` - connection registry and per-request routing.
- `LEVELS.md` - maturity model with implementation status.
- `PRD.md` - delivery-tracking PRD (shipped vs planned).
- `CAPABILITY_CONTRACT.md` - shipped runtime capability matrix (supported/partial/planned).
- `METADATAOPS_FOUNDATION.md` - philosophy, priorities, and contributor alignment for metadata quality + observability.
- `SESSION_MEMORY.md` - chat history + memory strategy for follow-ups across UI/CLI/API.
- `PHASE1_KPI_GATES.md` - CI and release KPI gates for Phase 1 operational hardening.
- `PHASE14_SIMPLE_ENTRY_LAYER.md` - quickstart/train wrappers and UI onboarding flow.
- `ROUTING_POLICY.md` - routing thresholds, decision trace model, and route eval checks.
- `MANUAL_EVAL_SCORECARD.md` - manual scoring rubric + runner commands for UI/CLI comparison.
- `OPERATIONS.md` - deployment and operational guidance.
- `DEMO_PLAYBOOK.md` - demo setup and persona flows.
- `DATAPOINT_SCHEMA.md` - DataPoint model and conventions.
- `DATAPOINT_CONTRACTS.md` - lintable metadata contracts for DataPoint quality gates.
- `PLAYBOOK.md` - development workflows.
- `OSS_SPLIT_CHECKLIST.md` - step-by-step plan to keep advanced features private and publish SQL-only community edition.
- `PROMPTS.md` - prompt architecture and guardrails.
- `DATAPOINT_EXAMPLES_TESTING.md` - end-to-end DataPoint-driven manual test playbook for grocery + fintech examples.
- `finance/FINANCE_WORKFLOW_V1_MANUAL_TEST.md` - manual runbook and release quality bar for Finance Workflow v1.
- `finance/FINANCE_END_USER_QUICKSTART.md` - end-user step-by-step guide to seed fintech demo data, sync finance datapoints, and run value-driving prompts.
- `finance/FINANCE_PROMPT_PACK_V1.md` - scripted 20-prompt finance demo pack with datapoint mapping and expected signals.
- `finance/FINANCE_WORKFLOW_VALUE_PROOF.md` - one-page business value proof for finance workflow demos and pilot conversations.
- `templates/finance_workflow_scorecard.csv` - scorecard template consumed by `scripts/finance_workflow_gate.py`.
- `templates/COMMUNITY_REPO_README_TEMPLATE.md` - starter README template for `datachat-community` public repo.

## Prompt Files

Prompt sources live in `prompts/`. See `prompts/README.md` and `PROMPTS.md`.
