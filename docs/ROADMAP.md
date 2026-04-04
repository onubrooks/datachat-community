# DataChat Unified Roadmap

**Version:** 1.1  
**Last Updated:** February 23, 2026

This document is the single source of truth for **delivery status, sequencing, and initiative tracking**.

---

## Must-Win Workflow Charter (Finance)

Current product wedge:

- **Workflow:** Revenue variance and liquidity risk investigation.
- **Buyer persona:** Head of Finance / CFO delegate.
- **Operator persona:** FP&A lead, finance manager, treasury/risk analyst.
- **Business problem:** slow, low-trust cross-system reconciliation for decision-grade finance answers.

Workflow steps to optimize:

1. Ask: submit finance question with period/segment context.
2. Ground: resolve canonical metric definitions + ownership.
3. Retrieve: gather evidence across DataPoints, docs, and governed sources.
4. Verify: run checks, surface caveats, and produce confidence.
5. Decide: return answer package with drill-down and audit trail.

Hard prioritization rule:

- roadmap items must map to at least one workflow step above and one charter KPI in `docs/PRD.md`.
- if no direct mapping, item moves to backlog.

## Execution Mode (Finance-Led Platform Build)

- Keep a dual-track build: finance outcomes first, platform hardening second.
- Recommended effort split per cycle:
  - ~70% finance workflow delivery (`WDG-*` and directly blocking fixes)
  - ~30% reusable platform foundation (`FND-*`, selected `DYN-*` slices)
- Platform work is eligible only if it does at least one of:
  - removes a repeated finance workflow failure mode
  - materially improves finance trust/speed KPI movement
  - reduces recurring operational risk for finance workflows

### Recommended Next Promotion Order

1. `DYN-001` (pre-WDG slices A-F) - ship loop control-plane primitives in bounded rollout.
2. `FND-005` retrieval evaluation baseline - ship retrieval-only traceability + evaluator surface.
3. `WDG-001` - complete finance workflow quality-bar acceptance criteria using loop-enabled finance prompts.
4. `FND-007` - ship governed finance knowledge pack and authority source registry.
5. `FND-006` - close feedback-to-training loop for retrieval misses and fallback hotspots.
6. `FND-002` - lifecycle/ownership controls to reduce metric-definition drift.

## Current Sprint Priority Lock (from `docs/SPRINT_NEXT_FOCUS_CHECKLIST.md`)

P0 (must complete first):

1. Onboarding clarity and setup guardrails.
2. Wizard stability and step transitions.
3. Retrieval and context accuracy.

P1 (finish reliability):

4. Metadata generation quality.
5. Train-from-answer loop reliability.
6. Reset and state hygiene.
7. Engine parity hardening (Postgres, MySQL, ClickHouse).

P2 (expansion after reliability):

8. Guided workflow harness (V1).
9. Document context V1 start (`PLT-004`) without blocking P0/P1 reliability goals.

### Pre-WDG Execution Order (Approved)

Before broad workflow-mode defaulting, execute `DYN-001` in this order:

1. Slice A: `ActionState` + loop budgets + terminal states.
2. Slice B: shadow-mode loop controller and parity traces.
3. Slice C: tool availability and forced-run policy hooks.
4. Slice D: self-heal error taxonomy and bounded retry decisions.
5. Slice E: API/UI/CLI trace visibility and replay.
6. Slice F: finance gate run against `WDG-001` thresholds.

Gate:

- do not promote workflow-mode defaulting until Slice F passes.

## Retrieval Quality Program (Finance-First, Approved)

This program hardens RAG/DataPoint/graph retrieval before additional deterministic expansion.

| Task ID | Initiative Mapping | Task | Priority | Status | Acceptance Signal |
|---------|--------------------|------|----------|--------|-------------------|
| RQ-01 | FND-005 | Add retrieval-only execution mode (API + CLI + UI toggle) that returns retrieved context without SQL/answer synthesis | High | Planned | Retrieval-only run returns stage payload and final context set |
| RQ-02 | FND-005 | Add retrieval trace schema with vector hits, graph hits, rerank scores, selected/not-selected reasons | High | Planned | Every run stores inspectable trace with reason codes |
| RQ-03 | FND-005, WDG-001 | Build Retrieval Evaluation page for finance prompts (question input, trace viewer, filters) | High | Planned | Operator can debug misses without reading backend logs |
| RQ-04 | FND-006 | Add relevance labeling (`relevant`, `irrelevant`, `missing`) on retrieved datapoints | High | Planned | Labels persist and can be queried per prompt/datapoint |
| RQ-05 | FND-006, FND-002, FND-004 | Add training queue that proposes datapoint edits (tags/synonyms/relationships/query templates) from labels | High | Planned | Suggested edits can be approved/rejected with audit trail |
| RQ-06 | FND-003, DYN-007 | Add retrieval eval gates in CI (`precision@k`, `recall@k`, source coverage, clarification-due-to-retrieval-miss) | Medium | Planned | Builds fail when retrieval quality falls below thresholds |
| RQ-07 | WDG-001 | Run finance prompt-pack retrieval benchmark weekly and track drift | Medium | Planned | Weekly scorecard with per-prompt regression diffs |
| RQ-08 | FND-005, FND-006 | Add before/after impact view for datapoint edits on retrieval quality | Medium | Planned | Each approved change includes measured quality delta |
| RQ-09 | FND-006, FND-002 | Extend "Train DataChat" loop beyond Query datapoints to guided Business/Schema/Process datapoint authoring | Medium | Planned | User can correct a failed answer and choose datapoint type; approved artifact improves follow-up runs |

### Retrieval Quality Gates

- `Precision@10 (finance prompts) >= 0.80`
- `Recall@20 (must-win prompts) >= 0.95`
- `Source attribution coverage >= 0.95`
- `Average clarifications due to retrieval miss <= 0.10`
- `Zero unresolved P1 retrieval regressions before release`

## Document Context Program (Approved)

This program adds business-document context as a first-class retrieval source alongside DataPoints.

Scope split:

- V1 is community + private baseline.
- V2 and V3 are private/enterprise lanes.

| Task ID | Initiative Mapping | Scope | Task | Priority | Status | Acceptance Signal |
|---------|--------------------|-------|------|----------|--------|-------------------|
| DC-01 | PLT-004, FND-005 | V1 Community + Private | Add document-directed chat mode for local docs (folders, PDFs, websites) | High | Planned | User can ask document questions and receive grounded answers without SQL execution |
| DC-02 | PLT-004, DYN-004 | V1 Community + Private | Add local document ingestion/index status surfaces (UI + CLI) | High | Planned | Operator can see indexed local document sources and freshness state |
| DC-03 | PLT-004, FND-005 | V1 Community + Private | Add hybrid retrieval fusion (DataPoints + documents) with citations | High | Planned | Answer output contains source-attributed document citations when docs are used |
| DC-04 | PLT-004, WDG-001 | V1 Community + Private | Add finance prompts that validate document + datastore grounding together | Medium | Planned | Manual scorecard captures document-context coverage and failure modes |
| DC-05 | PLT-005, DYN-002 | V2 Private/Enterprise | Add remote connectors (Confluence, Google Drive, S3) | High | Planned | Remote sources can be connected and indexed with source metadata |
| DC-06 | PLT-005, FND-004 | V2 Private/Enterprise | Add sync operations (schedule, retry, staleness, source health) | High | Planned | Operators can run and monitor sync jobs with deterministic state transitions |
| DC-07 | PLT-006, FND-004 | V3 Private/Enterprise | Add ACL-aware retrieval policy layer for document access | High | Planned | Retrieval excludes unauthorized sources with auditable reason codes |
| DC-08 | PLT-006, DYN-004 | V3 Private/Enterprise | Add doc-data linking graph (doc <-> table/metric/datapoint edges) | Medium | Planned | Retrieval traces include explicit linked-context edges used in final answers |
| DC-09 | PLT-006, FND-006 | V3 Private/Enterprise | Add feedback-driven document reranking loop | Medium | Planned | Relevance labels measurably improve document precision on benchmark prompts |

## Packaging and Config UX Program (Approved)

This program reduces first-run friction by making install and configuration easier for evaluators.

| Task ID | Initiative Mapping | Task | Priority | Status | Acceptance Signal |
|---------|--------------------|------|----------|--------|-------------------|
| PX-01 | SMP-001, PLT-001 | Publish installable package flow (`pip install datachat`) with versioned release process | High | Planned | Fresh environment installs CLI/API from package index without editable install |
| PX-02 | SMP-001, FND-004 | Add settings-first configuration path (UI + CLI) so users can onboard without manual `.env` editing for common setup | High | Planned | User can connect DB + provider keys through product settings and complete first question flow |
| PX-03 | SMP-001, FND-003 | Add config health checks/migration hints for env-to-settings compatibility | Medium | Planned | Existing `.env` users get non-breaking migration guidance and validation warnings |

## AI System Reliability Program (Approved)

This program operationalizes the platform against three system benchmarks:

- explicit dataflow
- boundary data quality
- monitoring and diagnosability

The first slice is shared across private and community builds and is tracked by `OPS-001`.

| Task ID | Initiative Mapping | Task | Priority | Status | Acceptance Signal |
|---------|--------------------|------|----------|--------|-------------------|
| OPR-01 | OPS-001 | Persist run and step records for chat, profiling, generation, approval, sync, and reset | High | Completed | Operator can inspect a full run without reading backend logs |
| OPR-02 | OPS-001, FND-005 | Persist retrieval traces for selected and rejected sources with reason codes | High | Completed | Every retrieval-backed answer has inspectable source-selection history |
| OPR-03 | OPS-001, FND-004 | Add quality findings engine for stale context, attribution gaps, retrieval conflicts, and schema mismatch | High | Completed | Quality issues are visible by run and by entity |
| OPR-04 | OPS-001, FND-006 | Add telemetry event store and rollups for latency, failure classes, fallback rate, clarification rate, and retrieval misses | High | Completed | Monitoring UI renders from persisted aggregates |
| OPR-05 | OPS-001 | Add simple `Runs`, `Quality`, and `Monitoring` dashboards | High | Completed | A new operator can understand system state in under one minute |
| OPR-06 | OPS-001, FND-003, DYN-007 | Extend smoke/integration/eval coverage to assert run persistence and monitoring correctness | Medium | Completed | Regression in run or monitoring behavior becomes test-detectable |

## Finance Authority Knowledge Program (Approved)

This program adds governed external finance knowledge as a canonical metric pack, not ad hoc runtime browsing.

| Task ID | Initiative Mapping | Task | Priority | Status | Acceptance Signal |
|---------|--------------------|------|----------|--------|-------------------|
| FK-01 | FND-007 | Define authority source registry with approval and licensing metadata | High | Planned | Registry exists and blocks unapproved sources |
| FK-02 | FND-007, FND-001 | Extend datapoint contract with authority metadata fields | High | Planned | Contract lint fails when authority metadata is missing for finance-global metrics |
| FK-03 | FND-007, FND-002 | Define canonical finance metric schema and stable `metric_id` mapping | High | Planned | Canonical schema adopted by initial finance-global metric set |
| FK-04 | FND-007 | Build ingestion pipeline to normalize approved sources into `managed/finance_global` datapoints | High | Planned | Reproducible publish job outputs versioned finance metric pack |
| FK-05 | FND-007, FND-005 | Add runtime retrieval precedence: org/local datapoints > finance-global canonical pack > external fallback | High | Planned | Trace includes authority-tier selection reason |
| FK-06 | FND-007, WDG-001 | Add finance definition eval track (formula correctness + authority citation) | Medium | Planned | Eval suite produces pass/fail summary and prompt-level diagnostics |
| FK-07 | FND-007, FND-006 | Add drift monitor for source refreshes and conflict-review queue | Medium | Planned | Source updates produce conflict report and review queue |
| FK-08 | FND-007, WDG-001 | Add optional definition prompts to finance manual runbook and scorecard | Medium | Planned | Manual test includes definition consistency checks |

### Finance Authority Gates

- `Authority citation coverage >= 0.95` on finance definition prompts.
- `Formula correctness >= 0.90` on canonical metric prompts.
- `Unresolved authority conflicts = 0` for published finance-global pack.
- `Runtime fallback-to-external <= 0.05` for must-win finance prompts.

---

## Ownership Model

- `docs/PRD.md` owns product intent (`what` and `why`).
- `docs/ARCHITECTURE.md` owns technical design (`how`).
- `docs/LEVELS.md` owns maturity definitions (`which capabilities define each level`).
- `docs/ROADMAP.md` owns delivery plan (`when`, `status`, `dependency`).

Conflict resolution:

1. Product scope conflict -> `PRD.md` wins.
2. Design conflict -> `ARCHITECTURE.md` wins.
3. Level classification conflict -> `LEVELS.md` wins.
4. Status/timeline conflict -> `ROADMAP.md` wins.

---

## Status Legend

- `Done` - shipped and validated.
- `In Progress` - actively being implemented.
- `Planned` - accepted but not started.
- `Blocked` - cannot proceed due to unresolved dependency.

## Execution Model (Solo)

Current operator model:

- single maintainer workflow (Onuh)
- no required owner column per initiative
- no issue/epic dependency on GitHub for roadmap operation

---

## Initiative Index

| ID | Initiative | Area | Status | Depends On | Spec |
|----|------------|------|--------|------------|------|
| FND-001 | Metadata contracts + linting gates | MetadataOps | In Progress | - | [FND-001](specs/FND-001.md) |
| FND-002 | Metadata authoring lifecycle controls | MetadataOps | Planned | FND-001 | [FND-002](specs/FND-002.md) |
| FND-003 | AI-readiness eval gates in CI | MetadataOps | In Progress | FND-001 | [FND-003](specs/FND-003.md) |
| FND-004 | Governance metadata APIs | MetadataOps | Planned | FND-001 | [FND-004](specs/FND-004.md) |
| FND-005 | Retrieval/source trace observability + retrieval evaluation surfaces | MetadataOps | In Progress | FND-001 | [FND-005](specs/FND-005.md) |
| FND-006 | Runtime telemetry loops + feedback-driven retrieval training queue | MetadataOps | Planned | FND-003, FND-005 | [FND-006](specs/FND-006.md) |
| FND-007 | Governed finance knowledge pack + authority source registry | MetadataOps | Planned | FND-001, FND-002, FND-005 | [FND-007](specs/FND-007.md) |
| FND-008 | Query DataPoint generation v1 (grouped, ranked, temporal analytics patterns) | MetadataOps | In Progress | FND-001, FND-005 | [FND-008](specs/FND-008.md) |
| OPS-001 | AI system reliability foundation (run registry, quality findings, monitoring dashboards) | Platform Reliability | Completed | FND-004, FND-005, FND-006 | [OPS-001](specs/OPS-001.md) |
| SMP-001 | Simple entry layer refresh (onboarding wizard as default) | UX | In Progress | FND-001 | [SMP-001](specs/SMP-001.md) |
| SMP-002 | Deterministic simplicity package (template/function lane) | Runtime | Planned | FND-001..FND-007 stable | [SMP-002](specs/SMP-002.md) |
| PLT-001 | Runtime connector expansion | Platform | Planned | FND foundation stable | [PLT-001](specs/PLT-001.md) |
| PLT-002 | Deterministic coverage for additional catalog intents | Runtime | Planned | FND-003 | [PLT-002](specs/PLT-002.md) |
| PLT-003 | Semantic accuracy improvements in low-context mode | Runtime | Planned | FND-005, FND-006 | [PLT-003](specs/PLT-003.md) |
| PLT-004 | Document context V1 (local docs ingestion + hybrid retrieval + citations) | Platform | Planned | FND-005 | - |
| PLT-005 | Document context V2 (remote connectors + sync operations) | Platform | Planned | PLT-004, FND-004 | - |
| PLT-006 | Document context V3 (ACL retrieval + doc-data linking + rerank loop) | Platform | Planned | PLT-005, FND-006 | - |
| DYN-001 | Dynamic planner + verifier loop foundation | Dynamic Agent | In Progress | FND foundation stable | [DYN-001](specs/DYN-001.md) |
| DYN-002 | Unified workspace state (data + docs + org context) | Dynamic Agent | Planned | DYN-001 | [DYN-002](specs/DYN-002.md) |
| DYN-003 | Data-first tool harness with policy/approval classes | Dynamic Agent | Planned | DYN-001 | [DYN-003](specs/DYN-003.md) |
| DYN-004 | Unified knowledge fabric (DataPoints + docs + org retrieval) | Dynamic Agent | Planned | DYN-002 | [DYN-004](specs/DYN-004.md) |
| DYN-005 | Checkpoints, memory layers, replayable traces | Dynamic Agent | Planned | DYN-001 | [DYN-005](specs/DYN-005.md) |
| DYN-006 | Domain subagents + skills | Dynamic Agent | Planned | DYN-001..DYN-005 | [DYN-006](specs/DYN-006.md) |
| DYN-007 | Dynamic-agent eval gates and operational scorecards | Dynamic Agent | Planned | DYN-001..DYN-005 | [DYN-007](specs/DYN-007.md) |
| WDG-001 | Finance wedge workflow v1 (revenue variance + liquidity risk) | Product | In Progress | FND-001, FND-005 | [WDG-001](specs/WDG-001.md) |

---

## Initiative-to-Workflow Mapping (Finance Wedge)

| Initiative IDs | Workflow Step(s) | Expected KPI Movement |
|----------------|------------------|-----------------------|
| FND-001, FND-002, FND-004 | Ground | reduce wrong-definition incidents, reduce rework |
| FND-003, FND-005, FND-006 | Retrieve, Verify | improve retrieval precision/recall, increase attribution coverage, reduce retrieval-driven clarification loops |
| FND-007 | Ground, Retrieve, Verify | improve definition consistency, authority-backed citations, and formula trust |
| OPS-001 | Retrieve, Verify, Decide | improve diagnosability, operational trust, and regression control |
| FND-008 | Ground, Retrieve, Verify | improve reusable analytics coverage, reduce ad hoc SQL generation, improve question-to-template matching |
| SMP-001, SMP-002 | Ask, Decide | reduce time-to-answer package |
| PLT-001, PLT-002 | Retrieve | improve coverage/speed across systems |
| PLT-003 | Verify, Decide | improve answer quality in low-context runs |
| PLT-004, PLT-005, PLT-006 | Ground, Retrieve, Verify | improve business-context grounding and source-attributed trust |
| DYN-001, DYN-002, DYN-003 | Ask, Retrieve, Verify | reduce latency + operator effort |
| DYN-004, DYN-005 | Ground, Verify | improve trust, auditability, replayability |
| DYN-006, DYN-007 | Decide | stabilize outcome quality and operational scorecards |
| WDG-001 | Ask, Ground, Retrieve, Verify, Decide | reduce time-to-trusted-answer and rework |

---

## Prioritization Scoring Rubric

Use this rubric before moving any initiative from `Planned` to `In Progress`.

Score each dimension from 1 to 5:

- **Workflow Impact:** expected improvement to finance wedge workflow outcome.
- **Trust/Risk Reduction:** improvement to provenance, governance, or wrong-answer prevention.
- **Speed/Operator Efficiency:** reduction in time-to-trusted-answer.
- **Feasibility (Solo):** realistic implementation/test burden for a single maintainer.
- **Reusability:** value of capability beyond first wedge without diluting wedge focus.

Composite score:

- `Priority Score = 0.35*Workflow Impact + 0.25*Trust + 0.20*Speed + 0.15*Feasibility + 0.05*Reusability`

Promotion threshold:

- `>= 3.8`: eligible for next active slot.
- `3.0-3.7`: keep planned, needs tighter scope.
- `< 3.0`: backlog.

Mandatory gate:

- any item with Trust/Risk Reduction `< 3` cannot be promoted, regardless of composite score.

---

## Sequencing Plan

### Phase A: Foundation Stabilization

Scope:

- FND-001..FND-007
- SMP-001 (only thin wrappers, no semantic/routing changes)
- Retrieval Quality Program tasks `RQ-01` through `RQ-06`

Exit criteria:

- foundation KPIs green for one full release cycle
- deterministic intent regressions blocked by CI
- provenance/traces inspectable in production workflows

### Phase B: Simplicity + Platform Expansion

Scope:

- SMP-002
- PLT-001..PLT-003

Gate:

- Phase A exit criteria complete

### Phase C: Dynamic Data Agent Foundation

Scope:

- DYN-001..DYN-005

Gate:

- Phase B stable operations

### Phase D: Dynamic Data Agent Productization

Scope:

- DYN-006..DYN-007

Gate:

- replayable and auditable dynamic loop with safety controls in place

---

## Level Mapping

| Level | Initiative IDs |
|-------|----------------|
| Level 1 | FND-001, FND-003, FND-005 |
| Level 1.4 | SMP-001 |
| Level 1.5 | FND-001..FND-006 |
| Level 1.6 | SMP-002 |
| Level 2 | PLT-002, PLT-003 |
| Level 3 | SMP-002 + DYN-001 |
| Level 4 | DYN-003, DYN-004 |
| Level 5 | DYN-006, DYN-007 |

---

## Maintenance Rules

When adding/changing work:

1. Create or update initiative ID here first.
2. Create/update a spec in `docs/specs/<ID>.md`.
3. Do not move an initiative to `In Progress` unless its spec has no placeholders and includes concrete acceptance criteria + test plan.
4. Reference the ID in PRD/Architecture/Levels instead of duplicating status prose.
5. Update dependencies and phase placement.
6. Mark status changes only in this file.
