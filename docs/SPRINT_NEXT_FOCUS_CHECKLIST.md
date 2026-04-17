# Next Sprint Focus Checklist

## Current Immediate Program

Canonical implementation specs:

- `docs/specs/OPS-001.md`
- `docs/specs/FND-008.md`

Immediate build target:

- keep the reliability surfaces stable after merge
- improve answer quality by generating richer query and business datapoints from profiled schemas
- add narrow eval coverage for reusable analytics patterns before expanding the training surface further

What just landed:

- AI System Reliability Foundation (`OPS-001`) across `datachat` and `datachat-community`
- persisted runs, retrieval traces, quality findings, monitoring rollups, dashboards, compare view
- retrieval/generation quality fixes for scope-aware retrieval, graph fallback, better numeric detection, and schema-qualified generation keys

What this sprint is about:

- query datapoint generation from profiled tables
- metric-family and temporal query coverage for common business questions
- better template parameter filling from user questions
- question-bank-aligned eval coverage for generated query patterns

Explicitly out of scope for this pass:

- major training UI redesign
- remote document connectors
- new workflow harness surfaces
- broad domain packs beyond generic heuristics plus current grocery/fintech focus

## Reliability Sprint Status

OPS-001 is complete:

1. [x] OPS-001A: Persisted run graph
2. [x] OPS-001B: Retrieval trace persistence
3. [x] OPS-001C: Runs dashboard and run detail UI
4. [x] OPS-001D: Quality findings engine and Quality dashboard
5. [x] OPS-001E: Monitoring event rollups and Monitoring dashboard
6. [x] OPS-001F: Automated test coverage and manual QA checklist
7. [x] P2: run-to-run comparison view for retry and train-before/after inspection
8. [x] P2: advisory drift checks beyond the initial stale-profile and retrieval-conflict set

## Sprint Goal

Raise answer quality on real business questions by generating reusable query datapoints that retrieval and SQL planning can use directly.

## Prioritization Order (This Sprint)

P0 (must complete first):

1. FND-008A: Query datapoint generation from profiled schemas
2. FND-008B: Metric-family and temporal query variants
3. FND-008C: Template parameter extraction from user phrasing

P1 (must land before release candidate):

4. FND-008D: Generation pipeline integration across API, CLI, and profiling tools
5. FND-008E: Question-bank-aligned QA eval coverage
6. FND-008F: Community parity + docs/spec updates

P2 (only if time remains):

7. richer domain heuristics for fintech and grocery
8. retrieval evals for generated query datapoints

## Numbered Backlog

1. Query DataPoint Generation V1

- [ ] Generate deterministic query datapoints from profiled tables using time, dimension, and numeric-column heuristics.
- [ ] Cover common reusable shapes:
  - top-N by dimension
  - weekly trend by primary time column
  - net-flow style patterns when paired inflow/outflow metrics exist
- [ ] Emit connector-aware SQL templates and parameter metadata.

2. Metric Families and Temporal Variants

- [ ] Promote core measures into reusable grouped, ranked, and temporal query variants.
- [ ] Improve measure naming so retrieval sees business-friendly phrasing instead of only raw column names.
- [ ] Keep generated query coverage compact enough to avoid index noise.

3. SQL Template Parameter Filling

- [ ] Infer values like `top 5`, `last 8 weeks`, and similar numeric windows from the user question.
- [ ] Fall back cleanly to template defaults when the question does not provide explicit values.
- [ ] Keep parameter extraction deterministic and easy to audit.

4. Generation Pipeline Integration

- [ ] Persist generated query datapoints through profiling API flows.
- [ ] Persist generated query datapoints through CLI and tool-based profiling flows.
- [ ] Include query datapoints in generation quality summaries and approval workflows.

5. Eval Coverage for Generated Query Patterns

- [ ] Add QA eval cases for generated query-datapoint style prompts.
- [ ] Cover at least fintech grouped, ranked, and temporal patterns.
- [ ] Keep expected SQL assertions broad enough to remain connector-safe.

6. Community Parity and Documentation

- [ ] Port the full FND-008 slice to `datachat-community`.
- [ ] Update sprint checklist and roadmap to reflect completed reliability work and current query-generation focus.
- [ ] Add a formal initiative spec for Query DataPoint Generation V1.

## Historical Backlog

The items below were the prior focus and are retained only as historical context.

1. Onboarding Clarity and Setup Guardrails

- [ ] Make setup requirements explicit in UI and docs (DB URL path, provider key path, embedding key dependency).
- [ ] Ensure the wizard is the single default setup path when system is not initialized.
- [ ] Add inline “not initialized” guidance with direct action buttons.

2. Wizard Stability and Step Transitions

- [ ] Fix metadata generation status lifecycle (no flicker, no premature reset to idle).
- [ ] Keep progress visible until terminal state (`completed` or `failed`).
- [ ] Ensure successful generation advances to next step automatically.

3. Metadata Generation Quality

- [ ] Include column sample values / distinct values from profiling for better SQL grounding.
- [ ] Ensure generated datapoints consistently include contract-required metadata (`grain`, `exclusions`, `confidence_notes`).
- [ ] Improve generated descriptions/metrics usefulness for non-finance schemas.

4. Train-From-Answer Loop Reliability

- [ ] Ensure “Train DataChat” create/update flow is stable and immediate (no page reload required to see changes).
- [ ] Ensure training updates retrieval behavior measurably on retry.
- [ ] Add clear sync/indexing completion signal after training save.

5. Retrieval and Context Accuracy

- [ ] Reduce empty/incorrect SQL answers caused by stale date windows or enum mismatch.
- [ ] Improve retrieval ranking toward newly approved/managed datapoints for relevant questions.
- [ ] Add focused regression tests for high-value domain questions.

6. Reset and State Hygiene

- [ ] Make reset clear conversations/history/UI state consistently.
- [ ] Ensure reset clears metadata views and vector state predictably.
- [ ] Keep reset behavior engine-safe and consistent across CLI and UI.

7. Deferred This Sprint

- [ ] Advanced UI theming/polish beyond usability fixes.
- [ ] New channel integrations / external surfaces.

8. Deferred This Sprint

- [ ] Major expansion of non-core datapoint types in training UI.
- [ ] Large framework migrations.

9. Engine Parity Hardening (Postgres/MySQL/ClickHouse)

- [ ] Run onboarding + ask + train + reset validation for each engine.
- [ ] Close parity gaps in reset/profile/generate flows.
- [ ] Keep smoke matrix green and expand assertions where needed.

## Added Task: Harness Workflow Improvement

H1. Guided Workflow Harness (V1)

- [ ] Add a minimal workflow runner for multi-step tasks (`plan -> execute -> verify -> summarize`) with explicit step logs.
- [ ] Restrict V1 to safe, read-first operations and existing tool contracts.
- [ ] Add a single “Run as workflow” option in UI for selected question types (pilot mode).
- [ ] Track outcomes: completion rate, retries, and user corrections.

## Parallel Design Track (Docs + Planning)

D1. Document Context Layer (Business Docs)

- [ ] Define V1 community scope: local folder docs, PDFs, website ingestion + hybrid retrieval + citations.
- [ ] Define V2 private scope: Confluence/S3/Drive connectors + operational sync controls.
- [ ] Define V3 private scope: ACL-aware retrieval, doc-data linking, and feedback-driven reranking.
- [ ] Update PRD/Architecture/Roadmap to separate document context from code-first workspace indexing.

## Exit Criteria

- [ ] Generated query datapoints appear in profiling/generation outputs without manual authoring.
- [ ] At least one previously weak question is answered via a generated query datapoint path.
- [ ] No critical regressions in existing profiling, retrieval, or launch-path tests.
- [ ] New QA eval cases cover generated grouped, ranked, and temporal query patterns.
- [ ] Operators can inspect one bad answer and determine whether the issue came from retrieval, validation, execution, or synthesis without reading backend logs.
- [ ] `Runs`, `Quality`, and `Monitoring` dashboards stay understandable without additional product training.

## OPS-001F Status

- [x] Route-level tests cover `runs`, `quality`, and `monitoring` summaries/trends.
- [x] Reset regression covers clearing `ai_quality_findings` along with `ai_runs`.
- [x] Manual QA checklist is captured in `docs/specs/OPS-001.md` sections `9.1` through `9.6`.
