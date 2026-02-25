# DataChat Community Architecture

This document describes the architecture that is shipped and supported in `datachat-community` today, plus near-term community enhancements.

## 1. Scope

Community scope is defined by:

- `docs/COMMUNITY_V0_1_SCOPE_LOCK.md`
- `docs/CAPABILITY_CONTRACT.md`

This architecture doc is intentionally limited to public, shipped behavior and practical next enhancements around DataPoints and retrieval quality.

## 2. System Goals

- Get a new user from DB credentials to first useful answer in 5 minutes or less.
- Keep SQL execution read-only and policy-safe.
- Improve answer trust through retrieval, evidence, and explicit caveats.
- Keep onboarding and operations simple for evaluators.

## 3. Runtime Components

### 3.1 Entry Surfaces

- CLI (`datachat ask`, `datachat chat`, `datachat onboarding wizard`)
- API (`/api/v1/chat`, `/api/v1/tools/*`, `/api/v1/databases/*`)
- Web UI (chat, SQL/table/visualization/evidence, onboarding/database manager)

### 3.2 Orchestration Flow

High-level path per request:

1. Query analyzer classifies intent and route.
2. Route handler executes one of:
   - SQL route
   - context-only route
   - tool route
   - end/clarification route
3. SQL path uses compiler/generator + validator + executor.
4. Response synthesis packages answer, SQL, table/visualization data, evidence, and timing.

### 3.3 Retrieval and Knowledge Layer

- DataPoint loader (Schema/Business/Process/Query)
- Vector retrieval for semantic relevance
- Knowledge graph for structural relationships
- Scoped retrieval using connection metadata when available

### 3.4 MetadataOps Layer

- Profiling jobs discover table metadata
- Managed pending DataPoints are generated for review
- Approved DataPoints improve retrieval quality and answer grounding

### 3.5 Governance and Safety

- Read-only SQL execution model in user-facing flows
- Policy checks in tool execution
- Explicit caveats and trace/timing surfaces in responses

## 4. 5-Minute Onboarding Architecture

The community onboarding path is built to minimize user decisions:

1. User provides database URL (or saved connection).
2. System validates connectivity and schema visibility.
3. User can run quick metadata generation from UI/CLI.
4. User asks first question and gets SQL + answer + evidence.

This flow avoids requiring manual DataPoint authoring before first value.

## 5. Community-Focused Planned Enhancements

The next public enhancements should focus on retrieval quality and DataPoint lifecycle, not feature breadth.

### 5.1 Retrieval Explainability Surface

- Add a retrieval-debug view (UI and API) showing:
  - top retrieved DataPoints
  - score/rank signals
  - reason codes (match type, scope hit, semantic fallback)

### 5.2 Onboarding Metadata Quality Upgrade

- During onboarding, generate table summaries and candidate business descriptions.
- Store as pending managed DataPoints for quick approve/edit.
- Include confidence and coverage indicators.

### 5.3 Retrieval Evaluation Loop

- Add a lightweight evaluation workflow:
  - submit question
  - inspect retrieved context only (without full answer)
  - provide feedback for relevance/coverage
  - track retrieval quality trends over time

### 5.4 Authority Packs (Finance-first)

- Provide optional, governed metric definition packs as DataPoints.
- Keep source tiering explicit and auditable.
- Use packs to reduce ambiguous metric interpretation.

## 6. Out of Scope for Community v0.1

- Private GTM/roadmap/spec planning artifacts
- Proprietary enterprise workflow packs
- Premium automation layers beyond public capability contract

## 7. Testing and Reliability Expectations

Before each release cut:

- lint and tests pass
- docs have no broken links
- clean install path validated
- 5-minute onboarding path manually verified

See:

- `TESTING.md`
- `docs/COMMUNITY_V0_1_SCOPE_LOCK.md`
- `CONTRIBUTING.md` (community contribution workflow)
