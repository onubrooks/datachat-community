# Community v0.1.0 Scope Lock

## Release Goal
Ship `datachat-community` with a stable, usable foundation for data teams evaluating DataChat in real environments.

## Included in v0.1.0

- Database connection + schema exploration
- Chat UX for natural-language analytics and SQL answers
- SQL editor mode (read-only execution)
- Current reliable harness/runtime path (as-shipped)
- Result surfaces: Answer, SQL, Table, Visualization, Evidence, Timing
- Onboarding wizard flows (UI + CLI)
- Managed DataPoint generation/editor/review workflows
- CLI parity for core user flows (`setup`, `ask/chat`, onboarding, datapoint sync/generation)

## Explicitly Out of Scope for v0.1.0

- Proprietary workflow packs and premium enterprise automation layers
- Enterprise document-context extensions (V2/V3): remote connectors, ACL retrieval, and doc-data linking
- Internal strategy, GTM, and roadmap planning documents
- Internal evaluation artifacts not needed by OSS users

## Quality Bar

- Backend unit/integration tests pass for included capabilities
- Frontend tests pass for core chat + visualization behavior
- Lint/format checks pass
- Fresh install + quickstart validated on at least one demo database
- No broken links in public docs

## Release Policy

- `datachat` private repo remains the source of truth
- Community repo receives promoted, public-safe commits only
- Any feature not explicitly in this scope is treated as out-of-scope for `v0.1.0`

## Post-v0.1 Candidates (Planned)

- Extend the **Train DataChat** correction loop from Query datapoints to guided creation of:
  - Business datapoints
  - Schema datapoints
  - Process datapoints
- Keep managed lifecycle + approval semantics consistent across all datapoint types.
- Document Context V1 (community): local docs/PDF/web ingestion with cited retrieval.

Enterprise/private lane (not in community v0.1):

- Document Context V2: Confluence/Drive/S3 connectors + sync operations.
- Document Context V3: ACL-aware retrieval + doc-data relationship graph + feedback reranking.
