# MetadataOps Foundation

## Why this matters now

In an AI-driven data stack, the bottleneck is no longer SQL generation alone.  
The bottleneck is metadata quality, metadata governance, and metadata observability.

LLMs can generate fluent answers from poor context, which creates a dangerous failure mode:

- confident but incorrect answers
- silent metric drift across teams
- repeated clarifications that feel like "agent intelligence problems" but are actually metadata problems

DataChat treats this as a product and platform concern, not a documentation afterthought.

## DataChat thesis

DataChat is not just an NL->SQL interface.  
It is a metadata-aware runtime with progressive enhancement:

- Level 1: credentials-only querying with deterministic catalog intelligence
- Level 2: DataPoint-enhanced business context
- Levels 3-5: advanced automation built on top of reliable metadata foundations

The core belief:

- better metadata authoring quality + better observability loops beat prompt-only cleverness for sustained quality

## DataChat MetadataOps philosophy

1. Determinism before model cleverness
- Prefer catalog/system queries for schema-shape intents.
- Use LLM reasoning where metadata and deterministic paths are insufficient.

2. Truth over aspiration
- "Supported" means implemented + test-covered + documented.
- Avoid roadmap language that implies runtime capability.

3. Metadata is a runtime contract
- DataPoints are executable context assets, not static notes.
- Contracts must define grain, units, freshness, owners, exclusions, and business meaning.

4. Observability is part of product quality
- Every answer should be traceable to sources, tiers, and retrieval path.
- Retrieval and fallback behavior must be inspectable and testable.

5. CI-enforced quality gates
- Regressions in retrieval/source behavior should fail CI, not wait for user reports.
- Evaluation suites are part of the architecture, not optional tooling.

## Foundation pillars (Priority 0 lane)

These pillars are tracked before major Level 3-5 expansion.

### 1. Metadata contracts + linting
- enforce required metadata fields and reject invalid DataPoints early
- contract v2: validate business_meaning on columns, calculation SQL fragments, synonym coverage

### 2. Metadata authoring lifecycle
- ownership, review discipline, versioning, and conflict handling

### 3. AI-readiness evaluation in CI
- retrieval/qa/intent/catalog thresholded checks

### 4. Governance metadata APIs
- expose lineage/freshness/quality context as first-class runtime data

### 5. RAG observability + traceability
- source tier, source path, score, fallback path, and provenance visibility

### 6. Runtime telemetry loops
- clarification churn, wrong-table patterns, fallback rates, low-confidence hotspots
- DataPoint improvement suggestions based on failure patterns

### 7. Knowledge Graph enhancement (NEW)
- column-level edges: `DERIVES_FROM`, `COMPUTES`, `FILTERS_BY`
- semantic edges connecting metrics to source columns
- grain edges: `HAS_GRAIN` for table granularity
- lineage traversal for better SQL context

### 8. Feedback telemetry loop (NEW)
- track wrong-table selections → suggest DataPoint table hints
- track clarification churn → suggest missing synonyms
- track low-confidence queries → suggest missing business_meaning
- CLI: `datachat telemetry report`, `datachat dp suggest --datapoint <id>`

## How contributors should align

When proposing a new feature:

1. define metadata and observability impact first
2. include deterministic behavior where possible
3. add eval/test coverage for failure modes
4. update docs with shipped-vs-planned clarity

Preferred implementation order:

1. improve metadata quality and traceability
2. improve deterministic coverage
3. then increase model sophistication

## Definition of progress

The foundation lane is working when:

- metadata contract lint pass rates are stable and high
- eval suites pass in CI with meaningful thresholds
- source provenance is visible for most answers
- clarification loops decrease release-over-release
- production regressions shift from "mystery behavior" to quickly diagnosable failures

## Relationship to levels and roadmap

MetadataOps Foundation is cross-level ("Level 1.5").  
It supports Levels 1-5 and is treated as a prerequisite for scaling advanced Levels 3-5 safely.

