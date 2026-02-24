# Capability Contract

This document is the runtime truth table for what DataChat supports **today**.

Legend:

- `Supported`: shipped, test-covered, documented.
- `Partial`: works in common cases, with explicit limits.
- `Planned`: not shipped in runtime yet.

## Runtime Contract

| Capability | Status | Contract |
| --- | --- | --- |
| Credentials-only chat (no DataPoints) | Supported | SQL answers work from live schema/catalog metadata only. |
| DataPoint-enhanced answers | Supported | Retrieval augments SQL/context answers when DataPoints are available. |
| Multi-database routing (`target_database`) | Supported | Request-scoped DB type/URL is used end-to-end. |
| DataPoint retrieval scoping | Supported | Retrieval prefers matching `metadata.connection_id`; global/shared DataPoints are included; unscoped fallback is legacy compatibility. |
| Multi-question decomposition (single prompt with multiple asks) | Partial | Deterministic split into up to 3 sub-questions, aggregated into one response with `sub_answers`. |
| Per-subquestion clarification | Partial | Clarifications are returned with per-subquestion tags (`[Q1]`, `[Q2]`) when decomposition is active. |
| Streaming per-subquestion agent events | Partial | WebSocket emits decomposition metadata and final aggregated output; full per-agent streaming is strongest for single-question prompts. |
| Workspace/folder indexing + retrieval | Planned | Not yet shipped as runtime feature. |
| BigQuery/Redshift runtime query connectors | Planned | Templates exist in some areas, but runtime connectors are not shipped. |

## Interface Parity Notes

- CLI/API/WebSocket all use the same pipeline orchestration.
- When a prompt is decomposed, responses include:
  - aggregated `answer`
  - optional `clarifying_questions` with subquestion labels
  - `sub_answers` entries (`query`, `answer`, `answer_source`, `sql`, etc.)

## Non-Goals (Current Phase)

- Cross-turn workflow planner over arbitrary non-data tasks.
- General-purpose autonomous agent platform behavior.
- Guaranteed perfect decomposition for highly ambiguous compound prompts.
