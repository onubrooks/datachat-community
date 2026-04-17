# Initiative Specs

This folder contains implementation-ready technical specs per roadmap initiative.

Use `SPEC_TEMPLATE.md` for new specs.

## Workflow

1. Define/update initiative ID in `docs/ROADMAP.md`.
2. Create/update `docs/specs/<ID>.md`.
3. Link the spec from `docs/ROADMAP.md`.
4. Implement against the spec and update acceptance checkboxes.

## Contract

Each initiative spec should be sufficient for an engineer or LLM to execute work with minimal ambiguity.

Required sections:

- problem and scope
- architecture delta
- contracts/api changes
- data model/migrations
- safety/policy controls
- acceptance criteria
- test plan
- rollout/rollback

Readiness rule before implementation:

- Do not mark an initiative `In Progress` in `docs/ROADMAP.md` if its spec still contains placeholders (`TBD`, `TODO`) or missing acceptance/test details.

## Initiative Index

| ID | Spec |
|----|------|
| FND-001 | [FND-001.md](FND-001.md) |
| FND-002 | [FND-002.md](FND-002.md) |
| FND-003 | [FND-003.md](FND-003.md) |
| FND-004 | [FND-004.md](FND-004.md) |
| FND-005 | [FND-005.md](FND-005.md) |
| FND-006 | [FND-006.md](FND-006.md) |
| FND-007 | [FND-007.md](FND-007.md) |
| FND-008 | [FND-008.md](FND-008.md) |
| OPS-001 | [OPS-001.md](OPS-001.md) |
| SMP-001 | [SMP-001.md](SMP-001.md) |
| SMP-002 | [SMP-002.md](SMP-002.md) |
| PLT-001 | [PLT-001.md](PLT-001.md) |
| PLT-002 | [PLT-002.md](PLT-002.md) |
| PLT-003 | [PLT-003.md](PLT-003.md) |
| DYN-001 | [DYN-001.md](DYN-001.md) |
| DYN-002 | [DYN-002.md](DYN-002.md) |
| DYN-003 | [DYN-003.md](DYN-003.md) |
| DYN-004 | [DYN-004.md](DYN-004.md) |
| DYN-005 | [DYN-005.md](DYN-005.md) |
| DYN-006 | [DYN-006.md](DYN-006.md) |
| DYN-007 | [DYN-007.md](DYN-007.md) |
| WDG-001 | [WDG-001.md](WDG-001.md) |
