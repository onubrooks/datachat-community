# Phase 1.4: Simple Entry Layer

This phase adds a thin onboarding wrapper without changing core retrieval/routing semantics.

## Status Update (February 2026)

This document captures the original thin-wrapper entry layer.

Current recommended onboarding flow is now:

- `datachat onboarding wizard`

The wizard replaces multi-step setup/profile/generate/approve handoffs with a single
deep metadata onboarding run. Keep Phase 1.4 wrappers as fallback/advanced controls,
but treat wizard flow as the default evaluator path.

## Goals

- reduce setup friction with guided wrappers
- keep behavior parity with existing `connect/setup/profile/sync` paths
- add lightweight telemetry for onboarding progress/failures

## CLI Features

### `datachat quickstart`

Wrapper over existing commands:

1. `datachat connect`
2. `datachat setup`
3. optional `datachat demo`
4. optional `datachat ask`

Example:

```bash
datachat quickstart \
  --database-url postgresql://postgres:@localhost:5432/postgres \
  --dataset grocery \
  --question "list all grocery stores"
```

### `datachat train`

Wrapper over existing sync/profile flows:

- `--mode sync`: wraps `datachat dp sync`
- `--mode profile`: wraps `datachat profile start` with optional `datachat dp generate`

Examples:

```bash
datachat train --mode sync --datapoints-dir datapoints/examples/grocery_store

datachat train \
  --mode profile \
  --profile-connection-id <connection_id> \
  --generate-after-profile
```

CLI writes best-effort event lines to:

- `~/.datachat/entry_events.jsonl`

## UI Features

Database Manager now includes a **Quick Start (Phase 1.4)** panel that orchestrates existing actions:

1. connect
2. profile
3. generate
4. approve
5. sync

Panel behavior:

- step statuses are derived from current connection/job/pending/sync state
- action buttons call the same existing handlers used elsewhere on the page
- starter query suggestions are provided for first chat runs

Telemetry:

- UI emits best-effort events to `POST /api/v1/system/entry-event`

## Manual Testing

## CLI

1. Run help checks:

```bash
datachat quickstart --help
datachat train --help
```

2. Validate quickstart guardrail:

```bash
datachat quickstart --non-interactive
```

Expected: fails with missing database URL guidance.

3. Validate sync wrapper:

```bash
datachat train --mode sync --datapoints-dir datapoints/examples/fintech_bank
```

Expected: same output/behavior as `datachat dp sync` for that directory.

## UI

1. Open `/databases`.
2. Confirm **Quick Start (Phase 1.4)** card is visible.
3. Add a connection (or use an existing one).
4. Use quick-start buttons in order:
   - Connect
   - Profile
   - Generate
   - Approve
   - Sync
5. Verify step statuses update as state changes.
6. Open Chat and run a starter query.

## Non-Goals (Preserved Guardrails)

- no new SQL generation path
- no new retriever precedence/routing path
- no bypass of metadata contracts/eval gates
