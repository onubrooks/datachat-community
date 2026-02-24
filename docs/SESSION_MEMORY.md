# Session Memory and Chat History

DataChat now uses a hybrid short-term memory strategy for chat follow-ups:

- full turn messages (recent window) for local conversational context
- compact `session_summary` text for low-token continuity
- structured `session_state` for deterministic slot reuse (`last_goal`, table hints, clarifications)

This is used in both API/UI and CLI flows.

## Why This Exists

Sending full chat history forever increases token usage and latency. Sending no memory causes context loss on follow-ups like:

- `How many products do we have?`
- `What about stores?`

The hybrid model keeps continuity while bounding cost.

## Runtime Behavior

On each turn:

1. Client sends:
- `conversation_history` (recent chat turns)
- `session_summary` (optional compact summary from previous turn)
- `session_state` (optional structured memory from previous turn)

2. Pipeline:
- merges incoming `session_state` into current intent summary
- appends memory as system context for downstream agents
- performs contextual follow-up rewrite when appropriate (for example `what about stores`)

3. Pipeline returns updated:
- `session_summary`
- `session_state`

4. Client stores them and sends them on the next turn.

## Fields

`POST /api/v1/chat` and `WS /ws/chat` support:

- request:
  - `session_summary?: string`
  - `session_state?: object`
- response:
  - `session_summary?: string`
  - `session_state?: object`

## Current Scope

- session memory is in-client (UI local storage + CLI local files under `~/.datachat/sessions.json`)
- no durable server-side history yet
- CLI supports explicit persistence and resume via:
  - `datachat chat --session-id <id>`
  - `datachat session list`
  - `datachat session resume <id>`
  - `datachat session clear <id>`
- memory resets when chat is cleared (UI) or when CLI sessions are explicitly cleared

## Manual Testing (UI)

1. Open chat page and select one database.
2. Ask: `How many products do we have?`
3. Ask follow-up: `What about stores?`
4. Expected:
- second question should continue prior intent (counting stores), not ask generic clarification
- response metadata should still show normal SQL/context source behavior
5. Clear chat with the UI clear action.
6. Ask only: `What about stores?`
7. Expected:
- without prior context, assistant should ask for clarification or interpret as a fresh query

## Manual Testing (CLI)

1. Run:

```bash
datachat chat
```

2. Ask:
- `How many products do we have?`
- `What about stores?`

3. Expected:
- second turn should behave as continuation, not reset intent

4. Exit and restart `datachat chat`.
5. Ask:
- `What about stores?`

6. Expected:
- no prior memory should exist after restart; assistant asks clarifying question or treats as new intent.

7. Persist and resume:

```bash
datachat chat --session-id memory-test
datachat session list
datachat session resume memory-test
datachat session clear memory-test
```
