# Frontend Product Requirements Document

**Version:** 1.1  
**Last Updated:** February 23, 2026

This document describes the frontend architecture, current state, and roadmap for the DataChat web UI.

For operator/end-user instructions, see `docs/UI_HOWTO.md`.

---

## Technology Stack

| Layer | Technology |
|-------|------------|
| Framework | Next.js 14 (App Router) |
| Styling | Tailwind CSS |
| State Management | Zustand |
| API Layer | REST + WebSocket |
| Testing | Jest + React Testing Library |
| Charts | Native SVG (no external library) |

---

## Current State: What Has Been Done ✅

### Core Chat Experience

| Feature | Description | Status |
|---------|-------------|--------|
| Chat Interface | Full-height chat with message list and input | ✅ Implemented |
| Real-time Streaming | WebSocket integration for agent updates | ✅ Implemented |
| Message Display | User/assistant messages with formatting | ✅ Implemented |
| SQL Code Blocks | Syntax display with copy button | ✅ Implemented |
| Data Tables | Result tables with expand/collapse | ✅ Implemented |
| Table Pagination | Default 10 rows/page with navigation controls and user-settable page size | ✅ Implemented |
| Visualizations | Bar, line, scatter, pie charts (SVG) | ✅ Implemented |
| Clarifying Questions | Interactive question prompts | ✅ Implemented |
| Multi-Question Support | Sub-answers with Q1/Q2 selector | ✅ Implemented |
| Conversation Persistence | Backend-synced snapshots with localStorage fallback | ✅ Implemented |
| Error Recovery | Retry button with error categorization | ✅ Implemented |
| Conversation History Sidebar | Resume prior local sessions | ✅ Implemented |
| Schema Explorer Sidebar | Browse tables/columns with search | ✅ Implemented |
| Metadata Explorer Sidebar | Switchable schema/metadata view to inspect generated and managed DataPoints | ✅ Implemented |
| Query Templates | Quick-action buttons for common prompts | ✅ Implemented |
| SQL Editor Mode | Edit and run SQL drafts from composer or generated responses | ✅ Implemented |
| SQL Mode Visualization Inference | Direct SQL responses infer chart type from result shape | ✅ Implemented |
| Theme Override | Light, dark, and system theme settings | ✅ Implemented |
| Chart Interaction | Tooltips, zoom controls, legend toggles | ✅ Implemented |
| Chart Configuration | Per-chart axis + display settings panel | ✅ Implemented |
| Accessibility Labels | ARIA labels, dialog semantics, live regions | ✅ Implemented |
| Keyboard Navigation | Tabs + global shortcuts + modal focus handling | ✅ Implemented |
| Result Export Actions | CSV/JSON downloads and markdown table copy | ✅ Implemented |
| Share Links | Copy deep link that restores a shared query result | ✅ Implemented |
| Answer Feedback | Helpful / not helpful feedback actions on responses | ✅ Implemented |
| Issue Reporting | Structured issue reports from response cards | ✅ Implemented |
| Improvement Suggestions | Structured improvement suggestions from response cards | ✅ Implemented |

### Database Management

| Feature | Description | Status |
|---------|-------------|--------|
| Connection CRUD | Add, edit, delete database connections | ✅ Implemented |
| Connection Selector | Dropdown to switch databases | ✅ Implemented |
| Profiling Workflow | Profile database with progress tracking | ✅ Implemented |
| DataPoint Approval | Review pending DataPoints | ✅ Implemented |
| Bulk Approve | Approve all pending DataPoints | ✅ Implemented |
| Managed DataPoint Editor | Load/create/update/delete managed DataPoint JSON (including Query DataPoints) | ✅ Implemented |

### Observability

| Feature | Description | Status |
|---------|-------------|--------|
| Agent Status | Real-time agent progress display | ✅ Implemented |
| Agent Timing Breakdown | Per-agent latency metrics | ✅ Implemented |
| Thinking Notes | Live reasoning stream | ✅ Implemented |
| LLM Call Counter | Track LLM usage per query | ✅ Implemented |
| Decision Trace | Query routing decisions | ✅ Implemented |

### Tool Integration

| Feature | Description | Status |
|---------|-------------|--------|
| Tool Approval Modal | Approve/reject tool executions | ✅ Implemented |
| Cost Estimates | Show expected LLM calls for tools | ✅ Implemented |

### Settings

| Feature | Description | Status |
|---------|-------------|--------|
| Result Layout Mode | Stacked vs tabbed view | ✅ Implemented |
| Agent Timing Toggle | Show/hide timing breakdown | ✅ Implemented |
| Live Reasoning Toggle | Show/hide thinking notes | ✅ Implemented |
| Simple SQL Synthesis | Toggle for simple SQL responses | ✅ Implemented |

---

## Needs Improvement ⚠️

### P2: Discovery Friction

| Issue | Impact | Recommendation |
|-------|--------|----------------|
| ✅ **Schema Explorer Added** | Users can inspect tables/columns directly | Collapsible schema browser sidebar with search |
| ✅ **Conversation History Added** | Users can resume past sessions across reloads/devices (shared backend) | Collapsible conversation list sidebar with search + backend persistence |
| ✅ **Query Templates Added** | Faster repeated workflows | Quick-action buttons for common query patterns |

### P3: Visualization Polish

| Issue | Impact | Recommendation |
|-------|--------|----------------|
| ✅ **Chart Interaction Added** | Users can inspect datapoints and control visual density | Tooltips + zoom + legend toggles across chart types |
| ✅ **Chart Configuration Added** | Users can adjust chart behavior without re-querying | Per-chart settings panel (axes, max points/slices, grid/legend) |

### P4: Accessibility

| Issue | Impact | Recommendation |
|-------|--------|----------------|
| ✅ **ARIA Coverage Expanded** | Better screen reader support across chat/sidebar/modal/chart surfaces | Region labels, control labels, dialog roles, status/live semantics |
| ✅ **Keyboard Navigation Added** | Faster non-pointer workflows | Tab keyboard navigation + global shortcuts + focus restoration |

**Implemented keyboard shortcuts**

- `Ctrl/Cmd + K`: Focus chat input
- `Ctrl/Cmd + H`: Toggle conversation history sidebar
- `Ctrl/Cmd + /`: Open/close shortcut reference modal
- `/`: Focus chat input (when not typing in an input)
- `Esc`: Close open modal and restore chat input focus

---

## Recently Delivered ✅

### P1: Essential Features

| Feature | Description | Effort |
|---------|-------------|--------|
| ✅ **Conversation Sidebar + Search** | List of past conversations with local search/filter | Delivered |
| ✅ **Schema Browser** | Interactive table/column explorer with search + use-in-query actions | Delivered |

### P2: Productivity Features

| Feature | Description | Effort |
|---------|-------------|--------|
| ✅ **Query Templates** | Pre-defined patterns (Top N, trends, breakdown) | Delivered |
| ✅ **SQL Editor Mode** | Edit SQL before execution and run direct read-only SQL requests | Delivered |
| ✅ **Keyboard Shortcuts** | Cmd/Ctrl+K, Cmd/Ctrl+H, Cmd/Ctrl+/, Esc | Delivered |
| ✅ **Dark Mode Toggle** | Manual light/dark/system theme override in Settings | Delivered |

### P3: Export & Sharing

| Feature | Description | Effort |
|---------|-------------|--------|
| ✅ **Export CSV** | Download result data | Delivered |
| ✅ **Export JSON** | JSON format download | Delivered |
| ✅ **Export Markdown** | Copy table as markdown | Delivered |
| ✅ **Share Link** | Deep link to query result | Delivered |

### P4: Feedback Loop

| Feature | Description | Effort |
|---------|-------------|--------|
| ✅ **Answer Feedback** | Thumbs up/down on responses | Delivered |
| ✅ **Issue Reporting** | Report problems with context | Delivered |
| ✅ **Improvement Suggestions** | UI to suggest DataPoint/retrieval improvements | Delivered |

---

## Backlog ➕

### P5: Retrieval Evaluation & Training Workbench (Finance-First)

| Feature | Description | Priority | Status |
|---------|-------------|----------|--------|
| Retrieval-Only Run Mode | Submit a question and execute retrieval only (no SQL generation/answer synthesis) | High | ⏳ Planned |
| Retrieval Context Inspector | Show vector/graph candidates, rerank scores, inclusion/exclusion reasons, and final selected context | High | ⏳ Planned |
| Relevance Labeling UI | Mark retrieved items as relevant / irrelevant / missing and attach short reviewer notes | High | ⏳ Planned |
| DataPoint Training Suggestions | Generate draft metadata/query-template improvements from reviewer feedback for approval | High | ⏳ Planned |
| Knowledge Graph Coverage View | Visualize join paths, orphan entities, and weakly connected tables for a given workflow question | Medium | ⏳ Planned |
| Prompt-Pack Retrieval Evaluation Runner | Run a saved question pack and compute retrieval quality metrics per prompt and overall | Medium | ⏳ Planned |
| Change Impact Preview | Compare retrieval results before/after datapoint edits and show expected quality movement | Medium | ⏳ Planned |
| Retrieval Governance Log | Track who labeled what, approved which datapoint change, and resulting metric movement | Medium | ⏳ Planned |

**Retrieval Workbench Scope (v1)**

- Retrieval mode selector: `full pipeline` vs `retrieval only`.
- Stage-by-stage retrieval trace: vector candidates, graph expansion, rerank shortlist, final context set.
- Reviewer labeling: relevant, irrelevant, missing context, low-confidence context.
- Suggested edits panel: tags/synonyms/relationships/description/query-template suggestions linked to each datapoint.
- Training queue: queued suggestions with approve/reject and reason capture.
- Exportable evaluation report for finance prompt packs.

---

## Should Remove 🗑️

### Dead Code

| Location | Issue | Action |
|----------|-------|--------|
| ✅ `loadingUx.ts` | Multiple modes removed | Consolidated to single waiting status label logic |
| ✅ Redundant job state | Multiple similar state variables removed | Consolidated async flags into a single `jobs` object |

### Technical Debt

| Issue | Impact | Action |
|-------|--------|--------|
| ✅ React Query Migration | Server-state loading no longer relies on manual refresh orchestration | Chat + Database Manager now use query/invalidation flows for connections, schema, profiling, pending/approved DataPoints, sync, and generation jobs |
| ✅ Error boundaries missing | Crashes killed whole app | Added app-level error boundary with retry/back-to-chat recovery |
| ✅ Inline chart rendering | Hard to maintain | Extracted to dedicated `visualizations/` components |

---

## Architecture Recommendations

### State Management

Current: Zustand (UI/session state) + React Query (server state, cache, polling, invalidation)

**Status: Implemented in chat and database manager flows**

```typescript
// Adopted server-state pattern
const { data: connections, isLoading, error } = useQuery({
  queryKey: ['connections'],
  queryFn: () => api.listDatabases(),
});
```

### Conversation Persistence

✅ **Implemented** - Chat sessions now use backend conversation snapshots (system DB) with localStorage fallback.

```typescript
// In chat store - currently implemented
export const useChatStore = create<ChatState>()(
  persist(
    (set, get) => ({
      messages: [],
      conversationId: null,
      // ... other state
    }),
    {
      name: 'datachat.chat.session.v1',
      storage: createJSONStorage(() => localStorage),
      partialize: (state) => ({
        messages: state.messages.slice(-50), // Keep last 50 messages
        conversationId: state.conversationId,
        sessionSummary: state.sessionSummary,
        sessionState: state.sessionState,
      }),
    }
  )
);
```

**What's persisted (backend + fallback local cache):**

- Last 50 messages (compacted)
- SQL queries and results (up to 50 rows)
- Visualization hints and metadata
- Sources and evidence
- Agent timing metrics
- Sub-answers (up to 5)
- Conversation ID and session state
- Snapshot timestamps for cross-device ordering and resume

### Error Recovery

✅ **Implemented** - Retry button with error categorization for failed queries.

**Error Categories:**

| Category | Icon | Triggers | Suggestion |
|----------|------|----------|------------|
| Network | Wifi | connection, econnrefused, enotfound, fetch failed | Check internet connection |
| Timeout | Clock | timeout, timed out, deadline exceeded | Simplify query |
| Validation | AlertTriangle | invalid, syntax, required | Check input |
| Database | Database | sql, table, column, schema, query | Rephrase query |
| Unknown | AlertCircle | All other errors | Try again |

**Features:**

- Retry button re-populates input with failed query
- Attempt counter shows retry count
- Contextual suggestions based on error type
- Dismiss button to clear error state
- Error state stored for retry functionality

**Implementation:**

```typescript
const categorizeError = (errorMessage: string) => {
  const lower = errorMessage.toLowerCase();
  if (lower.includes("network") || lower.includes("connection")) {
    return "network";
  }
  if (lower.includes("timeout")) {
    return "timeout";
  }
  // ... more categories
  return "unknown";
};
```

### Component Extraction

| Item | Scope | Status |
|------|-------|--------|
| ✅ Visualization extraction | `Message.tsx` chart rendering moved into dedicated components | Done |
| ✅ Chat sidebar extraction | Conversation history + schema explorer moved to dedicated components | Done |
| Remaining extraction | Break down more `Message.tsx` sections (actions/feedback blocks) | Pending |

Implemented visualization structure:

```
frontend/src/components/visualizations/
├── BarChart.tsx
├── LineChart.tsx
├── ScatterChart.tsx
├── PieChart.tsx
├── ChartContainer.tsx
└── types.ts
```

---

## Roadmap

### Completed

| Item | Priority | Status |
|------|----------|--------|
| Conversation persistence (localStorage) | P1 | ✅ Done |
| Table pagination (default 10 + user-settable) | P1 | ✅ Done |
| Retry button + categorized error recovery | P1 | ✅ Done |
| Schema browser sidebar | P1 | ✅ Done |
| Conversation history sidebar + search | P2 | ✅ Done |
| Query templates | P2 | ✅ Done |
| Keyboard shortcuts + focus management | P2 | ✅ Done |
| Light/dark/system theme toggle | P2 | ✅ Done |
| Chart tooltips, zoom, legend toggles | P3 | ✅ Done |
| Chart settings panel (per chart type) | P3 | ✅ Done |
| Export JSON / markdown (CSV existed) | P3 | ✅ Done |
| Answer feedback / issue reports / suggestions | P4 | ✅ Done |
| Chart component extraction (`visualizations/`) | P3 | ✅ Done |
| App-level error boundary | P3 | ✅ Done |

### Remaining

| Item | Priority | Status | Notes |
|------|----------|--------|-------|
| Consolidate redundant job state into a single `jobs` object | Medium | ✅ Done | `DatabaseManager` async state now uses unified `jobs` object |
| Backend conversation persistence (cross-device/session) | Medium | ✅ Done | `/api/v1/conversations` backed by system DB with local fallback |
| Continue component extraction beyond charts | Low | ✅ Done (phase 1) | Chat sidebars extracted; additional `Message` extraction remains optional |
| Validate and improve schema discovery-time KPI | Low | ✅ Done | Added `schema_loaded` + first-interaction telemetry events |
| Retrieval Evaluation/Training page (`retrieval-only` mode) | High | ⏳ Planned | Dedicated page to evaluate RAG/DataPoint/graph retrieval without running full answer pipeline |
| Retrieval trace inspector (vector + graph + rerank + final context) | High | ⏳ Planned | Needed for debugging retrieval misses and false clarifications |
| Relevance feedback + datapoint training queue | High | ⏳ Planned | User feedback converts to governed datapoint change proposals |
| Prompt-pack retrieval benchmarking UI + report export | Medium | ⏳ Planned | Enables repeatable retrieval quality testing before releases |
| Retrieval quality gates in manual test workflow | Medium | ⏳ Planned | Precision/recall/source coverage gates alongside answer-quality checks |

---

## UI/UX Specifications

### Schema Browser

```
┌─────────────────────────────────────────────────────────────┐
│ [Schema Browser ▼]                              [Collapse] │
├─────────────────────────────────────────────────────────────┤
│ 📊 fact_sales                                    1.2M rows │
│    ├── id (BIGINT) - Surrogate key                         │
│    ├── customer_id (BIGINT) - FK to dim_customer           │
│    ├── amount (DECIMAL) - Transaction value                │
│    └── transaction_time (TIMESTAMP) - When occurred         │
│                                                             │
│ 📊 dim_customer                                  50K rows   │
│    ├── customer_id (BIGINT) - PK                           │
│    ├── name (VARCHAR) - Full name                          │
│    └── segment (VARCHAR) - Customer segment                │
│                                                             │
│ 📈 metric_revenue                                          │
│    Calculation: SUM(amount) WHERE status='completed'       │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Conversation Sidebar

```
┌─────────────────────────────────────────┐
│ Conversations              [+ New Chat] │
├─────────────────────────────────────────┤
│ 📊 Sales analysis yesterday             │
│    3 queries · 2 min ago                │
│                                         │
│ 📈 Revenue trends last quarter          │
│    5 queries · 2 hours ago              │
│                                         │
│ 🔍 Customer lookup                      │
│    2 queries · yesterday                │
│                                         │
└─────────────────────────────────────────┘
```

### Keyboard Shortcuts

| Shortcut | Action | Context |
|----------|--------|---------|
| `Cmd+K` / `Ctrl+K` | Focus query input | Global |
| `Cmd+H` / `Ctrl+H` | Toggle history sidebar | Global |
| `Cmd+/` / `Ctrl+/` | Toggle schema browser | Global |
| `Cmd+Enter` | Send message | Input focused |
| `Esc` | Cancel streaming / close modal | Contextual |
| `?` | Show keyboard shortcuts | Global |

---

## Metrics & Success Criteria

| Metric | Before | After | Status |
|--------|--------|-------|--------|
| Time to first query | ~30s (setup) | <10s (with saved connection) | ✅ |
| Query recovery rate | 0% (no retry) | 80% (with retry button) | ✅ Implemented |
| Session continuation | 0% (no persistence) | 60% (with localStorage) | ✅ Implemented |
| Schema discovery time | Ask → Wait → Answer | Browse sidebar → Instant + event telemetry (`schema_loaded`, `first_schema_interaction`) | ✅ Instrumented |
| Retrieval precision@10 (finance prompt pack) | Not measured | ≥ 0.80 | ⏳ Planned |
| Retrieval recall@20 for must-win prompts | Not measured | ≥ 0.95 | ⏳ Planned |
| Clarification rate due to retrieval misses | Not measured | ≤ 10% | ⏳ Planned |
| Datapoint training turnaround (feedback to approved change) | Ad hoc | < 2 business days | ⏳ Planned |

---

## Appendix: File Structure

```
frontend/src/
├── app/
│   ├── page.tsx              # Main chat page
│   ├── layout.tsx            # Root layout
│   ├── settings/page.tsx     # Settings page
│   └── databases/page.tsx    # Database manager
├── components/
│   ├── chat/
│   │   ├── ChatInterface.tsx # Main chat component
│   │   ├── ConversationHistorySidebar.tsx
│   │   ├── SchemaExplorerSidebar.tsx
│   │   ├── Message.tsx       # Message display
│   │   ├── loadingUx.ts      # Loading states
│   │   ├── AgentStatus.tsx   # Agent progress
│   │   └── chatTypes.ts
│   ├── system/
│   │   ├── DatabaseManager.tsx
│   │   └── SystemSetup.tsx
│   ├── agents/
│   │   └── AgentStatus.tsx
│   └── ui/
│       ├── button.tsx
│       ├── input.tsx
│       └── card.tsx
├── lib/
│   ├── api.ts                # REST + WebSocket client
│   ├── stores/chat.ts        # Zustand store
│   ├── settings.ts           # User preferences
│   └── utils.ts              # Utilities
└── test/
    └── setup.ts              # Test configuration
```

---

*This document should be updated as features are implemented and requirements evolve.*
