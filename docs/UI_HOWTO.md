# DataChat UI How-To Guide

**Last Updated:** February 20, 2026

This guide explains how to use the web UI features that are implemented today, including chat workflows and the database/DataPoint management flows.

---

## 1. Start and Open the App

1. Start backend and frontend.
2. Open the app in your browser (usually `http://localhost:3000`).
3. Confirm the status chip in the top bar shows `Ready` or `Streaming`.

---

## 2. First-Time Setup (if prompted)

If the system is not initialized, the chat page shows a setup card.

1. Enter your target `DATABASE_URL`.
2. Optionally enter a system database URL.
3. Submit setup.
4. After setup, go to **Manage DataPoints** to profile/generate/sync.

---

## 3. Navigate Main Pages

- `/` : Chat workspace
- `/databases` : Database and DataPoint management
- `/settings` : UI behavior and theme settings

---

## 4. Chat Header Controls

In the chat header you can:

1. Select **Target database**.
2. Select **Workflow mode** (`Auto` or `Finance Brief v1`).
3. Start a **New** conversation.
4. Open **Shortcuts** reference.
5. Open **Settings**.
6. Open **Manage DataPoints**.
7. Clear current chat.
8. Toggle left/right sidebars.

---

## 5. Select a Target Database

1. Use the **Target database** dropdown in the header.
2. Pick the connection you want to query.
3. If you switch to a different database, the next message starts a fresh context.

Tip: if responses look unrelated, verify this selector first.

---

## 6. Ask Questions (Natural Language Mode)

1. Keep composer mode on **Ask**.
2. Type a question in the input box.
3. Press `Enter` or click **Send**.

Examples:
- `List all available tables`
- `Show columns for grocery_sales_transactions`
- `Show top 10 products by sales`

Workflow mode tip:
- Use **Workflow: Finance Brief v1** for finance wedge prompts when you want structured workflow artifacts (`summary`, `metrics`, `drivers`, `caveats`, `sources`, `follow_ups`) consistently.
- Use **Workflow: Auto** for general-purpose chat behavior.

---

## 7. Use SQL Editor Mode (Direct SQL)

1. Switch composer mode to **SQL Editor**.
2. Enter a SQL query.
3. Press `Ctrl/Cmd + Enter` or click **Run SQL**.

Important behavior:
- SQL mode executes SQL directly and deterministically.
- Only read-only SQL is accepted (`SELECT`, `WITH`, `SHOW`, `DESCRIBE`, `EXPLAIN`).
- Visualization hints are inferred from SQL result shape, so chart-ready SQL still opens a chart view.

Example:
- `SELECT * FROM public.grocery_inventory_snapshots LIMIT 10`

---

## 8. Use Query Templates

Templates appear above the composer and quickly fill common prompts:
- `List Tables`
- `Show Columns`
- `Sample 100 Rows`
- `Count Rows`
- `Top 10`
- `Trend`
- `Category Breakdown`

How to use:
1. Click a template chip.
2. Edit the prompt if needed.
3. Send it.

---

## 9. Use Conversation History Sidebar

On the left sidebar:
1. View saved local conversations.
2. Search by title/database using the search box.
3. Click a conversation to restore it.
4. Delete old entries with the trash action.

Storage note:
- Primary persistence: backend `ui_conversations` table (system database), so sessions can be resumed across browser reloads/devices that point to the same backend.
- Fallback: browser local storage cache (`datachat.conversation.history.v1`) if backend persistence is unavailable.

---

## 10. Use Schema Explorer Sidebar

On the right sidebar:
1. Search tables/columns.
2. Expand a table to inspect columns, types, and PK/FK tags.
3. Click **Use In Query**:
   - In Ask mode: inserts a natural-language prompt.
   - In SQL mode: inserts a SQL starter query.

---

## 11. Work with Result Tabs

Assistant responses support:

- **Answer** tab (narrative response)
- **SQL** tab (generated/executed SQL, with copy/edit actions)
- **Table** tab (tabular results)
- **Visualization** tab (chart rendering)
- **Sources** tab (sources/evidence)
- **Timing** tab (latency metrics, if available)

Multi-question responses also include `Q1/Q2/...` selectors.

---

## 12. Work with Result Tables and Pagination

When query results include tabular data:
1. Open the **Results** section.
2. Use **Previous** / **Next** to move between pages.
3. Use **Rows per page** to set any positive page size.

Current default:
- `10` rows per page.

---

## 13. Use Visualization Features

For chartable results:
1. Open the **Visualization** tab.
2. Hover/focus chart elements to see tooltip details.
3. Use **Chart settings** to configure:
   - axis columns
   - max points/slices
   - zoom
   - grid/legend visibility (chart-dependent)
4. Use legend toggles to hide/show series.

---

## 14. Export and Share Results

For assistant messages with table data, use message action buttons:

1. **Download CSV** for spreadsheet workflows.
2. **Download JSON** for API/data pipeline workflows.
3. **Copy Markdown** to paste a markdown table into docs/issues/PRs.
4. **Share Link** to copy a deep link containing the query result payload.

Open a copied share link to restore the shared result in chat view.

---

## 15. Feedback Loop

Each assistant response now supports feedback actions:

1. **Helpful** / **Not Helpful** for quick answer quality rating.
2. **Report Issue** to submit concrete problems (wrong table, wrong metric logic, missing context).
3. **Suggest Improvement** to capture ideas for retrieval, DataPoints, or response quality.

Storage and usage:

- Feedback is persisted to the `ui_feedback` table in the configured system database (`SYSTEM_DATABASE_URL`).
- If no system database is configured, feedback is captured in API logs as structured payloads.
- Teams can use this data to prioritize DataPoint fixes, tune prompts, and validate retrieval quality.

---

## 16. Tool Approval Modal

If a tool call requires approval:

1. Review tool name and arguments in the modal.
2. Review the cost hint.
3. Click **Approve** to continue or **Cancel** to abort.

---

## 17. Keyboard Shortcuts

- `Ctrl/Cmd + K`: Focus query input
- `Ctrl/Cmd + H`: Toggle conversation history sidebar
- `Ctrl/Cmd + /`: Open shortcuts dialog
- `/`: Focus query input (when not typing in a form field)
- `Esc`: Close active modal and restore focus

---

## 18. Database Management Page (`/databases`)

### 18.1 Quick Start Card

Use the guided sequence:

1. **Connect** database
2. **Profile** schema
3. **Generate** pending DataPoints
4. **Approve** DataPoints
5. **Sync** retrieval index

Status dots/checkmarks show step state (`done`, `ready`, `blocked`).

### 18.2 Add/Edit/Delete Connections

1. Add connection with name, URL, type, and optional description.
2. Mark default if needed.
3. Edit managed connections inline.
4. Delete unused managed connections.

Note:
- Environment-backed connection entries are protected from edit/profile flows.

### 18.3 Profile and Generate DataPoints

1. Select connection.
2. Start profiling.
3. Wait for job completion and review discovered tables.
4. Select table subset and generation depth.
5. Start DataPoint generation.

### 18.4 Review Pending DataPoints

1. Open pending list.
2. Expand an item to inspect/edit JSON draft.
3. Approve or reject individual items.
4. Use **Bulk approve** when ready.

### 18.5 Sync Retrieval Index

Run sync after approvals so retrieval uses latest approved DataPoints.

Scope options:
- `auto`
- `global`
- `database` (requires selecting a connection id)

### 18.6 Quality / Tool Actions

From management screens you can run:
- quality report tooling
- profile+generate tooling (approval gated)

### 18.7 Managed DataPoint Editor

The `/databases` page includes a managed DataPoint editor for manual authoring:

1. Enter a `datapoint_id` and click **Load** to fetch an existing managed DataPoint.
2. Edit JSON directly in the editor.
3. Use **Create New** to add a new DataPoint.
4. Use **Update Existing** to update an existing managed DataPoint by `datapoint_id`.
5. Use **Delete** to remove a managed DataPoint.

This supports Schema/Business/Process and Query DataPoint authoring flows.

---

## 19. Settings Page

Open **Settings** from the header to configure:
- Result layout (`stacked` / `tabbed`)
- Show/hide live reasoning
- Show/hide agent timing breakdown
- Simple SQL synthesis toggle
- Theme mode (`Light`, `Dark`, `System`)

---

## 20. Error Recovery

If a query fails:
1. Read the categorized error card (network, timeout, validation, database, unknown).
2. Click **Retry Query** to retry the last query.
3. If needed, adjust query scope or limit and resubmit.

---

## 21. Accessibility Notes

Implemented accessibility support includes:
- labeled regions for chat/workspace
- ARIA labels on interactive controls
- dialog semantics for modals
- keyboard navigation for tabs and modal flows
- live status regions for streaming/progress

---

## 22. Quick Validation Checklist

1. Send one natural-language question in Ask mode.
2. Run one direct SQL query in SQL Editor mode.
3. Verify a chart appears for chartable SQL-mode query results.
4. Expand a result table and change `Rows per page`.
5. Use `Download CSV`, `Download JSON`, `Copy Markdown`, and `Share Link`.
6. Use `Helpful` / `Not Helpful`, `Report Issue`, and `Suggest Improvement`.
7. Open Visualization tab and toggle chart settings.
8. Load a prior conversation from history.
9. Use `Ctrl/Cmd + H` and `Ctrl/Cmd + /`.
10. Open `/databases`, run quick start steps, approve at least one pending DataPoint, and test DataPoint Editor load/create/update.
11. Run sync and confirm status changes from running to completed.
12. Open `/settings` and switch theme + layout modes.

If all checks pass, core UI flows are healthy.
