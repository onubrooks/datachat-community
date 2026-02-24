# DataChat: Finance-First Decision Workflow Platform

## Project Vision

DataChat is built as a finance-first decision workflow platform that uses AI to turn governed data and business context into decision-ready outputs. The long-term direction is a reusable AI platform for business decision makers across domains, but finance remains the execution wedge.

**Core Differentiator:** DataChat unifies database truth, business definitions, and organizational context with explicit provenance, governance, and verification loops.

## Document Role

`ARCHITECTURE.md` is the source of truth for technical design decisions and runtime architecture.

- It answers: "How does the system work?"
- It does not own delivery status sequencing.

For initiative sequencing/status/dependencies, use `docs/ROADMAP.md`.
For product intent/prioritization, use `docs/PRD.md`.
For maturity definitions, use `docs/LEVELS.md`.

---

## Accepted Direction: Dynamic Data Agent Harness

Accepted target direction:

- DataChat evolves into a **dynamic data agent harness** (not a coding agent).
- Core capability focus:
  - databases/datastores
  - business logic in docs/files
  - organizational knowledge context
- Runtime shift:
  - from fixed linear pipeline to bounded plan/act/verify/adapt loops
  - with policy classes, approvals, and replayable traces

Canonical target-design reference:

- `docs/ARCHITECTURE_DYNAMIC_DATA_AGENT.md` (accepted)

Roadmap linkage:

- `DYN-001`..`DYN-007` in `docs/ROADMAP.md`
- `FND-001`..`FND-007` in `docs/ROADMAP.md` (metadata/retrieval/authority foundation lane)

## Architecture Prioritization Contract

- The architecture may evolve broadly, but implementation priority is constrained by finance workflow outcomes.
- New platform capabilities should be shipped in slices that directly improve:
  - time-to-trusted finance answer
  - source attribution coverage
  - clarification/fallback reduction
  - auditability and replayability

## Harness Pattern Mapping

DataChat uses a hybrid harness strategy:

- **Decision-tree and self-heal controls** (pattern parity with Elysia):
  - explicit action-state traversal
  - tool availability checks
  - reason-coded retry/path-switch logic
- **Bounded operator harness controls** (pattern parity with Claude Code/Codex):
  - budgeted execution loops
  - deterministic safety gates
  - replayable and auditable traces

This does not change product wedge or scope. It upgrades runtime control quality for finance workflows.

## Pre-WDG Architecture Sequence

Before broad finance workflow defaulting, architecture implementation sequence is:

1. `DYN-001` Slice A-F control-plane rollout.
2. `WDG-001` quality-gate completion on loop-enabled prompts.
3. Remaining foundation and telemetry initiatives.

Reference:

- sequencing details and status: `docs/ROADMAP.md`
- execution checklist: `docs/specs/DYN-001.md`
- workflow quality bar: `docs/specs/WDG-001.md`

---

## Implementation Status Snapshot (February 2026)

This section is the source of truth for shipped vs planned architecture scope.

Implemented now:

- Intent gate before the main pipeline, with clarification loops and max-turn limits
- Query compiler stage before SQL generation (deterministic table/operator selection with bounded mini-LLM refinement)
  - Runtime details: `docs/QUERY_COMPILER_RUNTIME.md`
- Deterministic catalog flows for schema-shape intents (tables/columns/sample rows/row count)
- Credentials-only live schema mode as a first-class path
- Multi-database routing via `target_database` with strict override resolution
- Tooling reliability upgrades:
  - `/api/v1/tools` typed parameter schemas
  - `/api/v1/tools/execute` target-database propagation
  - runtime context injection for built-in tools
  - planner argument coercion to schema types
- DataPoint-driven retrieval and synthesis enhancements
- Metadata quality and observability foundation lane started:
  - deterministic eval suites (retrieval/qa/intent/catalog)
  - source-tier precedence and retrieval traceability hooks
  - telemetry-first tuning for clarification and fallback behavior

Planned (not yet implemented in runtime):

- Workspace index/status/search APIs and full filesystem retrieval workflows
- Runtime connectors for BigQuery and Redshift
- Governed finance authority knowledge pack (canonical finance-global metrics with source-tier policy)
- Levels 3-5 as fully productized automation tiers

Note:

- Catalog/profiling templates may exist for additional engines, but templates alone do not imply runtime connector support.

---

## Architecture Overview

### The Three-Layer Model

```txt
Layer 1: Database (structured data)
         ↓
Layer 2: Business Logic & Metrics (semantic meaning via DataPoints)
         ↓
Layer 3: Filesystem (code, docs, configs via WorkspaceDataPoints)
```

### MetadataOps Control Plane (Cross-Cutting)

This control plane governs all levels, especially before deeper Level 3-5 automation:

- Metadata contracts and linting
- Evaluation gates in CI (retrieval, qa, intent, catalog)
- Retrieval/answer provenance traces
- Runtime behavior telemetry (clarification churn, fallback reasons, low-confidence hotspots)

Design intent:

- improve metadata authoring quality first
- use observability loops to direct improvements
- avoid compensating for poor metadata with prompt-only complexity

Sequencing policy:

- MetadataOps foundation (Level 1.5) is the release gate for deeper runtime expansions.
- A thin onboarding wrapper layer (Level 1.4) may ship earlier if it does not alter retrieval/routing truth paths.
- The full deterministic simplicity package (Level 1.6) lands only after foundation KPIs are stable.

### Current Implementation vs Target State

**Important:** This document describes both current implementation and target architecture.

| Aspect | Current Implementation | Target State |
|--------|------------------------|--------------|
| DataPoint Types | JSON-based (Schema, Business, Process) | YAML-based with execution blocks |
| Knowledge Graph | Basic edges (BELONGS_TO, JOINS_WITH) | Column-level + semantic edges |
| Session Memory | last_goal, table_hints | Entity memory + temporal context |
| Levels 3-5 | Planned architecture | Executable metrics, materialization, intelligence |

### High-Level Flow (Current Implementation)

```txt
User Query (Natural Language)
    ↓
┌─────────────────────────────┐
│ QueryAnalyzerAgent          │ ← Unified routing: intent + route classification
└────────┬────────────────────┘
         ↓
┌─────────────────────────────┐
│ RouteDispatcher             │ ← Branch to sql/context/tool/end route
└────────┬────────────────────┘
         ↓
    ┌────┴────┬──────────┬──────────┐
    ↓         ↓          ↓          ↓
┌───────┐ ┌───────┐ ┌────────┐ ┌─────┐
│ SQL   │ │Context│ │ Tool   │ │ End │
│ Route │ │ Route │ │ Route  │ │Route│
└───┬───┘ └───┬───┘ └───┬────┘ └─────┘
    ↓         ↓          ↓
┌───────┐ ┌───────────┐ ┌────────┐
│Query  │ │Context    │ │Tool    │
│Compiler│ │Answer     │ │Executor│
└───┬───┘ └─────┬─────┘ └────────┘
    ↓           ↓
┌───────┐ ┌─────┴─────┐
│SQL    │ │ SQL (if   │
│Agent  │ │ needs_sql)│
└───┬───┘ └───────────┘
    ↓
┌───────┐
│Valid- │
│ator   │
└───┬───┘
    ↓
┌───────┐
│Exec-  │
│utor   │
└───────┘
    ↓
┌─────────────────────────────┐
│ Answer + Metrics            │ ← Includes decision_trace and timing telemetry
└─────────────────────────────┘
```

### Target Architecture Flow (Levels 3-5)

The following represents the target state for executable metrics and beyond:

```txt
User Query
    ↓
┌─────────────────────────────┐
│ Query Analyzer              │ ← Intent + route + DataPoint matching
└────────┬────────────────────┘
         ↓
    ┌────┴────┐
    ↓         ↓
┌───────┐ ┌───────┐
│Template│ │ Free  │
│Path    │ │ Form  │
│(L3+)   │ │ Path  │
└───┬───┘ └───┬───┘
    ↓         ↓
┌───────┐ ┌───────┐
│Execute │ │SQL Gen│
│Template│ │ + Val │
└───┬───┘ └───┬───┘
    ↓         ↓
┌─────────────────────────────┐
│ Materialization Router (L4) │ ← Route to pre-computed results if available
└────────┬────────────────────┘
         ↓
┌─────────────────────────────┐
│ Intelligence Layer (L5)     │ ← Anomaly detection, root cause analysis
└─────────────────────────────┘
```

---

## DataPoint Type System

### Current Implementation (JSON-based)

DataPoints are currently implemented as JSON-based Pydantic models with three types:

| Type | Purpose | Key Fields |
|------|---------|------------|
| `SchemaDataPoint` | Tables/views with column metadata | `table_name`, `key_columns`, `relationships` |
| `BusinessDataPoint` | Metrics and business concepts | `calculation`, `synonyms`, `related_tables` |
| `ProcessDataPoint` | ETL/scheduled processes | `schedule`, `data_freshness`, `target_tables` |

**Planned types:**
- `QueryDataPoint` - Reusable SQL templates (Level 2.5)
- `ConstraintDataPoint` - Business rules affecting WHERE clauses
- `DashboardDataPoint` - Pre-built visualizations (Level 3)

### Target Architecture (YAML-based)

The DATAPOINT_SCHEMA.md describes a YAML-based system with richer types:

| Target YAML Type | Current JSON Type | Notes |
|------------------|-------------------|-------|
| `metric` | `BusinessDataPoint` | Partial match - needs execution block |
| `dimension` | - | Planned |
| `entity` | - | Planned |
| `concept` | - | Planned |
| `query` | `QueryDataPoint` | Planned (Level 2.5) |
| - | `SchemaDataPoint` | No direct YAML equivalent |
| - | `ProcessDataPoint` | No direct YAML equivalent |

**Migration path:** The JSON types will remain the runtime format for Levels 1-2. YAML-based DataPoints with execution blocks become the target for Levels 3-5. See docs/DATAPOINT_MIGRATION.md for details.

---

## The Value Ladder (Progressive Enhancement)

Note: level boundaries and maturity status are canonical in `docs/LEVELS.md`. This section focuses on technical implications of each level.

### Level 1: Schema-Aware Querying (Zero Setup)

**What it does:**

- Connects with only target database credentials
- Uses live schema snapshots (tables + columns) for SQL generation context
- Enables immediate natural language querying without DataPoints
- Optionally adds ManagedDataPoints via profiling for higher answer quality

**Technical Implementation:**

- Live metadata path:
  - SQL agent fetches a schema snapshot from the active target database
  - Snapshot is injected into SQL generation and correction prompts
  - SQL agent derives a semantic schema digest (dimensions/measures/time columns + visualization hints) even when no DataPoints exist
- Profiling path (optional):
  - Schema profiler generates ManagedDataPoints with richer metadata
  - Generated metadata includes semantic role classification and display hints per table/metric
  - ManagedDataPoints are stored in `datapoints/managed/` and indexed for retrieval

**User Experience:**

```txt
User connects to database → DataChat can immediately query in live schema mode
"Show me top 10 customers by revenue" → Works instantly
```

**Storage Decision:** ManagedDataPoints remain YAML files in `datapoints/managed/` for version control and transparency when profiling is enabled. Live schema mode does not require persisted DataPoints.

---

### Level 2: Context-Enhanced Querying (User Adds DataPoints)

**What it does:**

- Users create Type 1 DataPoints (context only) in `datapoints/user/`
- System merges user context with ManagedDataPoints
- LLM gets both schema metadata AND business semantics

**DataPoint Type 1 Schema:**

```yaml
# datapoints/user/sales/revenue.yaml
datapoint:
  id: revenue
  name: Revenue
  type: concept
  
  definition: |
    Total value of completed sales transactions, excluding refunds and taxes.
    Finance team's canonical definition as of 2024-Q4.
  
  owner: finance-team
  tags: [sales, finance, critical]
  
  data_sources:
    - table: transactions
      columns: [amount, status, transaction_type]
      filters:
        - "status = 'completed'"
        - "transaction_type = 'sale'"
  
  related_concepts:
    - net_revenue
    - gross_margin
  
  documentation_url: https://wiki.company.com/metrics/revenue
```

**Context Merging:**

```python
# System combines ManagedDataPoint + UserDataPoint
merged_context = {
    "schema": managed_dp.schema,        # From Level 1 profiling
    "business_rules": user_dp.filters,  # From Level 2 user context
    "owner": user_dp.owner,
    "related_concepts": user_dp.related_concepts
}
```

**User Experience:**

```txt
User asks: "What was revenue last month?"
→ System knows: revenue = sum(transactions.amount) where status='completed'
→ System knows: owner is finance-team, relates to net_revenue
→ Generates more accurate SQL with proper business logic
```

---

### Level 3: Executable Metrics (SQL Templates)

**What it does:**

- Users upgrade DataPoints to Type 2 (add execution block)
- System uses pre-defined SQL templates instead of LLM generation
- Ensures consistency and performance for known metrics

**DataPoint Type 2 Schema:**

```yaml
# datapoints/user/sales/revenue.yaml
datapoint:
  id: revenue
  name: Revenue
  type: metric
  
  definition: |
    Total value of completed sales transactions, excluding refunds and taxes.
  
  owner: finance-team
  tags: [sales, finance, critical]
  
  # NEW: Execution specification
  execution:
    sql_template: |
      SELECT 
        SUM(amount) as value,
        DATE_TRUNC('{granularity}', transaction_time) as period
      FROM transactions
      WHERE status = 'completed'
        AND transaction_type = 'sale'
        AND transaction_time >= {start_time}
        AND transaction_time < {end_time}
      GROUP BY period
      ORDER BY period
    
    parameters:
      granularity:
        type: enum
        values: [day, week, month, quarter, year]
        default: day
      
      start_time:
        type: timestamp
        required: true
      
      end_time:
        type: timestamp
        required: true
    
    backend_variants:
      clickhouse: |
        SELECT 
          SUM(amount) as value,
          toStartOf{granularity}(transaction_time) as period
        FROM transactions
        WHERE status = 'completed' AND {filters}
        GROUP BY period
      
      bigquery: |
        SELECT 
          SUM(amount) as value,
          TIMESTAMP_TRUNC(transaction_time, {granularity}) as period
        FROM transactions
        WHERE status = 'completed' AND {filters}
        GROUP BY period
```

**Query Execution Flow:**

```txt
User: "What was revenue last month?"
    ↓
ContextAgent: Find DataPoint 'revenue'
    ↓
Check: Does it have execution block?
    ↓
YES → Use template (fast, consistent)
    ↓
Template compiler: Substitute parameters
    ↓
Execute query → Return results
```

**User Experience:**

- 3-5x faster (no LLM generation)
- 100% consistent (same SQL every time)
- Auditable (Git tracks metric definition changes)

---

### Level 4: Performance Layer (Adaptive Materialization)

**What it does:**

- System monitors query patterns
- Automatically creates materialized views for frequent queries
- Routes queries to pre-aggregated tables transparently

**DataPoint Type 3 Schema (Materialization Hints):**

```yaml
datapoint:
  id: daily_revenue
  type: metric
  
  execution:
    sql_template: {...}
    
    # NEW: Materialization configuration
    materialization:
      strategy: adaptive  # System decides based on usage patterns
      
      # OR manual configuration:
      # strategy: manual
      # granularity: day
      # refresh_interval: 1 hour
      # partition_by: DATE(transaction_time)
      # incremental: true
      # lookback_window: 7 days
  
  # System tracks usage automatically
  _analytics:
    queries_per_day: 47
    avg_execution_time_ms: 3200
    p95_execution_time_ms: 5100
    last_7_days_trend: increasing
    cache_hit_rate: 0.15
  
  # System generates recommendation
  _recommendation:
    should_materialize: true
    reason: "High query frequency (47/day) + expensive computation (3.2s avg)"
    estimated_speedup: "20x (3.2s → 160ms)"
    estimated_cost: "50MB storage, 1hr refresh"
```

**Materialization Manager:**

```python
class MaterializationManager:
    """
    Monitors query patterns and manages materialized views.
    Inspired by Cube.dev but adaptive and learning-based.
    """
    
    def analyze_query_patterns(self) -> List[MaterializationCandidate]:
        """
        Analyze query logs to identify materialization candidates.
        
        Criteria:
        - Query frequency > 10/day
        - Avg execution time > 1s
        - Result set is bounded (not full table scan)
        - Query pattern is consistent (parameterized, not ad-hoc)
        """
        pass
    
    def create_materialization(self, datapoint_id: str, backend: str):
        """
        Create materialized view/table for DataPoint.
        
        Backend-specific implementations:
        - ClickHouse: MATERIALIZED VIEW with SummingMergeTree
        - PostgreSQL: MATERIALIZED VIEW with REFRESH strategy
        - BigQuery: Partitioned table with scheduled query
        """
        pass
    
    def refresh_materialization(self, datapoint_id: str, mode: str):
        """
        Refresh materialized data.
        
        Modes:
        - incremental: Only refresh recent data (uses lookback_window)
        - full: Drop and recreate (for schema changes)
        - smart: Incremental if possible, full if needed
        """
        pass
```

**User Experience:**

```txt
Week 1: User queries "daily revenue" frequently
Week 2: System detects pattern, shows suggestion in UI
Week 3: User enables materialization (one click)
Week 4: Query time drops from 3.2s → 160ms (20x faster)
```

**CLI User Experience:**

```bash
# System notifies via CLI
$ datachat query "daily revenue last 30 days"
⚡ Tip: This query is slow (3.2s avg). Enable materialization for 20x speedup?
   Run: datachat materialize enable daily_revenue

# User enables
$ datachat materialize enable daily_revenue
Creating materialized view... ✓
Estimated speedup: 20x
Storage cost: ~50MB

# Subsequent queries are fast
$ datachat query "daily revenue last 30 days"
Result: ... (executed in 160ms, from materialized view)
```

---

### Level 5: Intelligence Layer (Knowledge Graph + AI Diagnostics)

**What it does:**

- Builds knowledge graph from DataPoint relationships
- Monitors for anomalies automatically
- Provides root cause analysis via graph traversal
- Enables auto-remediation workflows

**DataPoint Type 4 Schema (Full Intelligence):**

```yaml
datapoint:
  id: revenue
  type: metric
  
  execution: {...}
  materialization: {...}
  
  # NEW: Business intelligence configuration
  intelligence:
    sla:
      target: 1000000  # $1M daily
      warning_threshold: 0.90  # Alert at 10% below target
      critical_threshold: 0.80  # Escalate at 20% below target
      alert_channel: "#finance-alerts"
      escalation_owner: "finance-director@company.com"
    
    anomaly_detection:
      enabled: true
      algorithm: ensemble  # SPC + Prophet + IsolationForest
      sensitivity: 0.05  # Alert on 5% deviation from forecast
      baseline_period_days: 30
      
    auto_remediation:
      - condition: "value < sla.warning_threshold AND pricing_coverage < 0.95"
        action: trigger_dag
        dag_id: refresh_pricing_data
        reason: "Low revenue often correlates with stale pricing data"
      
      - condition: "value < sla.critical_threshold"
        action: page_oncall
        service: finance-ops
        reason: "Critical revenue SLA breach"
  
  # NEW: Knowledge graph relationships
  relationships:
    depends_on:
      - id: transaction_count
        type: input_metric
        impact_coefficient: 0.6  # 60% of revenue variance from txn count
        
      - id: average_order_value
        type: input_metric
        impact_coefficient: 0.35  # 35% from AOV
      
      - id: pricing_coverage
        type: data_quality
        impact_coefficient: 0.05  # 5% from pricing issues
    
    impacts:
      - id: gross_margin
        type: downstream_metric
        relationship: "Revenue is primary input to gross margin calculation"
      
      - id: revenue_per_customer
        type: derived_metric
        relationship: "Used to compute per-customer metrics"
    
    related_systems:
      - name: payment_gateway
        type: external_dependency
        criticality: high
        monitoring_url: "https://status.stripe.com"
      
      - name: pricing_service
        type: internal_service
        criticality: medium
        health_check: "http://pricing-svc/health"
```

**Knowledge Graph Schema (Neo4j):**

```cypher
// DataPoint node
(:DataPoint {
  id: "revenue",
  name: "Revenue",
  type: "metric",
  owner: "finance-team",
  sla_target: 1000000
})

// Relationships
(:DataPoint {id: "revenue"})
  -[:DEPENDS_ON {impact: 0.6, type: "input"}]->
(:DataPoint {id: "transaction_count"})

(:DataPoint {id: "revenue"})
  -[:IMPACTS {relationship: "primary input"}]->
(:DataPoint {id: "gross_margin"})

(:DataPoint {id: "revenue"})
  -[:USES_SYSTEM {criticality: "high"}]->
(:System {name: "payment_gateway"})
```

**Root Cause Analysis Example:**

```
User: "Why did revenue drop 15% yesterday?"
    ↓
Investigation Pipeline:
  1. Query revenue history → Confirm 15% drop
  2. Load DataPoint → Get dependency graph
  3. Query all dependencies:
     - transaction_count: DOWN 20% ← ROOT CAUSE
     - average_order_value: UP 2% (normal)
     - pricing_coverage: 98% (normal)
  4. Traverse transaction_count dependencies:
     - payment_gateway: 3 outages yesterday (2hrs downtime) ← SMOKING GUN
  5. Cross-reference system health:
     - Check payment_gateway monitoring
     - Find incident #4521
    ↓
Response:
"Revenue dropped 15% because the payment gateway (Stripe) had 3 outages 
yesterday totaling 2 hours of downtime. This caused a 20% drop in 
transaction count, which accounts for your revenue decline.

Incident: #4521 (see https://status.stripe.com/incidents/4521)
Impact: ~$150K lost revenue
Status: Resolved as of 11:30 AM today

Related metrics also affected:
• Gross margin: Down 14%
• Revenue per customer: Up 6% (customers who could pay spent more)"
```

**User Experience:**

- AI proactively monitors metrics
- Alerts when anomalies detected
- Explains root causes without human investigation
- Can auto-trigger remediation (with approval)

---

## APEX: Semantic Layer Engine

**APEX = Agentic Performance & Execution Layer**

APEX is the semantic layer engine that powers Levels 3-5. It's implemented as a modular component within DataChat but architected for potential extraction as a standalone library.

### APEX Architecture

```
datachat/
├── apex/                    # Semantic layer engine (modular)
│   ├── __init__.py
│   ├── engine.py           # Main APEX engine
│   ├── compiler.py         # SQL template compiler
│   ├── materializer.py     # Materialization manager
│   ├── optimizer.py        # Query optimizer/router
│   ├── monitor.py          # Query pattern analyzer
│   └── intelligence/       # Level 5 features
│       ├── anomaly.py      # Anomaly detection
│       ├── graph.py        # Knowledge graph manager
│       └── diagnostics.py  # Root cause analysis
```

**Design Principles:**

1. **Modular:** APEX can be imported independently of DataChat's NLQ layer
2. **Backend-agnostic:** Works with any SQL database
3. **Configuration-driven:** All behavior defined via DataPoint YAML
4. **Observable:** Full instrumentation and audit logging

**Future Extraction Path:**

```python
# Today (embedded in DataChat)
from datachat.apex import APEXEngine

# Future (standalone library)
from apex import APEXEngine
```

---

## WorkspaceDataPoints: Filesystem Integration

WorkspaceDataPoints enable DataChat to understand code, documentation, and configurations.

### WorkspaceDataPoint Schema

```yaml
# Auto-generated from filesystem indexing
workspace_datapoint:
  file_path: models/metrics/customer_lifetime_value.sql
  file_type: sql
  language: sql
  
  # Extracted metadata
  symbols:
    - customer_lifetime_value  # Model name
    - customers               # Referenced table
    - purchases              # Referenced table
  
  docstrings: |
    Computes lifetime value per customer by summing all purchase amounts.
    Materialized incrementally, refreshed daily.
  
  domain_tags:
    - metrics
    - finance
    - customer_analytics
  
  extracted_entities:
    tables: [customers, purchases]
    models: [customer_lifetime_value]
    config:
      materialization: incremental
      partition_by: customer_id
      cluster_by: [signup_date]
  
  # Change tracking
  last_modified: 2026-01-15T08:30:00Z
  checksum: abc123def456...
  git_commit: a1b2c3d4
  
  # Links to DataPoints
  implements_datapoint: customer_lifetime_value
  depends_on_datapoints:
    - stg_customers
    - stg_purchases
```

### Filesystem Watcher

```python
class WorkspaceWatcher:
    """
    Monitors filesystem for changes and updates WorkspaceDataPoints incrementally.
    """
    
    def __init__(self, workspace_path: str, debounce_ms: int = 500):
        """
        Args:
            workspace_path: Root directory to watch
            debounce_ms: Delay before processing file changes (batches rapid edits)
        """
        self.workspace_path = workspace_path
        self.debounce_ms = debounce_ms
    
    def watch(self):
        """
        Start watching filesystem for changes.
        
        On file change:
        1. Debounce (wait for rapid edits to settle)
        2. Compute checksum
        3. If changed, re-index file → update WorkspaceDataPoint
        4. Update knowledge graph relationships
        """
        pass
```

### User Configuration

```yaml
# datachat_config.yaml
workspace:
  enabled: true
  
  paths:
    # Narrow scope (recommended for v1.0)
    - path: ./dbt/models
      type: dbt_models
      recursive: true
    
    - path: ./sql
      type: sql_scripts
      recursive: true
    
    # Medium scope (future)
    # - path: ./airflow/dags
    #   type: airflow_dags
    
    # Broad scope (user opt-in)
    # - path: ./src
    #   type: application_code
  
  exclude_patterns:
    - "**/.git/**"
    - "**/.env"
    - "**/secrets/**"
    - "**/__pycache__/**"
  
  index_schedule: "0 2 * * *"  # Re-index daily at 2 AM
```

**Scope Decision:** Start with **narrow scope** (data/metrics code only) for v1.0. Allow users to expand to medium/broad via configuration. Broad scope requires opt-in due to performance/relevance tradeoff.

---

## Tool System: Safe, Auditable, Extensible

### Tool Categories & Policies

**Tier 1: Read-Only Database Tools (Always Enabled)**

```yaml
tools:
  query_database:
    enabled: true
    read_only: true
    max_rows: 10000
    max_execution_time_seconds: 30
    allowed_operations: [SELECT, EXPLAIN, DESCRIBE]
```

**Tier 2: Filesystem Tools (Sandboxed)**

```yaml
tools:
  read_file:
    enabled: true
    allowed_paths:
      - /workspace/dbt/**
      - /workspace/sql/**
      - /workspace/docs/**
    blocked_patterns:
      - "**/.env"
      - "**/secrets/**"
    max_file_size_mb: 10
```

**Tier 3: Analysis Tools (No Side Effects)**

```yaml
tools:
  detect_anomaly:
    enabled: true
    algorithms: [spc, prophet, isolation_forest]
  
  explain_query_plan:
    enabled: true
    databases: all
```

**Tier 4: Write Operations (Requires Approval)**

```yaml
tools:
  create_materialized_view:
    enabled: true
    requires_approval: true
    approval_method: cli_prompt  # or: slack, api_callback
    approval_timeout_seconds: 300
  
  update_datapoint:
    enabled: true
    requires_approval: true
    git_commit: true  # Auto-commit changes
```

**Tier 5: Custom Tools (User-Installed)**

```yaml
tools:
  custom_tools:
    enabled: true
    plugin_directory: /workspace/.datachat/tools
    sandbox: true
    network_access: false
```

### Custom Tool API

**Option A: Python Plugin**

```python
# ~/.datachat/tools/query_crm.py
from datachat.tools import tool, ToolContext

@tool(
    name="query_crm",
    description="Query our Salesforce CRM",
    requires_approval=True,
    category="external_api"
)
def query_crm(query: str, ctx: ToolContext) -> dict:
    """
    Args:
        query: SOQL query string
        ctx: Tool execution context (user, audit_id, etc.)
    
    Returns:
        Query results as dict
    """
    # Custom implementation
    client = SalesforceClient(api_key=os.getenv("SALESFORCE_KEY"))
    results = client.query(query)
    
    # Audit logging
    ctx.log_action("crm_query", {"query": query, "result_count": len(results)})
    
    return results
```

**Option B: YAML + HTTP API**

```yaml
# ~/.datachat/tools/crm.yaml
name: query_crm
description: Query our Salesforce CRM
type: http_api
requires_approval: true

endpoint: https://crm.company.com/api/query
method: POST
auth:
  type: bearer_token
  token_env: SALESFORCE_API_TOKEN

request_schema:
  query:
    type: string
    required: true
    description: SOQL query string

response_schema:
  results:
    type: array
    description: Query results
```

---

## Risk Mitigation (Non-Negotiable)

### 1. LLM Hallucination Mitigation

**Problem:** LLM generates invalid SQL or incorrect business logic.

**Mitigations:**

**A. Multi-Agent Validation Pipeline**

```
SQLAgent generates query
    ↓
ValidatorAgent checks:
  • Syntax validation (sqlglot parser)
  • Semantic validation (references valid tables/columns)
  • Safety checks (no DROP, DELETE, TRUNCATE)
  • Cost estimation (EXPLAIN PLAN analysis)
    ↓
If validation fails: Regenerate with error feedback (max 3 attempts)
```

**B. Confidence Scoring**

```python
class QueryConfidence:
    """Track confidence in generated query."""
    
    def compute(self, query: str, context: dict) -> float:
        """
        Confidence factors:
        - Used DataPoint template: +0.4
        - All tables exist in schema: +0.3
        - Passed validation: +0.2
        - Similar query executed before: +0.1
        
        Returns: 0.0-1.0 confidence score
        """
        pass

# In UI: Show confidence, allow user review for low-confidence queries
```

**C. User Review for Low Confidence**

```
Confidence < 0.7:
  "I generated this query but I'm not confident. Please review:
   
   SELECT ... FROM ... WHERE ...
   
   [Edit Query] [Execute Anyway] [Cancel]"
```

**D. Query Result Validation**

```python
class ResultValidator:
    """Validate query results make sense."""
    
    def validate(self, query: str, results: pd.DataFrame) -> ValidationResult:
        """
        Checks:
        - Expected columns present
        - Row count reasonable (not 0 or 1M+ unexpectedly)
        - Numeric values in expected ranges
        - No NULL in required fields
        """
        pass
```

**E. Feedback Loop**

```
User marks query as incorrect
    ↓
System logs: (user_query, generated_sql, error_type, correction)
    ↓
Periodically fine-tune prompts based on failure patterns
```

---

### 2. Performance Optimization

**Problem:** Slow queries, expensive LLM calls, database overload.

**Mitigations:**

**A. Query Caching**

```python
class QueryCache:
    """Cache query results and generation artifacts."""
    
    def get_cached_sql(self, user_query: str, context: dict) -> Optional[str]:
        """
        Cache key: hash(user_query + schema_version + datapoint_version)
        TTL: 24 hours for generated SQL
        
        Hit: Return cached SQL (skip LLM call)
        Miss: Generate fresh, cache result
        """
        pass
    
    def get_cached_results(self, sql: str, params: dict) -> Optional[pd.DataFrame]:
        """
        Cache query results for expensive queries.
        TTL: Configurable per query (default 5 minutes)
        """
        pass
```

**B. Query Optimization**

```python
class QueryOptimizer:
    """Optimize generated SQL before execution."""
    
    def optimize(self, sql: str, backend: str) -> str:
        """
        Optimizations:
        - Add LIMIT if missing (prevent full table scans)
        - Push down filters (WHERE before JOIN)
        - Use indexes (suggest missing indexes)
        - Simplify subqueries
        """
        pass
```

**C. Rate Limiting**

```python
class RateLimiter:
    """Prevent database overload."""
    
    limits = {
        "queries_per_user_per_minute": 30,
        "concurrent_queries_per_user": 3,
        "max_query_duration_seconds": 60,
    }
    
    def check_limit(self, user_id: str, query_type: str) -> bool:
        """Return False if limit exceeded."""
        pass
```

**D. Background Execution for Expensive Queries**

```
Query estimated cost > threshold:
  → Execute in background
  → Return job_id immediately
  → User polls for results or gets webhook notification
```

**E. Materialization (Level 4)**

- Pre-compute expensive aggregations
- Route frequent queries to materialized views
- 10-50x speedup for common patterns

---

### 3. Security Hardening

**Problem:** SQL injection, unauthorized access, data exfiltration.

**Mitigations:**

**A. Parameterized Queries**

```python
# NEVER string concatenation
sql = f"SELECT * FROM users WHERE id = {user_input}"  # ❌ VULNERABLE

# ALWAYS parameterized
sql = "SELECT * FROM users WHERE id = ?"
params = [user_input]  # ✅ SAFE
```

**B. SQL Injection Detection**

```python
class SQLInjectionDetector:
    """Detect SQL injection attempts in generated queries."""
    
    patterns = [
        r";\s*DROP",
        r"UNION\s+SELECT",
        r"--\s*$",
        r"\/\*.*\*\/",
        r"xp_cmdshell",
    ]
    
    def detect(self, sql: str) -> bool:
        """Return True if injection detected."""
        for pattern in self.patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                return True
        return False
```

**C. Row-Level Security**

```yaml
# User access policy
access_control:
  user: analyst@company.com
  
  databases:
    production:
      mode: read_only
      
      tables:
        customers:
          allowed: true
          row_filter: "region = 'US'"  # Only see US customers
        
        financial_data:
          allowed: false  # Completely blocked
```

**D. Query Audit Logging**

```python
class AuditLogger:
    """Log all queries for compliance and security review."""
    
    def log_query(self, event: QueryEvent):
        """
        Log to append-only store:
        - timestamp
        - user_id
        - query_text
        - data_accessed (tables, row_count)
        - execution_time
        - result_hash (for change detection)
        """
        pass
```

**E. Data Masking**

```python
class DataMasker:
    """Mask PII in query results."""
    
    def mask(self, df: pd.DataFrame, schema: dict) -> pd.DataFrame:
        """
        Mask sensitive columns:
        - email: j***@example.com
        - ssn: ***-**-1234
        - credit_card: ****-****-****-1234
        """
        pass
```

**F. Network Isolation**

```yaml
# Database connection config
databases:
  production:
    host: db.internal.company.com
    port: 5432
    ssl: required
    ssl_verify: true
    
    network_policy:
      allowed_ips:
        - 10.0.0.0/8  # Internal network only
      
      vpn_required: true
```

---

## Project Structure (Updated)

```
datachat/
├── backend/
│   ├── agents/              # Multi-agent system
│   ├── llm/                 # LLM provider abstraction
│   ├── knowledge/           # Knowledge system (Levels 1-5)
│   │   ├── datapoints.py   # DataPoint loader/validator
│   │   ├── managed.py      # ManagedDataPoint generator (Level 1)
│   │   ├── graph.py        # Knowledge graph (Level 5)
│   │   ├── vectors.py      # Vector store for retrieval
│   │   └── retriever.py    # Combined retrieval
│   ├── apex/                # Semantic layer engine (Levels 3-5)
│   │   ├── engine.py       # Main APEX engine
│   │   ├── compiler.py     # SQL template compiler
│   │   ├── materializer.py # Materialization manager
│   │   ├── optimizer.py    # Query optimizer
│   │   ├── monitor.py      # Query pattern analyzer
│   │   └── intelligence/   # Level 5 features
│   │       ├── anomaly.py
│   │       ├── graph.py
│   │       └── diagnostics.py
│   ├── workspace/           # Filesystem integration
│   │   ├── indexer.py      # WorkspaceDataPoint generator
│   │   ├── watcher.py      # Filesystem watcher
│   │   └── parsers/        # Language-specific parsers
│   │       ├── sql.py
│   │       ├── python.py
│   │       └── yaml.py
│   ├── tools/               # Tool system
│   │   ├── registry.py     # Tool registration
│   │   ├── executor.py     # Tool execution with policies
│   │   ├── policies.py     # Access control
│   │   └── builtin/        # Built-in tools
│   │       ├── database.py
│   │       ├── filesystem.py
│   │       └── analysis.py
│   ├── profiling/           # Schema profiling (Level 1)
│   ├── connectors/          # Database connectors
│   ├── pipeline/            # Agent orchestration
│   ├── api/                 # FastAPI endpoints
│   ├── security/            # Security layer
│   │   ├── validator.py    # SQL injection detection
│   │   ├── masker.py       # PII masking
│   │   └── audit.py        # Audit logging
│   └── config.py
├── datapoints/
│   ├── managed/             # Auto-generated (Level 1)
│   └── user/                # User-created (Levels 2-5)
│       ├── sales/
│       ├── finance/
│       └── operations/
├── workspace/               # User's code/docs (indexed)
│   ├── dbt/
│   ├── sql/
│   └── docs/
├── tests/
│   ├── unit/
│   ├── integration/
│   └── security/            # Security-focused tests
├── docs/
│   ├── USER_GUIDE.md
│   ├── DATAPOINT_SCHEMA.md
│   ├── SECURITY.md
│   └── ARCHITECTURE.md
└── scripts/
    ├── profile_schema.py    # Schema profiler CLI
    └── benchmark.py
```

---

## Delivery Sequencing Contract

Architecture sequencing is tracked by initiative IDs in `docs/ROADMAP.md`.

This document enforces technical gating constraints:

1. MetadataOps foundation (`FND-*`) must stabilize before broad semantic automation.
2. Deterministic runtime paths (`SMP-*`) should precede deeper autonomous loops.
3. Dynamic data-agent initiatives (`DYN-*`) must be introduced with:
   - strict policy classes
   - bounded loop budgets
   - replayable traces
   - verification-first recovery behavior

---

## Key Decisions & Rationale

### ManagedDataPoint Storage

**Decision:** YAML files in `datapoints/managed/` (read-only)

**Rationale:**

- Transparency: Users can inspect what DataChat learned
- Version control: Git tracks schema evolution
- Simplicity: No additional database needed for v1.0
- Performance: Load into memory on startup (<1s for 1000 tables)

**Future:** Add PostgreSQL cache for large schemas (1000+ tables)

---

### Materialization Strategy

**Decision:** Start with manual (Level 4), add adaptive in v2.0

**Rationale:**

- User learns system before automation
- Manual gives better understanding of trade-offs
- Adaptive requires query pattern data (need time to collect)

**CLI Experience:**

```bash
# Manual materialization (v1.1)
$ datachat materialize enable revenue --granularity day --refresh 1h

# Adaptive suggestions (v2.0)
$ datachat materialize suggest
Recommendation: Materialize 'daily_revenue' (47 queries/day, 20x speedup)
Run: datachat materialize enable daily_revenue --auto
```

---

### Filesystem Scope

**Decision:** Start narrow (data/metrics code), allow expansion

**Rationale:**

- Focus: Most value from understanding data logic
- Performance: Indexing everything is slow and noisy
- Privacy: Avoid accidentally indexing secrets/credentials

**Configuration:**

```yaml
workspace:
  scope: narrow  # narrow | medium | broad
  
  # narrow: Just data/metrics code
  # medium: + orchestration (Airflow/Dagster)
  # broad: + application code (requires explicit opt-in)
```

---

### Neo4j for Knowledge Graph

**Decision:** Use Neo4j unless better alternative emerges

**Rationale:**

- Industry standard for graph databases
- Excellent Cypher query language for graph traversal
- Good Python client (neo4j-driver)
- Can be embedded (Neo4j Community Edition)

**Alternative Considered:** NetworkX (Python library)

- Pro: No separate database, simpler deployment
- Con: In-memory only, doesn't scale beyond 10K nodes
- Decision: Use NetworkX for v1.0 (simple), migrate to Neo4j in v2.0 (scale)

---

## Getting Started (For Claude Code)

### Prerequisites

- Python 3.11+
- PostgreSQL (for user database)
- Neo4j (for Level 5, optional in v1.0)
- ClickHouse or BigQuery (test databases)

### Setup

```bash
# Install dependencies
poetry install

# Set up environment
cp .env.example .env
# Edit .env with database credentials

# Initialize DataChat
python scripts/init_datachat.py

# Profile first database (generates ManagedDataPoints)
datachat database add my_db postgres://user:pass@host/db
datachat profile my_db

# Start server
datachat serve
```

### First Query

```bash
# CLI
datachat query "Show me top 10 customers by revenue"

# API
curl -X POST http://localhost:8000/api/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "Show me top 10 customers by revenue", "database": "my_db"}'
```

---

## Open Source Strategy

**Timeline:** Build internally first (4-6 months), then open source

**Rationale:**

- Focus: Avoid distraction of community management during core development
- Quality: Ship something excellent, not rushed
- Validation: Test with real users (Moniepoint) before public launch

**Pre-Launch Checklist:**

- [ ] Documentation complete (user guide, API docs, tutorials)
- [ ] Security audit passed
- [ ] Performance benchmarks published
- [ ] 10+ real users providing feedback
- [ ] License decided (Apache 2.0 or MIT)
- [ ] Contributor guidelines written
- [ ] Demo video recorded
- [ ] Launch post drafted (Hacker News, LinkedIn, Twitter)

---

## Success Metrics

### Technical Metrics

- Query success rate: >95%
- Average query time: <2s (generation) + <5s (execution)
- Cache hit rate: >40%
- Uptime: >99.5%

### User Metrics

- Time to first successful query: <5 minutes
- Daily active users: Target 50+ by end of internal testing
- User satisfaction: >4.5/5 in surveys

### Business Metrics

- GitHub stars: 1000+ within 3 months of launch
- Contributors: 10+ within 6 months
- Enterprise inquiries: 5+ per month

---

## Contact & Support

**Internal (Moniepoint):** #data-chat Slack channel
**External (Post-Launch):** GitHub Issues, Discord community

---

*This document is the single source of truth for DataChat architecture and implementation. Update as decisions evolve.*
