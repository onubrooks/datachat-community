# DataPoint Migration Path

**Version:** 1.0  
**Last Updated:** February 18, 2026

This document describes the migration path from the current JSON-based DataPoint types to the target YAML-based architecture.

---

## Current State

DataPoints are implemented as JSON-based Pydantic models:

```
backend/models/datapoint.py
├── BaseDataPoint          # Common fields (id, name, type, owner, tags, metadata)
├── SchemaDataPoint        # Tables/views (table_name, key_columns, relationships)
├── BusinessDataPoint      # Metrics (calculation, synonyms, related_tables, unit)
└── ProcessDataPoint       # ETL processes (schedule, data_freshness, target_tables)
```

**Storage format:** YAML files loaded into JSON models at runtime.

**Contract enforcement:** `backend/knowledge/contracts.py`

---

## Target State

YAML-based DataPoints with execution blocks, materialization, and intelligence:

```yaml
datapoint:
  id: revenue
  type: metric
  definition: "Total completed sales"
  execution:
    sql_template: "SELECT SUM(amount)..."
    parameters: {...}
  materialization:
    enabled: true
    strategy: adaptive
  intelligence:
    sla: {...}
    anomaly_detection: {...}
```

See `docs/DATAPOINT_SCHEMA.md` for full target schema.

---

## Migration Strategy

### Principle: JSON types remain for Levels 1-2

The current JSON types (Schema, Business, Process) are sufficient for:
- Level 1: Schema-aware querying
- Level 2: Context enhancement
- Level 1.5: MetadataOps foundation

YAML-based execution blocks become relevant at Level 3+.

### New Types (Added Incrementally)

| Type | Purpose | Level | Priority |
|------|---------|-------|----------|
| `QueryDataPoint` | Reusable SQL templates | 2.5 | High |
| `ConstraintDataPoint` | Business rules for WHERE | 2.5 | Medium |
| `DashboardDataPoint` | Pre-built visualizations | 3 | Low |

### Migration Path

```
Phase 1: Add QueryDataPoint (JSON model)
    ↓
Phase 2: Expand contracts for QueryDataPoint
    ↓
Phase 3: Add feedback telemetry loop
    ↓
Phase 4: Add column-level graph edges
    ↓
Phase 5: Entity memory in session context
    ↓
Phase 6: YAML execution blocks (Level 3+)
```

---

## QueryDataPoint (Level 2.5)

### Purpose

Store and retrieve pre-validated SQL templates for common queries:
- Skip SQL generation for known patterns (faster, more consistent)
- Enable parameterized execution with validation
- Provide backend-specific variants

### JSON Model

```python
class QueryDataPoint(BaseDataPoint):
    type: Literal["Query"]
    sql_template: str
    parameters: dict[str, Any]
    description: str
    backend_variants: dict[str, str] | None = None
    validation: dict[str, Any] | None = None
```

### Example YAML

```yaml
datapoint:
  id: top_customers_by_revenue
  name: Top Customers by Revenue
  type: Query
  owner: sales-team
  tags: [sales, revenue, customers]
  
  sql_template: |
    SELECT 
      customer_id,
      customer_name,
      SUM(amount) as total_revenue
    FROM transactions
    WHERE status = 'completed'
      AND transaction_time >= {start_time}
      AND transaction_time < {end_time}
    GROUP BY customer_id, customer_name
    ORDER BY total_revenue DESC
    LIMIT {limit}
  
  parameters:
    limit:
      type: integer
      default: 10
      description: Number of top customers to return
    start_time:
      type: timestamp
      required: true
    end_time:
      type: timestamp
      required: true
  
  backend_variants:
    clickhouse: |
      SELECT customer_id, SUM(amount) as total_revenue
      FROM transactions
      WHERE status = 'completed'
        AND transaction_time >= {start_time}
      GROUP BY customer_id
      ORDER BY total_revenue DESC
      LIMIT {limit}
  
  validation:
    expected_columns: [customer_id, customer_name, total_revenue]
    max_rows: 100
```

### CLI Commands

```bash
# Add a QueryDataPoint
datachat dp add-query datapoints/user/sales/top_customers.yaml

# List all QueryDataPoints
datachat dp list --type Query

# Validate SQL template
datachat dp validate top_customers_by_revenue --check-sql

# Execute with parameters
datachat query "top 5 customers this month" --template top_customers_by_revenue
```

### Pipeline Integration

1. **ContextAgent**: Retrieves QueryDataPoints matching user query
2. **SQLAgent**: Uses template directly if `QueryDataPoint` found (skip generation)
3. **Validator**: Validates template parameters and result shape
4. **decision_trace**: Includes `query_template_used` field

---

## ConstraintDataPoint (Planned)

### Purpose

Define business rules that affect WHERE clauses:

```yaml
datapoint:
  id: active_customers_filter
  type: Constraint
  definition: "Only include active, non-test customers"
  
  filters:
    - column: customer_status
      operator: "="
      value: "active"
    - column: is_test_account
      operator: "="
      value: false
  
  applies_to:
    - table: customers
    - table: transactions
      join_through: customers
```

### Usage

When querying `transactions` or `customers`, automatically inject:
```sql
WHERE customer_status = 'active' AND is_test_account = false
```

---

## Feedback Telemetry Loop

### Purpose

Use runtime telemetry to improve DataPoint quality:

| Telemetry | Suggestion |
|-----------|------------|
| Wrong table selected | Add `related_tables` hint |
| Clarification churn | Add `synonyms` to BusinessDataPoint |
| Low confidence | Add `business_meaning` to key columns |
| Fallback to vector-only | Add `grain` to SchemaDataPoint |

### CLI

```bash
# View telemetry summary
datachat telemetry report

# Get suggestions for a specific DataPoint
datachat dp suggest --datapoint metric_revenue_001

# Apply suggestions (with review)
datachat dp apply-suggestions --datapoint metric_revenue_001 --dry-run
```

---

## Contract Evolution

### Current Contracts (v1)

| Rule | Type | Severity |
|------|------|----------|
| `freshness` present | Schema, Process | error |
| `unit` present | Business | error |
| `grain` present | All | warning |
| `exclusions` present | All | warning |

### Expanded Contracts (v2)

| Rule | Type | Severity |
|------|------|----------|
| `key_columns[].business_meaning` present | Schema | warning → error (strict) |
| `calculation` valid SQL fragment | Business | warning |
| `synonyms` non-empty | Business | warning |
| `relationships` reference valid tables | Schema | error |
| `grain` matches actual table grain | Schema | error (strict) |
| `sql_template` valid SQL | Query | error |
| `parameters` match template placeholders | Query | error |

---

## Timeline

| Phase | Scope | Effort |
|-------|-------|--------|
| Phase 1: QueryDataPoint | Model + CLI + Integration | 12h |
| Phase 2: Contract v2 | Rules + CI integration | 8h |
| Phase 3: Telemetry | Collector + CLI | 10h |
| Phase 4: Graph Edges | Column-level + semantic | 16h |
| Phase 5: Session Memory | Entity + temporal | 8h |
| Phase 6: YAML Execution | Level 3 templates | TBD |

**Total: ~54 hours for Levels 1-2.5 completion**

---

## References

- `docs/DATAPOINT_SCHEMA.md` - Target YAML schema
- `docs/LEVELS.md` - Level maturity tracking
- `docs/PRD.md` - Product requirements and phase tracking
- `backend/models/datapoint.py` - Current JSON models
- `backend/knowledge/contracts.py` - Contract validation
