# DataPoint Schema Specification

**Version:** 1.1  
**Last Updated:** February 18, 2026

> **Note:** This document describes the **target architecture** for Levels 3-5. The current implementation uses JSON-based Pydantic models (SchemaDataPoint, BusinessDataPoint, ProcessDataPoint). See the "Current Implementation" section below and docs/DATAPOINT_MIGRATION.md for details.

This document defines the complete schema for DataPoints across all levels (1-5) of the DataChat system.

---

## Current Implementation vs Target State

### Current Implementation (JSON-based)

DataPoints are currently implemented as JSON-based Pydantic models with three types:

| Type | Purpose | Key Fields |
|------|---------|------------|
| `SchemaDataPoint` | Tables/views with column metadata | `table_name`, `key_columns`, `relationships`, `freshness` |
| `BusinessDataPoint` | Metrics and business concepts | `calculation`, `synonyms`, `related_tables`, `unit` |
| `ProcessDataPoint` | ETL/scheduled processes | `schedule`, `data_freshness`, `target_tables`, `dependencies` |

**Planned types (Level 2.5+):**
- `QueryDataPoint` - Reusable SQL templates with parameters
- `ConstraintDataPoint` - Business rules affecting WHERE clauses
- `DashboardDataPoint` - Pre-built visualizations

### Target Architecture (YAML-based)

This document describes YAML-based DataPoints with execution blocks:

| Target YAML Type | Current JSON Type | Implementation Status |
|------------------|-------------------|----------------------|
| `metric` | `BusinessDataPoint` | Partial (missing execution block) |
| `dimension` | - | Planned |
| `entity` | - | Planned |
| `concept` | - | Planned |
| `query` | `QueryDataPoint` | Planned (Level 2.5) |
| - | `SchemaDataPoint` | Implemented (no YAML equivalent) |
| - | `ProcessDataPoint` | Implemented (no YAML equivalent) |

**Migration path:** See docs/DATAPOINT_MIGRATION.md for the plan to bridge current JSON types to target YAML types.

---

## Overview

DataPoints are the fundamental knowledge artifacts in DataChat. They can range from simple documentation (Level 1) to full-featured semantic layer metrics with AI intelligence (Level 5).

**Progressive Enhancement:** A DataPoint can start minimal and grow more sophisticated as needs evolve.

---

## Core Schema (Required for All Levels)

```yaml
datapoint:
  # Identity
  id: string                    # REQUIRED: Unique identifier (lowercase, alphanumeric + underscores)
  name: string                  # REQUIRED: Human-readable name
  type: enum                    # REQUIRED: concept | metric | dimension | entity
  
  # Documentation
  definition: string            # REQUIRED: What this DataPoint represents
  owner: string                 # REQUIRED: Team or individual responsible
  tags: array[string]           # OPTIONAL: Categorization tags
  
  # Metadata
  created_at: datetime          # AUTO: When DataPoint was created
  updated_at: datetime          # AUTO: Last modification time
  version: string               # OPTIONAL: Semantic version (e.g., "1.2.0")
```

### Field Specifications

#### `id` (required)

- **Format:** Lowercase alphanumeric with underscores
- **Examples:** `revenue`, `customer_lifetime_value`, `pool_match_rate`
- **Validation:** Regex `^[a-z0-9_]+$`
- **Uniqueness:** Must be unique across all DataPoints

#### `type` (required)

- **concept:** Abstract business concept (e.g., "Revenue", "Customer Churn")
- **metric:** Measurable quantity with formula (e.g., "Monthly Recurring Revenue")
- **dimension:** Attribute for grouping (e.g., "Product Category", "Region")
- **entity:** Business object (e.g., "Customer", "Transaction")

#### `definition` (required)

- **Format:** Markdown text
- **Purpose:** Explain what this DataPoint means in business terms
- **Length:** Recommended 1-3 paragraphs
- **Example:**

  ```txt
  Total value of completed sales transactions, excluding refunds and taxes.
  Finance team's canonical definition as of 2024-Q4. Used for monthly
  reporting and executive dashboards.
  ```

#### `owner` (required)

- **Format:** Team name or email
- **Examples:** `finance-team`, `data-platform`, `sarah@company.com`
- **Purpose:** Who to contact about this DataPoint

#### `tags` (optional)

- **Format:** Array of lowercase strings
- **Examples:** `[sales, finance, critical]`, `[experimental]`
- **Purpose:** Categorization, searchability, filtering

---

## Level 2: Context Enhancement

Level 2 adds business context that helps DataChat generate better queries.

```yaml
datapoint:
  # ... core fields ...
  
  # Level 2: Business Context
  data_sources:
    - table: string             # Table name
      columns: array[string]    # Relevant columns
      filters: array[string]    # Business rules (SQL WHERE clauses)
      joins: array[object]      # OPTIONAL: How to join related tables
  
  related_concepts: array[string]  # IDs of related DataPoints
  documentation_url: string        # Link to wiki/confluence page
  examples: array[string]          # Example values or use cases
```

### Example: Level 2 DataPoint

```yaml
datapoint:
  id: revenue
  name: Revenue
  type: metric
  definition: |
    Total value of completed sales transactions, excluding refunds and taxes.
    Finance team's canonical definition as of 2024-Q4.
  owner: finance-team
  tags: [sales, finance, critical]
  
  # Level 2 additions
  data_sources:
    - table: transactions
      columns: [amount, status, transaction_type, transaction_time]
      filters:
        - "status = 'completed'"
        - "transaction_type = 'sale'"
      joins:
        - table: customers
          type: left
          on: "transactions.customer_id = customers.id"
  
  related_concepts:
    - net_revenue
    - gross_margin
    - revenue_per_customer
  
  documentation_url: https://wiki.company.com/metrics/revenue
  
  examples:
    - "$1.2M in January 2024"
    - "Q4 2023 revenue: $15.8M"
```

---

## Level 3: Executable Metrics

Level 3 adds SQL templates for consistent, fast query execution.

```yaml
datapoint:
  # ... core + Level 2 fields ...
  
  # Level 3: Execution Specification
  execution:
    sql_template: string          # REQUIRED: SQL template with parameters
    parameters: object            # REQUIRED: Parameter definitions
    backend_variants: object      # OPTIONAL: Backend-specific SQL
    validation: object            # OPTIONAL: Result validation rules
```

### Field Specifications

#### `sql_template` (required)

- **Format:** SQL string with `{parameter}` placeholders
- **Constraints:**
  - Must be SELECT only (no INSERT/UPDATE/DELETE)
  - Must include GROUP BY if aggregation used
  - Should include ORDER BY for consistency
- **Example:**

  ```sql
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
  ```

#### `parameters` (required)

- **Format:** Map of parameter names to specifications
- **Required fields per parameter:**
  - `type`: Data type (string | integer | float | timestamp | enum)
  - `required`: Boolean (is this parameter mandatory?)
  - `default`: Default value if not provided
- **Optional fields:**
  - `description`: What this parameter controls
  - `validation`: Regex or constraints
  - `values`: Allowed values (for enum type)

#### `backend_variants` (optional)

- **Purpose:** Handle SQL dialect differences
- **Format:** Map of backend names to SQL templates
- **Supported backends:** `postgres`, `clickhouse`, `bigquery`, `snowflake`
- **Fallback:** If backend not specified, use `sql_template`

### Example: Level 3 DataPoint

```yaml
datapoint:
  id: revenue
  name: Revenue
  type: metric
  definition: Total completed sales, excluding refunds and taxes.
  owner: finance-team
  tags: [sales, finance, critical]
  
  data_sources:
    - table: transactions
      columns: [amount, status, transaction_type]
  
  # Level 3 additions
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
        required: false
        default: day
        values: [hour, day, week, month, quarter, year]
        description: Time bucket size for aggregation
      
      start_time:
        type: timestamp
        required: true
        description: Start of time range (inclusive)
      
      end_time:
        type: timestamp
        required: true
        description: End of time range (exclusive)
    
    backend_variants:
      clickhouse: |
        SELECT 
          SUM(amount) as value,
          toStartOfInterval(transaction_time, INTERVAL 1 {granularity}) as period
        FROM transactions
        WHERE status = 'completed'
          AND transaction_type = 'sale'
          AND transaction_time >= {start_time}
          AND transaction_time < {end_time}
        GROUP BY period
        ORDER BY period
      
      bigquery: |
        SELECT 
          SUM(amount) as value,
          TIMESTAMP_TRUNC(transaction_time, {granularity}) as period
        FROM transactions
        WHERE status = 'completed'
          AND transaction_type = 'sale'
          AND transaction_time >= TIMESTAMP('{start_time}')
          AND transaction_time < TIMESTAMP('{end_time}')
        GROUP BY period
        ORDER BY period
    
    validation:
      expected_columns: [value, period]
      value_range:
        min: 0
        max: 100000000  # $100M sanity check
      row_count_range:
        min: 1
        max: 10000
```

---

## Level 4: Performance Optimization

Level 4 adds materialization configuration for pre-computed results.

```yaml
datapoint:
  # ... core + Level 2 + Level 3 fields ...
  
  # Level 4: Materialization
  materialization:
    enabled: boolean              # REQUIRED: Enable pre-aggregation?
    strategy: enum                # REQUIRED: adaptive | manual
    
    # Manual strategy fields
    granularity: string           # Time bucket (hour | day | week | month)
    partition_by: string          # Partition key for incremental refresh
    refresh_interval: string      # How often to update (e.g., "1 hour", "15 minutes")
    incremental: boolean          # Use incremental refresh?
    lookback_window: string       # How far back to refresh (e.g., "7 days")
    
    # Metadata (auto-populated by system)
    _analytics:
      queries_per_day: integer
      avg_execution_time_ms: float
      cache_hit_rate: float
      last_refreshed_at: datetime
    
    _recommendation:
      should_materialize: boolean
      reason: string
      estimated_speedup: string
```

### Field Specifications

#### `strategy` (required)

- **adaptive:** System decides when/how to materialize based on query patterns
- **manual:** User explicitly configures materialization

#### `granularity` (manual only)

- **Format:** Time unit string
- **Values:** `hour`, `day`, `week`, `month`, `quarter`, `year`
- **Purpose:** Pre-aggregate at this time bucket

#### `refresh_interval` (manual only)

- **Format:** Duration string (e.g., "10 minutes", "1 hour", "1 day")
- **Purpose:** How often to update materialized data

#### `incremental` (manual only)

- **Type:** Boolean
- **true:** Only refresh recent data (uses `lookback_window`)
- **false:** Full refresh every time (slower but simpler)

#### `lookback_window` (incremental only)

- **Format:** Duration string (e.g., "7 days", "1 month")
- **Purpose:** How much recent data to re-process on refresh

### Example: Level 4 DataPoint

```yaml
datapoint:
  id: daily_revenue
  name: Daily Revenue
  type: metric
  definition: Total revenue aggregated by day.
  owner: finance-team
  
  execution:
    sql_template: |
      SELECT 
        DATE(transaction_time) as date,
        SUM(amount) as revenue
      FROM transactions
      WHERE status = 'completed' AND {filters}
      GROUP BY date
      ORDER BY date
    parameters: {...}
  
  # Level 4 additions
  materialization:
    enabled: true
    strategy: manual
    
    granularity: day
    partition_by: "DATE(transaction_time)"
    refresh_interval: "1 hour"
    incremental: true
    lookback_window: "7 days"
    
    # System-populated analytics
    _analytics:
      queries_per_day: 47
      avg_execution_time_ms: 3200
      cache_hit_rate: 0.15
      last_refreshed_at: "2026-01-30T14:30:00Z"
    
    _recommendation:
      should_materialize: true
      reason: "High query frequency (47/day) + expensive computation (3.2s avg)"
      estimated_speedup: "20x (3.2s → 160ms)"
```

---

## Level 5: AI Intelligence

Level 5 adds anomaly detection, root cause analysis, and auto-remediation.

```yaml
datapoint:
  # ... core + Level 2 + Level 3 + Level 4 fields ...
  
  # Level 5: Intelligence Configuration
  intelligence:
    # SLA monitoring
    sla:
      target: number              # Target value
      warning_threshold: float    # Alert at X% below target (0.0-1.0)
      critical_threshold: float   # Escalate at X% below target
      alert_channel: string       # Slack channel or email
      escalation_owner: string    # Who to notify on critical breach
    
    # Anomaly detection
    anomaly_detection:
      enabled: boolean
      algorithm: enum             # spc | prophet | isolation_forest | ensemble
      sensitivity: float          # 0.01-0.10 (5% = alert on 5% deviation)
      baseline_period_days: integer  # How much history to use for baseline
      cooldown_minutes: integer   # Minimum time between alerts
    
    # Auto-remediation
    auto_remediation:
      - condition: string         # When to trigger (Python expression)
        action: enum              # trigger_dag | page_oncall | send_alert
        config: object            # Action-specific configuration
        reason: string            # Why this action helps
  
  # Level 5: Knowledge Graph Relationships
  relationships:
    depends_on:
      - id: string                # DataPoint ID
        type: enum                # input_metric | data_quality | system_dependency
        impact_coefficient: float # 0.0-1.0 (how much variance explained)
        description: string       # How this dependency affects the metric
    
    impacts:
      - id: string                # DataPoint ID
        type: enum                # downstream_metric | derived_metric | operational_cost
        relationship: string      # How this affects the downstream metric
        quantified_impact: string # e.g., "1% drop = $5K daily loss"
    
    related_systems:
      - name: string              # System name
        type: enum                # external_dependency | internal_service
        criticality: enum         # low | medium | high
        monitoring_url: string    # Link to system status page
        health_check: string      # API endpoint to check health
```

### Field Specifications

#### SLA Configuration

**`target`**: Expected value for the metric

- Examples: `1000000` (revenue target $1M), `0.95` (95% match rate)

**`warning_threshold`**: Percentage below target to alert

- Format: 0.0-1.0 (0.9 = alert at 10% below target)
- Triggers investigation workflow

**`critical_threshold`**: Percentage below target to escalate

- Format: 0.0-1.0 (0.8 = escalate at 20% below target)
- Triggers high-priority alert and auto-remediation

#### Anomaly Detection

**`algorithm`**: Detection method

- **spc**: Statistical Process Control (3-sigma, fast, interpretable)
- **prophet**: Facebook's time series forecasting (trend-aware)
- **isolation_forest**: ML-based outlier detection (unsupervised)
- **ensemble**: Combine multiple algorithms (require 2+ to agree)

**`sensitivity`**: How aggressive to detect anomalies

- Format: 0.01-0.10 (0.05 = alert on 5% deviation)
- Lower = fewer alerts but might miss issues
- Higher = more alerts but might have false positives

#### Auto-Remediation

**`condition`**: Python expression (evaluated with metric context)

- Variables available: `value`, `target`, `warning_threshold`, `critical_threshold`
- Example: `"value < sla.warning_threshold AND pricing_coverage < 0.95"`

**`action`**: What to do when condition triggers

- **trigger_dag**: Run Airflow DAG (specify `dag_id` in config)
- **page_oncall**: Alert via PagerDuty (specify `service` in config)
- **send_alert**: Post to Slack/email (specify `channel` in config)

#### Knowledge Graph Relationships

**`depends_on`**: What this metric relies on

- **input_metric**: Another metric that feeds into this calculation
- **data_quality**: Data quality check that affects accuracy
- **system_dependency**: External system whose health impacts this metric

**`impact_coefficient`**: How much variance explained

- Format: 0.0-1.0 (0.8 = 80% of variance from this dependency)
- Used to prioritize root cause investigation

**`impacts`**: What this metric affects

- **downstream_metric**: Metrics calculated from this one
- **derived_metric**: Metrics that reference this one
- **operational_cost**: Business costs impacted by this metric

### Example: Level 5 DataPoint (Complete)

```yaml
datapoint:
  id: pool_match_rate
  name: Pool Match Rate
  type: metric
  
  definition: |
    Percentage of transactions successfully matched in reconciliation pool.
    Core indicator of reconciliation system health and data quality.
  
  owner: finance-operations
  tags: [reconciliation, sla, critical]
  
  data_sources:
    - table: reconciliation.pool_summary
      columns: [match_status, reconciliation_time]
  
  execution:
    sql_template: |
      SELECT 
        (countIf(match_status = 'matched') / count(*)) * 100 as value,
        toStartOfHour(reconciliation_time) as timestamp
      FROM reconciliation.pool_summary
      WHERE reconciliation_time >= {start_time}
        AND reconciliation_time < {end_time}
      GROUP BY timestamp
      ORDER BY timestamp
    
    parameters:
      start_time: {type: timestamp, required: true}
      end_time: {type: timestamp, required: true}
  
  materialization:
    enabled: true
    strategy: manual
    granularity: hour
    refresh_interval: "10 minutes"
    incremental: true
    lookback_window: "24 hours"
  
  # Level 5: Intelligence
  intelligence:
    sla:
      target: 0.95  # 95% match rate
      warning_threshold: 0.92
      critical_threshold: 0.88
      alert_channel: "#finance-recon-alerts"
      escalation_owner: "finance-director@company.com"
    
    anomaly_detection:
      enabled: true
      algorithm: ensemble
      sensitivity: 0.02  # Alert on 2% deviation
      baseline_period_days: 30
      cooldown_minutes: 30
    
    auto_remediation:
      - condition: "value < 0.92 AND pricing_coverage < 0.95"
        action: trigger_dag
        config:
          dag_id: refresh_pricing_data
        reason: "Stale pricing often causes match failures"
      
      - condition: "value < 0.88"
        action: page_oncall
        config:
          service: finance-ops
        reason: "Critical SLA breach requires immediate attention"
  
  # Level 5: Relationships
  relationships:
    depends_on:
      - id: decimal_precision_status
        type: data_quality
        impact_coefficient: 0.80
        description: "Decimal mismatches cause 80% of failures"
      
      - id: pricing_coverage
        type: data_quality
        impact_coefficient: 0.10
        description: "Missing pricing causes 10% of failures"
      
      - id: cba_ingestion_lag
        type: system_dependency
        impact_coefficient: 0.08
        description: "Late CBA records cause 5-8% of failures"
    
    impacts:
      - id: unrecovered_revenue
        type: downstream_metric
        relationship: "Unmatched transactions become unrecovered revenue"
        quantified_impact: "1% drop = $5K-8K daily loss"
      
      - id: manual_investigation_hours
        type: operational_cost
        relationship: "Failed matches require manual investigation"
        quantified_impact: "1% drop = 2 engineer-hours"
    
    related_systems:
      - name: CBA
        type: external_dependency
        criticality: high
        monitoring_url: "https://status.cba.com"
      
      - name: pricing_service
        type: internal_service
        criticality: medium
        health_check: "http://pricing-svc.internal/health"
```

---

## Validation Rules

DataChat validates DataPoints on load. Here are the rules:

### Required Field Validation

- `id`, `name`, `type`, `definition`, `owner` must be present
- `id` must match regex `^[a-z0-9_]+$`
- `type` must be one of: `concept`, `metric`, `dimension`, `entity`

### Level-Specific Validation

**Level 2:**

- If `data_sources` present, each must have `table` and `columns`
- `related_concepts` must reference valid DataPoint IDs

**Level 3:**

- `execution.sql_template` must be valid SQL
- `execution.sql_template` must be SELECT only
- `execution.parameters` must define all template placeholders
- Each parameter must have `type` and `required` fields

**Level 4:**

- If `materialization.enabled = true`, must have `strategy`
- If `strategy = manual`, must have `granularity` and `refresh_interval`
- If `incremental = true`, must have `lookback_window`

**Level 5:**

- All DataPoint IDs in `relationships` must exist
- `impact_coefficient` must be 0.0-1.0
- `auto_remediation.condition` must be valid Python expression

### Cross-Field Validation

- If `type = metric`, should have `execution` block (Level 3+)
- If `materialization.enabled`, must have `execution` block
- If `intelligence` present, must have `execution` and `materialization`

---

## Usage Examples

### Creating a New DataPoint

```bash
# 1. Create YAML file
cat > datapoints/user/sales/revenue.yaml <<EOF
datapoint:
  id: revenue
  name: Revenue
  type: metric
  definition: Total completed sales, excluding refunds
  owner: finance-team
  tags: [sales, finance]
EOF

# 2. Validate
datachat datapoint validate datapoints/user/sales/revenue.yaml

# 3. Load into DataChat
datachat datapoint load datapoints/user/sales/revenue.yaml
```

### Upgrading DataPoint Level

```bash
# Start with Level 2 (context only)
datachat datapoint create revenue --level 2

# User tests, sees it works well

# Upgrade to Level 3 (add SQL template)
datachat datapoint upgrade revenue --to-level 3 \
  --sql-file revenue_template.sql

# System measures query patterns

# System recommends Level 4 (materialization)
datachat materialize suggest
# → "Revenue queried 50x/day, 3.2s avg. Enable materialization?"

# Enable materialization
datachat materialize enable revenue
```

---

## Best Practices

### 1. Start Simple, Add Complexity

❌ **Don't:** Create Level 5 DataPoint on day 1

```yaml
datapoint:
  id: new_metric
  # ... 200 lines of config
  intelligence:
    # Complex anomaly detection
  relationships:
    # Extensive dependency graph
```

✅ **Do:** Start with Level 2, upgrade incrementally

```yaml
# Week 1: Level 2
datapoint:
  id: new_metric
  name: New Metric
  type: metric
  definition: What this means
  owner: my-team
  data_sources:
    - table: my_table
      columns: [col1, col2]

# Week 2: Add Level 3 after validation
# Week 4: Add Level 4 based on usage
# Month 2: Add Level 5 if needed
```

### 2. Keep SQL Templates Readable

❌ **Don't:** Complex, unreadable SQL

```sql
SELECT SUM(CASE WHEN a='x' THEN b*c ELSE d END) AS v,DATE_TRUNC('d',t) 
FROM t1 LEFT JOIN t2 ON t1.id=t2.fk WHERE t>=NOW()-INTERVAL 7 DAY
```

✅ **Do:** Formatted, documented SQL

```sql
SELECT 
  SUM(
    CASE 
      WHEN transaction_type = 'sale' THEN amount * quantity
      ELSE refund_amount 
    END
  ) as total_value,
  DATE_TRUNC('day', transaction_time) as date
FROM transactions t1
LEFT JOIN customers t2 ON t1.customer_id = t2.id
WHERE transaction_time >= NOW() - INTERVAL 7 DAY
  AND status = 'completed'
GROUP BY date
ORDER BY date
```

### 3. Use Descriptive Names and Tags

❌ **Don't:** Cryptic names

```yaml
datapoint:
  id: m1
  name: M1
  tags: [a, b]
```

✅ **Do:** Clear, searchable names

```yaml
datapoint:
  id: monthly_recurring_revenue
  name: Monthly Recurring Revenue (MRR)
  tags: [sales, finance, subscription, critical]
```

### 4. Document Edge Cases

❌ **Don't:** Assume definition is obvious

```yaml
definition: Total revenue
```

✅ **Do:** Explain nuances

```yaml
definition: |
  Total revenue from completed sales transactions.
  
  Excludes:
  - Refunds (handled in separate metric)
  - Pending transactions (status != 'completed')
  - Internal test transactions (customer_type != 'internal')
  
  Edge case: Multi-currency transactions are converted to USD
  at transaction time using daily exchange rates.
```

---

## Migration Guide

### Upgrading Existing Metrics to DataPoints

If you have existing metric definitions (in dbt, Looker, etc.), here's how to migrate:

**1. Inventory existing metrics**

```bash
# List metrics from dbt
cat dbt_project.yml | grep -A 5 "metrics:"

# List metrics from Looker
looker-cli metrics list
```

**2. Convert to DataPoint format**

**From dbt metric:**

```yaml
# dbt_project.yml
metrics:
  - name: revenue
    label: Revenue
    model: ref('fact_transactions')
    calculation_method: sum
    expression: amount
    filters:
      - field: status
        value: completed
```

**To DataPoint:**

```yaml
datapoint:
  id: revenue
  name: Revenue
  type: metric
  definition: Total completed sales, from dbt metric
  owner: finance-team
  
  execution:
    sql_template: |
      SELECT SUM(amount) as value
      FROM {{ ref('fact_transactions') }}
      WHERE status = 'completed'
        AND transaction_time >= {start_time}
        AND transaction_time < {end_time}
    parameters:
      start_time: {type: timestamp, required: true}
      end_time: {type: timestamp, required: true}
```

**3. Validate migration**

```bash
# Compare results
datachat query "revenue last month" > new_results.csv
dbt run --select revenue > old_results.csv
diff new_results.csv old_results.csv
```

---

## Appendix: JSON Schema

For programmatic validation, here's the JSON Schema:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["datapoint"],
  "properties": {
    "datapoint": {
      "type": "object",
      "required": ["id", "name", "type", "definition", "owner"],
      "properties": {
        "id": {
          "type": "string",
          "pattern": "^[a-z0-9_]+$"
        },
        "name": {"type": "string"},
        "type": {
          "type": "string",
          "enum": ["concept", "metric", "dimension", "entity"]
        },
        "definition": {"type": "string"},
        "owner": {"type": "string"},
        "tags": {
          "type": "array",
          "items": {"type": "string"}
        }
      }
    }
  }
}
```

---

## Support

**Questions?**

- File issue: GitHub Issues
- Discuss: #datachat Slack channel

**Found a schema bug?**
Please report with:

- DataPoint YAML that fails validation
- Expected vs actual behavior
- DataChat version

---

*This schema is versioned. Breaking changes increment major version (1.0 → 2.0). Non-breaking additions increment minor version (1.0 → 1.1).*
