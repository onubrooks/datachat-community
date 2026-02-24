# DataChat Scripts

This directory contains utility scripts for testing and development.

## Available Scripts

### 0. `demo_seed.sql` - Demo Database Seed

**Purpose**: Create demo tables (`users`, `orders`) with sample data for onboarding.

**Usage**:

```bash
psql "$SYSTEM_DATABASE_URL" -f scripts/demo_seed.sql
```

**Related DataPoints**: `datapoints/demo/*.json`

### 0b. `grocery_seed.sql` - Grocery Business Seed

**Purpose**: Create grocery operations tables with realistic sample data for DataPoint-driven evaluation.

Tables created:

- `grocery_stores`
- `grocery_suppliers`
- `grocery_products`
- `grocery_inventory_snapshots`
- `grocery_sales_transactions`
- `grocery_purchase_orders`
- `grocery_waste_events`

**Usage**:

```bash
createdb datachat_grocery
psql "postgresql://postgres:@localhost:5432/datachat_grocery" -f scripts/grocery_seed.sql
```

**Related DataPoints**: `datapoints/examples/grocery_store/*.json`
**Related eval datasets**: `eval/grocery/*.json`

### 0c. `fintech_seed.sql` - Fintech Banking Seed

**Purpose**: Create banking/fintech operational tables with realistic sample data for DataPoint-driven testing.

Tables created:

- `bank_customers`
- `bank_accounts`
- `bank_transactions`
- `bank_cards`
- `bank_loans`
- `bank_loan_payments`
- `bank_fx_rates`

**Usage**:

```bash
createdb datachat_fintech
psql "postgresql://postgres:@localhost:5432/datachat_fintech" -f scripts/fintech_seed.sql
```

**Related DataPoints**: `datapoints/examples/fintech_bank/*.json`
**Manual query pack**: `scripts/fintech_demo_queries.sql`

### 1. `test_sql_agent.py` - Comprehensive SQLAgent Testing

**Purpose**: Test SQLAgent with predefined sample queries to verify SQL generation, self-correction, and metadata.

**Requirements**:

- Valid OpenAI API key (set in `.env` or as environment variable)
- Installed dependencies: `openai`, `tiktoken`, `pydantic`

**Usage**:

```bash
# Make sure you're in the project root
cd /Users/onuh/Documents/Work/Open\ Source/datachat

# Set your OpenAI API key (if not in .env)
export OPENAI_API_KEY=sk-...

# Run the test script
python scripts/test_sql_agent.py
```

**What it does**:

- Creates sample DataPoints mimicking ContextAgent output
- Tests multiple query types:
  - Simple aggregation: "What was the total sales amount?"
  - Date filtering: "Show sales from last quarter"
  - Joins: "Revenue by region and product category"
  - Business rules: "Calculate monthly recurring revenue"
  - CTEs: "Top 10 customers by sales with their average order value"
- Displays for each query:
  - Generated SQL
  - Explanation
  - Confidence score
  - Used DataPoints (for citations)
  - Assumptions made
  - Self-correction attempts (if any)
  - Timing and token usage

**Expected Output**:

```text
Testing SQLAgent with sample data
=================================

Test 1: Simple aggregation query
Query: What was the total sales amount?
Generated SQL:
  SELECT SUM(amount) AS total_sales
  FROM analytics.fact_sales
Explanation: Calculates total sales by summing all amounts
Confidence: 0.95
Used DataPoints: ['table_fact_sales_001']
...
```

---

### 2. `sql_agent_demo.py` - Interactive SQLAgent Demo

**Purpose**: Interactive REPL for testing SQLAgent with custom queries in real-time.

**Requirements**:

- Valid OpenAI API key (set in `.env` or as environment variable)
- Installed dependencies: `openai`, `tiktoken`, `pydantic`

**Usage**:

```bash
# Make sure you're in the project root
cd /Users/onuh/Documents/Work/Open\ Source/datachat

# Set your OpenAI API key (if not in .env)
export OPENAI_API_KEY=sk-...

# Run the interactive demo
python scripts/sql_agent_demo.py
```

**What it does**:

- Starts an interactive prompt
- You type natural language queries
- SQLAgent generates SQL in real-time
- Shows SQL, explanation, confidence, timing, and tokens
- Press Ctrl+C to exit

**Example Session**:

```text
SQLAgent Interactive Demo
========================

Sample Context:
- Table: analytics.fact_sales
- Columns: customer_id, amount, date

Type your queries (Ctrl+C to exit):

Query: Show me sales over $100
Processing...

Generated SQL:
  SELECT customer_id, amount, date
  FROM analytics.fact_sales
  WHERE amount > 100

Explanation: Filters sales transactions exceeding $100
Confidence: 0.92
Execution Time: 1.2s
Tokens Used: 450

Query: _
```

---

### 3. `eval_runner.py` - Minimal RAG Evaluation

**Purpose**: Run basic retrieval + end-to-end checks against the local API.

**Usage**:

```bash
python scripts/eval_runner.py --mode retrieval --dataset eval/retrieval.json
python scripts/eval_runner.py --mode qa --dataset eval/qa.json
python scripts/eval_runner.py --mode intent --dataset eval/intent_credentials.json
python scripts/eval_runner.py --mode catalog --dataset eval/catalog/mysql_credentials.json
python scripts/eval_runner.py --mode route --dataset eval/routes_credentials.json
python scripts/eval_runner.py --mode retrieval --dataset eval/grocery/retrieval.json --min-hit-rate 0.6 --min-recall 0.5 --min-mrr 0.4
python scripts/eval_runner.py --mode qa --dataset eval/grocery/qa.json --min-sql-match-rate 0.6 --min-answer-type-rate 0.6
```

**Notes**:

- Retrieval mode uses `sources` from `/api/v1/chat` as proxies for retrieved DataPoints.
- Answer types support both API columnar payloads and row-oriented payloads.
- Route mode validates deterministic orchestration path decisions from `decision_trace`.
- Optional thresholds return non-zero exit codes to support CI gating.

### 3b. `finance_workflow_gate.py` - Finance Workflow Quality Gate

**Purpose**: Validate manual Finance Workflow v1 scorecards against release thresholds (source coverage, clarification overhead, driver quality, consistency, reproducibility).

**Usage**:

```bash
python scripts/finance_workflow_gate.py \
  --scorecard reports/finance_workflow_scorecard.csv \
  --report-json reports/finance_workflow_gate.json
```

**Input**:

- CSV scorecard with columns:
  - `prompt_id`
  - `has_source_attribution`
  - `source_count`
  - `clarification_count`
  - `driver_quality_pass`
  - `consistency_applicable`
  - `consistency_pass`
  - `reproducibility_pass`

Template: `docs/templates/finance_workflow_scorecard.csv`

### 4. `benchmark_latency_progressive.py` - Progressive Latency Benchmark

**Purpose**: Measure latency + quality guard metrics across cumulative performance stages:

- baseline
- stage1: SQL two-stage generation
- stage2: SQL prompt budget
- stage3: simple-SQL synthesis OFF
- stage4: classifier deep-gate tuning
- stage5: selective tool planner
- stage6: schema snapshot cache

**Usage**:

```bash
python scripts/benchmark_latency_progressive.py --iterations 2
python scripts/benchmark_latency_progressive.py --iterations 3 --queries-file eval/latency_queries.txt
python scripts/benchmark_latency_progressive.py --iterations 2 --mode isolated --queries-file eval/latency_queries.txt
```

**Outputs**:

- `reports/latency_progressive_<timestamp>.json`
- `reports/latency_progressive_<timestamp>.md`

### 5. `phase1_kpi_gate.py` - Phase 1 CI/Release Gates

**Purpose**: Enforce Phase 1 (core runtime) KPI checks in CI and release verification.

**Commands**:

```bash
python scripts/phase1_kpi_gate.py --mode ci
python scripts/phase1_kpi_gate.py --mode release --api-base http://localhost:8000
python scripts/phase1_kpi_gate.py --mode ci --report-json reports/phase1_ci_gate.json --report-md reports/phase1_ci_gate.md
```

**Config**: `config/phase1_kpi.json`

**Checks include**:

- core API parity test suite
- deterministic MySQL summary regressions
- connection type/url mismatch validation
- release smoke checks (health/ready/system status)
- release eval thresholds (intent + catalog)
- release eval thresholds (intent + catalog + route)
- release SLO/quality thresholds (intent latency, LLM-call budget, source accuracy, clarification match)
- connector-aware release eval preconditions (`required_database_type` + `on_missing`)

**Notes**:

- Uses your current `DATABASE_URL` / configured target DB.
- Applies stage flags via `PIPELINE_*` env vars in-process and rebuilds settings per stage.
- See `docs/LATENCY_TUNING.md` for env var guidance and rollout recommendations.

### 6. `manual_eval_runner.py` - Interactive Manual Scoring Runner

**Purpose**: Send domain question-bank prompts to `/api/v1/chat`, capture answers, and record manual rubric scores.

**Commands**:

```bash
python scripts/manual_eval_runner.py --domain grocery --mode-label without_dp_grocery --target-database <connection_id>
python scripts/manual_eval_runner.py --domain fintech --mode-label with_dp_fintech --target-database <connection_id>
python scripts/manual_eval_runner.py --domain all --no-score-prompt
```

**Inputs**:

- Question source: `docs/DOMAIN_QUESTION_BANK.md`
- Rubric guide: `docs/MANUAL_EVAL_SCORECARD.md`

**Outputs**:

- `reports/manual_eval/manual_eval_<run_id>.json`
- `reports/manual_eval/manual_eval_<run_id>.csv`

### 7. `lint_datapoints.py` - DataPoint Contract Lint

**Purpose**: Validate DataPoint metadata contracts (quality + governance fields) before sync/runtime usage.

**Usage**:

```bash
python scripts/lint_datapoints.py --path datapoints --recursive
python scripts/lint_datapoints.py --path datapoints --recursive --strict
python scripts/lint_datapoints.py --path datapoints --recursive --fail-on-warnings
```

**Notes**:

- `--strict` escalates advisory metadata gaps to errors.
- `--fail-on-warnings` is useful for tightening CI gradually.
- same contract checks are now wired into `datachat dp add` and `datachat dp sync`.

## Common Issues & Solutions

### Issue: `ModuleNotFoundError: No module named 'openai'`

**Solution**:

```bash
pip install openai tiktoken pydantic
```

### Issue: `OpenAI API key not found`

**Solution**:

```bash
# Option 1: Set environment variable
export OPENAI_API_KEY=sk-...

# Option 2: Add to .env file in project root
echo "OPENAI_API_KEY=sk-..." >> .env
```

### Issue: `FileNotFoundError: [Errno 2] No such file or directory`

**Solution**: Make sure you run scripts from the project root:

```bash
cd /Users/onuh/Documents/Work/Open\ Source/datachat
python scripts/test_sql_agent.py  # ✅ Correct
```

Not from the scripts directory:

```bash
cd scripts
python test_sql_agent.py  # ❌ Incorrect - import paths will break
```

---

## Development Notes

### Modifying Sample Data

Both scripts use minimal sample DataPoints for testing. To add more context:

**Edit the `create_sample_context()` function in either script:**

```python
def create_sample_context():
    # Add more tables
    fact_orders = SchemaDataPoint(
        datapoint_id="table_fact_orders_001",
        type="Schema",
        name="Fact Orders Table",
        table_name="analytics.fact_orders",
        # ... more fields
    )

    # Add more business definitions
    churn_rate = BusinessDataPoint(
        datapoint_id="metric_churn_001",
        type="Business",
        name="Customer Churn Rate",
        calculation="COUNT(churned) / COUNT(total)",
        # ... more fields
    )
```

### Testing Different Providers

To test with different LLM providers (Claude, Gemini, local models), modify the config:

```python
# In the script, find where SQLAgent is initialized
sql_agent = SQLAgent()  # Uses default from config

# To override, modify backend/config.py or set environment variables:
# ANTHROPIC_API_KEY=... for Claude
# GOOGLE_API_KEY=... for Gemini
```

---

## Next Steps

After manual testing, consider:

1. Adding your successful queries to unit tests
2. Creating integration tests for end-to-end pipeline
3. Building a FastAPI endpoint wrapping these agents
4. Creating a simple web UI for non-technical users

---

## Related Documentation

- [ARCHITECTURE.md](../ARCHITECTURE.md) - Full development guide
- [Backend Agents](../backend/agents/) - Agent implementations
- [Tests](../tests/) - Unit and integration tests
