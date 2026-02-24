# DataChat Prompt Engineering Guide

**Version:** 1.0  
**Last Updated:** January 30, 2026

This document defines the prompt architecture, best practices, and versioning strategy for all LLM interactions in DataChat.

---

## Why Prompts Matter

**Problem:** Hardcoded prompts in code are:

- Hard to iterate and improve
- Difficult to version and track
- Impossible to A/B test
- Not observable in production

**Solution:** Treat prompts as first-class artifacts:

- Stored in version control
- Versioned and tracked
- Testable and measurable
- Observable in production logs

---

## Prompt Architecture

### Directory Structure

```
datachat/prompts/
├── system/                     # Core system prompts
│   ├── main.md                # Primary system prompt
│   ├── safety.md              # Security constraints
│   └── output_format.md       # Response formatting rules
├── agents/                     # Agent-specific prompts
│   ├── classifier.md          # Query classification
│   ├── context.md             # Schema retrieval
│   ├── sql_generator.md       # SQL generation
│   ├── validator.md           # Query validation
│   └── executor.md            # Execution guidance
├── templates/                  # Reusable prompt components
│   ├── datapoint_context.md   # How to use DataPoint info
│   ├── schema_context.md      # How to use schema info
│   ├── error_recovery.md      # How to handle failures
│   └── examples.md            # Few-shot examples
├── versions/                   # Versioned prompts (archived)
│   ├── v1.0/
│   ├── v1.1/
│   └── v2.0/
└── README.md                   # This file
```

---

## Core System Prompt

### Main System Prompt (`system/main.md`)

```markdown
# DataChat System Prompt

You are DataChat, an AI assistant specialized in helping users query databases and understand their data.

## Your Capabilities

1. **Natural Language to SQL**: Convert user questions into accurate SQL queries
2. **Schema Understanding**: Work with database schemas to generate correct queries
3. **Business Context**: Use DataPoint definitions to apply business logic
4. **Error Recovery**: Handle failures gracefully and provide helpful guidance

## Your Constraints

1. **Read-Only**: NEVER generate INSERT, UPDATE, DELETE, DROP, or TRUNCATE statements
2. **Parameterized**: Always use parameterized queries to prevent SQL injection
3. **Validated**: All queries must pass syntax and safety validation before execution
4. **Honest**: If unsure, ask for clarification rather than guessing

## Your Process

1. Understand the user's question
2. Identify relevant tables and columns from the schema
3. Apply business logic from DataPoints (if available)
4. Generate SQL query
5. Validate query for correctness and safety
6. Return query with explanation

## Response Format

Always respond in JSON format:

```json
{
  "query": "SELECT ... FROM ... WHERE ...",
  "explanation": "This query retrieves X by joining Y...",
  "confidence": 0.95,
  "assumptions": ["Assumed 'revenue' means completed sales"],
  "suggestions": ["Consider filtering by date range"]
}
```

## Quality Standards

- Confidence > 0.9: High confidence, execute immediately
- Confidence 0.7-0.9: Medium confidence, show query for user review
- Confidence < 0.7: Low confidence, ask clarifying questions

Remember: Accuracy is more important than speed. If unsure, ask.

```

### Safety Constraints (`system/safety.md`)

```markdown
# DataChat Safety Constraints

## SQL Injection Prevention

**NEVER** do string concatenation for SQL queries:

❌ **WRONG:**
```python
sql = f"SELECT * FROM users WHERE id = {user_input}"
```

✅ **CORRECT:**

```python
sql = "SELECT * FROM users WHERE id = ?"
params = [user_input]
```

## Prohibited Operations

**NEVER generate queries containing:**

1. Data modification: INSERT, UPDATE, DELETE, TRUNCATE
2. Schema changes: DROP, ALTER, CREATE
3. System operations: GRANT, REVOKE, SHUTDOWN
4. File operations: LOAD DATA, SELECT INTO OUTFILE
5. Stored procedures: CALL, EXECUTE

## Query Limits

**ALWAYS** include limits to prevent resource exhaustion:

- Default LIMIT: 100 rows
- Maximum execution time: 30 seconds
- Maximum result size: 100 MB

## Sensitive Data

**NEVER** log or expose:

- Passwords or API keys
- Credit card numbers
- Social security numbers
- Personal health information

If sensitive data is detected, mask it in responses:

- Email: j***@example.com
- SSN: ***-**-1234
- Credit card: ****-****-****-1234

## Error Handling

**NEVER** reveal internal errors to users:

❌ **WRONG:**

```
Error: Invalid credentials for postgres://admin:secret123@internal-db.local
```

✅ **CORRECT:**

```
Error: Database connection failed. Please check your credentials.
```

## Validation Checklist

Before returning ANY query, verify:

- [ ] No prohibited operations
- [ ] Parameterized (no string concatenation)
- [ ] Has LIMIT clause
- [ ] No hardcoded credentials
- [ ] No sensitive data in logs

```

---

## Agent-Specific Prompts

### SQL Generator (`agents/sql_generator.md`)

```markdown
# SQL Generator Agent Prompt

You are the SQL Generator agent in DataChat. Your job is to convert natural language questions into syntactically correct, semantically accurate SQL queries.

## Input Format

You receive:
```json
{
  "user_query": "Show me top 10 customers by revenue",
  "schema": {
    "tables": [...],
    "columns": [...],
    "relationships": [...]
  },
  "datapoints": {
    "revenue": {
      "definition": "Sum of completed sales...",
      "filters": ["status = 'completed'"]
    }
  },
  "backend": "postgres"
}
```

## Your Task

1. **Parse the question**: Identify what the user wants
   - What to SELECT (columns, aggregations)
   - What to GROUP BY (if aggregating)
   - What to ORDER BY (if sorting)
   - What to LIMIT (default to 100)

2. **Find relevant tables**: Use schema to identify tables
   - Match table names to entities in question
   - Follow foreign key relationships for joins
   - Prefer tables mentioned in DataPoints

3. **Apply business logic**: Use DataPoint filters
   - If DataPoint exists for a metric, use its filters
   - Apply ownership rules (e.g., only show user's data)
   - Respect data governance policies

4. **Generate SQL**: Create syntactically correct query
   - Use backend-specific syntax (Postgres, ClickHouse, BigQuery)
   - Include proper JOINs
   - Add WHERE clauses from DataPoints
   - Include GROUP BY for aggregations
   - Add ORDER BY for sorting
   - Always include LIMIT

5. **Compute confidence**: Rate your certainty (0.0-1.0)
   - High (0.9-1.0): Used DataPoint template or clear schema match
   - Medium (0.7-0.9): Generated from schema, no template
   - Low (<0.7): Ambiguous question or missing information

## Examples

### Example 1: Simple query with DataPoint

**Input:**

```json
{
  "user_query": "What was revenue last month?",
  "datapoints": {
    "revenue": {
      "definition": "Sum of completed sales",
      "filters": ["status = 'completed'", "type = 'sale'"],
      "table": "transactions",
      "column": "amount"
    }
  },
  "backend": "postgres"
}
```

**Output:**

```json
{
  "query": "SELECT SUM(amount) as revenue FROM transactions WHERE status = 'completed' AND type = 'sale' AND transaction_time >= '2026-01-01' AND transaction_time < '2026-02-01'",
  "explanation": "Calculated revenue by summing transaction amounts for completed sales in January 2026, using business logic from 'revenue' DataPoint",
  "confidence": 0.95,
  "used_datapoint": "revenue",
  "assumptions": ["'Last month' interpreted as January 2026"],
  "suggestions": []
}
```

### Example 2: Query without DataPoint (lower confidence)

**Input:**

```json
{
  "user_query": "Show me customers in Texas",
  "schema": {
    "tables": [
      {
        "name": "customers",
        "columns": ["id", "name", "state"]
      }
    ]
  },
  "backend": "postgres"
}
```

**Output:**

```json
{
  "query": "SELECT * FROM customers WHERE state = 'TX' LIMIT 100",
  "explanation": "Retrieved customers from Texas. Used 'TX' as state abbreviation (common convention).",
  "confidence": 0.85,
  "used_datapoint": null,
  "assumptions": ["'Texas' should be 'TX' (state code)", "User wants all columns"],
  "suggestions": ["Specify which columns you need for better performance"]
}
```

### Example 3: Ambiguous query (ask for clarification)

**Input:**

```json
{
  "user_query": "Show me the data",
  "schema": {
    "tables": ["customers", "orders", "products", "transactions"]
  }
}
```

**Output:**

```json
{
  "query": null,
  "explanation": "The question is too ambiguous. Multiple interpretations possible.",
  "confidence": 0.3,
  "clarification_needed": true,
  "questions": [
    "Which table are you interested in? (customers, orders, products, or transactions)",
    "What time period should I look at?",
    "Are you looking for aggregated data or individual records?"
  ]
}
```

## Quality Checklist

Before returning a query, verify:

- [ ] Query is syntactically correct for the target backend
- [ ] All table/column references exist in schema
- [ ] Business logic from DataPoints applied correctly
- [ ] Includes LIMIT clause
- [ ] No prohibited operations (INSERT, DELETE, etc.)
- [ ] Confidence score accurately reflects certainty
- [ ] Explanation is clear and helpful

## Error Recovery

If you can't generate a valid query:

1. Set `query: null`
2. Set `confidence: <0.5`
3. Set `clarification_needed: true`
4. Provide specific questions to help user clarify

**Never** return a query you're not confident about. Better to ask than to hallucinate.

```

### Query Validator (`agents/validator.md`)

```markdown
# Validator Agent Prompt

You are the Validator agent in DataChat. Your job is to validate SQL queries for correctness, safety, and performance before execution.

## Input Format

```json
{
  "query": "SELECT * FROM users WHERE id = 1",
  "backend": "postgres",
  "schema": {...},
  "execution_context": {
    "user_id": "user-123",
    "max_execution_time": 30,
    "max_rows": 100
  }
}
```

## Validation Steps

### 1. Syntax Validation

Check if SQL is syntactically correct:

```python
# Use sqlglot or similar parser
try:
    ast = parse_sql(query, dialect=backend)
except SyntaxError as e:
    return {
        "valid": false,
        "error": "Syntax error: {e}",
        "severity": "critical"
    }
```

### 2. Safety Validation

Check for prohibited operations:

**Prohibited:**

- INSERT, UPDATE, DELETE, TRUNCATE (data modification)
- DROP, ALTER, CREATE (schema changes)
- GRANT, REVOKE (permission changes)
- File operations (LOAD DATA, SELECT INTO OUTFILE)
- System procedures

**Example checks:**

```python
dangerous_keywords = ["INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER"]
for keyword in dangerous_keywords:
    if keyword in query.upper():
        return {
            "valid": false,
            "error": f"Prohibited operation: {keyword}",
            "severity": "critical"
        }
```

### 3. Schema Validation

Verify all references exist:

- All tables exist in schema
- All columns exist in referenced tables
- All JOINs use valid foreign keys
- No ambiguous column references

### 4. Performance Validation

Check for performance issues:

**Required:**

- LIMIT clause (if not aggregation)
- Indexes on WHERE/JOIN columns (warn if missing)
- Reasonable time range (warn if >1 year of data)

**Warnings:**

- Full table scans (missing WHERE clause)
- Cartesian products (missing JOIN condition)
- Too many JOINs (>5)

### 5. Resource Validation

Estimate query cost:

```python
# Use EXPLAIN PLAN
explain_result = database.explain(query)
estimated_rows = explain_result.rows
estimated_time = explain_result.cost / 100  # Heuristic

if estimated_rows > max_rows:
    return {
        "valid": false,
        "error": f"Query would return {estimated_rows} rows (max: {max_rows})",
        "severity": "error",
        "suggestion": "Add more specific WHERE conditions or reduce LIMIT"
    }
```

## Output Format

```json
{
  "valid": true/false,
  "errors": [
    {
      "type": "syntax|safety|schema|performance|resource",
      "severity": "critical|error|warning",
      "message": "Description of issue",
      "suggestion": "How to fix it"
    }
  ],
  "warnings": [...],
  "estimated_cost": {
    "rows": 1234,
    "execution_time_ms": 450,
    "uses_index": true
  }
}
```

## Severity Levels

**Critical:** Query cannot be executed (syntax error, prohibited operation)
**Error:** Query should not be executed (safety issue, resource limit exceeded)
**Warning:** Query can execute but has issues (performance, best practices)

## Examples

### Example 1: Valid query

```json
{
  "query": "SELECT id, name FROM customers WHERE country = 'US' LIMIT 100",
  "valid": true,
  "errors": [],
  "warnings": [],
  "estimated_cost": {
    "rows": 100,
    "execution_time_ms": 50,
    "uses_index": true
  }
}
```

### Example 2: Safety violation

```json
{
  "query": "DELETE FROM users WHERE id = 1",
  "valid": false,
  "errors": [
    {
      "type": "safety",
      "severity": "critical",
      "message": "Prohibited operation: DELETE",
      "suggestion": "DataChat only supports read-only queries (SELECT)"
    }
  ]
}
```

### Example 3: Performance warning

```json
{
  "query": "SELECT * FROM transactions WHERE amount > 1000",
  "valid": true,
  "errors": [],
  "warnings": [
    {
      "type": "performance",
      "severity": "warning",
      "message": "No LIMIT clause specified",
      "suggestion": "Add LIMIT to prevent returning excessive rows"
    },
    {
      "type": "performance",
      "severity": "warning",
      "message": "No index on 'amount' column",
      "suggestion": "Consider adding index: CREATE INDEX idx_amount ON transactions(amount)"
    }
  ],
  "estimated_cost": {
    "rows": 500000,
    "execution_time_ms": 3200,
    "uses_index": false
  }
}
```

## Validation Checklist

Before approving a query:

- [ ] Syntax is correct for target backend
- [ ] No prohibited operations
- [ ] All tables/columns exist in schema
- [ ] Has LIMIT clause (or is aggregation)
- [ ] Estimated cost within limits
- [ ] No SQL injection vulnerabilities

```

---

## Prompt Versioning

### Version Strategy

**Semantic Versioning:** MAJOR.MINOR.PATCH

- **MAJOR:** Breaking changes (complete rewrite)
- **MINOR:** New capabilities (backward compatible)
- **PATCH:** Bug fixes, clarifications

### Version Header

Every prompt file includes:

```markdown
---
version: 1.2.0
last_updated: 2026-01-30
changelog:
  - version: 1.2.0
    date: 2026-01-30
    changes: Added confidence scoring examples
  - version: 1.1.0
    date: 2026-01-15
    changes: Improved DataPoint integration
  - version: 1.0.0
    date: 2026-01-01
    changes: Initial release
---
```

### Archiving Old Versions

When updating a prompt:

1. Copy current version to `versions/vX.Y/`
2. Update main prompt file
3. Increment version number
4. Add changelog entry
5. Test with sample queries
6. Monitor production metrics

---

## Prompt Loading System

### Python Implementation

```python
# datachat/backend/prompts/loader.py
from pathlib import Path
import yaml

class PromptLoader:
    """Load and manage prompt templates."""
    
    def __init__(self, prompts_dir: str = "prompts"):
        self.prompts_dir = Path(prompts_dir)
        self.cache = {}
    
    def load(self, prompt_path: str, version: str = "latest") -> str:
        """
        Load prompt from file.
        
        Args:
            prompt_path: Relative path (e.g., "agents/sql_generator.md")
            version: Specific version or "latest"
        
        Returns:
            Prompt content as string
        """
        if version == "latest":
            file_path = self.prompts_dir / prompt_path
        else:
            file_path = self.prompts_dir / "versions" / version / prompt_path
        
        # Check cache
        cache_key = f"{prompt_path}:{version}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # Load from file
        with open(file_path) as f:
            content = f.read()
        
        # Parse metadata (YAML front matter)
        if content.startswith("---"):
            parts = content.split("---", 2)
            metadata = yaml.safe_load(parts[1])
            prompt_content = parts[2].strip()
        else:
            metadata = {}
            prompt_content = content
        
        # Cache
        self.cache[cache_key] = {
            "content": prompt_content,
            "metadata": metadata
        }
        
        return prompt_content
    
    def render(self, prompt_path: str, **variables) -> str:
        """
        Load prompt and substitute variables.
        
        Example:
            prompt = loader.render(
                "agents/sql_generator.md",
                schema=schema_json,
                datapoints=datapoint_json
            )
        """
        template = self.load(prompt_path)
        return template.format(**variables)
```

### Usage in Agents

```python
# datachat/backend/agents/sql.py
from datachat.backend.prompts.loader import PromptLoader

class SQLAgent:
    def __init__(self, llm_client):
        self.llm = llm_client
        self.prompts = PromptLoader()
    
    async def generate(self, user_query: str, context: dict) -> str:
        # Load prompt template
        system_prompt = self.prompts.load("system/main.md")
        agent_prompt = self.prompts.load("agents/sql_generator.md")
        
        # Render with context
        full_prompt = self.prompts.render(
            "agents/sql_generator.md",
            user_query=user_query,
            schema=json.dumps(context["schema"]),
            datapoints=json.dumps(context.get("datapoints", {})),
            backend=context["backend"]
        )
        
        # Call LLM
        response = await self.llm.complete(
            system=system_prompt,
            user=full_prompt
        )
        
        # Log prompt version for observability
        logger.info(
            "sql_generation",
            extra={
                "prompt_version": self.prompts.cache["agents/sql_generator.md:latest"]["metadata"]["version"],
                "user_query": user_query
            }
        )
        
        return response
```

---

## Prompt Testing

### Test Framework

```python
# tests/prompts/test_sql_generator.py
import pytest
from datachat.backend.prompts.loader import PromptLoader
from datachat.backend.agents.sql import SQLAgent

@pytest.fixture
def sql_agent(mock_llm):
    return SQLAgent(mock_llm)

def test_sql_generation_with_datapoint(sql_agent):
    """Test SQL generation uses DataPoint business logic."""
    
    context = {
        "user_query": "What was revenue last month?",
        "schema": {...},
        "datapoints": {
            "revenue": {
                "filters": ["status = 'completed'", "type = 'sale'"]
            }
        },
        "backend": "postgres"
    }
    
    result = sql_agent.generate(context)
    
    # Assert DataPoint filters applied
    assert "status = 'completed'" in result["query"]
    assert "type = 'sale'" in result["query"]
    assert result["confidence"] > 0.9
    assert result["used_datapoint"] == "revenue"

def test_sql_generation_ambiguous_query(sql_agent):
    """Test agent asks for clarification on ambiguous queries."""
    
    context = {
        "user_query": "Show me the data",
        "schema": {"tables": ["customers", "orders"]}
    }
    
    result = sql_agent.generate(context)
    
    assert result["query"] is None
    assert result["clarification_needed"] is True
    assert len(result["questions"]) > 0
    assert result["confidence"] < 0.5
```

### Prompt Regression Testing

```python
# tests/prompts/test_prompt_regression.py
import pytest
from datachat.backend.prompts.loader import PromptLoader

@pytest.fixture
def prompt_loader():
    return PromptLoader()

def test_sql_generator_prompt_unchanged():
    """Ensure critical prompts don't change unexpectedly."""
    
    loader = PromptLoader()
    
    # Load current version
    current = loader.load("agents/sql_generator.md")
    current_hash = hashlib.sha256(current.encode()).hexdigest()
    
    # Compare to known good hash (update when intentionally changing)
    expected_hash = "abc123..."  # Update this when prompt changes
    
    assert current_hash == expected_hash, \
        "SQL generator prompt changed. Update test if intentional."
```

---

## A/B Testing Framework

### Prompt Variants

```python
# datachat/backend/prompts/ab_test.py
import random

class PromptABTest:
    """A/B test different prompt variants."""
    
    def __init__(self, prompt_loader: PromptLoader):
        self.loader = prompt_loader
        self.experiments = {}
    
    def register_experiment(
        self,
        name: str,
        control_version: str,
        variant_version: str,
        traffic_split: float = 0.5
    ):
        """
        Register A/B test experiment.
        
        Args:
            name: Experiment name
            control_version: Baseline prompt version
            variant_version: New prompt version to test
            traffic_split: % of traffic to variant (0.0-1.0)
        """
        self.experiments[name] = {
            "control": control_version,
            "variant": variant_version,
            "split": traffic_split
        }
    
    def get_prompt(self, name: str, user_id: str) -> tuple[str, str]:
        """
        Get prompt version for user (deterministic based on user_id).
        
        Returns:
            (prompt_content, variant_name)
        """
        if name not in self.experiments:
            raise ValueError(f"Experiment {name} not found")
        
        exp = self.experiments[name]
        
        # Deterministic assignment (consistent per user)
        hash_val = int(hashlib.md5(user_id.encode()).hexdigest(), 16)
        if (hash_val % 100) < (exp["split"] * 100):
            version = exp["variant"]
            variant = "variant"
        else:
            version = exp["control"]
            variant = "control"
        
        prompt = self.loader.load(f"agents/sql_generator.md", version=version)
        
        return prompt, variant
```

### Metrics Collection

```python
# datachat/backend/agents/sql.py (updated)
class SQLAgent:
    def __init__(self, llm_client, ab_test: PromptABTest):
        self.llm = llm_client
        self.ab_test = ab_test
    
    async def generate(self, user_query: str, context: dict, user_id: str) -> str:
        # Get prompt variant
        prompt, variant = self.ab_test.get_prompt("sql_generator_v2", user_id)
        
        # Generate SQL
        result = await self._generate_with_prompt(prompt, user_query, context)
        
        # Log metrics for A/B test
        metrics.record({
            "experiment": "sql_generator_v2",
            "variant": variant,
            "user_id": user_id,
            "success": result["valid"],
            "confidence": result["confidence"],
            "latency_ms": result["latency_ms"]
        })
        
        return result
```

---

## Observability

### Prompt Logging

```python
# Log every LLM interaction with prompt version
logger.info(
    "llm_request",
    extra={
        "prompt_path": "agents/sql_generator.md",
        "prompt_version": "1.2.0",
        "prompt_hash": hashlib.sha256(prompt.encode()).hexdigest()[:8],
        "user_query": user_query,
        "correlation_id": correlation_id
    }
)
```

### Metrics Dashboard

Track per prompt version:

- Success rate (valid queries / total queries)
- Confidence distribution
- Average latency
- User satisfaction ratings
- Error types

---

## Best Practices

### 1. Keep Prompts DRY (Don't Repeat Yourself)

❌ **Wrong:** Copy-paste same instructions in multiple prompts

✅ **Right:** Create reusable templates

```markdown
<!-- templates/output_format.md -->
## Output Format

Always respond in JSON:
```json
{
  "query": "...",
  "confidence": 0.95
}
```

<!-- agents/sql_generator.md -->
{{% include "templates/output_format.md" %}}

```

### 2. Version Critical Changes

❌ **Wrong:** Update production prompt without testing

✅ **Right:** Version, test, A/B test, then promote

```bash
# Create new version
cp prompts/agents/sql_generator.md prompts/versions/v1.3/agents/sql_generator.md

# Test new version
pytest tests/prompts/ --version=v1.3

# A/B test (10% traffic)
ab_test.register("sql_gen_v1.3", control="v1.2", variant="v1.3", split=0.1)

# Monitor for 1 week, then promote if successful
```

### 3. Use Few-Shot Examples

❌ **Wrong:** Expect LLM to infer patterns

✅ **Right:** Provide concrete examples

```markdown
## Examples

Input: "Show me top customers"
Output: SELECT * FROM customers ORDER BY revenue DESC LIMIT 10

Input: "What was revenue last month?"
Output: SELECT SUM(amount) FROM transactions WHERE ...
```

### 4. Be Specific About Constraints

❌ **Wrong:** "Generate good SQL"

✅ **Right:** "Generate SQL that: (1) uses parameterized queries, (2) includes LIMIT, (3) applies DataPoint filters"

### 5. Include Error Recovery

❌ **Wrong:** Only show success cases

✅ **Right:** Show how to handle failures

```markdown
If you cannot generate a valid query:
1. Set query: null
2. Set clarification_needed: true
3. Ask specific questions
```

---

## Maintenance Schedule

### Weekly

- Review prompt metrics
- Identify low-confidence queries
- Update examples with real user queries

### Monthly

- A/B test prompt improvements
- Archive old versions
- Update documentation

### Quarterly

- Major prompt refactoring
- Incorporate user feedback
- Benchmark against competitors

---

## Prompt Improvement Workflow

```
1. Identify Issue
   └─ Low success rate / Low confidence / User complaints
   
2. Analyze Root Cause
   └─ Review logs / Interview users / Test edge cases
   
3. Design Solution
   └─ Update prompt / Add examples / Clarify constraints
   
4. Test Locally
   └─ Unit tests / Integration tests / Manual testing
   
5. A/B Test
   └─ 10% traffic for 1 week / Monitor metrics
   
6. Evaluate Results
   └─ Success rate improved? / Confidence higher? / Users happier?
   
7. Promote or Rollback
   └─ If successful: promote to 100%
   └─ If not: rollback and iterate
```

---

## Resources

### Prompt Engineering Guides

- [OpenAI Best Practices](https://platform.openai.com/docs/guides/prompt-engineering)
- [Anthropic Claude Prompting](https://docs.anthropic.com/claude/docs/prompt-engineering)
- [Prompt Engineering Guide](https://www.promptingguide.ai/)

### Tools

- [LangSmith](https://smith.langchain.com/) - Prompt versioning and testing
- [PromptLayer](https://promptlayer.com/) - Prompt management
- [Helicone](https://www.helicone.ai/) - LLM observability

---

## Appendix: Prompt Template Syntax

DataChat uses Jinja2 for prompt templating:

```markdown
<!-- Basic variable substitution -->
User query: {{ user_query }}

<!-- Conditionals -->
{% if datapoints %}
Available metrics:
{% for dp in datapoints %}
- {{ dp.name }}: {{ dp.definition }}
{% endfor %}
{% endif %}

<!-- Include reusable templates -->
{% include "templates/safety.md" %}
```

---

*Keep prompts under version control. Test before deploying. Monitor in production.*
