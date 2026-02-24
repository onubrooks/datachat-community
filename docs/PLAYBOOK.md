# DataChat Development Playbook

This playbook contains implementation patterns, coding standards, and development workflows for building DataChat.

---

## Core Development Principles

### 1. Security First

Every feature must pass security review before merging:

- SQL injection prevention (parameterized queries only)
- Input validation at every boundary
- Principle of least privilege (read-only by default)
- Audit logging for all data access

### 2. Performance by Default

Assume scale from day one:

- Cache aggressively (query results, LLM responses, schema metadata)
- Implement rate limiting (prevent abuse)
- Use background jobs for expensive operations
- Monitor query execution times

### 3. Fail Gracefully

Never crash on user input:

- Validate all inputs with Pydantic models
- Return structured errors with helpful messages
- Log failures with full context for debugging
- Retry transient failures (LLM timeouts, DB connection issues)

### 4. Observable Systems

Instrument everything:

- Structured logging (JSON format)
- Correlation IDs across components
- Performance metrics (latency, throughput, errors)
- User analytics (feature usage, success rates)

### 5. Deterministic-First Catalog Flows

For schema/shape intents, do not start with LLM generation:

- Detect deterministic intents first: list tables, list columns, sample rows, row counts.
- Use system-catalog query templates per engine (`postgresql/mysql/clickhouse/bigquery/redshift`).
- Only call the SQL-generation LLM when deterministic planning is not applicable.
- If required slots are missing (for example table name), return targeted clarifying questions.
- Pass compact ranked schema context into LLM prompts when the flow continues to generation.

---

## Code Organization Patterns

### Agent Pattern

```python
# datachat/backend/agents/base.py
from abc import ABC, abstractmethod
from pydantic import BaseModel
from typing import Generic, TypeVar

TInput = TypeVar("TInput", bound=BaseModel)
TOutput = TypeVar("TOutput", bound=BaseModel)

class BaseAgent(ABC, Generic[TInput, TOutput]):
    """
    Base class for all agents in DataChat.
    
    Agents are single-responsibility components that perform one task well.
    They receive typed input, perform processing, and return typed output.
    """
    
    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    async def execute(self, input_data: TInput) -> TOutput:
        """
        Execute the agent's primary function.
        
        Must be idempotent where possible.
        Must not have side effects beyond logging/metrics.
        """
        pass
    
    async def __call__(self, input_data: TInput) -> TOutput:
        """
        Convenience wrapper that adds instrumentation.
        """
        start = time.time()
        correlation_id = input_data.correlation_id
        
        self.logger.info(
            "agent_start",
            extra={
                "agent": self.__class__.__name__,
                "correlation_id": correlation_id,
            }
        )
        
        try:
            result = await self.execute(input_data)
            
            self.logger.info(
                "agent_complete",
                extra={
                    "agent": self.__class__.__name__,
                    "correlation_id": correlation_id,
                    "duration_ms": (time.time() - start) * 1000,
                }
            )
            
            return result
            
        except Exception as e:
            self.logger.error(
                "agent_error",
                extra={
                    "agent": self.__class__.__name__,
                    "correlation_id": correlation_id,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise
```

**Example Usage:**

```python
# datachat/backend/agents/sql.py
class SQLGenerationInput(BaseModel):
    user_query: str
    context: dict
    backend: str
    correlation_id: str

class SQLGenerationOutput(BaseModel):
    sql: str
    confidence: float
    used_template: bool
    datapoint_id: Optional[str]
    correlation_id: str

class SQLAgent(BaseAgent[SQLGenerationInput, SQLGenerationOutput]):
    """Generates SQL from natural language queries."""
    
    def __init__(self, config: dict, llm_client: LLMClient, metric_registry: MetricRegistry):
        super().__init__(config)
        self.llm = llm_client
        self.registry = metric_registry
    
    async def execute(self, input_data: SQLGenerationInput) -> SQLGenerationOutput:
        # Check if query matches known metric (Level 3)
        metric = self._identify_metric(input_data.user_query)
        
        if metric:
            # Use template (fast, consistent)
            sql = self._compile_template(metric, input_data)
            return SQLGenerationOutput(
                sql=sql,
                confidence=0.95,
                used_template=True,
                datapoint_id=metric.id,
                correlation_id=input_data.correlation_id,
            )
        else:
            # Generate with LLM (flexible)
            sql = await self._generate_with_llm(input_data)
            return SQLGenerationOutput(
                sql=sql,
                confidence=self._compute_confidence(sql, input_data.context),
                used_template=False,
                datapoint_id=None,
                correlation_id=input_data.correlation_id,
            )
```

---

### DataPoint Pattern

```python
# datachat/backend/knowledge/datapoints.py
from pathlib import Path
from typing import Optional, List
import yaml
from pydantic import BaseModel, validator

class DataPointExecution(BaseModel):
    """Level 3: Executable DataPoint specification."""
    sql_template: str
    parameters: dict
    backend_variants: Optional[dict] = None

class DataPointMaterialization(BaseModel):
    """Level 4: Materialization configuration."""
    strategy: str  # adaptive | manual
    granularity: Optional[str] = None
    refresh_interval: Optional[str] = None
    partition_by: Optional[str] = None

class DataPointIntelligence(BaseModel):
    """Level 5: Intelligence configuration."""
    sla: Optional[dict] = None
    anomaly_detection: Optional[dict] = None
    auto_remediation: Optional[List[dict]] = None

class DataPointRelationships(BaseModel):
    """Level 5: Knowledge graph relationships."""
    depends_on: Optional[List[dict]] = None
    impacts: Optional[List[dict]] = None
    related_systems: Optional[List[dict]] = None

class DataPoint(BaseModel):
    """
    Universal DataPoint model supporting all levels (1-5).
    
    Type 1 (Level 2): Just definition + metadata
    Type 2 (Level 3): Adds execution block
    Type 3 (Level 4): Adds materialization
    Type 4 (Level 5): Adds intelligence + relationships
    """
    id: str
    name: str
    type: str  # concept | metric | dimension | entity
    definition: str
    owner: str
    tags: List[str] = []
    
    # Level 2+
    data_sources: Optional[List[dict]] = None
    related_concepts: Optional[List[str]] = None
    documentation_url: Optional[str] = None
    
    # Level 3+
    execution: Optional[DataPointExecution] = None
    
    # Level 4+
    materialization: Optional[DataPointMaterialization] = None
    
    # Level 5+
    intelligence: Optional[DataPointIntelligence] = None
    relationships: Optional[DataPointRelationships] = None
    
    @validator("id")
    def validate_id(cls, v):
        """Ensure ID is valid (alphanumeric + underscores)."""
        if not re.match(r"^[a-z0-9_]+$", v):
            raise ValueError("ID must be lowercase alphanumeric with underscores")
        return v
    
    @property
    def level(self) -> int:
        """Determine which level this DataPoint implements."""
        if self.relationships or self.intelligence:
            return 5
        if self.materialization:
            return 4
        if self.execution:
            return 3
        if self.data_sources:
            return 2
        return 1

class DataPointLoader:
    """Loads and validates DataPoints from YAML files."""
    
    def __init__(self, user_dir: str = "datapoints/user", managed_dir: str = "datapoints/managed"):
        self.user_dir = Path(user_dir)
        self.managed_dir = Path(managed_dir)
        self.datapoints: dict[str, DataPoint] = {}
    
    def load_all(self) -> dict[str, DataPoint]:
        """Load all DataPoints from user and managed directories."""
        # Load user DataPoints (higher priority)
        for yaml_file in self.user_dir.rglob("*.yaml"):
            dp = self._load_file(yaml_file)
            self.datapoints[dp.id] = dp
        
        # Load managed DataPoints (lower priority, don't override)
        for yaml_file in self.managed_dir.rglob("*.yaml"):
            dp = self._load_file(yaml_file)
            if dp.id not in self.datapoints:
                self.datapoints[dp.id] = dp
        
        return self.datapoints
    
    def _load_file(self, path: Path) -> DataPoint:
        """Load and validate single DataPoint file."""
        try:
            with open(path) as f:
                data = yaml.safe_load(f)
            
            # Extract datapoint section
            dp_data = data.get("datapoint", {})
            
            # Validate with Pydantic
            return DataPoint(**dp_data)
            
        except Exception as e:
            raise ValueError(f"Failed to load DataPoint from {path}: {e}")
    
    def get(self, datapoint_id: str) -> Optional[DataPoint]:
        """Retrieve DataPoint by ID."""
        return self.datapoints.get(datapoint_id)
    
    def search(self, query: str) -> List[DataPoint]:
        """Search DataPoints by name, definition, or tags."""
        query_lower = query.lower()
        results = []
        
        for dp in self.datapoints.values():
            if (query_lower in dp.name.lower() or
                query_lower in dp.definition.lower() or
                any(query_lower in tag.lower() for tag in dp.tags)):
                results.append(dp)
        
        return results
```

---

### Tool Pattern

```python
# datachat/backend/tools/base.py
from typing import Callable, Optional
from pydantic import BaseModel
from enum import Enum

class ToolCategory(str, Enum):
    DATABASE = "database"
    FILESYSTEM = "filesystem"
    ANALYSIS = "analysis"
    EXTERNAL = "external"

class ToolPolicy(BaseModel):
    """Policy configuration for tool execution."""
    enabled: bool = True
    requires_approval: bool = False
    max_execution_time_seconds: int = 30
    allowed_users: Optional[List[str]] = None
    rate_limit_per_minute: Optional[int] = None

class ToolDefinition(BaseModel):
    """Tool metadata and configuration."""
    name: str
    description: str
    category: ToolCategory
    policy: ToolPolicy
    parameters_schema: dict  # JSON Schema
    return_schema: dict      # JSON Schema

class ToolContext(BaseModel):
    """Context provided to tool during execution."""
    user_id: str
    correlation_id: str
    approved: bool = False
    
    def log_action(self, action: str, metadata: dict):
        """Log tool action for audit trail."""
        logger.info(
            "tool_action",
            extra={
                "user_id": self.user_id,
                "correlation_id": self.correlation_id,
                "action": action,
                "metadata": metadata,
            }
        )

def tool(
    name: str,
    description: str,
    category: ToolCategory,
    requires_approval: bool = False,
    **policy_kwargs
):
    """
    Decorator to register a function as a DataChat tool.
    
    Example:
        @tool(
            name="query_database",
            description="Execute read-only SQL query",
            category=ToolCategory.DATABASE,
            requires_approval=False,
        )
        def query_database(sql: str, database: str, ctx: ToolContext) -> dict:
            # Implementation
            pass
    """
    def decorator(func: Callable):
        # Create tool definition
        tool_def = ToolDefinition(
            name=name,
            description=description,
            category=category,
            policy=ToolPolicy(requires_approval=requires_approval, **policy_kwargs),
            parameters_schema=_extract_parameters_schema(func),
            return_schema=_extract_return_schema(func),
        )
        
        # Register tool
        ToolRegistry.register(tool_def, func)
        
        return func
    
    return decorator

# Example tool implementation
@tool(
    name="query_database",
    description="Execute read-only SQL query",
    category=ToolCategory.DATABASE,
    requires_approval=False,
    max_execution_time_seconds=30,
)
def query_database(sql: str, database: str, ctx: ToolContext) -> dict:
    """
    Execute a read-only SQL query.
    
    Args:
        sql: SQL query to execute (SELECT only)
        database: Database ID from registry
        ctx: Tool execution context
    
    Returns:
        {"rows": [...], "columns": [...], "row_count": N}
    """
    # Validate SQL is read-only
    if not is_read_only(sql):
        raise ValueError("Only SELECT queries allowed")
    
    # Get database connector
    connector = DatabaseRegistry.get(database)
    
    # Execute with timeout
    results = connector.execute(sql, timeout=30)
    
    # Audit log
    ctx.log_action("query_executed", {
        "database": database,
        "row_count": len(results),
        "query_hash": hashlib.sha256(sql.encode()).hexdigest(),
    })
    
    return {
        "rows": results.to_dict(orient="records"),
        "columns": results.columns.tolist(),
        "row_count": len(results),
    }
```

---

## Testing Patterns

### Unit Testing

```python
# tests/unit/agents/test_sql_agent.py
import pytest
from datachat.backend.agents.sql import SQLAgent, SQLGenerationInput

@pytest.fixture
def sql_agent(mock_llm_client, mock_metric_registry):
    """Create SQLAgent with mocked dependencies."""
    return SQLAgent(
        config={"max_retries": 3},
        llm_client=mock_llm_client,
        metric_registry=mock_metric_registry,
    )

@pytest.mark.asyncio
async def test_sql_generation_with_template(sql_agent):
    """Test SQL generation using DataPoint template."""
    # Arrange
    input_data = SQLGenerationInput(
        user_query="What was revenue last month?",
        context={"schema": {...}},
        backend="postgres",
        correlation_id="test-123",
    )
    
    # Act
    output = await sql_agent.execute(input_data)
    
    # Assert
    assert output.used_template is True
    assert output.confidence > 0.9
    assert "SELECT SUM(amount)" in output.sql
    assert output.datapoint_id == "revenue"

@pytest.mark.asyncio
async def test_sql_generation_without_template(sql_agent):
    """Test SQL generation using LLM when no template matches."""
    input_data = SQLGenerationInput(
        user_query="Show me customers in Texas",
        context={"schema": {...}},
        backend="postgres",
        correlation_id="test-456",
    )
    
    output = await sql_agent.execute(input_data)
    
    assert output.used_template is False
    assert 0.5 < output.confidence < 1.0
    assert "SELECT" in output.sql
    assert output.datapoint_id is None
```

### Integration Testing

```python
# tests/integration/test_pipeline.py
import pytest
from datachat.backend.pipeline.orchestrator import PipelineOrchestrator

@pytest.mark.integration
async def test_end_to_end_query_execution(test_database):
    """Test complete pipeline from query to results."""
    # Arrange
    pipeline = PipelineOrchestrator(config=test_config)
    
    # Act
    result = await pipeline.execute_query(
        user_query="Show me top 10 customers by revenue",
        database_id=test_database.id,
        user_id="test-user",
    )
    
    # Assert
    assert result.success is True
    assert len(result.rows) == 10
    assert "revenue" in result.columns
    assert result.execution_time_ms < 5000
    
    # Verify audit log was created
    audit_logs = AuditLogger.get_logs(user_id="test-user")
    assert len(audit_logs) == 1
    assert audit_logs[0].query_type == "SELECT"
```

### Security Testing

```python
# tests/security/test_sql_injection.py
import pytest
from datachat.backend.security.validator import SQLValidator

@pytest.mark.security
def test_sql_injection_detection():
    """Test that SQL injection attempts are detected."""
    validator = SQLValidator()
    
    malicious_queries = [
        "SELECT * FROM users; DROP TABLE users;--",
        "SELECT * FROM users WHERE id = 1 OR 1=1",
        "SELECT * FROM users UNION SELECT * FROM passwords",
        "'; DELETE FROM users WHERE '1'='1",
    ]
    
    for sql in malicious_queries:
        with pytest.raises(SecurityError):
            validator.validate(sql)

@pytest.mark.security
def test_parameterized_queries_safe():
    """Test that parameterized queries are safe."""
    validator = SQLValidator()
    
    # Should pass - properly parameterized
    sql = "SELECT * FROM users WHERE id = ?"
    params = ["1 OR 1=1"]  # Injection attempt in param (safe)
    
    validator.validate(sql)  # Should not raise
```

---

## Performance Optimization Patterns

### Caching Strategy

```python
# datachat/backend/cache/manager.py
from functools import wraps
import hashlib
import json

class CacheManager:
    """Multi-tier caching for DataChat."""
    
    def __init__(self, redis_client, ttl_seconds: int = 300):
        self.redis = redis_client
        self.ttl = ttl_seconds
        self.memory_cache = {}  # L1: In-memory
    
    def cache_key(self, prefix: str, *args, **kwargs) -> str:
        """Generate cache key from function arguments."""
        data = json.dumps([args, kwargs], sort_keys=True)
        hash_val = hashlib.sha256(data.encode()).hexdigest()
        return f"{prefix}:{hash_val}"
    
    def get(self, key: str):
        """Get from L1 (memory) then L2 (Redis)."""
        # L1: Memory cache
        if key in self.memory_cache:
            return self.memory_cache[key]
        
        # L2: Redis
        value = self.redis.get(key)
        if value:
            # Promote to L1
            self.memory_cache[key] = value
            return value
        
        return None
    
    def set(self, key: str, value, ttl: int = None):
        """Set in both L1 and L2."""
        ttl = ttl or self.ttl
        
        # L1: Memory
        self.memory_cache[key] = value
        
        # L2: Redis
        self.redis.setex(key, ttl, value)

def cached(prefix: str, ttl: int = 300):
    """Decorator to cache function results."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key
            key = cache_manager.cache_key(prefix, *args, **kwargs)
            
            # Check cache
            cached_result = cache_manager.get(key)
            if cached_result:
                return cached_result
            
            # Execute function
            result = await func(*args, **kwargs)
            
            # Store in cache
            cache_manager.set(key, result, ttl)
            
            return result
        
        return wrapper
    return decorator

# Usage example
@cached(prefix="sql_generation", ttl=3600)
async def generate_sql(user_query: str, context: dict) -> str:
    """Generate SQL with caching (1 hour TTL)."""
    # Expensive LLM call
    return await llm_client.generate(user_query, context)
```

### Query Optimization

```python
# datachat/backend/apex/optimizer.py
from sqlglot import parse_one, exp

class QueryOptimizer:
    """Optimize generated SQL before execution."""
    
    def optimize(self, sql: str, backend: str) -> str:
        """
        Apply optimization rules to SQL query.
        
        Rules:
        1. Add LIMIT if missing (prevent full table scans)
        2. Push down filters (WHERE before JOIN)
        3. Remove redundant subqueries
        4. Add indexes hints (backend-specific)
        """
        ast = parse_one(sql, dialect=backend)
        
        # Rule 1: Add LIMIT
        if not self._has_limit(ast):
            ast = self._add_default_limit(ast, limit=10000)
        
        # Rule 2: Push down filters
        ast = self._push_down_filters(ast)
        
        # Rule 3: Remove redundant subqueries
        ast = self._flatten_subqueries(ast)
        
        # Rule 4: Add index hints (if available)
        ast = self._add_index_hints(ast, backend)
        
        return ast.sql(dialect=backend)
    
    def _has_limit(self, ast: exp.Expression) -> bool:
        """Check if query has LIMIT clause."""
        for node in ast.walk():
            if isinstance(node, exp.Limit):
                return True
        return False
    
    def _add_default_limit(self, ast: exp.Expression, limit: int) -> exp.Expression:
        """Add LIMIT clause to prevent full table scans."""
        return ast.limit(limit)
    
    def estimate_cost(self, sql: str, backend: str) -> dict:
        """
        Estimate query execution cost.
        
        Uses EXPLAIN PLAN to estimate:
        - Row count
        - Execution time
        - Memory usage
        """
        connector = DatabaseRegistry.get(backend)
        explain_result = connector.explain(sql)
        
        return {
            "estimated_rows": explain_result.get("rows", 0),
            "estimated_cost": explain_result.get("cost", 0),
            "uses_index": "Index Scan" in str(explain_result),
        }
```

---

## Error Handling Patterns

### Structured Errors

```python
# datachat/backend/errors.py
from enum import Enum

class ErrorCode(str, Enum):
    # LLM errors
    LLM_TIMEOUT = "llm_timeout"
    LLM_INVALID_RESPONSE = "llm_invalid_response"
    LLM_RATE_LIMIT = "llm_rate_limit"
    
    # SQL errors
    SQL_SYNTAX_ERROR = "sql_syntax_error"
    SQL_INJECTION_DETECTED = "sql_injection_detected"
    SQL_TIMEOUT = "sql_timeout"
    SQL_PERMISSION_DENIED = "sql_permission_denied"
    
    # DataPoint errors
    DATAPOINT_NOT_FOUND = "datapoint_not_found"
    DATAPOINT_INVALID = "datapoint_invalid"
    
    # System errors
    DATABASE_CONNECTION_FAILED = "database_connection_failed"
    INTERNAL_ERROR = "internal_error"

class DataChatError(Exception):
    """Base exception for DataChat errors."""
    
    def __init__(
        self,
        code: ErrorCode,
        message: str,
        details: Optional[dict] = None,
        user_message: Optional[str] = None,
    ):
        self.code = code
        self.message = message
        self.details = details or {}
        self.user_message = user_message or self._default_user_message()
        super().__init__(message)
    
    def _default_user_message(self) -> str:
        """Generate user-friendly error message."""
        messages = {
            ErrorCode.LLM_TIMEOUT: "The AI service took too long to respond. Please try again.",
            ErrorCode.SQL_SYNTAX_ERROR: "I couldn't generate valid SQL. Could you rephrase your question?",
            ErrorCode.DATAPOINT_NOT_FOUND: "I couldn't find a metric definition for this query.",
        }
        return messages.get(self.code, "An error occurred. Please try again.")
    
    def to_dict(self) -> dict:
        """Serialize error for API response."""
        return {
            "error": {
                "code": self.code,
                "message": self.user_message,
                "details": self.details,
            }
        }

# Usage example
try:
    sql = await generate_sql(user_query, context)
except LLMTimeoutException as e:
    raise DataChatError(
        code=ErrorCode.LLM_TIMEOUT,
        message=f"LLM timeout after {e.timeout}s",
        details={"timeout": e.timeout, "provider": e.provider},
    )
```

### Retry Logic

```python
# datachat/backend/utils/retry.py
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((LLMTimeoutException, DatabaseConnectionError)),
)
async def execute_with_retry(func, *args, **kwargs):
    """
    Execute function with exponential backoff retry.
    
    Retries 3 times with delays: 1s, 2s, 4s
    Only retries on transient errors (timeout, connection)
    """
    return await func(*args, **kwargs)
```

---

## Monitoring & Observability

### Structured Logging

```python
# datachat/backend/logging.py
import logging
import json
from datetime import datetime

class JSONFormatter(logging.Formatter):
    """Format logs as JSON for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": getattr(record, "correlation_id", None),
            "user_id": getattr(record, "user_id", None),
        }
        
        # Add extra fields
        if hasattr(record, "extra"):
            log_data.update(record.extra)
        
        # Add exception info
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("datachat.log"),
    ],
)

# Apply JSON formatter
for handler in logging.root.handlers:
    handler.setFormatter(JSONFormatter())
```

### Metrics Collection

```python
# datachat/backend/metrics.py
from prometheus_client import Counter, Histogram, Gauge

# Counters
queries_total = Counter(
    "datachat_queries_total",
    "Total number of queries processed",
    ["status", "database_type"],
)

# Histograms
query_duration_seconds = Histogram(
    "datachat_query_duration_seconds",
    "Time spent processing queries",
    ["stage"],  # generation, validation, execution
)

# Gauges
active_users = Gauge(
    "datachat_active_users",
    "Number of active users",
)

# Usage example
with query_duration_seconds.labels(stage="generation").time():
    sql = await generate_sql(user_query, context)

queries_total.labels(status="success", database_type="postgres").inc()
```

---

## Development Workflow

### 1. Feature Development

```bash
# Create feature branch
git checkout -b feature/level-4-materialization

# Make changes
# Write tests
# Update documentation

# Run tests locally
pytest tests/ -v

# Run security checks
bandit -r datachat/

# Run linters
black datachat/ tests/
ruff datachat/ tests/

# Commit with conventional commit message
git commit -m "feat(apex): implement adaptive materialization"

# Push and create PR
git push origin feature/level-4-materialization
```

### 2. Code Review Checklist

- [ ] Code follows project patterns (Agent, Tool, DataPoint)
- [ ] All inputs validated with Pydantic models
- [ ] Security considerations addressed (SQL injection, auth, etc.)
- [ ] Performance considerations addressed (caching, indexing, etc.)
- [ ] Unit tests added (>80% coverage)
- [ ] Integration tests added for critical paths
- [ ] Error handling implemented with structured errors
- [ ] Logging/metrics added for observability
- [ ] Documentation updated (docstrings, ARCHITECTURE.md, etc.)

### 3. Testing Strategy

```bash
# Unit tests (fast, isolated)
pytest tests/unit/ -v

# Integration tests (slower, requires test DB)
pytest tests/integration/ -v --db-url=postgres://test

# Security tests
pytest tests/security/ -v

# Performance tests (load testing)
pytest tests/performance/ -v --benchmark-only

# Full test suite
pytest tests/ -v --cov=datachat --cov-report=html
```

### 4. Deployment

```bash
# Build Docker image
docker build -t datachat:v1.0.0 .

# Run migrations
docker exec datachat python scripts/migrate.py

# Deploy (internal testing)
docker-compose up -d

# Smoke tests
curl http://localhost:8000/health
datachat query "SELECT 1"

# Monitor logs
docker logs -f datachat

# Monitor metrics
open http://localhost:9090  # Prometheus
```

---

## Common Patterns & Recipes

### Pattern: Context Merging (Level 2)

```python
def merge_context(managed: ManagedDataPoint, user: DataPoint) -> dict:
    """
    Merge ManagedDataPoint (schema) with UserDataPoint (business logic).
    
    Priority: User context > Managed context
    """
    return {
        # Schema from managed
        "tables": managed.tables,
        "columns": managed.columns,
        "relationships": managed.relationships,
        
        # Business logic from user
        "filters": user.data_sources[0].get("filters", []),
        "owner": user.owner,
        "definition": user.definition,
        
        # Merged
        "all_tags": list(set(managed.tags + user.tags)),
    }
```

### Pattern: Template Compilation (Level 3)

```python
def compile_template(datapoint: DataPoint, user_query: str, backend: str) -> str:
    """
    Compile DataPoint SQL template with user parameters.
    
    1. Extract parameters from user query (time range, filters)
    2. Select backend-specific template variant
    3. Substitute parameters into template
    4. Validate resulting SQL
    """
    # Extract parameters
    params = extract_parameters(user_query)
    
    # Select template
    template = datapoint.execution.backend_variants.get(
        backend,
        datapoint.execution.sql_template,
    )
    
    # Substitute
    sql = template.format(**params)
    
    # Validate
    validator.validate(sql)
    
    return sql
```

### Pattern: Anomaly Detection (Level 5)

```python
def detect_anomalies(datapoint: DataPoint, values: List[float]) -> List[Anomaly]:
    """
    Detect anomalies using ensemble approach.
    
    1. Statistical Process Control (SPC)
    2. Prophet forecasting
    3. Isolation Forest (ML)
    
    Returns anomalies confirmed by 2+ algorithms.
    """
    # Algorithm 1: SPC
    spc_anomalies = spc_detection(values, sensitivity=0.05)
    
    # Algorithm 2: Prophet
    prophet_anomalies = prophet_detection(values)
    
    # Algorithm 3: Isolation Forest
    ml_anomalies = isolation_forest_detection(values)
    
    # Ensemble: Require 2+ algorithms to agree
    confirmed = []
    for anomaly in spc_anomalies:
        agreement_count = 1
        if anomaly in prophet_anomalies:
            agreement_count += 1
        if anomaly in ml_anomalies:
            agreement_count += 1
        
        if agreement_count >= 2:
            confirmed.append(anomaly)
    
    return confirmed
```

---

## Quick Reference

### Key Files

- `ARCHITECTURE.md` - Architecture & system design
- `PLAYBOOK.md` - Implementation patterns (this file)
- `PRD.md` - Product requirements
- `DATAPOINT_SCHEMA.md` - DataPoint specification

### Important Commands

```bash
# Development
datachat serve                        # Start server
datachat profile <db>                 # Profile database
datachat datapoint validate <file>    # Validate DataPoint

# Testing
pytest tests/unit/                    # Unit tests
pytest tests/integration/             # Integration tests
pytest tests/security/                # Security tests

# Deployment
docker-compose up -d                  # Start services
docker logs -f datachat               # View logs
```

### Code Locations

- Agents: `datachat/backend/agents/`
- DataPoints: `datachat/backend/knowledge/`
- APEX Engine: `datachat/backend/apex/`
- Tools: `datachat/backend/tools/`
- Security: `datachat/backend/security/`

---

*Keep this playbook updated as patterns evolve. When you discover a better way, document it here.*
