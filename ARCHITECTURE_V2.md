# DataChat Architecture v2.0

Enhanced architecture with multi-database support, auto-initialization, and intelligent DataPoint generation.

---

## Core Improvements

### 1. System Initialization Flow

```
┌─────────────────────────────────────────────────────────────┐
│ User First Access (Web UI / CLI / API)                      │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
              ┌───────────────┐
              │ Check Status  │
              └───────┬───────┘
                      │
         ┌────────────┴────────────┐
         │                         │
         ▼                         ▼
    [Configured]              [Empty State]
         │                         │
         ▼                         ▼
    Normal Flow         ┌──────────────────────┐
                        │ Initialization       │
                        │ Wizard               │
                        └──────────┬───────────┘
                                   │
                        ┌──────────┴───────────┐
                        │ 1. Add Database(s)   │
                        │ 2. Profile Schema    │
                        │ 3. Generate DataPts  │
                        │ 4. Review & Confirm  │
                        │ 5. Sync Knowledge    │
                        └──────────┬───────────┘
                                   │
                                   ▼
                              [Ready to Query]
```

### 2. Multi-Database Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ DataChat System                                              │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │ Database Registry                                   │    │
│  ├────────────────────────────────────────────────────┤    │
│  │ - connection_id: "prod_analytics"                  │    │
│  │ - connection_id: "staging_db"                      │    │
│  │ - connection_id: "warehouse"                       │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │ Knowledge Base (per database)                       │    │
│  ├────────────────────────────────────────────────────┤    │
│  │ prod_analytics:                                     │    │
│  │   - Vector Store (embeddings)                      │    │
│  │   - Knowledge Graph (schema + relationships)       │    │
│  │   - DataPoints (Schema, Business, Process)         │    │
│  │                                                     │    │
│  │ staging_db:                                         │    │
│  │   - Vector Store                                    │    │
│  │   - Knowledge Graph                                 │    │
│  │   - DataPoints                                      │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │ Query Router                                        │    │
│  ├────────────────────────────────────────────────────┤    │
│  │ Detects which database(s) to query based on:       │    │
│  │ - Explicit mention ("in prod_analytics")           │    │
│  │ - Context (previous queries)                       │    │
│  │ - DataPoint relevance                              │    │
│  │ - Default database setting                         │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 3. Auto-Profiling & DataPoint Generation Pipeline

```
Database Connected
       │
       ▼
┌─────────────────┐
│ Schema          │ ← information_schema queries
│ Introspection   │   (tables, columns, types, constraints)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Data Profiling  │ ← Statistical analysis
│                 │   - Row counts
│                 │   - NULL percentages
│                 │   - Cardinality
│                 │   - Sample values
│                 │   - Data patterns
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Relationship    │ ← FK analysis + heuristics
│ Discovery       │   - Foreign keys
│                 │   - Naming patterns (user_id → users.id)
│                 │   - Value overlap analysis
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ LLM Analysis    │ ← GPT-4o for understanding
│                 │   Input: schema + samples + stats
│                 │   Output: business meanings
│                 │   - Table purposes
│                 │   - Column semantics
│                 │   - Likely metrics
│                 │   - Common use cases
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ DataPoint       │ ← Auto-generate JSON
│ Generation      │   - Schema DataPoints
│                 │   - Business DataPoints (suggested metrics)
│                 │   - Process DataPoints (ETL hints)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ User Review     │ ← Optional editing
│ & Approval      │   - Confirm/edit/reject
│                 │   - Add custom fields
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Sync to         │ ← Vector store + Knowledge graph
│ Knowledge Base  │
└─────────────────┘
```

---

## New Components

### 1. DatabaseConnectionManager

```python
class DatabaseConnectionManager:
    """Manages multiple database connections."""

    async def add_connection(
        self,
        name: str,
        database_url: str,
        database_type: str,
        tags: list[str] = None,
        description: str = None,
    ) -> DatabaseConnection:
        """Add a new database connection."""

    async def list_connections(
        self,
        active_only: bool = True,
        tags: list[str] = None,
    ) -> list[DatabaseConnection]:
        """List all database connections."""

    async def get_connection(
        self,
        connection_id: str
    ) -> DatabaseConnection:
        """Get specific connection."""

    async def test_connection(
        self,
        connection_id: str
    ) -> bool:
        """Test if connection is valid."""

    async def set_default(
        self,
        connection_id: str
    ) -> None:
        """Set default database for queries."""
```

### 2. SchemaProfiler

```python
class SchemaProfiler:
    """Profiles database schema and data."""

    async def profile_database(
        self,
        connection_id: str,
        sample_size: int = 100,
    ) -> DatabaseProfile:
        """
        Complete database profiling.

        Returns:
            DatabaseProfile with:
            - Table metadata
            - Column statistics
            - Relationship graph
            - Data samples
            - Profiling insights
        """

    async def introspect_schema(
        self,
        connection: BaseConnector
    ) -> SchemaMetadata:
        """Query information_schema for structure."""

    async def analyze_data(
        self,
        connection: BaseConnector,
        table_info: TableInfo,
        sample_size: int = 100,
    ) -> TableProfile:
        """
        Analyze actual data:
        - Row count
        - NULL percentages
        - Unique value counts (cardinality)
        - Min/max for numeric columns
        - Sample values
        - Data type validation
        """

    async def discover_relationships(
        self,
        schema: SchemaMetadata,
        profiles: list[TableProfile],
    ) -> list[Relationship]:
        """
        Discover relationships beyond FKs:
        - Named patterns (user_id → users.id)
        - Value overlap analysis
        - Cardinality hints (1:1, 1:N, N:M)
        """
```

### 3. DataPointGenerator

```python
class DataPointGenerator:
    """Auto-generates DataPoints using LLM."""

    async def generate_from_profile(
        self,
        database_profile: DatabaseProfile,
        llm_provider: BaseLLMProvider,
    ) -> GeneratedDataPoints:
        """
        Generate DataPoints from profiling results.

        Uses LLM to understand:
        - Table business purposes
        - Column business meanings
        - Likely metrics and KPIs
        - Common query patterns

        Returns:
            - Schema DataPoints (one per table)
            - Business DataPoints (suggested metrics)
            - Confidence scores
        """

    async def generate_schema_datapoint(
        self,
        table_profile: TableProfile,
        relationships: list[Relationship],
        llm: BaseLLMProvider,
    ) -> SchemaDataPoint:
        """Generate Schema DataPoint for one table."""

    async def suggest_metrics(
        self,
        schema: SchemaMetadata,
        llm: BaseLLMProvider,
    ) -> list[BusinessDataPoint]:
        """
        Suggest business metrics based on schema.

        Examples:
        - Revenue: SUM(orders.total) WHERE status = 'completed'
        - Active Users: COUNT(DISTINCT users.id) WHERE last_login > NOW() - INTERVAL '30 days'
        - Conversion Rate: (completed_orders / total_visitors) * 100
        """
```

### 4. QueryRouter

```python
class QueryRouter:
    """Routes queries to appropriate database(s)."""

    async def route_query(
        self,
        query: str,
        conversation_history: list[Message] = None,
    ) -> QueryRouting:
        """
        Determine which database(s) to query.

        Routing logic:
        1. Explicit mention: "in prod_analytics, show me..."
        2. Context: previous queries in conversation
        3. DataPoint relevance: vector search across all DBs
        4. Default: user's default database

        Returns:
            QueryRouting with:
            - target_databases: list[connection_id]
            - routing_reason: str
            - confidence: float
        """

    async def execute_cross_database(
        self,
        queries: dict[str, str],  # {connection_id: sql}
    ) -> CrossDatabaseResult:
        """Execute queries across multiple databases and merge results."""
```

### 5. SystemInitializer

```python
class SystemInitializer:
    """Handles first-time setup."""

    async def check_initialization_status(self) -> InitStatus:
        """
        Check if system is ready.

        Returns:
            InitStatus with:
            - is_initialized: bool
            - has_databases: bool
            - has_datapoints: bool
            - missing_steps: list[str]
        """

    async def run_initialization_wizard(
        self,
        databases: list[DatabaseConnectionInput],
        auto_profile: bool = True,
        auto_generate_datapoints: bool = True,
    ) -> InitializationResult:
        """
        Complete initialization workflow:
        1. Add database connections
        2. Test connections
        3. Profile schemas (if auto_profile)
        4. Generate DataPoints (if auto_generate)
        5. Sync to knowledge base
        6. Verify setup
        """
```

---

## Updated API Endpoints

### Database Management

```python
# Add database connection
POST /api/v1/databases
{
  "name": "Production Analytics",
  "database_url": "postgresql://user:pass@host:5432/analytics",
  "database_type": "postgresql",
  "tags": ["production", "analytics"],
  "description": "Main production analytics database"
}

# List databases
GET /api/v1/databases
Response: [
  {
    "connection_id": "prod_analytics_001",
    "name": "Production Analytics",
    "database_type": "postgresql",
    "is_active": true,
    "is_default": true,
    "tags": ["production", "analytics"],
    "datapoint_count": 15,
    "last_profiled": "2026-01-17T18:00:00Z"
  }
]

# Get database details
GET /api/v1/databases/{connection_id}

# Test connection
POST /api/v1/databases/{connection_id}/test

# Profile database (trigger auto-profiling)
POST /api/v1/databases/{connection_id}/profile
{
  "sample_size": 100,
  "generate_datapoints": true
}

# Set default database
PUT /api/v1/databases/{connection_id}/default

# Delete database
DELETE /api/v1/databases/{connection_id}
```

### DataPoint Management (Enhanced)

```python
# Auto-generate DataPoints from profiling
POST /api/v1/datapoints/generate
{
  "connection_id": "prod_analytics_001",
  "tables": ["users", "orders"],  # Optional: specific tables
  "auto_approve": false  # If true, auto-sync; if false, return for review
}

# Review generated DataPoints
GET /api/v1/datapoints/pending
Response: [
  {
    "datapoint": {...},  # Generated DataPoint
    "confidence": 0.85,
    "source": "auto_generated",
    "status": "pending_review"
  }
]

# Approve/reject generated DataPoint
POST /api/v1/datapoints/pending/{id}/approve
POST /api/v1/datapoints/pending/{id}/reject

# Sync DataPoints (existing + manual trigger)
POST /api/v1/datapoints/sync
{
  "connection_id": "prod_analytics_001",  # Optional: specific database
  "source": "filesystem"  # or "database_profile"
}

# List DataPoints (with filtering)
GET /api/v1/datapoints?connection_id=prod_analytics_001&type=Schema

# Add/update/delete DataPoint (existing endpoints)
POST /api/v1/datapoints
PUT /api/v1/datapoints/{id}
DELETE /api/v1/datapoints/{id}
```

### System Initialization

```python
# Check initialization status
GET /api/v1/system/status
Response: {
  "is_initialized": false,
  "has_databases": false,
  "has_datapoints": false,
  "setup_required": true,
  "missing_steps": [
    "add_database_connection",
    "profile_schema",
    "generate_datapoints"
  ]
}

# Run initialization wizard
POST /api/v1/system/initialize
{
  "databases": [
    {
      "name": "Production DB",
      "database_url": "postgresql://...",
      "database_type": "postgresql"
    }
  ],
  "auto_profile": true,
  "auto_generate_datapoints": true,
  "auto_approve_datapoints": false  # Require review
}

Response: {
  "status": "completed",
  "databases_added": 1,
  "datapoints_generated": 12,
  "pending_review": 12,
  "next_step": "review_datapoints"
}
```

### Enhanced Chat Endpoint

```python
# Chat with database selection
POST /api/v1/chat
{
  "message": "How many users in production?",
  "conversation_id": "conv_123",
  "target_database": "prod_analytics_001",  # Optional: specific database
  "cross_database": false  # If true, search all databases
}

# If system not initialized:
Response (503):
{
  "error": "system_not_initialized",
  "message": "DataChat requires setup. Please initialize the system first.",
  "setup_url": "/api/v1/system/initialize",
  "documentation": "https://docs.datachat.ai/getting-started"
}

# Normal response (when initialized):
Response (200):
{
  "answer": "Production has 1,247 active users.",
  "sql": "SELECT COUNT(*) FROM users WHERE is_active = true",
  "data": [...],
  "database_used": "prod_analytics_001",
  "sources": [...],
  "metrics": {...}
}
```

---

## Data Models

### DatabaseConnection

```python
class DatabaseConnection(BaseModel):
    connection_id: str = Field(default_factory=lambda: f"db_{uuid.uuid4().hex[:12]}")
    name: str
    database_url: str = Field(repr=False)  # Don't log credentials
    database_type: Literal["postgresql", "clickhouse", "mysql", "bigquery", "snowflake"]
    is_active: bool = True
    is_default: bool = False
    tags: list[str] = []
    description: str | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_profiled: datetime | None = None
    datapoint_count: int = 0
```

### DatabaseProfile

```python
class DatabaseProfile(BaseModel):
    connection_id: str
    profiled_at: datetime
    table_count: int
    total_rows: int
    tables: list[TableProfile]
    relationships: list[Relationship]
    insights: ProfilingInsights
```

### TableProfile

```python
class TableProfile(BaseModel):
    table_name: str
    schema: str
    row_count: int
    column_count: int
    columns: list[ColumnProfile]
    sample_data: list[dict]  # First N rows
    primary_keys: list[str]
    foreign_keys: list[ForeignKey]
    indexes: list[Index]
```

### ColumnProfile

```python
class ColumnProfile(BaseModel):
    name: str
    data_type: str
    is_nullable: bool
    null_percentage: float  # 0.0 to 1.0
    unique_count: int  # Cardinality
    sample_values: list[Any]
    min_value: Any | None
    max_value: Any | None
    avg_value: float | None
    patterns: list[str]  # e.g., "email", "phone", "uuid"
```

### GeneratedDataPoints

```python
class GeneratedDataPoints(BaseModel):
    connection_id: str
    generated_at: datetime
    schema_datapoints: list[SchemaDataPoint]
    business_datapoints: list[BusinessDataPoint]
    confidence_scores: dict[str, float]
    llm_model_used: str
```

---

## Implementation Priority

### Phase 1: Foundation (Week 1-2)
- [ ] DatabaseConnectionManager
- [ ] Multi-database support in pipeline
- [ ] System initialization check
- [ ] Empty state API responses

### Phase 2: Auto-Profiling (Week 3-4)
- [ ] SchemaProfiler
- [ ] Data profiling queries
- [ ] Relationship discovery

### Phase 3: Auto-Generation (Week 5-6)
- [ ] DataPointGenerator
- [ ] LLM-powered understanding
- [ ] DataPoint approval workflow

### Phase 4: UI/UX (Week 7-8)
- [ ] Initialization wizard (Web UI)
- [ ] Database management UI
- [ ] DataPoint review/edit UI
- [ ] Multi-database selector

---

## Migration Path

### For Existing Users

1. **Auto-migrate current setup:**
   ```python
   # Detect current DATABASE_URL
   # Create default connection: "default_database"
   # Migrate existing DataPoints to new structure
   # Set as default database
   ```

2. **Backward compatibility:**
   - Old API endpoints still work
   - Single-database mode by default
   - Opt-in to multi-database features

---

## Benefits

### User Experience
✅ **Zero-config testing** - Profile and auto-generate on first use
✅ **Clear guidance** - Know exactly what's missing
✅ **Multi-database** - Query across your entire data infrastructure
✅ **Auto-sync** - No manual sync commands
✅ **Intelligent routing** - System knows which DB to query

### Developer Experience
✅ **Easier onboarding** - Guided setup process
✅ **Better errors** - Actionable error messages
✅ **Extensible** - Easy to add new database types
✅ **Testable** - Sample DBs can be auto-generated

### Business Value
✅ **Faster time-to-value** - Minutes instead of hours
✅ **Broader adoption** - Non-technical users can set up
✅ **Multi-tenant ready** - Different teams, different databases
✅ **Cost optimization** - Mix prod/dev/warehouse databases

---

## Open Questions

1. **Storage:** Where to persist database connections?
   - Option A: PostgreSQL system database
   - Option B: Configuration file
   - Option C: Both (config override)

2. **Security:** How to securely store database credentials?
   - Option A: Encrypted at rest
   - Option B: External secrets manager (Vault, AWS Secrets)
   - Option C: User must provide on each request

3. **Profiling:** How much data to sample?
   - Small tables (<10K rows): 100%
   - Medium tables (10K-1M): 1,000 rows
   - Large tables (>1M): 10,000 rows stratified sample

4. **LLM Costs:** Auto-generation uses LLM calls
   - Batch processing to reduce calls
   - Cache generated DataPoints
   - User can opt-out of auto-generation

---

This architecture dramatically improves the user experience while maintaining backward compatibility.
