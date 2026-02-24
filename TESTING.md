# DataChat Testing Guide

Comprehensive testing guide for Backend API, CLI, and Frontend.

For a quick verification pass, see `smoke_testing.md`.

---

## Current Capability Demo (Base + Analyst)

Use these to validate the system as it exists today (Levels 1–2).

```bash
datachat demo --persona base --reset
datachat demo --persona analyst --reset
```

Suggested questions:
- "What was total revenue last 30 days?"
- "Top 5 users by orders"
- "How many active users are there?"

Expected: answers include SQL + results; `validation_errors` is empty or minimal.

---

## Prerequisites

Before testing, ensure you have:

1. **Environment Setup:**

   ```bash
   cp .env.example .env
   # Edit .env and add your OpenAI API key
   # Generate encryption key for saved DB credentials:
   python -c "import secrets; print(secrets.token_hex(32))"
   # Set DATABASE_CREDENTIALS_KEY in .env
   ```

2. **CLI Installed:**

   ```bash
   pip install -e .
   ```

3. **Start Backend + Frontend (if not running):**

   ```bash
   # Backend (from repo root)
   uvicorn backend.api.main:app --reload --port 8000
   ```

   ```bash
   # Frontend (separate terminal)
   cd frontend
   npm install
   npm run dev
   ```

   ```bash
   # Or run both with the CLI (requires frontend deps installed)
   datachat dev
   ```

   Frontend should be on `http://localhost:3000`.

4. **Reset everything (optional):**

   ```bash
   datachat reset
   # Add --include-target to clear demo tables in the target DB
   # Add --drop-all-target to drop all public tables (dangerous)
   ```

5. **Setup Persistence:**

   Setup saves database URLs to `~/.datachat/config.json` for reuse.

5. **PostgreSQL Database:**

   ```bash
   # Create database user (matches .env)
   createuser -P datachat  # set password to datachat_password

   # Create database owned by datachat user
   dropdb datachat # if it already exists
   createdb -O datachat datachat

   # Or using Docker
   docker run -d \
     --name datachat-postgres \
     -e POSTGRES_DB=datachat \
     -e POSTGRES_USER=datachat \
     -e POSTGRES_PASSWORD=datachat_password \
     -p 5432:5432 \
     postgres:16-alpine
   ```

3. **Environment Variables Required:**

   ```env
   OPENAI_API_KEY=sk-...  # Required for LLM functionality
   DATABASE_URL=postgresql://datachat:datachat_password@localhost:5432/datachat
   SYSTEM_DATABASE_URL=postgresql://datachat:datachat_password@localhost:5432/datachat
   ```

   **AWS RDS note:** many instances require SSL:
   `postgresql://user:pass@host:5432/dbname?sslmode=require`

   **Credentials:** URL must include username/password.

4. **Optional (Multi-DB Registry):**

   If you want to test the database registry endpoints, set:

   ```env
   DATABASE_CREDENTIALS_KEY=... # 32 url-safe base64 bytes
   ```

---

## Option 1: Testing with Docker Compose (Recommended)

### Quick Start

```bash
# 1. Start all services
docker-compose up

# For live code changes in development, docker-compose will
# automatically include docker-compose.override.yml if present.

# This will start:
# - PostgreSQL on :5432
# - Backend API on :8000
# - Frontend UI on :3000
```

### Verify Services

```bash
# Check all containers are running
docker-compose ps

# Expected output:
# NAME                  STATUS              PORTS
# datachat-postgres     Up (healthy)        0.0.0.0:5432->5432/tcp
# datachat-backend      Up (healthy)        0.0.0.0:8000->8000/tcp
# datachat-frontend     Up (healthy)        0.0.0.0:3000->3000/tcp

# Check logs
docker-compose logs backend
docker-compose logs frontend
```

### Test Services

1. **Backend Health Check:**

   ```bash
   curl http://localhost:8000/api/v1/health
   # Expected: {"status":"healthy","version":"0.1.0","timestamp":"..."}
   ```

2. **Frontend Access:**
   - Open <http://localhost:3000>
   - Should see DataChat UI

3. **API Documentation:**
   - Open <http://localhost:8000/docs>
   - Interactive Swagger UI

---

## Option 2: Manual Testing (Development)

### Step 1: Start Backend API

```bash
# 1. Create virtual environment (run from repo root)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -e .

# 3. Verify installation
pip list | grep datachat

# 4. Start the server
uvicorn backend.api.main:app --reload --port 8000

# Server should start on http://localhost:8000
```

**Expected Output:**

```text
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [xxxxx] using WatchFiles
INFO:     Started server process [xxxxx]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
```

### Step 2: Test Backend API

Open a new terminal and run these tests:

#### 2.1 Health Check

```bash
curl http://localhost:8000/api/v1/health

# Expected response:
{
  "status": "healthy",
  "version": "0.1.0",
  "timestamp": "2026-01-17T18:30:00.000Z"
}
```

#### 2.2 Readiness Check

```bash
curl http://localhost:8000/api/v1/ready

# Expected response:
{
  "status": "ready",
  "version": "0.1.0",
  "timestamp": "2026-01-17T18:30:00.000Z",
  "checks": {
    "database": true,
    "vector_store": true,
    "pipeline": true
  }
}
```

#### 2.3 System Status / Initialize

```bash
# Status (shows setup steps)
curl http://localhost:8000/api/v1/system/status

# Initialize with a database URL (auto-profiling optional)
curl -X POST http://localhost:8000/api/v1/system/initialize \
  -H "Content-Type: application/json" \
  -d '{
    "database_url": "postgresql://datachat:datachat_password@localhost:5432/datachat",
    "system_database_url": "postgresql://datachat:datachat_password@localhost:5432/datachat",
    "auto_profile": true
  }'
```

Auto-profiling requires `SYSTEM_DATABASE_URL` + `DATABASE_CREDENTIALS_KEY`.

Auto-profiling and DataPoint generation notes:
- Generation is async and batched (10 tables per LLM call).
- Depth levels: `schema_only`, `metrics_basic`, `metrics_full`.
- UI lets you select tables and track generation progress (WebSocket updates).
- Profiling is bounded by default (`max_tables`, `max_columns_per_table`, and timeouts).
- Profiling job progress reports include `tables_failed` and `tables_skipped` for partial coverage.

##### 2.3.1 Profiling hardening verification

```bash
curl -X POST http://localhost:8000/api/v1/databases/<connection_id>/profile \
  -H "Content-Type: application/json" \
  -d '{
    "sample_size": 50,
    "max_tables": 5,
    "max_columns_per_table": 20,
    "query_timeout_seconds": 3,
    "per_table_timeout_seconds": 10,
    "total_timeout_seconds": 60,
    "fail_fast": false
  }'
```

Expected:
- Job is accepted with `202`.
- `GET /api/v1/profiling/jobs/{job_id}` eventually reports `status=completed`.
- `progress.tables_failed` and `progress.tables_skipped` are present (may be `0`).

#### 2.3 Chat Endpoint (Simple Query)

```bash
curl -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{
    "message": "What tables are available in the database?",
    "conversation_id": "test_conv_1"
  }'

# Expected response (will take a few seconds):
{
  "answer": "Based on the database schema...",
  "sql": "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
  "data": [...],
  "visualization_hint": "table",
  "sources": [...],
  "validation_errors": [],
  "validation_warnings": [],
  "metrics": {
    "total_latency_ms": 1500,
    "agent_timings": {
      "ClassifierAgent": 200,
      "ContextAgent": 150,
      "SQLAgent": 800,
      "ValidatorAgent": 100,
      "ExecutorAgent": 250
    },
    "llm_calls": 2,
    "retry_count": 0
  },
  "conversation_id": "test_conv_1"
}
```

If validation fails, `validation_errors` will include details about why the SQL was rejected.

#### 2.4 Database Registry (Optional)

Requires `DATABASE_CREDENTIALS_KEY` in your environment.

```bash
# Create a connection
curl -X POST http://localhost:8000/api/v1/databases \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Local Postgres",
    "database_url": "postgresql://datachat:datachat_password@localhost:5432/datachat",
    "database_type": "postgresql",
    "tags": ["local"],
    "is_default": true
  }'

# List connections
curl http://localhost:8000/api/v1/databases
```

#### 2.4 API Documentation

```bash
# Open in browser
open http://localhost:8000/docs

# Or test via curl
curl http://localhost:8000/openapi.json
```

### Step 3: Test CLI

The CLI requires the backend to be running.

#### 3.1 Install CLI

```bash
# Ensure you're at the repo root with venv activated
source venv/bin/activate

# Install in editable mode (if not already done)
pip install -e .

# Verify CLI is installed
datachat --help
```

Optional profiling + generation via CLI:

```bash
# Start profiling (requires a registered connection_id)
datachat profile start --connection-id <uuid> --sample-size 100

# Start DataPoint generation with batching + depth
# If --profile-id is omitted, the latest profile on the default connection is used.
datachat dp generate --max-tables 10 --depth metrics_full --batch-size 10

# Review pending items (requires backend)
datachat dp pending list
datachat dp pending approve-all --latest
```

Note: `--max-tables` limits the number of tables profiled, but each table can generate
multiple DataPoints (schema + metrics), so the pending count can exceed the table limit.

UI reset:
- Database Manager has a **Reset System** button that clears registry/profiling,
  local vectors, and saved setup config (does not touch target DB tables).

**Expected Output:**

```text
Usage: datachat [OPTIONS] COMMAND [ARGS]...

  DataChat - Natural language interface for data warehouses.

Options:
  --version  Show the version and exit.
  --help     Show this message and exit.

Commands:
  ask      Ask a single question and exit.
  chat     Interactive REPL mode for conversations.
  connect  Set database connection string.
  dp       Manage DataPoints (knowledge base).
  status   Show connection and system status.
```

#### 3.2 Test Connection Command

```bash
# Set database connection
datachat connect postgresql://datachat:datachat_password@localhost:5432/datachat

# Expected output:
✓ Connection string saved
Host: localhost
Port: 5432
Database: datachat
User: datachat
```

#### 3.3 Test Status Command

```bash
datachat status

# Expected output:
                 DataChat Status
┏━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┓
┃ Component      ┃ Status ┃ Details              ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━┩
│ Configuration  │ ✓      │ Environment: dev...  │
│ Database       │ ✓      │ Connected            │
│ Vector Store   │ ✓      │ 0 datapoints         │
│ Knowledge Graph│ ✓      │ 0 nodes, 0 edges     │
│ LLM Provider   │ ✓      │ openai (gpt-4o-mini) │
└────────────────┴────────┴──────────────────────┘
```

#### 3.4 Test Ask Command

```bash
# Single query mode
datachat ask "How many tables are in the database?"

# For long outputs, use a scrollable pager
datachat ask --pager "Describe the public.events table"

# Expected output (will show agent status during processing):
╭─ Answer ──────────────────────────────────╮
│ Based on the database schema, there are   │
│ currently X tables in the public schema.  │
╰───────────────────────────────────────────╯

╭─ Generated SQL ───────────────────────────╮
│ SELECT COUNT(*) as table_count            │
│ FROM information_schema.tables            │
│ WHERE table_schema = 'public'             │
╰───────────────────────────────────────────╯

╭─ Results (1 rows) ────────────────────────╮
│ table_count                               │
│ X                                         │
╰───────────────────────────────────────────╯

⏱ 1500ms  🤖 2 LLM calls  🔄 0 retries
```

#### 3.5 Test Onboarding Guardrails (Missing DB/DataPoints)

If you haven't configured a database, the CLI should block queries. If you have
a database but no DataPoints, the CLI should proceed with live schema only.

```bash
datachat ask "How many users signed up last week?"
```

Expected output (no database configured):
```text
DataChat requires setup before queries can run.
Note: SYSTEM_DATABASE_URL enables registry/profiling and demo data.
- Connect a database: ...
- Load DataPoints: ...
Hint: Run 'datachat setup' or 'datachat demo' to continue.
```

Expected output (database configured, no DataPoints):
```text
No DataPoints loaded. Continuing with live schema only.
Hint: Run 'datachat dp sync' or enable profiling for richer answers.
```

#### 3.6 Test Interactive Chat Mode

```bash
datachat chat

# Interactive session:
```

```text
╭──────────────────────────────────────────╮
│ DataChat Interactive Mode                │
│ Ask questions in natural language.       │
│ Type 'exit' or 'quit' to leave.         │
╰──────────────────────────────────────────╯

✓ Pipeline initialized

You: What is the current date?
```

*System will process query and show agent status*

```text
╭─ Answer ──────────────────────────────────╮
│ The current date is 2026-01-17.          │
╰───────────────────────────────────────────╯

You: exit

Goodbye!
```

#### 3.7 Test DataPoint Commands

```bash
# List DataPoints
datachat dp list

# Expected output (if no DataPoints yet):
No DataPoints found in knowledge base.

# Review pending DataPoints (requires backend running)
datachat dp pending list
datachat dp pending approve <pending_id>
datachat dp pending approve-all

# Add a sample DataPoint (create one first)
cat > /tmp/sample_table.json << 'EOF'
{
  "datapoint_id": "table_users_001",
  "type": "Schema",
  "name": "Users Table",
  "table_name": "public.users",
  "schema": "public",
  "business_purpose": "Stores user account information",
  "key_columns": [
    {
      "name": "id",
      "type": "INTEGER",
      "business_meaning": "Unique user identifier",
      "nullable": false
    },
    {
      "name": "email",
      "type": "VARCHAR(255)",
      "business_meaning": "User email address",
      "nullable": false
    }
  ],
  "relationships": [],
  "common_queries": ["SELECT * FROM users WHERE email = ?"],
  "gotchas": ["Always use parameterized queries for email lookups"],
  "freshness": "Real-time",
  "owner": "engineering@company.com"
}
EOF

# Add the DataPoint
datachat dp add schema /tmp/sample_table.json

# Expected output:
✓ DataPoint added: table_users_001 (Schema)

# List again
datachat dp list

# Expected output:
                    DataPoints
┏━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┓
┃ Type      ┃ Name            ┃ Owner          ┃
┡━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━┩
│ Schema    │ Users Table     │ engineering... │
└───────────┴─────────────────┴────────────────┘

1 DataPoint(s) found
```

#### 3.8 Load Demo Data (Optional)

Use the built-in demo to seed a small dataset and load demo DataPoints.

```bash
# Create demo tables + rows and load datapoints/demo into the vector store + graph
datachat demo --persona base --reset

# Expected output:
✓ Demo tables created
✓ Demo rows inserted
✓ Demo DataPoints loaded
```

Note: The demo uses `SYSTEM_DATABASE_URL` for its sample data.

After this, you can ask:

```bash
datachat ask "How many users signed up in the last 30 days?"
```

### Step 4: Start Frontend

Open a new terminal:

```bash
# 1. Navigate to frontend
cd frontend

# 2. Install dependencies
npm install

# 3. Set up environment
cp .env.example .env.local
# Edit .env.local if backend is not on localhost:8000

# 4. Start development server
npm run dev

# Frontend should start on http://localhost:3000
```

Or run both servers together:

```bash
datachat dev
```

**Expected Output:**

```text
  ▲ Next.js 15.5.9
  - Local:        http://localhost:3000
  - Environments: .env.local

 ✓ Ready in 2.5s
```

### Step 5: Test Frontend UI

#### 5.1 Open Application

```bash
# Open in browser
open http://localhost:3000
```

#### 5.2 Visual Checks

**Initial Load:**

- [ ] Page loads without errors
- [ ] "DataChat" header is visible
- [ ] "Ask questions in natural language" subtitle
- [ ] Connection status shows "Connected" (green dot)
- [ ] Welcome message: "Welcome to DataChat!"
- [ ] Input field is visible at bottom
- [ ] Send button is visible

#### 5.3 Test Chat Functionality

**Test 1: Simple Query**

1. Type: "What is 2 + 2?"
2. Press Enter or click Send button
3. Verify:
   - [ ] Input clears after sending
   - [ ] User message appears (right-aligned, blue background)
   - [ ] Agent status component appears showing pipeline progress
   - [ ] Agent icons appear: Classifier → Context → SQL → Validator → Executor
   - [ ] Assistant response appears (left-aligned, gray background)
   - [ ] Response contains answer text
   - [ ] Performance metrics shown (latency, LLM calls)

**Test 2: Database Query**

1. Type: "Show me all tables in the database"
2. Press Enter
3. Verify:
   - [ ] Agent pipeline executes (5 agents)
   - [ ] SQL code block appears with generated query
   - [ ] Data table appears with results
   - [ ] Source citations appear (if DataPoints exist)
   - [ ] Metrics show timing breakdown

**Test 3: Follow-up Question**

1. After previous query, type: "How many rows are in each table?"
2. Press Enter
3. Verify:
   - [ ] Conversation ID persists
   - [ ] Context from previous query is maintained
   - [ ] New SQL is generated based on context

**Test 4: Error Handling**

1. Type: "SELECT * FROM nonexistent_table"
2. Press Enter
3. Verify:
   - [ ] Error message appears (red card with alert icon)
   - [ ] Error is descriptive
   - [ ] UI remains functional

**Test 5: Clear Conversation**

1. Click "Clear" button in header
2. Confirm dialog
3. Verify:
   - [ ] All messages are removed
   - [ ] Welcome message returns
   - [ ] Conversation ID resets

#### 5.4 Test WebSocket Connection

**Real-time Updates:**

1. Submit a query
2. Watch agent status component
3. Verify:
   - [ ] Status updates in real-time (not just after completion)
   - [ ] Each agent shows as "running" while executing
   - [ ] Completed agents show green checkmark
   - [ ] Agent execution history accumulates at bottom

**Connection Status:**

1. Check connection indicator in header
2. Verify:
   - [ ] Shows "Connected" (green) when WebSocket active
   - [ ] If backend is stopped, shows "Disconnected" (red)
   - [ ] Auto-reconnects when backend restarts

#### 5.5 Test Responsive Design

**Desktop:**

- [ ] Layout looks good on wide screen (>1200px)
- [ ] Messages are properly aligned
- [ ] Data tables scroll horizontally if needed

**Tablet:**

- [ ] Resize browser to ~768px width
- [ ] Layout adjusts
- [ ] Agent status badges stack properly

**Mobile:**

- [ ] Resize to ~375px width
- [ ] Input and send button work
- [ ] Messages are readable
- [ ] Tables scroll horizontally

#### 5.6 Browser Console Checks

Open Developer Tools (F12):

- [ ] No console errors on page load
- [ ] No console errors during query execution
- [ ] WebSocket connection established (check Network tab)
- [ ] API calls to /api/v1/chat succeed (check Network tab)

---

## Integration Testing

### Full Stack Test

**Scenario: New User Complete Workflow**

1. **Start All Services:**

   ```bash
   docker-compose up
   ```

2. **Add Sample DataPoint:**

   ```bash
   # Create sample DataPoint
   mkdir -p datapoints/tables

   cat > datapoints/tables/users.json << 'EOF'
   {
     "datapoint_id": "table_users_001",
     "type": "Schema",
     "name": "Users Table",
     "table_name": "public.users",
     "schema": "public",
     "business_purpose": "Stores user information",
     "key_columns": [
       {
         "name": "id",
         "type": "INTEGER",
         "business_meaning": "User ID",
         "nullable": false
       }
     ],
     "relationships": [],
     "common_queries": [],
     "gotchas": [],
     "freshness": "Real-time",
     "owner": "team@company.com"
   }
   EOF

   # Sync DataPoints
   docker-compose exec backend datachat dp sync --datapoints-dir datapoints/managed
   ```

3. **Test via Web UI:**
   - Open <http://localhost:3000>
   - Ask: "What columns are in the users table?"
   - Verify correct response with SQL and data

4. **Test via CLI:**

   ```bash
   docker-compose exec backend datachat ask "Describe the users table"
   ```

5. **Test via API:**

   ```bash
   curl -X POST http://localhost:8000/api/v1/chat \
     -H "Content-Type: application/json" \
     -d '{"message": "What is the users table used for?"}'
   ```

**All three interfaces should provide consistent answers.**

---

## Performance Testing

### Response Time Test

```bash
# Test backend response time
time curl -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "What is 1+1?"}'

# Expected: < 3 seconds for simple query
```

### Load Test (Optional)

```bash
# Install hey
go install github.com/rakyll/hey@latest

# Run load test (100 requests, 10 concurrent)
hey -n 100 -c 10 -m POST \
  -H "Content-Type: application/json" \
  -d '{"message": "test"}' \
  http://localhost:8000/api/v1/chat
```

---

## Troubleshooting

### Backend Issues

**Error: "Connection refused"**

```bash
# Check if backend is running
curl http://localhost:8000/api/v1/health

# Check logs
docker-compose logs backend
# or
tail -f backend/logs/datachat.log
```

**Error: "Database connection failed"**

```bash
# Check PostgreSQL is running
docker-compose ps postgres

# Test database connection
psql postgresql://datachat:datachat_password@localhost:5432/datachat
```

**Error: "OpenAI API key not set"**

```bash
# Check environment variables
docker-compose exec backend env | grep OPENAI

# Set in .env file
echo "OPENAI_API_KEY=sk-your-key" >> .env
docker-compose restart backend
```

### Frontend Issues

**Error: "Failed to fetch"**

- Check backend is running: `curl http://localhost:8000/api/v1/health`
- Check CORS configuration in backend
- Check browser console for exact error

**Error: "WebSocket connection failed"**

- Check backend WebSocket endpoint: `/ws/chat`
- Verify `NEXT_PUBLIC_WS_URL` in .env.local
- Check browser console Network tab

**Error: "Module not found"**

```bash
cd frontend
rm -rf node_modules package-lock.json
npm install
```

### CLI Issues

**Error: "datachat: command not found"**

```bash
# Ensure pip install was successful
pip install -e .

# Check if CLI is in PATH
which datachat

# Or run directly
python -m backend.cli --help
```

**Error: "No configuration found"**

```bash
# Set up connection
datachat connect postgresql://localhost/datachat

# Check config file
cat ~/.datachat/config.json
```

---

## Test Checklist

### Backend API ✓

- [ ] Health check endpoint responds
- [ ] Readiness check passes all checks
- [ ] Chat endpoint accepts queries
- [ ] Returns proper JSON structure
- [ ] Agent pipeline executes
- [ ] SQL is generated
- [ ] Results are returned
- [ ] Metrics are included
- [ ] API documentation loads

### CLI ✓

- [ ] Help command works
- [ ] Connect command saves connection
- [ ] Status command shows all components
- [ ] Ask command executes queries
- [ ] Chat mode works interactively
- [ ] DataPoint list command works
- [ ] DataPoint add command works
- [ ] DataPoint sync command works

### Frontend ✓

- [ ] Page loads without errors
- [ ] Can send messages
- [ ] Receives responses
- [ ] SQL is displayed
- [ ] Data tables render
- [ ] Agent status updates in real-time
- [ ] WebSocket connects
- [ ] Connection status accurate
- [ ] Clear conversation works
- [ ] Responsive on different sizes
- [ ] No console errors

### Integration ✓

- [ ] All three interfaces work together
- [ ] Data persists across services
- [ ] Conversation context maintained
- [ ] DataPoints accessible from all interfaces

---

## Success Criteria

Your DataChat installation is working correctly if:

1. ✅ Backend API responds to health checks
2. ✅ CLI can execute queries and get answers
3. ✅ Frontend UI loads and can chat
4. ✅ WebSocket connection is active (real-time updates)
5. ✅ Agent pipeline completes (5 agents: Classifier → Context → SQL → Validator → Executor)
6. ✅ SQL is generated and executed
7. ✅ Results are displayed properly
8. ✅ No errors in console/logs

---

## Next Steps

After successful testing:

1. **Add Your Data:**
   - Create DataPoints for your database schema
   - Use `datachat dp sync --datapoints-dir datapoints/managed` to load them

2. **Customize:**
   - Adjust LLM models in .env
   - Configure different providers per agent
   - Tune temperature and other parameters

3. **Deploy:**
   - Use docker-compose for production
   - Set up monitoring and logging
   - Configure backup for volumes

4. **Integrate:**
   - Add to your team's workflow
   - Set up Slack/Teams notifications (coming soon)
   - Create saved queries/reports

---

**Happy Testing! 🚀**
