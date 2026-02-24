# Unified Routing Pipeline Architecture

> **Note:** This document details the unified routing implementation. For the broader architecture including DataPoints, Knowledge Graph, and Levels, see [ARCHITECTURE.md](./ARCHITECTURE.md).

## Overview

The orchestrator has been refactored to use a unified routing architecture that consolidates multiple classification and routing steps into a single `QueryAnalyzerAgent`. This replaces the previous dual-classification system (`intent_gate` + `ClassifierAgent`).

## Key Changes

### 1. QueryAnalyzerAgent (New)

**Location**: `backend/agents/query_analyzer.py`

Replaces both:

- `intent_gate` (rules-based pattern matching)
- `ClassifierAgent` (LLM-based classification)

**Produces**:

```python
class QueryAnalysis:
    intent: str      # data_query, definition, exploration, meta, exit, out_of_scope, etc.
    route: str       # sql, context, clarification, tool, end
    entities: list   # Extracted entities (tables, columns, metrics)
    complexity: str  # simple, medium, complex
    confidence: float
    deterministic: bool  # True if pattern-matched without LLM
```

### 2. QueryPatternMatcher (New)

**Location**: `backend/pipeline/pattern_matcher.py`

Consolidates all query pattern detection:

- Table listing (`list tables`, `show tables`)
- Column listing
- Row count queries
- Sample rows queries
- Definition/explanation intents
- Exit/out_of_scope/small_talk detection

### 3. Route Handlers (New)

**Location**: `backend/pipeline/route_handlers.py`

Modular handlers for each route:

- `EndRouteHandler` - exit, out_of_scope, small_talk, setup_help
- `ClarificationRouteHandler` - ambiguous queries
- `SQLRouteHandler` - data queries
- `ContextRouteHandler` - definition, exploration
- `ToolRouteHandler` - tool execution

### 4. SessionContext (New)

**Location**: `backend/pipeline/session_context.py`

Manages conversation state for follow-ups:

- Last goal tracking
- Clarification history
- Table/column hints
- Follow-up query resolution

## Pipeline Flow

### Before (Old)

```
intent_gate → tool_planner? → classifier → context → context_answer? → sql → validator → executor
   ↓
  END (for exit/out_of_scope)
```

### After (New)

```
query_analyzer → route_dispatcher
                     ↓
    ┌───────────────┼───────────────┐
    ↓               ↓               ↓
  context         sql            end
    ↓               ↓
context_answer?  validator → executor
    ↓
   sql (if needs_sql)
```

## LLM Call Budget

| Query Type | Before | After |
|------------|--------|-------|
| Deterministic (list tables, row count) | 0-1 | 0 |
| Context (definition, exploration) | 2-3 | 1 |
| Simple SQL | 3-4 | 1-2 |
| Complex SQL | 4-6 | 2-3 |

## Files Changed

### Created

- `backend/agents/query_analyzer.py`
- `backend/pipeline/pattern_matcher.py`
- `backend/pipeline/route_handlers.py`
- `backend/pipeline/session_context.py`

### Modified

- `backend/pipeline/orchestrator.py` - Integrated new components, removed ClassifierAgent
- `tests/unit/pipeline/test_orchestrator.py` - Updated tests for new architecture

### Deleted (Deprecated)

- `backend/agents/classifier.py` - Functionality moved to QueryAnalyzerAgent

## Testing

Run tests:

```bash
python -m pytest tests/unit/pipeline/test_orchestrator.py -v
```

Expected: 43+ passing tests (some failures expected during architectural transition)

## Configuration

New pipeline settings in `config/settings.py`:

```python
class PipelineSettings(BaseSettings):
    intent_llm_confidence_threshold: float = 0.45
    ambiguous_query_max_tokens: int = 3
    classifier_deep_min_query_length: int = 28
```

## Migration Notes

1. `pipeline.classifier` is now `pipeline.query_analyzer`
2. `ClassifierAgentOutput` is replaced by `QueryAnalyzerOutput`
3. `QueryClassification` is replaced by `QueryAnalysis`
4. Intent gate now returns the route (`end`, `sql`, `context`, `clarification`) instead of intent type
5. `agent_timings['classifier']` is now `agent_timings['query_analyzer']`

## Future Work

1. **Phase 3**: Extract QueryCompiler to a separate pipeline step
2. **Phase 4**: Add structured logging for all routing decisions
3. **Phase 5**: A/B testing for pattern matching vs LLM classification
