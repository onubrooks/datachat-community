# SQL Routing and Visualization Decision Tree

This document summarizes how DataChat routes SQL generation and visualization selection after the latest runtime updates.

> **Note**: Intent classification and initial routing is now handled by `QueryAnalyzerAgent` (see `UNIFIED_ROUTING_ARCHITECTURE.md`). This document describes the SQL-specific routing once the query has been routed to the SQL path.

## SQL Routing Overview

```mermaid
flowchart TD
  A["User Query"] --> B{"Catalog operation?<br/>(list tables/columns/row count/sample)"}
  B -->|Yes| C["Run deterministic catalog SQL"]
  B -->|No| D{"Deterministic metric fallback match?<br/>(revenue-margin-waste, stockout-risk)"}
  D -->|Yes| E["Generate deterministic SQL (no LLM)"]
  D -->|No| F{"Table Resolver enabled?"}
  F -->|No| G["Build SQL prompt from schema/business/live context"]
  F -->|Yes| H["Mini LLM resolves likely tables + columns"]
  H --> I{"Low confidence + ambiguity?"}
  I -->|Yes| J["Return targeted clarification question"]
  I -->|No| K{"Fallback match with resolved tables?"}
  K -->|Yes| E
  K -->|No| G
  G --> L["Main SQL generation LLM"]
  L --> M{"Malformed SQL JSON?"}
  M -->|Yes| N["Formatter fallback model repairs JSON"]
  M -->|No| O["Validate SQL"]
  N --> O
  O --> P{"Validation errors?"}
  P -->|Yes| Q["SQL self-correction loop"]
  P -->|No| R["Execute SQL"]
  Q --> R
```

## Visualization Hint Overview

```mermaid
flowchart TD
  V0["Query Result"] --> V1{"No rows / single scalar?"}
  V1 -->|Yes| V2["Visualization: none"]
  V1 -->|No| V3{"User explicitly requested chart type?"}
  V3 -->|Yes| V4{"Request valid for data shape?"}
  V4 -->|Yes| V5["Use requested chart"]
  V4 -->|No| V6["Run mini LLM visualization planner"]
  V3 -->|No| V6
  V6 --> V7{"Planner output valid + confident?"}
  V7 -->|Yes| V8["Use planner chart"]
  V7 -->|No| V9["Use deterministic rule fallback"]
  V8 --> V10{"Requested chart was overridden?"}
  V10 -->|Yes| V11["Attach visualization_note explanation"]
  V10 -->|No| V12["No note"]
  V9 --> V13{"Requested chart invalid?"}
  V13 -->|Yes| V11
  V13 -->|No| V12
```
