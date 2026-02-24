"""
DataChat Models Module

Pydantic models for type-safe data validation throughout the application.

Available Models:
    Agent Models:
        - AgentInput: Base input for all agents
        - AgentOutput: Base output for all agents
        - AgentMetadata: Execution tracking metadata
        - Message: Conversation message
        - AgentError: Base exception for agent errors
        - ValidationError: Data validation errors
        - LLMError: LLM API errors
        - DatabaseError: Database operation errors
        - RetrievalError: Context retrieval errors

    DataPoint Models:
        - DataPoint: Discriminated union of all DataPoint types
        - BaseDataPoint: Base class with common fields
        - SchemaDataPoint: Table/column metadata
        - BusinessDataPoint: Business logic definitions
        - ProcessDataPoint: ETL process information
        - ColumnMetadata: Column information
        - Relationship: Table relationships

    API Models:
        - ChatRequest: API request model
        - ChatResponse: API response model
        - HealthResponse: Health check response
        - ReadinessResponse: Readiness check response
        - ErrorResponse: Error response
        - DataSource: Data source information
        - ChatMetrics: Performance metrics

Usage:
    from backend.models.agent import AgentInput, AgentOutput, AgentError
    from backend.models.datapoint import DataPoint, SchemaDataPoint
    from backend.models.api import ChatRequest, ChatResponse
    from backend.models import Message
"""

from backend.models.agent import (
    AgentError,
    AgentInput,
    AgentMetadata,
    AgentOutput,
    # ClassifierAgent models
    ClassifierAgentInput,
    ClassifierAgentOutput,
    # ContextAgent models
    ContextAgentInput,
    ContextAgentOutput,
    ContextAnswer,
    ContextAnswerAgentInput,
    ContextAnswerAgentOutput,
    CorrectionAttempt,
    DatabaseError,
    EvidenceItem,
    # ExecutorAgent models
    ExecutedQuery,
    ExecutorAgentInput,
    ExecutorAgentOutput,
    ExtractedEntity,
    GeneratedSQL,
    InvestigationMemory,
    LLMError,
    Message,
    QueryClassification,
    QueryResult,
    RetrievalError,
    RetrievedDataPoint,
    # SQLAgent models
    SQLAgentInput,
    SQLAgentOutput,
    SQLGenerationError,
    # ValidatorAgent models
    SQLValidationError,
    ToolCall,
    ToolPlan,
    ToolPlannerAgentInput,
    ToolPlannerAgentOutput,
    ValidatedSQL,
    ValidationError,
    ValidationIssue,
    ValidationWarning,
    ValidatorAgentInput,
    ValidatorAgentOutput,
)
from backend.models.api import (
    ChatMetrics,
    ChatRequest,
    ChatResponse,
    DataSource,
    ErrorResponse,
    HealthResponse,
    ReadinessResponse,
)
from backend.models.database import DatabaseConnection
from backend.models.datapoint import (
    BaseDataPoint,
    BusinessDataPoint,
    ColumnMetadata,
    DataPoint,
    ProcessDataPoint,
    Relationship,
    SchemaDataPoint,
)
from backend.profiling.models import (
    DatabaseProfile,
    GeneratedDataPoint,
    GeneratedDataPoints,
    PendingDataPoint,
    ProfilingJob,
    ProfilingProgress,
    TableProfile,
)

__all__ = [
    # Core agent models
    "AgentInput",
    "AgentOutput",
    "AgentMetadata",
    "Message",
    # Error types
    "AgentError",
    "ValidationError",
    "LLMError",
    "DatabaseError",
    "RetrievalError",
    "SQLGenerationError",
    # SQLAgent models
    "SQLAgentInput",
    "SQLAgentOutput",
    "GeneratedSQL",
    "ValidationIssue",
    "CorrectionAttempt",
    # ValidatorAgent models
    "SQLValidationError",
    "ValidationWarning",
    "ValidatedSQL",
    "ValidatorAgentInput",
    "ValidatorAgentOutput",
    # ContextAgent models
    "ContextAgentInput",
    "ContextAgentOutput",
    "ContextAnswer",
    "ContextAnswerAgentInput",
    "ContextAnswerAgentOutput",
    "ToolCall",
    "ToolPlan",
    "ToolPlannerAgentInput",
    "ToolPlannerAgentOutput",
    "EvidenceItem",
    "InvestigationMemory",
    "RetrievedDataPoint",
    # ClassifierAgent models
    "ClassifierAgentInput",
    "ClassifierAgentOutput",
    "ExtractedEntity",
    "QueryClassification",
    # ExecutorAgent models
    "ExecutorAgentInput",
    "ExecutorAgentOutput",
    "ExecutedQuery",
    "QueryResult",
    # DataPoint models
    "DataPoint",
    "BaseDataPoint",
    "SchemaDataPoint",
    "BusinessDataPoint",
    "ProcessDataPoint",
    "ColumnMetadata",
    "Relationship",
    "DatabaseConnection",
    "DatabaseProfile",
    "GeneratedDataPoint",
    "GeneratedDataPoints",
    "PendingDataPoint",
    "ProfilingJob",
    "ProfilingProgress",
    "TableProfile",
    # API models
    "ChatRequest",
    "ChatResponse",
    "ChatMetrics",
    "DataSource",
    "HealthResponse",
    "ReadinessResponse",
    "ErrorResponse",
]
