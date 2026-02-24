"""
DataChat Agents Module

Multi-agent pipeline for natural language to SQL conversion.

Available Agents:
    - BaseAgent: Abstract base class for all agents
    - ContextAgent: Knowledge graph and vector retrieval (pure retrieval, no LLM)
    - QueryAnalyzerAgent: Unified intent detection and entity extraction
    - QueryCompilerAgent: Pre-compiles query plans before SQL generation
    - SQLAgent: SQL query generation with self-correction
    - ValidatorAgent: SQL validation with security and performance checks
    - ExecutorAgent: Query execution and response formatting

Usage:
    from backend.agents import BaseAgent, ContextAgent, SQLAgent, ValidatorAgent

    class MyAgent(BaseAgent):
        async def execute(self, input: AgentInput) -> AgentOutput:
            return AgentOutput(
                success=True,
                data={"result": "value"},
                metadata=self._create_metadata()
            )
"""

from backend.agents.base import BaseAgent
from backend.agents.context import ContextAgent
from backend.agents.context_answer import ContextAnswerAgent
from backend.agents.executor import ExecutorAgent
from backend.agents.query_analyzer import QueryAnalyzerAgent
from backend.agents.query_compiler import QueryCompilerAgent
from backend.agents.response_synthesis import ResponseSynthesisAgent
from backend.agents.sql import SQLAgent
from backend.agents.tool_planner import ToolPlannerAgent
from backend.agents.validator import ValidatorAgent

__all__ = [
    "BaseAgent",
    "ContextAgent",
    "ContextAnswerAgent",
    "ExecutorAgent",
    "QueryAnalyzerAgent",
    "QueryCompilerAgent",
    "ToolPlannerAgent",
    "ResponseSynthesisAgent",
    "SQLAgent",
    "ValidatorAgent",
]
