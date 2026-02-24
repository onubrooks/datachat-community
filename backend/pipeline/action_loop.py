"""DYN-001 action-loop foundations for bounded plan/act/verify execution."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class LoopTerminalState(StrEnum):
    COMPLETED = "completed"
    NEEDS_USER_INPUT = "needs_user_input"
    BLOCKED = "blocked"
    IMPOSSIBLE = "impossible"


class LoopStopReason(StrEnum):
    EXECUTION_COMPLETED = "execution_completed"
    USER_CLARIFICATION_REQUIRED = "user_clarification_required"
    TOOL_APPROVAL_REQUIRED = "tool_approval_required"
    BUDGET_STEPS_EXCEEDED = "budget_steps_exceeded"
    BUDGET_LATENCY_EXCEEDED = "budget_latency_exceeded"
    BUDGET_TOKENS_EXCEEDED = "budget_tokens_exceeded"
    BUDGET_CLARIFICATIONS_EXCEEDED = "budget_clarifications_exceeded"
    ERROR = "error"
    ERROR_SCHEMA_MISMATCH = "error_schema_mismatch"
    ERROR_SEMANTIC_MISMATCH = "error_semantic_mismatch"
    ERROR_VALIDATION = "error_validation"
    ERROR_CONNECTOR_FAILURE = "error_connector_failure"
    ERROR_PERMISSION = "error_permission"
    ERROR_TIMEOUT = "error_timeout"
    UNKNOWN = "unknown"


class LoopErrorClass(StrEnum):
    VALIDATION = "validation"
    SCHEMA_MISMATCH = "schema_mismatch"
    SEMANTIC_MISMATCH = "semantic_mismatch"
    CONNECTOR_FAILURE = "connector_failure"
    PERMISSION = "permission"
    TIMEOUT = "timeout"


class LoopBudget(BaseModel):
    max_steps: int = Field(default=12, ge=1, le=64)
    max_latency_ms: int = Field(default=45_000, ge=1_000, le=600_000)
    max_llm_tokens: int = Field(default=120_000, ge=1_000, le=1_000_000)
    max_clarifications: int = Field(default=3, ge=0, le=20)


class ActionVerification(BaseModel):
    status: str = "unknown"
    reason: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class ActionState(BaseModel):
    version: str = "v1"
    step: int
    stage: str
    selected_action: str
    inputs: dict[str, Any] = Field(default_factory=dict)
    outputs: dict[str, Any] = Field(default_factory=dict)
    verification: ActionVerification = Field(default_factory=ActionVerification)
    error_class: str | None = None
    stop_reason: str | None = None
    terminal_state: str | None = None


class ActionLoopController:
    """Small helper for bounded loop checks and terminal-state normalization."""

    def __init__(self, budget: LoopBudget):
        self.budget = budget

    def budget_stop_reason(self, state: dict[str, Any]) -> LoopStopReason | None:
        steps_taken = int(state.get("loop_steps_taken", 0) or 0)
        if steps_taken >= self.budget.max_steps:
            return LoopStopReason.BUDGET_STEPS_EXCEEDED

        total_latency_ms = float(state.get("total_latency_ms", 0.0) or 0.0)
        if total_latency_ms >= float(self.budget.max_latency_ms):
            return LoopStopReason.BUDGET_LATENCY_EXCEEDED

        total_tokens_used = int(state.get("total_tokens_used", 0) or 0)
        if total_tokens_used >= self.budget.max_llm_tokens:
            return LoopStopReason.BUDGET_TOKENS_EXCEEDED

        clarification_turns = int(state.get("clarification_turn_count", 0) or 0)
        if clarification_turns >= self.budget.max_clarifications:
            return LoopStopReason.BUDGET_CLARIFICATIONS_EXCEEDED

        return None

    @staticmethod
    def normalize_terminal(
        *,
        answer_source: str | None,
        has_error: bool,
        clarification_needed: bool,
        tool_approval_required: bool,
        error_class: str | None,
    ) -> tuple[LoopTerminalState, LoopStopReason]:
        if tool_approval_required:
            return (
                LoopTerminalState.NEEDS_USER_INPUT,
                LoopStopReason.TOOL_APPROVAL_REQUIRED,
            )
        if clarification_needed:
            return (
                LoopTerminalState.NEEDS_USER_INPUT,
                LoopStopReason.USER_CLARIFICATION_REQUIRED,
            )
        if has_error:
            if error_class == LoopErrorClass.SCHEMA_MISMATCH:
                return (
                    LoopTerminalState.IMPOSSIBLE,
                    LoopStopReason.ERROR_SCHEMA_MISMATCH,
                )
            if error_class == LoopErrorClass.SEMANTIC_MISMATCH:
                return (
                    LoopTerminalState.IMPOSSIBLE,
                    LoopStopReason.ERROR_SEMANTIC_MISMATCH,
                )
            if error_class == LoopErrorClass.VALIDATION:
                return (LoopTerminalState.BLOCKED, LoopStopReason.ERROR_VALIDATION)
            if error_class == LoopErrorClass.CONNECTOR_FAILURE:
                return (
                    LoopTerminalState.BLOCKED,
                    LoopStopReason.ERROR_CONNECTOR_FAILURE,
                )
            if error_class == LoopErrorClass.PERMISSION:
                return (LoopTerminalState.BLOCKED, LoopStopReason.ERROR_PERMISSION)
            if error_class == LoopErrorClass.TIMEOUT:
                return (LoopTerminalState.BLOCKED, LoopStopReason.ERROR_TIMEOUT)
            return (LoopTerminalState.BLOCKED, LoopStopReason.ERROR)

        if answer_source:
            return (LoopTerminalState.COMPLETED, LoopStopReason.EXECUTION_COMPLETED)

        return (LoopTerminalState.BLOCKED, LoopStopReason.UNKNOWN)
