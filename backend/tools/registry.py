"""Tool registry for DataChat tool system."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml

from backend.tools.base import ToolDefinition

logger = logging.getLogger(__name__)


class ToolRegistry:
    _definitions: dict[str, ToolDefinition] = {}
    _handlers: dict[str, Callable[..., Any]] = {}
    _availability_hooks: dict[str, Callable[[dict[str, Any]], bool]] = {}
    _forced_run_hooks: dict[str, Callable[[dict[str, Any]], bool]] = {}

    @classmethod
    def register(
        cls,
        definition: ToolDefinition,
        handler: Callable[..., Any],
        *,
        is_tool_available: Callable[[dict[str, Any]], bool] | None = None,
        run_if_true: Callable[[dict[str, Any]], bool] | None = None,
    ) -> None:
        cls._definitions[definition.name] = definition
        cls._handlers[definition.name] = handler
        if is_tool_available:
            cls._availability_hooks[definition.name] = is_tool_available
        if run_if_true:
            cls._forced_run_hooks[definition.name] = run_if_true
        logger.debug(f"Registered tool: {definition.name}")

    @classmethod
    def get_definition(cls, name: str) -> ToolDefinition | None:
        return cls._definitions.get(name)

    @classmethod
    def get_handler(cls, name: str) -> Callable[..., Any] | None:
        return cls._handlers.get(name)

    @classmethod
    def get_is_tool_available_hook(
        cls, name: str
    ) -> Callable[[dict[str, Any]], bool] | None:
        return cls._availability_hooks.get(name)

    @classmethod
    def get_run_if_true_hook(cls, name: str) -> Callable[[dict[str, Any]], bool] | None:
        return cls._forced_run_hooks.get(name)

    @classmethod
    def list_definitions(cls) -> list[ToolDefinition]:
        return list(cls._definitions.values())

    @classmethod
    def load_policy_config(cls, path: str | Path) -> None:
        policy_path = Path(path)
        if not policy_path.exists():
            logger.warning(f"Tool policy file not found: {policy_path}")
            return

        data = yaml.safe_load(policy_path.read_text()) or {}
        tool_policies = data.get("tools", [])
        for tool_policy in tool_policies:
            name = tool_policy.get("name")
            if not name or name not in cls._definitions:
                continue
            definition = cls._definitions[name]
            policy = definition.policy.model_copy()
            policy.enabled = tool_policy.get("enabled", policy.enabled)
            policy.requires_approval = tool_policy.get(
                "requires_approval", policy.requires_approval
            )
            policy.max_execution_time_seconds = tool_policy.get(
                "max_execution_time_seconds", policy.max_execution_time_seconds
            )
            policy.allowed_users = tool_policy.get("allowed_users", policy.allowed_users)
            cls._definitions[name] = ToolDefinition(
                name=definition.name,
                description=definition.description,
                category=definition.category,
                policy=policy,
                parameters_schema=definition.parameters_schema,
                return_schema=definition.return_schema,
            )
            logger.info(f"Loaded policy for tool: {name}")
