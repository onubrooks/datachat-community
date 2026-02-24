"""Tool system entrypoint."""

from __future__ import annotations

from pathlib import Path

from backend.tools.executor import ToolExecutor
from backend.tools.policy import PolicyEngine
from backend.tools.registry import ToolRegistry


def initialize_tools(policy_path: str | Path | None = None) -> None:
    # Register built-in tools
    from backend.tools.builtin import answers, database, profiling, quality  # noqa: F401

    if policy_path:
        ToolRegistry.load_policy_config(policy_path)


__all__ = ["ToolExecutor", "PolicyEngine", "ToolRegistry", "initialize_tools"]
