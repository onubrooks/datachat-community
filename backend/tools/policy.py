"""Policy enforcement for tool execution."""

from __future__ import annotations

from backend.tools.base import ToolContext, ToolDefinition


class ToolPolicyError(Exception):
    pass


class PolicyEngine:
    def enforce(self, definition: ToolDefinition, ctx: ToolContext) -> None:
        policy = definition.policy
        if not policy.enabled:
            raise ToolPolicyError(f"Tool '{definition.name}' is disabled by policy.")
        if policy.allowed_users and ctx.user_id not in policy.allowed_users:
            raise ToolPolicyError(f"User '{ctx.user_id}' not allowed for tool '{definition.name}'.")
        if policy.requires_approval and not ctx.approved:
            raise ToolPolicyError(
                f"Tool '{definition.name}' requires approval before execution."
            )
