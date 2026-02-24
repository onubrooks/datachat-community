"""Tool execution engine."""

from __future__ import annotations

import inspect
import logging
from typing import Any

from backend.tools.base import ToolContext
from backend.tools.policy import PolicyEngine, ToolPolicyError
from backend.tools.registry import ToolRegistry

logger = logging.getLogger(__name__)


class ToolExecutionError(Exception):
    pass


class ToolExecutor:
    def __init__(self, policy_engine: PolicyEngine | None = None) -> None:
        self.policy_engine = policy_engine or PolicyEngine()

    @staticmethod
    def _state_from_context(ctx: ToolContext) -> dict[str, Any]:
        state = ctx.state or {}
        return state if isinstance(state, dict) else {}

    def _is_tool_available(self, name: str, state: dict[str, Any]) -> bool:
        hook = ToolRegistry.get_is_tool_available_hook(name)
        if hook is None:
            return True
        try:
            return bool(hook(state))
        except Exception as exc:
            logger.warning("Tool availability hook failed for %s: %s", name, exc)
            return False

    def _append_forced_tools(
        self,
        tool_calls: list[dict[str, Any]],
        state: dict[str, Any],
    ) -> list[dict[str, Any]]:
        calls = [dict(call) for call in tool_calls]
        planned_names = {str(call.get("name")) for call in calls if call.get("name")}

        for definition in ToolRegistry.list_definitions():
            name = definition.name
            if name in planned_names:
                continue
            hook = ToolRegistry.get_run_if_true_hook(name)
            if hook is None:
                continue
            try:
                should_run = bool(hook(state))
            except Exception as exc:
                logger.warning("Tool forced-run hook failed for %s: %s", name, exc)
                continue
            if not should_run:
                continue

            required = (
                definition.parameters_schema.get("required", [])
                if isinstance(definition.parameters_schema, dict)
                else []
            )
            if required:
                logger.info(
                    "Skipping forced run for tool %s; required args are missing: %s",
                    name,
                    required,
                )
                continue
            calls.append({"name": name, "arguments": {}})
            planned_names.add(name)
        return calls

    async def execute(self, name: str, args: dict[str, Any], ctx: ToolContext) -> dict[str, Any]:
        definition = ToolRegistry.get_definition(name)
        handler = ToolRegistry.get_handler(name)
        if not definition or not handler:
            raise ToolExecutionError(f"Unknown tool: {name}")

        state = self._state_from_context(ctx)
        if not self._is_tool_available(name, state):
            raise ToolPolicyError(f"Tool '{name}' is not available for this request state.")

        self.policy_engine.enforce(definition, ctx)

        ctx.log_action("tool_invoked", {"tool": name, "args": list(args.keys())})

        try:
            if "ctx" in inspect.signature(handler).parameters:
                result = handler(**args, ctx=ctx)
            else:
                result = handler(**args)

            if inspect.isawaitable(result):
                result = await result

            ctx.log_action("tool_completed", {"tool": name})
            return {
                "tool": name,
                "success": True,
                "result": result,
            }
        except ToolPolicyError:
            raise
        except Exception as exc:
            logger.error(f"Tool execution failed: {name} - {exc}")
            raise ToolExecutionError(str(exc)) from exc

    async def execute_plan(
        self, tool_calls: list[dict[str, Any]], ctx: ToolContext
    ) -> list[dict[str, Any]]:
        state = self._state_from_context(ctx)
        expanded_calls = self._append_forced_tools(tool_calls, state)
        results = []
        for call in expanded_calls:
            name = call.get("name")
            args = call.get("arguments") or {}
            if not name:
                continue
            results.append(await self.execute(name, args, ctx))
        return results
