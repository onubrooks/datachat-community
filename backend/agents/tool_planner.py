"""ToolPlannerAgent: choose tools to execute for a query."""

from __future__ import annotations

import json
import logging
from typing import Any

from backend.agents.base import BaseAgent
from backend.config import get_settings
from backend.llm.factory import LLMProviderFactory
from backend.llm.models import LLMMessage, LLMRequest
from backend.models import ToolPlan, ToolPlannerAgentInput, ToolPlannerAgentOutput
from backend.prompts.loader import PromptLoader

logger = logging.getLogger(__name__)


class ToolPlannerAgent(BaseAgent):
    """Select tools for a given query."""

    def __init__(self, llm_provider=None) -> None:
        super().__init__(name="ToolPlannerAgent")
        self.config = get_settings()
        if llm_provider is None:
            self.llm = LLMProviderFactory.create_default_provider(
                self.config.llm, model_type="mini"
            )
        else:
            self.llm = llm_provider
        self.prompts = PromptLoader()

    async def execute(self, input: ToolPlannerAgentInput) -> ToolPlannerAgentOutput:
        logger.info(f"[{self.name}] Planning tools for query")

        tool_list = json.dumps(input.available_tools, indent=2)
        prompt = self.prompts.render(
            "agents/tool_planner.md",
            user_query=input.query,
            tool_list=tool_list,
        )
        response = await self.llm.generate(
            LLMRequest(
                messages=[
                    LLMMessage(role="system", content=self.prompts.load("system/main.md")),
                    LLMMessage(role="user", content=prompt),
                ],
                temperature=0.0,
                max_tokens=800,
            )
        )
        plan = self._parse_plan(response.content)
        plan = self._coerce_arguments_to_tool_schemas(plan, input.available_tools)

        metadata = self._create_metadata()
        metadata.llm_calls = 1
        return ToolPlannerAgentOutput(success=True, plan=plan, metadata=metadata)

    def _parse_plan(self, content: str) -> ToolPlan:
        payload = self._extract_json(content)
        if payload:
            try:
                return ToolPlan.model_validate(payload)
            except Exception:
                logger.debug("ToolPlannerAgent payload failed validation")

        return ToolPlan(tool_calls=[], rationale="Fallback to pipeline.", fallback="pipeline")

    def _coerce_arguments_to_tool_schemas(
        self, plan: ToolPlan, available_tools: list[dict[str, Any]]
    ) -> ToolPlan:
        schema_by_tool = {}
        for tool in available_tools:
            name = tool.get("name")
            if not name:
                continue
            schema_by_tool[name] = tool.get("parameters_schema") or {}

        for call in plan.tool_calls:
            schema = schema_by_tool.get(call.name) or {}
            properties = schema.get("properties") or {}
            coerced: dict[str, Any] = {}
            for arg_name, arg_value in call.arguments.items():
                field_schema = properties.get(arg_name) or {}
                coerced[arg_name] = self._coerce_argument_value(arg_value, field_schema)
            call.arguments = coerced

        return plan

    def _coerce_argument_value(self, value: Any, schema: dict[str, Any]) -> Any:
        if value is None:
            return None
        expected_type = schema.get("type")
        if not expected_type and "anyOf" in schema:
            for variant in schema.get("anyOf", []):
                if variant.get("type") == "null":
                    continue
                expected_type = variant.get("type")
                if expected_type:
                    break

        try:
            if expected_type == "integer":
                if isinstance(value, bool):
                    return value
                return int(value)
            if expected_type == "number":
                if isinstance(value, bool):
                    return value
                return float(value)
            if expected_type == "boolean":
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    lowered = value.strip().lower()
                    if lowered in {"true", "1", "yes", "y"}:
                        return True
                    if lowered in {"false", "0", "no", "n"}:
                        return False
                return value
            if expected_type == "array":
                if isinstance(value, list):
                    return value
                if isinstance(value, str):
                    raw = value.strip()
                    if raw.startswith("[") and raw.endswith("]"):
                        parsed = json.loads(raw)
                        if isinstance(parsed, list):
                            return parsed
                    if "," in raw:
                        return [item.strip() for item in raw.split(",") if item.strip()]
                    return [raw] if raw else []
            if expected_type == "object":
                if isinstance(value, dict):
                    return value
                if isinstance(value, str):
                    raw = value.strip()
                    if raw.startswith("{") and raw.endswith("}"):
                        parsed = json.loads(raw)
                        if isinstance(parsed, dict):
                            return parsed
        except Exception:
            logger.debug("ToolPlannerAgent argument coercion failed; leaving value unchanged")
        return value

    @staticmethod
    def _extract_json(content: str) -> dict[str, Any] | None:
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            start = content.find("{")
            end = content.rfind("}") + 1
            if start == -1 or end <= start:
                return None
            try:
                return json.loads(content[start:end])
            except json.JSONDecodeError:
                return None
