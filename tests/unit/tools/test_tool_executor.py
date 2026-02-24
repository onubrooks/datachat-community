import pytest

from backend.tools.base import ToolCategory, ToolContext, tool
from backend.tools.executor import ToolExecutor
from backend.tools.policy import ToolPolicyError
from backend.tools.registry import ToolRegistry


@tool(
    name="test_requires_approval",
    description="Test tool requiring approval",
    category=ToolCategory.SYSTEM,
    requires_approval=True,
)
def _test_tool(value: str, ctx: ToolContext | None = None):
    return {"value": value}


@tool(
    name="test_typed_schema",
    description="Tool with typed arguments",
    category=ToolCategory.SYSTEM,
)
def _typed_schema_tool(
    limit: int = 5,
    include_stats: bool = False,
    threshold: float = 0.25,
    tags: list[str] | None = None,
    options: dict[str, int] | None = None,
    ctx: ToolContext | None = None,
):
    return {
        "limit": limit,
        "include_stats": include_stats,
        "threshold": threshold,
        "tags": tags or [],
        "options": options or {},
    }


@tool(
    name="test_available_only_when_flagged",
    description="Tool availability depends on request state",
    category=ToolCategory.SYSTEM,
    is_tool_available=lambda state: bool(state.get("allow_available_tool")),
)
def _available_only_when_flagged(ctx: ToolContext | None = None):
    return {"ok": True}


@tool(
    name="test_force_run_hook",
    description="Runs automatically when state flag is enabled",
    category=ToolCategory.SYSTEM,
    run_if_true=lambda state: bool(state.get("force_auto_tool")),
)
def _force_run_hook(ctx: ToolContext | None = None):
    return {"forced": True}


@pytest.mark.asyncio
async def test_tool_executor_blocks_without_approval():
    executor = ToolExecutor()
    ctx = ToolContext(user_id="tester", correlation_id="test-1", approved=False)
    with pytest.raises(ToolPolicyError):
        await executor.execute("test_requires_approval", {"value": "hi"}, ctx)


@pytest.mark.asyncio
async def test_tool_executor_runs_with_approval():
    executor = ToolExecutor()
    ctx = ToolContext(user_id="tester", correlation_id="test-2", approved=True)
    result = await executor.execute("test_requires_approval", {"value": "hi"}, ctx)
    assert result["success"] is True
    assert result["result"]["value"] == "hi"


@pytest.mark.asyncio
async def test_tool_executor_blocks_when_availability_hook_returns_false():
    executor = ToolExecutor()
    ctx = ToolContext(
        user_id="tester",
        correlation_id="test-availability",
        approved=True,
        state={"allow_available_tool": False},
    )
    with pytest.raises(ToolPolicyError):
        await executor.execute("test_available_only_when_flagged", {}, ctx)


@pytest.mark.asyncio
async def test_tool_executor_appends_forced_tools_from_run_if_true_hook():
    executor = ToolExecutor()
    ctx = ToolContext(
        user_id="tester",
        correlation_id="test-forced-run",
        approved=True,
        state={"force_auto_tool": True},
    )
    results = await executor.execute_plan([], ctx)
    assert len(results) == 1
    assert results[0]["tool"] == "test_force_run_hook"
    assert results[0]["result"]["forced"] is True


def test_tool_schema_uses_typed_parameter_definitions():
    definition = ToolRegistry.get_definition("test_typed_schema")
    assert definition is not None
    schema = definition.parameters_schema
    props = schema["properties"]
    assert props["limit"]["type"] == "integer"
    assert props["include_stats"]["type"] == "boolean"
    assert props["threshold"]["type"] == "number"
    assert any(
        item.get("type") == "array"
        for item in props["tags"].get("anyOf", [])
        if isinstance(item, dict)
    )
    assert any(
        item.get("type") == "object"
        for item in props["options"].get("anyOf", [])
        if isinstance(item, dict)
    )
    assert schema["additionalProperties"] is False
