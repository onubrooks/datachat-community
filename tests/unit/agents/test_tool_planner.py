from backend.agents.tool_planner import ToolPlannerAgent


def test_tool_planner_parses_valid_plan():
    agent = ToolPlannerAgent.__new__(ToolPlannerAgent)
    content = """
    {
      "tool_calls": [
        {"name": "list_tables", "arguments": {"schema": "public"}}
      ],
      "rationale": "Need tables",
      "fallback": "pipeline"
    }
    """
    plan = agent._parse_plan(content)
    assert plan.tool_calls[0].name == "list_tables"


def test_tool_planner_falls_back_on_invalid():
    agent = ToolPlannerAgent.__new__(ToolPlannerAgent)
    plan = agent._parse_plan("not json")
    assert plan.fallback == "pipeline"


def test_tool_planner_coerces_arguments_from_schema_types():
    agent = ToolPlannerAgent.__new__(ToolPlannerAgent)
    content = """
    {
      "tool_calls": [
        {
          "name": "get_table_sample",
          "arguments": {
            "table": "orders",
            "limit": "7",
            "include_stats": "true",
            "tags": "a,b",
            "options": "{\\"retry\\": 2}"
          }
        }
      ],
      "rationale": "Need sample",
      "fallback": "pipeline"
    }
    """
    plan = agent._parse_plan(content)
    available_tools = [
        {
            "name": "get_table_sample",
            "parameters_schema": {
                "type": "object",
                "properties": {
                    "table": {"type": "string"},
                    "limit": {"type": "integer"},
                    "include_stats": {"type": "boolean"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "options": {"type": "object", "additionalProperties": {"type": "integer"}},
                },
            },
        }
    ]

    plan = agent._coerce_arguments_to_tool_schemas(plan, available_tools)
    args = plan.tool_calls[0].arguments
    assert args["limit"] == 7
    assert args["include_stats"] is True
    assert args["tags"] == ["a", "b"]
    assert args["options"] == {"retry": 2}
