from backend.prompts.loader import PromptLoader


def test_prompt_loader_strips_front_matter():
    loader = PromptLoader()
    content = loader.load("system/main.md")
    assert not content.startswith("---")
    assert "DataChat System Prompt" in content


def test_prompt_loader_renders_template():
    loader = PromptLoader()
    rendered = loader.render(
        "agents/sql_generator.md",
        user_query="Test query",
        schema_context="Tables: users",
        business_context="None",
        backend="postgresql",
        user_preferences={"default_limit": 100},
    )
    assert "Test query" in rendered
