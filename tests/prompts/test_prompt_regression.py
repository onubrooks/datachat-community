from hashlib import sha256

from backend.prompts.loader import PromptLoader


def _hash(content: str) -> str:
    return sha256(content.encode("utf-8")).hexdigest()[:12]


def test_prompt_hashes_unchanged():
    loader = PromptLoader()
    expected = {
        "system/main.md": "83b1fc0f071c",
        "agents/sql_generator.md": "304baabadf76",
        "agents/executor_summary.md": "62cdfade0184",
        "agents/sql_correction.md": "7334d4e253be",
    }

    for path, expected_hash in expected.items():
        content = loader.load(path)
        assert _hash(content) == expected_hash, (
            f"Prompt changed: {path}. Update hash if intentional."
        )
