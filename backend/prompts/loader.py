"""Prompt loading and rendering utilities."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from jinja2 import Environment, FileSystemLoader, TemplateNotFound


@dataclass(frozen=True)
class PromptEntry:
    """Loaded prompt content and metadata."""

    content: str
    metadata: dict[str, Any]


class FrontMatterLoader(FileSystemLoader):
    """Jinja2 loader that strips YAML front matter."""

    def get_source(self, environment: Environment, template: str):  # type: ignore[override]
        source, filename, uptodate = super().get_source(environment, template)
        if source.startswith("---"):
            parts = source.split("---", 2)
            if len(parts) == 3:
                source = parts[2].lstrip()
        return source, filename, uptodate


class PromptLoader:
    """Load and manage prompt templates."""

    def __init__(self, prompts_dir: str = "prompts") -> None:
        root = Path(__file__).resolve().parents[2]
        path = Path(prompts_dir)
        self.prompts_dir = path if path.is_absolute() else root / path
        self.cache: dict[str, PromptEntry] = {}
        self._env = Environment(loader=FrontMatterLoader(str(self.prompts_dir)))

    def load(self, prompt_path: str, version: str = "latest") -> str:
        """
        Load prompt from file.

        Args:
            prompt_path: Relative path (e.g., "agents/sql_generator.md")
            version: Specific version or "latest"

        Returns:
            Prompt content as string
        """
        file_path = self._resolve_path(prompt_path, version)
        cache_key = f"{prompt_path}:{version}"
        if cache_key in self.cache:
            return self.cache[cache_key].content

        if not file_path.exists():
            raise FileNotFoundError(f"Prompt not found: {file_path}")

        content = file_path.read_text(encoding="utf-8")
        metadata: dict[str, Any] = {}
        prompt_content = content

        if content.startswith("---"):
            parts = content.split("---", 2)
            if len(parts) == 3:
                metadata = yaml.safe_load(parts[1]) or {}
                prompt_content = parts[2].lstrip()

        self.cache[cache_key] = PromptEntry(content=prompt_content, metadata=metadata)
        return prompt_content

    def render(self, prompt_path: str, version: str = "latest", **variables: Any) -> str:
        """
        Load prompt and substitute variables using Jinja2.

        Example:
            prompt = loader.render(
                "agents/sql_generator.md",
                schema=schema_json,
                datapoints=datapoint_json
            )
        """
        template_path = self._template_path(prompt_path, version)
        try:
            template = self._env.get_template(template_path)
        except TemplateNotFound as exc:
            raise FileNotFoundError(f"Prompt not found: {template_path}") from exc
        return template.render(**variables)

    def get_metadata(self, prompt_path: str, version: str = "latest") -> dict[str, Any]:
        """Return metadata for a prompt (loads if needed)."""
        cache_key = f"{prompt_path}:{version}"
        if cache_key not in self.cache:
            self.load(prompt_path, version=version)
        return self.cache[cache_key].metadata

    def _resolve_path(self, prompt_path: str, version: str) -> Path:
        if version == "latest":
            return self.prompts_dir / prompt_path
        return self.prompts_dir / "versions" / version / prompt_path

    def _template_path(self, prompt_path: str, version: str) -> str:
        if version == "latest":
            return prompt_path
        return str(Path("versions") / version / prompt_path)
