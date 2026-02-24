"""Response synthesis agent: combine context and SQL results into final answer."""

from __future__ import annotations

import logging

from backend.agents.base import BaseAgent
from backend.config import get_settings
from backend.llm.factory import LLMProviderFactory
from backend.llm.models import LLMMessage, LLMRequest
from backend.prompts.loader import PromptLoader

logger = logging.getLogger(__name__)


class ResponseSynthesisAgent(BaseAgent):
    """Generate a unified response from context preface and SQL results."""

    def __init__(self, llm_provider=None) -> None:
        super().__init__(name="ResponseSynthesisAgent")
        self.config = get_settings()
        if llm_provider is None:
            self.llm = LLMProviderFactory.create_default_provider(
                self.config.llm, model_type="mini"
            )
        else:
            self.llm = llm_provider
        self.prompts = PromptLoader()

    async def execute(
        self,
        *,
        query: str,
        sql: str,
        result_summary: str,
        context_preface: str | None = None,
    ) -> str:
        prompt = self.prompts.render(
            "agents/response_synthesis.md",
            user_query=query,
            context_preface=context_preface or "None",
            sql=sql,
            result_summary=result_summary,
        )
        response = await self.llm.generate(
            LLMRequest(
                messages=[
                    LLMMessage(role="system", content=self.prompts.load("system/main.md")),
                    LLMMessage(role="user", content=prompt),
                ],
                temperature=0.2,
                max_tokens=600,
            )
        )
        return response.content.strip()
