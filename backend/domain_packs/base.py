"""Domain-pack primitives for retrieval and deterministic SQL overrides."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from backend.models.agent import GeneratedSQL, SQLAgentInput

DomainAliasExpander = Callable[[str], list[str]]
DomainTemplateScoreAdjuster = Callable[[str, str], float]
DomainSQLBuilder = Callable[[Any, "SQLAgentInput"], "GeneratedSQL | None"]


@dataclass(frozen=True)
class DomainPack:
    """Declarative heuristics for a specific business domain."""

    name: str
    expand_aliases: DomainAliasExpander | None = None
    adjust_template_score: DomainTemplateScoreAdjuster | None = None
    sql_builders: tuple[tuple[str, DomainSQLBuilder], ...] = field(default_factory=tuple)
