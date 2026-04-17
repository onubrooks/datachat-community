"""Domain-pack registry."""

from backend.domain_packs.base import DomainPack
from backend.domain_packs.packs import DOMAIN_PACKS

__all__ = ["DOMAIN_PACKS", "DomainPack"]
