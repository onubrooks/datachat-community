"""
Pipeline package for DataChat.

Contains the LangGraph orchestrator that connects all agents into a complete pipeline.
"""

from backend.pipeline.orchestrator import DataChatPipeline, create_pipeline

__all__ = ["DataChatPipeline", "create_pipeline"]
