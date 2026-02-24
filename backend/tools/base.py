"""Tool system base types and decorator."""

from __future__ import annotations

import inspect
import logging
import types
from collections.abc import Callable
from enum import StrEnum
from typing import Any, Literal, Union, get_args, get_origin, get_type_hints

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
NONE_TYPE = type(None)


class ToolCategory(StrEnum):
    DATABASE = "database"
    PROFILING = "profiling"
    KNOWLEDGE = "knowledge"
    SYSTEM = "system"


class ToolPolicy(BaseModel):
    enabled: bool = True
    requires_approval: bool = False
    max_execution_time_seconds: int = Field(default=30, ge=1)
    allowed_users: list[str] | None = None


class ToolDefinition(BaseModel):
    name: str
    description: str
    category: ToolCategory
    policy: ToolPolicy
    parameters_schema: dict[str, Any]
    return_schema: dict[str, Any]


class ToolContext(BaseModel):
    user_id: str
    correlation_id: str
    approved: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)
    state: dict[str, Any] | None = None

    def log_action(self, action: str, metadata: dict[str, Any]) -> None:
        logger.info(
            "tool_action",
            extra={
                "user_id": self.user_id,
                "correlation_id": self.correlation_id,
                "action": action,
                "metadata": metadata,
            },
        )


def _extract_parameters_schema(func: Callable[..., Any]) -> dict[str, Any]:
    signature = inspect.signature(func)
    type_hints = get_type_hints(func, include_extras=True)
    properties: dict[str, Any] = {}
    required: list[str] = []

    for name, param in signature.parameters.items():
        if name in ("ctx", "context"):
            continue
        resolved_annotation = type_hints.get(name, param.annotation)
        param_schema = _annotation_to_json_schema(resolved_annotation)
        if param.default is inspect.Parameter.empty:
            required.append(name)
        else:
            if param.default is None:
                param_schema = _ensure_nullable(param_schema)
            else:
                param_schema["default"] = param.default
        properties[name] = param_schema

    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


def _extract_return_schema(func: Callable[..., Any]) -> dict[str, Any]:
    return {"type": "object", "additionalProperties": True}


def _annotation_to_json_schema(annotation: Any) -> dict[str, Any]:
    if annotation is inspect.Parameter.empty or annotation is Any:
        return {}

    origin = get_origin(annotation)
    if origin is not None:
        return _origin_to_schema(origin, get_args(annotation))

    if annotation is bool:
        return {"type": "boolean"}
    if annotation is int:
        return {"type": "integer"}
    if annotation is float:
        return {"type": "number"}
    if annotation is str:
        return {"type": "string"}
    if annotation in (dict,):
        return {"type": "object", "additionalProperties": True}
    if annotation in (list, tuple, set, frozenset):
        return {"type": "array", "items": {}}
    if annotation is NONE_TYPE:
        return {"type": "null"}

    if inspect.isclass(annotation):
        if issubclass(annotation, BaseModel):
            schema = annotation.model_json_schema()
            schema.pop("title", None)
            return schema
        return {"type": "string"}

    return {"type": "string"}


def _origin_to_schema(origin: Any, args: tuple[Any, ...]) -> dict[str, Any]:
    if origin is Literal:
        values = list(args)
        if not values:
            return {}
        value_types = {type(value) for value in values}
        schema: dict[str, Any] = {"enum": values}
        if len(value_types) == 1:
            only = next(iter(value_types))
            if only is bool:
                schema["type"] = "boolean"
            elif only is int:
                schema["type"] = "integer"
            elif only is float:
                schema["type"] = "number"
            elif only is str:
                schema["type"] = "string"
        return schema

    if origin in (list, tuple, set, frozenset):
        item_schema = _annotation_to_json_schema(args[0]) if args else {}
        return {"type": "array", "items": item_schema}

    if origin is dict:
        value_schema = _annotation_to_json_schema(args[1]) if len(args) > 1 else {}
        return {"type": "object", "additionalProperties": value_schema or True}

    if origin in (Union,):
        return _union_to_schema(args)

    if origin is types.UnionType:
        return _union_to_schema(args)

    return _annotation_to_json_schema(origin)


def _union_to_schema(args: tuple[Any, ...]) -> dict[str, Any]:
    non_none = [arg for arg in args if arg is not NONE_TYPE]
    has_none = len(non_none) != len(args)
    if len(non_none) == 1:
        base_schema = _annotation_to_json_schema(non_none[0])
        return _ensure_nullable(base_schema) if has_none else base_schema
    variants = [_annotation_to_json_schema(arg) for arg in non_none]
    if has_none:
        variants.append({"type": "null"})
    return {"anyOf": variants}


def _ensure_nullable(schema: dict[str, Any]) -> dict[str, Any]:
    if not schema:
        return {"anyOf": [{}, {"type": "null"}]}
    if schema.get("type") == "null":
        return schema
    if "anyOf" in schema:
        variants = schema["anyOf"]
        if not any(variant.get("type") == "null" for variant in variants if isinstance(variant, dict)):
            return {**schema, "anyOf": [*variants, {"type": "null"}]}
        return schema
    return {"anyOf": [schema, {"type": "null"}]}


def tool(
    name: str,
    description: str,
    category: ToolCategory,
    requires_approval: bool = False,
    is_tool_available: Callable[[dict[str, Any]], bool] | None = None,
    run_if_true: Callable[[dict[str, Any]], bool] | None = None,
    **policy_kwargs: Any,
):
    def decorator(func: Callable[..., Any]):
        from backend.tools.registry import ToolRegistry

        tool_def = ToolDefinition(
            name=name,
            description=description,
            category=category,
            policy=ToolPolicy(requires_approval=requires_approval, **policy_kwargs),
            parameters_schema=_extract_parameters_schema(func),
            return_schema=_extract_return_schema(func),
        )
        ToolRegistry.register(
            tool_def,
            func,
            is_tool_available=is_tool_available,
            run_if_true=run_if_true,
        )
        return func

    return decorator
