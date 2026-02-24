"""Auto-profiling package."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backend.profiling.generator import DataPointGenerator
    from backend.profiling.profiler import SchemaProfiler
    from backend.profiling.store import ProfilingStore


__all__ = ["SchemaProfiler", "DataPointGenerator", "ProfilingStore"]


def __getattr__(name: str):
    if name == "SchemaProfiler":
        from backend.profiling.profiler import SchemaProfiler

        return SchemaProfiler
    if name == "DataPointGenerator":
        from backend.profiling.generator import DataPointGenerator

        return DataPointGenerator
    if name == "ProfilingStore":
        from backend.profiling.store import ProfilingStore

        return ProfilingStore
    raise AttributeError(name)
