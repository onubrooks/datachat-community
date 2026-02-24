#!/usr/bin/env python3
"""
Progressive latency benchmark for pipeline optimizations.

Runs the same query suite across cumulative stages:
  baseline -> stage1 -> stage2 -> ... -> stage6

Each stage toggles performance flags through env vars, rebuilds settings/pipeline,
and records latency + quality guard metrics.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.cli import create_pipeline_from_config
from backend.config import clear_settings_cache

DEFAULT_QUERIES = [
    "List all available tables",
    "Show first 5 rows from information_schema.tables",
    "How many rows are in information_schema.tables?",
]

FLAG_KEYS = [
    "PIPELINE_SQL_TWO_STAGE_ENABLED",
    "PIPELINE_SQL_PROMPT_BUDGET_ENABLED",
    "PIPELINE_SYNTHESIZE_SIMPLE_SQL_ANSWERS",
    "PIPELINE_SELECTIVE_TOOL_PLANNER_ENABLED",
    "PIPELINE_SCHEMA_SNAPSHOT_CACHE_ENABLED",
    "PIPELINE_CLASSIFIER_DEEP_LOW_CONFIDENCE_THRESHOLD",
    "PIPELINE_CLASSIFIER_DEEP_MIN_QUERY_LENGTH",
]


@dataclass
class StageConfig:
    name: str
    env: dict[str, str]


def stage_configs() -> list[StageConfig]:
    return [
        StageConfig(
            name="baseline",
            env={
                "PIPELINE_SQL_TWO_STAGE_ENABLED": "false",
                "PIPELINE_SQL_PROMPT_BUDGET_ENABLED": "false",
                "PIPELINE_SYNTHESIZE_SIMPLE_SQL_ANSWERS": "true",
                "PIPELINE_SELECTIVE_TOOL_PLANNER_ENABLED": "false",
                "PIPELINE_SCHEMA_SNAPSHOT_CACHE_ENABLED": "false",
                "PIPELINE_CLASSIFIER_DEEP_LOW_CONFIDENCE_THRESHOLD": "0.6",
                "PIPELINE_CLASSIFIER_DEEP_MIN_QUERY_LENGTH": "1",
            },
        ),
        StageConfig(
            name="stage1_sql_two_stage",
            env={"PIPELINE_SQL_TWO_STAGE_ENABLED": "true"},
        ),
        StageConfig(
            name="stage2_prompt_budget",
            env={"PIPELINE_SQL_PROMPT_BUDGET_ENABLED": "true"},
        ),
        StageConfig(
            name="stage3_simple_sql_synthesis_off",
            env={"PIPELINE_SYNTHESIZE_SIMPLE_SQL_ANSWERS": "false"},
        ),
        StageConfig(
            name="stage4_classifier_deep_gate",
            env={
                "PIPELINE_CLASSIFIER_DEEP_LOW_CONFIDENCE_THRESHOLD": "0.45",
                "PIPELINE_CLASSIFIER_DEEP_MIN_QUERY_LENGTH": "28",
            },
        ),
        StageConfig(
            name="stage5_selective_tool_planner",
            env={"PIPELINE_SELECTIVE_TOOL_PLANNER_ENABLED": "true"},
        ),
        StageConfig(
            name="stage6_schema_snapshot_cache",
            env={"PIPELINE_SCHEMA_SNAPSHOT_CACHE_ENABLED": "true"},
        ),
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Progressive latency benchmark")
    parser.add_argument(
        "--iterations",
        type=int,
        default=2,
        help="Iterations per query per stage (default: 2)",
    )
    parser.add_argument(
        "--queries-file",
        type=Path,
        default=None,
        help="Optional text file with one query per line.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports"),
        help="Directory for markdown/json reports.",
    )
    parser.add_argument(
        "--mode",
        choices=("cumulative", "isolated"),
        default="cumulative",
        help=(
            "Benchmark mode. "
            "'cumulative' applies stage flags progressively. "
            "'isolated' applies each stage on top of baseline only."
        ),
    )
    return parser.parse_args()


def load_queries(path: Path | None) -> list[str]:
    if path is None:
        return DEFAULT_QUERIES
    lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
    return [line for line in lines if line and not line.startswith("#")]


def apply_env_values(values: dict[str, str]) -> None:
    for key in FLAG_KEYS:
        if key in values:
            os.environ[key] = values[key]
    clear_settings_cache()


def apply_stage_env(accumulated: dict[str, str], stage: StageConfig) -> dict[str, str]:
    merged = dict(accumulated)
    merged.update(stage.env)
    apply_env_values(merged)
    return merged


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    idx = (len(ordered) - 1) * p
    lower = int(idx)
    upper = min(lower + 1, len(ordered) - 1)
    if lower == upper:
        return ordered[lower]
    frac = idx - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * frac


async def run_stage(
    stage_name: str,
    queries: list[str],
    iterations: int,
) -> dict[str, Any]:
    pipeline = await create_pipeline_from_config()
    try:
        # Warm-up
        await pipeline.run(query=queries[0], conversation_history=[])

        runs: list[dict[str, Any]] = []
        for _ in range(iterations):
            for query in queries:
                result = await pipeline.run(query=query, conversation_history=[])
                runs.append(
                    {
                        "query": query,
                        "latency_ms": float(result.get("total_latency_ms", 0.0)),
                        "llm_calls": int(result.get("llm_calls", 0)),
                        "answer_source": result.get("answer_source"),
                        "has_error": bool(result.get("error")),
                        "clarifications": len(result.get("clarifying_questions") or []),
                        "agent_timings": result.get("agent_timings", {}),
                    }
                )

        latencies = [run["latency_ms"] for run in runs]
        llm_calls = [float(run["llm_calls"]) for run in runs]
        success_runs = [run for run in runs if not run["has_error"]]
        clarification_runs = [run for run in runs if run["clarifications"] > 0]
        sql_runs = [run for run in runs if run["answer_source"] == "sql"]

        agent_agg: dict[str, list[float]] = {}
        for run in runs:
            for agent, ms in run["agent_timings"].items():
                agent_agg.setdefault(agent, []).append(float(ms))
        agent_mean = {
            agent: (sum(values) / len(values) if values else 0.0)
            for agent, values in agent_agg.items()
        }

        return {
            "stage": stage_name,
            "query_count": len(runs),
            "latency_ms": {
                "mean": sum(latencies) / len(latencies) if latencies else 0.0,
                "median": statistics.median(latencies) if latencies else 0.0,
                "p95": percentile(latencies, 0.95),
            },
            "llm_calls_mean": sum(llm_calls) / len(llm_calls) if llm_calls else 0.0,
            "success_rate": len(success_runs) / len(runs) if runs else 0.0,
            "clarification_rate": len(clarification_runs) / len(runs) if runs else 0.0,
            "sql_answer_rate": len(sql_runs) / len(runs) if runs else 0.0,
            "agent_mean_ms": dict(sorted(agent_mean.items(), key=lambda item: item[1], reverse=True)),
            "runs": runs,
        }
    finally:
        try:
            await pipeline.connector.close()
        except Exception:
            pass


def render_markdown(
    results: list[dict[str, Any]], queries: list[str], iterations: int, mode: str
) -> str:
    lines = [
        "# Progressive Latency Benchmark",
        "",
        f"- Timestamp (UTC): {datetime.now(UTC).isoformat()}",
        f"- Iterations: {iterations}",
        f"- Mode: {mode}",
        f"- Queries per iteration: {len(queries)}",
        "",
        "## Query Suite",
    ]
    for query in queries:
        lines.append(f"- `{query}`")
    lines.append("")

    lines.append("## Stage Summary")
    lines.append("")
    lines.append(
        "| Stage | Mean Latency (ms) | Median (ms) | P95 (ms) | Mean LLM Calls | Success Rate | Clarification Rate | SQL Answer Rate |"
    )
    lines.append(
        "|---|---:|---:|---:|---:|---:|---:|---:|"
    )
    for item in results:
        lines.append(
            "| "
            f"{item['stage']} | "
            f"{item['latency_ms']['mean']:.1f} | "
            f"{item['latency_ms']['median']:.1f} | "
            f"{item['latency_ms']['p95']:.1f} | "
            f"{item['llm_calls_mean']:.2f} | "
            f"{item['success_rate']:.2%} | "
            f"{item['clarification_rate']:.2%} | "
            f"{item['sql_answer_rate']:.2%} |"
        )
    lines.append("")

    lines.append("## Per-Agent Mean Latency (ms)")
    lines.append("")
    for item in results:
        lines.append(f"### {item['stage']}")
        for agent, ms in item["agent_mean_ms"].items():
            lines.append(f"- `{agent}`: {ms:.1f}ms")
        lines.append("")
    return "\n".join(lines)


async def main() -> None:
    args = parse_args()
    queries = load_queries(args.queries_file)
    stages = stage_configs()
    results: list[dict[str, Any]] = []

    if args.mode == "cumulative":
        env_accumulated: dict[str, str] = {}
        for stage in stages:
            env_accumulated = apply_stage_env(env_accumulated, stage)
            print(f"[benchmark] running {stage.name} ...")
            stage_result = await run_stage(stage.name, queries, args.iterations)
            results.append(stage_result)
            print(
                f"[benchmark] {stage.name}: "
                f"mean={stage_result['latency_ms']['mean']:.1f}ms, "
                f"p95={stage_result['latency_ms']['p95']:.1f}ms, "
                f"llm={stage_result['llm_calls_mean']:.2f}"
            )
    else:
        baseline = stages[0]
        for stage in stages:
            values = dict(baseline.env)
            if stage.name != baseline.name:
                values.update(stage.env)
            apply_env_values(values)
            print(f"[benchmark] running {stage.name} (isolated) ...")
            stage_result = await run_stage(stage.name, queries, args.iterations)
            results.append(stage_result)
            print(
                f"[benchmark] {stage.name}: "
                f"mean={stage_result['latency_ms']['mean']:.1f}ms, "
                f"p95={stage_result['latency_ms']['p95']:.1f}ms, "
                f"llm={stage_result['llm_calls_mean']:.2f}"
            )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    json_path = args.output_dir / f"latency_progressive_{ts}.json"
    md_path = args.output_dir / f"latency_progressive_{ts}.md"

    json_path.write_text(
        json.dumps({"queries": queries, "mode": args.mode, "results": results}, indent=2),
        encoding="utf-8",
    )
    md_path.write_text(render_markdown(results, queries, args.iterations, args.mode), encoding="utf-8")

    print(f"[benchmark] wrote {json_path}")
    print(f"[benchmark] wrote {md_path}")


if __name__ == "__main__":
    asyncio.run(main())
