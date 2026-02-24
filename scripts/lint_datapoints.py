"""Lint DataPoint contract quality."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from backend.knowledge.contracts import validate_contracts
from backend.knowledge.datapoints import DataPointLoader


def _collect_json_files(root: Path, recursive: bool) -> list[Path]:
    pattern = "**/*.json" if recursive else "*.json"
    return [path for path in root.glob(pattern) if path.is_file()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Lint DataPoint metadata contracts.")
    parser.add_argument(
        "--path",
        default="datapoints",
        help="DataPoints directory or file path.",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Recurse into subdirectories when --path is a directory.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat advisory metadata gaps as errors.",
    )
    parser.add_argument(
        "--fail-on-warnings",
        action="store_true",
        help="Exit non-zero when warnings are present.",
    )
    args = parser.parse_args()

    target = Path(args.path)
    if not target.exists():
        print(f"Path not found: {target}")
        return 1

    loader = DataPointLoader()
    datapoints = []
    files_to_load: list[Path]
    if target.is_file():
        files_to_load = [target]
    else:
        files_to_load = _collect_json_files(target, args.recursive)

    for file_path in files_to_load:
        try:
            datapoints.append(loader.load_file(file_path))
        except Exception as exc:  # pragma: no cover - exercised via loader tests
            print(f"[ERROR] {file_path}: {exc}")

    if not datapoints:
        print("No valid DataPoints loaded.")
        return 1

    reports = validate_contracts(datapoints, strict=args.strict)
    error_count = 0
    warning_count = 0

    for report in reports:
        for issue in report.issues:
            if issue.severity == "error":
                error_count += 1
            else:
                warning_count += 1
            field_hint = f" ({issue.field})" if issue.field else ""
            print(
                f"[{issue.severity.upper()}] {report.datapoint_id}: "
                f"{issue.code}{field_hint} - {issue.message}"
            )

    print(
        f"\nLint summary: datapoints={len(reports)} errors={error_count} warnings={warning_count}"
    )
    if error_count > 0:
        return 1
    if args.fail_on_warnings and warning_count > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
