"""
DataPoint Loader

Loads and validates DataPoint definitions from JSON files.
Supports single file and directory loading with error handling.
"""

import json
import logging
from pathlib import Path

from pydantic import TypeAdapter, ValidationError

from backend.knowledge.contracts import validate_datapoint_contract
from backend.models.datapoint import DataPoint

logger = logging.getLogger(__name__)

# TypeAdapter for validating DataPoint union
datapoint_adapter = TypeAdapter(DataPoint)


class DataPointLoadError(Exception):
    """Raised when a DataPoint fails to load."""

    def __init__(self, file_path: Path, message: str, original_error: Exception = None):
        self.file_path = file_path
        self.original_error = original_error
        super().__init__(f"Failed to load {file_path}: {message}")


class DataPointLoader:
    """
    Loader for DataPoint definitions from JSON files.

    Validates JSON files against Pydantic DataPoint models and returns
    typed DataPoint objects. Supports loading individual files or entire
    directories with graceful error handling.

    Usage:
        loader = DataPointLoader()

        # Load single file
        datapoint = loader.load_file("path/to/datapoint.json")

        # Load directory
        datapoints = loader.load_directory("path/to/datapoints/")

        # Get loading statistics
        stats = loader.get_stats()
    """

    def __init__(
        self,
        *,
        strict_contracts: bool = False,
        fail_on_contract_warnings: bool = False,
    ):
        """Initialize the DataPoint loader."""
        self.loaded_count = 0
        self.failed_count = 0
        self.failed_files: list[tuple[Path, str]] = []
        self.strict_contracts = strict_contracts
        self.fail_on_contract_warnings = fail_on_contract_warnings

        logger.info("DataPointLoader initialized")

    @staticmethod
    def _infer_source_tier(file_path: Path) -> str:
        """Infer DataPoint source tier from file path."""
        path_parts = [part.lower() for part in file_path.resolve().parts]
        datapoints_index = -1
        for idx, part in enumerate(path_parts):
            if part == "datapoints":
                datapoints_index = idx

        if datapoints_index >= 0 and datapoints_index + 1 < len(path_parts):
            tier_segment = path_parts[datapoints_index + 1]
            if tier_segment == "user":
                return "user"
            if tier_segment == "managed":
                return "managed"
            if tier_segment == "examples":
                return "example"
            if tier_segment == "demo":
                return "demo"
        return "custom"

    @classmethod
    def _annotate_source_metadata(cls, datapoint: DataPoint, file_path: Path) -> None:
        """Add internal source metadata used for retrieval precedence."""
        source_tier = cls._infer_source_tier(file_path)
        source_path = str(file_path.resolve())
        if datapoint.metadata is None:
            datapoint.metadata = {}
        datapoint.metadata["source_tier"] = source_tier
        datapoint.metadata["source_path"] = source_path

    def load_file(self, file_path: str | Path) -> DataPoint:
        """
        Load and validate a single DataPoint from a JSON file.

        Args:
            file_path: Path to the JSON file

        Returns:
            Validated DataPoint object (SchemaDataPoint, BusinessDataPoint, or ProcessDataPoint)

        Raises:
            DataPointLoadError: If file cannot be read or validated
        """
        file_path = Path(file_path)

        logger.debug(f"Loading DataPoint from {file_path}")

        try:
            # Read JSON file
            with open(file_path, encoding="utf-8") as f:
                data = json.load(f)

            # Validate against Pydantic model using TypeAdapter
            datapoint = datapoint_adapter.validate_python(data)
            self._annotate_source_metadata(datapoint, file_path)
            self._validate_contract(datapoint, file_path)

            self.loaded_count += 1
            logger.info(
                f"Successfully loaded DataPoint: {datapoint.datapoint_id} "
                f"(type: {datapoint.type}) from {file_path.name}"
            )

            return datapoint

        except FileNotFoundError as e:
            self.failed_count += 1
            self.failed_files.append((file_path, "File not found"))
            logger.error(f"File not found: {file_path}")
            raise DataPointLoadError(file_path, "File not found", e) from e

        except json.JSONDecodeError as e:
            self.failed_count += 1
            self.failed_files.append((file_path, f"Invalid JSON: {e}"))
            logger.error(f"Invalid JSON in {file_path}: {e}")
            raise DataPointLoadError(file_path, f"Invalid JSON: {e}", e) from e

        except ValidationError as e:
            self.failed_count += 1
            error_msg = self._format_validation_error(e)
            self.failed_files.append((file_path, error_msg))
            logger.error(f"Validation failed for {file_path}: {error_msg}")
            raise DataPointLoadError(file_path, f"Validation failed: {error_msg}", e) from e

        except Exception as e:
            self.failed_count += 1
            self.failed_files.append((file_path, str(e)))
            logger.error(f"Unexpected error loading {file_path}: {e}")
            raise DataPointLoadError(file_path, f"Unexpected error: {e}", e) from e

    def _validate_contract(self, datapoint: DataPoint, file_path: Path) -> None:
        """Validate metadata contract if contract checks are enabled."""
        if not self.strict_contracts and not self.fail_on_contract_warnings:
            return

        source_tier = str((datapoint.metadata or {}).get("source_tier", "")).lower()
        effective_strict = self.strict_contracts and source_tier != "demo"
        report = validate_datapoint_contract(datapoint, strict=effective_strict)
        contract_errors = list(report.errors)

        if self.fail_on_contract_warnings:
            contract_errors.extend(report.warnings)

        if not contract_errors:
            return

        issue_parts = [
            f"{issue.code} ({issue.field or 'metadata'}): {issue.message}"
            for issue in contract_errors[:5]
        ]
        if len(contract_errors) > 5:
            issue_parts.append(f"... +{len(contract_errors) - 5} more")
        message = (
            f"Contract validation failed for {datapoint.datapoint_id}: "
            + "; ".join(issue_parts)
        )
        raise DataPointLoadError(file_path, message)

    def load_directory(
        self,
        directory_path: str | Path,
        recursive: bool = False,
        skip_errors: bool = True,
    ) -> list[DataPoint]:
        """
        Load all DataPoint JSON files from a directory.

        Args:
            directory_path: Path to the directory
            recursive: Whether to search subdirectories (default: False)
            skip_errors: Whether to skip files with errors (default: True)
                        If False, raises on first error

        Returns:
            List of validated DataPoint objects

        Raises:
            DataPointLoadError: If directory doesn't exist or if skip_errors=False
                               and a file fails to load
        """
        directory_path = Path(directory_path)

        if not directory_path.exists():
            logger.error(f"Directory not found: {directory_path}")
            raise DataPointLoadError(directory_path, "Directory not found", FileNotFoundError())

        if not directory_path.is_dir():
            logger.error(f"Path is not a directory: {directory_path}")
            raise DataPointLoadError(
                directory_path, "Path is not a directory", NotADirectoryError()
            )

        logger.info(
            f"Loading DataPoints from directory: {directory_path} "
            f"(recursive={recursive}, skip_errors={skip_errors})"
        )

        # Find all JSON files
        pattern = "**/*.json" if recursive else "*.json"
        json_files = list(directory_path.glob(pattern))

        logger.info(f"Found {len(json_files)} JSON files")

        datapoints = []
        for json_file in json_files:
            try:
                datapoint = self.load_file(json_file)
                datapoints.append(datapoint)
            except DataPointLoadError:
                if not skip_errors:
                    raise
                # Error already logged in load_file
                continue

        logger.info(
            f"Loaded {len(datapoints)} DataPoints from {directory_path} "
            f"({self.failed_count} failed)"
        )

        return datapoints

    def get_stats(self) -> dict:
        """
        Get loading statistics.

        Returns:
            Dictionary with loaded_count, failed_count, and failed_files
        """
        return {
            "loaded_count": self.loaded_count,
            "failed_count": self.failed_count,
            "failed_files": [
                {"path": str(path), "error": error} for path, error in self.failed_files
            ],
        }

    def reset_stats(self):
        """Reset loading statistics."""
        self.loaded_count = 0
        self.failed_count = 0
        self.failed_files = []
        logger.debug("Statistics reset")

    def _format_validation_error(self, error: ValidationError) -> str:
        """
        Format Pydantic validation error for logging.

        Args:
            error: Pydantic ValidationError

        Returns:
            Formatted error message
        """
        errors = error.errors()
        if len(errors) == 1:
            e = errors[0]
            field = ".".join(str(x) for x in e["loc"])
            return f"{field}: {e['msg']}"
        else:
            return f"{len(errors)} validation errors"
