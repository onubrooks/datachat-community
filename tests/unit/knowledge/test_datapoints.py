"""
Tests for DataPoint Loader.

Tests loading and validation of DataPoint JSON files.
"""

import json
from pathlib import Path

import pytest

from backend.knowledge.datapoints import DataPointLoader, DataPointLoadError
from backend.models.datapoint import (
    BusinessDataPoint,
    ProcessDataPoint,
    SchemaDataPoint,
)

# Path to test fixtures
FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "datapoints"
EXAMPLES_DIR = Path(__file__).parent.parent.parent.parent / "datapoints" / "examples"


@pytest.fixture
def loader():
    """Create a fresh DataPointLoader instance."""
    return DataPointLoader()


@pytest.fixture
def valid_schema_file():
    """Path to valid schema DataPoint file."""
    return FIXTURES_DIR / "table_fact_sales_001.json"


@pytest.fixture
def valid_business_file():
    """Path to valid business DataPoint file."""
    return FIXTURES_DIR / "metric_revenue_001.json"


@pytest.fixture
def valid_process_file():
    """Path to valid process DataPoint file."""
    return FIXTURES_DIR / "proc_daily_etl_001.json"


@pytest.fixture
def invalid_json_file():
    """Path to file with invalid JSON."""
    return FIXTURES_DIR / "invalid_json.json"


@pytest.fixture
def invalid_schema_file():
    """Path to file with schema validation errors."""
    return FIXTURES_DIR / "invalid_schema.json"


class TestLoadFile:
    """Test loading individual DataPoint files."""

    def test_load_valid_schema_datapoint(self, loader, valid_schema_file):
        """Test loading a valid Schema DataPoint."""
        datapoint = loader.load_file(valid_schema_file)

        assert isinstance(datapoint, SchemaDataPoint)
        assert datapoint.datapoint_id == "table_fact_sales_001"
        assert datapoint.type == "Schema"
        assert datapoint.name == "Fact Sales Table"
        assert datapoint.table_name == "analytics.fact_sales"
        assert datapoint.schema_name == "analytics"
        assert len(datapoint.key_columns) == 4
        assert len(datapoint.relationships) == 2
        assert datapoint.row_count == 15000000
        assert datapoint.owner == "data-team@company.com"

        # Check statistics
        assert loader.loaded_count == 1
        assert loader.failed_count == 0

    def test_load_valid_business_datapoint(self, loader, valid_business_file):
        """Test loading a valid Business DataPoint."""
        datapoint = loader.load_file(valid_business_file)

        assert isinstance(datapoint, BusinessDataPoint)
        assert datapoint.datapoint_id == "metric_revenue_001"
        assert datapoint.type == "Business"
        assert datapoint.name == "Revenue"
        assert "SUM(fact_sales.amount)" in datapoint.calculation
        assert len(datapoint.synonyms) == 5
        assert "sales" in datapoint.synonyms
        assert len(datapoint.business_rules) == 3
        assert len(datapoint.related_tables) == 2

    def test_load_valid_process_datapoint(self, loader, valid_process_file):
        """Test loading a valid Process DataPoint."""
        datapoint = loader.load_file(valid_process_file)

        assert isinstance(datapoint, ProcessDataPoint)
        assert datapoint.datapoint_id == "proc_daily_etl_001"
        assert datapoint.type == "Process"
        assert datapoint.name == "Daily Sales ETL"
        assert datapoint.schedule == "0 2 * * *"
        assert len(datapoint.target_tables) == 2
        assert len(datapoint.dependencies) == 2
        assert datapoint.sla == "Must complete by 3am UTC"

    def test_load_file_not_found(self, loader):
        """Test loading a file that doesn't exist."""
        nonexistent_file = FIXTURES_DIR / "nonexistent.json"

        with pytest.raises(DataPointLoadError) as exc_info:
            loader.load_file(nonexistent_file)

        assert "File not found" in str(exc_info.value)
        assert exc_info.value.file_path == nonexistent_file
        assert loader.failed_count == 1

    def test_load_invalid_json(self, loader, invalid_json_file):
        """Test loading a file with malformed JSON."""
        with pytest.raises(DataPointLoadError) as exc_info:
            loader.load_file(invalid_json_file)

        assert "Invalid JSON" in str(exc_info.value)
        assert exc_info.value.file_path == invalid_json_file
        assert loader.failed_count == 1

    def test_load_invalid_schema(self, loader, invalid_schema_file):
        """Test loading a file that fails Pydantic validation."""
        with pytest.raises(DataPointLoadError) as exc_info:
            loader.load_file(invalid_schema_file)

        assert "Validation failed" in str(exc_info.value)
        assert exc_info.value.file_path == invalid_schema_file
        assert loader.failed_count == 1

    def test_load_file_with_path_string(self, loader, valid_schema_file):
        """Test loading with string path instead of Path object."""
        datapoint = loader.load_file(str(valid_schema_file))

        assert isinstance(datapoint, SchemaDataPoint)
        assert datapoint.datapoint_id == "table_fact_sales_001"

    def test_multiple_loads_increment_counter(self, loader, valid_schema_file, valid_business_file):
        """Test that loading multiple files increments counter correctly."""
        loader.load_file(valid_schema_file)
        loader.load_file(valid_business_file)

        assert loader.loaded_count == 2
        assert loader.failed_count == 0

    @pytest.mark.parametrize(
        ("relative_path", "expected_tier"),
        [
            ("datapoints/user/table_fact_sales_001.json", "user"),
            ("datapoints/managed/table_fact_sales_001.json", "managed"),
            ("datapoints/examples/table_fact_sales_001.json", "example"),
            ("datapoints/demo/table_fact_sales_001.json", "demo"),
            ("home/user/project/datapoints/managed/table_fact_sales_001.json", "managed"),
            ("fixtures/table_fact_sales_001.json", "custom"),
        ],
    )
    def test_load_file_annotates_source_tier(
        self, loader, valid_schema_file, tmp_path, relative_path, expected_tier
    ):
        """Loader should annotate source tier based on file path."""
        destination = tmp_path / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(valid_schema_file.read_text(), encoding="utf-8")

        datapoint = loader.load_file(destination)

        assert datapoint.metadata["source_tier"] == expected_tier
        assert datapoint.metadata["source_path"].endswith(destination.name)

    def test_load_file_strict_contracts_rejects_advisory_gaps(self, valid_business_file):
        """Strict contract mode should fail when advisory metadata is missing."""
        strict_loader = DataPointLoader(strict_contracts=True)

        with pytest.raises(DataPointLoadError) as exc_info:
            strict_loader.load_file(valid_business_file)

        assert "Contract validation failed" in str(exc_info.value)
        assert "missing_grain" in str(exc_info.value)

    def test_load_file_strict_contracts_accepts_contract_complete_examples(self):
        """Strict contract mode should allow contract-complete example datapoints."""
        strict_loader = DataPointLoader(strict_contracts=True)
        example_file = EXAMPLES_DIR / "grocery_store" / "table_grocery_stores_001.json"

        datapoint = strict_loader.load_file(example_file)

        assert datapoint.datapoint_id == "table_grocery_stores_001"

    def test_load_file_strict_contracts_exempts_demo_datapoints(self, valid_schema_file, tmp_path):
        """Bundled demo datapoints should not fail strict mode on advisory metadata gaps."""
        demo_file = tmp_path / "datapoints" / "demo" / "table_fact_sales_001.json"
        demo_file.parent.mkdir(parents=True, exist_ok=True)
        payload = json.loads(valid_schema_file.read_text(encoding="utf-8"))
        payload["metadata"] = {"source": "demo-seed"}
        demo_file.write_text(json.dumps(payload), encoding="utf-8")

        strict_loader = DataPointLoader(strict_contracts=True)
        datapoint = strict_loader.load_file(demo_file)

        assert datapoint.datapoint_id == "table_fact_sales_001"
        assert datapoint.metadata["source_tier"] == "demo"


class TestLoadDirectory:
    """Test loading DataPoints from directories."""

    def test_load_directory_all_files(self, loader):
        """Test loading all valid DataPoints from directory."""
        datapoints = loader.load_directory(FIXTURES_DIR, skip_errors=True)

        # Should load 3 valid files, skip invalid and non-JSON files
        assert len(datapoints) == 3
        assert loader.loaded_count == 3

        # Check that we got one of each type
        types = {dp.type for dp in datapoints}
        assert types == {"Schema", "Business", "Process"}

    def test_load_directory_skips_non_json_files(self, loader):
        """Test that non-JSON files are skipped."""
        datapoints = loader.load_directory(FIXTURES_DIR, skip_errors=True)

        # Should only load .json files
        assert all(
            isinstance(dp, (SchemaDataPoint, BusinessDataPoint, ProcessDataPoint))
            for dp in datapoints
        )

    def test_load_directory_with_errors_skip(self, loader):
        """Test loading directory with skip_errors=True."""
        datapoints = loader.load_directory(FIXTURES_DIR, skip_errors=True)

        # Should load valid files and skip invalid ones
        assert len(datapoints) == 3
        assert loader.failed_count == 2  # invalid_json.json and invalid_schema.json

    def test_load_directory_with_errors_no_skip(self, loader):
        """Test loading directory with skip_errors=False raises on error."""
        with pytest.raises(DataPointLoadError):
            loader.load_directory(FIXTURES_DIR, skip_errors=False)

        # Should fail on first invalid file
        assert loader.failed_count >= 1

    def test_load_directory_not_found(self, loader):
        """Test loading from non-existent directory."""
        nonexistent_dir = FIXTURES_DIR / "nonexistent"

        with pytest.raises(DataPointLoadError) as exc_info:
            loader.load_directory(nonexistent_dir)

        assert "Directory not found" in str(exc_info.value)

    def test_load_directory_not_a_directory(self, loader, valid_schema_file):
        """Test loading from a file path instead of directory."""
        with pytest.raises(DataPointLoadError) as exc_info:
            loader.load_directory(valid_schema_file)

        assert "not a directory" in str(exc_info.value)

    def test_load_directory_string_path(self, loader):
        """Test loading directory with string path."""
        datapoints = loader.load_directory(str(FIXTURES_DIR), skip_errors=True)

        assert len(datapoints) == 3

    def test_load_directory_recursive(self, loader, tmp_path):
        """Test recursive directory loading."""
        # Create subdirectory with a valid file
        subdir = tmp_path / "subdir"
        subdir.mkdir()

        # Copy a valid file to subdirectory
        with open(FIXTURES_DIR / "metric_revenue_001.json") as f:
            data = json.load(f)

        # Modify ID to avoid duplicates
        data["datapoint_id"] = "metric_revenue_002"
        with open(subdir / "metric_revenue_002.json", "w") as f:
            json.dump(data, f)

        # Copy another file to main directory
        with open(FIXTURES_DIR / "table_fact_sales_001.json") as f:
            data = json.load(f)
        with open(tmp_path / "table_fact_sales_001.json", "w") as f:
            json.dump(data, f)

        # Non-recursive should find 1
        datapoints_non_recursive = loader.load_directory(tmp_path, recursive=False)
        assert len(datapoints_non_recursive) == 1

        # Reset and try recursive
        loader.reset_stats()
        datapoints_recursive = loader.load_directory(tmp_path, recursive=True)
        assert len(datapoints_recursive) == 2


class TestStatistics:
    """Test loading statistics tracking."""

    def test_get_stats_initial(self, loader):
        """Test statistics on fresh loader."""
        stats = loader.get_stats()

        assert stats["loaded_count"] == 0
        assert stats["failed_count"] == 0
        assert stats["failed_files"] == []

    def test_get_stats_after_success(self, loader, valid_schema_file):
        """Test statistics after successful load."""
        loader.load_file(valid_schema_file)
        stats = loader.get_stats()

        assert stats["loaded_count"] == 1
        assert stats["failed_count"] == 0
        assert stats["failed_files"] == []

    def test_get_stats_after_failure(self, loader, invalid_json_file):
        """Test statistics after failed load."""
        with pytest.raises(DataPointLoadError):
            loader.load_file(invalid_json_file)

        stats = loader.get_stats()

        assert stats["loaded_count"] == 0
        assert stats["failed_count"] == 1
        assert len(stats["failed_files"]) == 1
        assert invalid_json_file.name in stats["failed_files"][0]["path"]
        assert "Invalid JSON" in stats["failed_files"][0]["error"]

    def test_get_stats_mixed_results(self, loader, valid_schema_file, invalid_json_file):
        """Test statistics with both successes and failures."""
        loader.load_file(valid_schema_file)

        with pytest.raises(DataPointLoadError):
            loader.load_file(invalid_json_file)

        stats = loader.get_stats()

        assert stats["loaded_count"] == 1
        assert stats["failed_count"] == 1
        assert len(stats["failed_files"]) == 1

    def test_reset_stats(self, loader, valid_schema_file):
        """Test resetting statistics."""
        loader.load_file(valid_schema_file)

        assert loader.loaded_count == 1

        loader.reset_stats()

        assert loader.loaded_count == 0
        assert loader.failed_count == 0
        assert loader.failed_files == []

        stats = loader.get_stats()
        assert stats["loaded_count"] == 0


class TestErrorMessages:
    """Test error message formatting."""

    def test_error_contains_file_path(self, loader, invalid_json_file):
        """Test that error message contains file path."""
        with pytest.raises(DataPointLoadError) as exc_info:
            loader.load_file(invalid_json_file)

        error_msg = str(exc_info.value)
        assert invalid_json_file.name in error_msg

    def test_error_contains_original_exception(self, loader, invalid_json_file):
        """Test that DataPointLoadError wraps original exception."""
        with pytest.raises(DataPointLoadError) as exc_info:
            loader.load_file(invalid_json_file)

        assert exc_info.value.original_error is not None
        assert isinstance(exc_info.value.original_error, json.JSONDecodeError)

    def test_validation_error_formatting(self, loader, invalid_schema_file):
        """Test that validation errors are formatted clearly."""
        with pytest.raises(DataPointLoadError) as exc_info:
            loader.load_file(invalid_schema_file)

        error_msg = str(exc_info.value)
        assert "Validation failed" in error_msg

        # Check failed files list has formatted error
        stats = loader.get_stats()
        assert len(stats["failed_files"]) == 1
        # Should mention either number of errors or specific field
        error_text = stats["failed_files"][0]["error"]
        assert "validation error" in error_text.lower() or "datapoint_id" in error_text


class TestDataPointTypes:
    """Test that correct DataPoint types are returned."""

    def test_discriminated_union_schema(self, loader, valid_schema_file):
        """Test that Schema type returns SchemaDataPoint."""
        datapoint = loader.load_file(valid_schema_file)

        assert isinstance(datapoint, SchemaDataPoint)
        assert datapoint.type == "Schema"
        assert hasattr(datapoint, "table_name")
        assert hasattr(datapoint, "key_columns")

    def test_discriminated_union_business(self, loader, valid_business_file):
        """Test that Business type returns BusinessDataPoint."""
        datapoint = loader.load_file(valid_business_file)

        assert isinstance(datapoint, BusinessDataPoint)
        assert datapoint.type == "Business"
        assert hasattr(datapoint, "calculation")
        assert hasattr(datapoint, "synonyms")

    def test_discriminated_union_process(self, loader, valid_process_file):
        """Test that Process type returns ProcessDataPoint."""
        datapoint = loader.load_file(valid_process_file)

        assert isinstance(datapoint, ProcessDataPoint)
        assert datapoint.type == "Process"
        assert hasattr(datapoint, "schedule")
        assert hasattr(datapoint, "target_tables")
