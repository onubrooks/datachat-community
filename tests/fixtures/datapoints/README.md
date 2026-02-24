# Test Fixtures for DataPoint Loader

This directory contains sample DataPoint JSON files for testing.

## Valid DataPoints

- `table_fact_sales_001.json` - Schema DataPoint example
- `metric_revenue_001.json` - Business DataPoint example
- `proc_daily_etl_001.json` - Process DataPoint example

## Invalid DataPoints (for error testing)

- `invalid_json.json` - Malformed JSON (missing closing brace)
- `invalid_schema.json` - Valid JSON but fails Pydantic validation (invalid datapoint_id)

## Non-JSON Files (should be skipped)

- `not_a_datapoint.txt` - Text file, not JSON
- `README.md` - Markdown file, not JSON
