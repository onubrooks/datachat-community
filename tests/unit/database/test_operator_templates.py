"""Tests for semantic operator-template guidance."""

from backend.database.operator_templates import (
    build_operator_guidance,
    match_operator_templates,
    operator_template_count,
)


def test_operator_template_bank_is_large_enough():
    """Ensure the operator-template library remains broad."""
    assert operator_template_count() >= 50


def test_match_operator_templates_for_inventory_sales_gap():
    query = "Which stores have the largest gap between inventory movement and recorded sales?"
    matches = match_operator_templates(query, limit=5)

    keys = [item.template.key for item in matches]
    assert "inventory_vs_sales_gap" in keys


def test_operator_guidance_adds_table_hints_when_available():
    query = "Which 5 SKUs have the highest stockout risk this week?"
    table_columns = {
        "public.grocery_inventory_snapshots": [
            "store_id",
            "product_id",
            "on_hand_qty",
            "reserved_qty",
            "snapshot_date",
        ],
        "public.grocery_products": ["product_id", "sku", "reorder_level"],
    }

    guidance = build_operator_guidance(
        query,
        table_columns=table_columns,
        max_templates=4,
    )

    assert "stockout_risk" in guidance
    assert "Likely tables:" in guidance
    assert "public.grocery_inventory_snapshots" in guidance


def test_operator_guidance_returns_empty_for_non_analytic_query():
    guidance = build_operator_guidance("hello", table_columns={"public.table": ["id"]})
    assert guidance == ""
