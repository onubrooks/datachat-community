"""Unit tests for finance workflow packaging caveat generation."""

from backend.api.workflow_packaging import build_workflow_artifacts
from backend.models.api import DataSource


def test_finance_workflow_caveats_include_method_specific_notes():
    artifacts = build_workflow_artifacts(
        query="Show concentration risk: top 10 customers by total balance and percent of total deposits.",
        answer="Here are the top customers by concentration risk.",
        answer_source="sql",
        data={
            "customer_code": ["CUST420", "CUST416"],
            "total_balance": [515920.0, 513065.0],
            "balance_share_pct": [0.9, 0.9],
        },
        sources=[
            DataSource(
                datapoint_id="query_bank_deposit_concentration_top_customers_001",
                type="Query",
                name="Top Customers by Deposit Concentration",
                relevance_score=0.95,
            )
        ],
        validation_warnings=[],
        clarifying_questions=[],
        has_datapoints=True,
        workflow_mode="finance_variance_v1",
        sql="SELECT customer_code, total_balance, balance_share_pct FROM x ORDER BY total_balance DESC LIMIT 10",
        retrieved_datapoints=[
            {
                "datapoint_id": "query_bank_deposit_concentration_top_customers_001",
                "metadata": {
                    "caveat_templates": [
                        "Concentration risk is measured as current balance share and does not include withdrawal velocity stress."
                    ],
                    "exclusions": "Includes active and restricted accounts; excludes frozen accounts.",
                },
            }
        ],
        used_datapoints=["query_bank_deposit_concentration_top_customers_001"],
    )

    assert artifacts is not None
    assert artifacts.caveats
    assert any("Top-N output" in item for item in artifacts.caveats)
    assert any("Scope exclusions:" in item for item in artifacts.caveats)
    assert not any(
        item == "Review source assumptions before sharing externally." for item in artifacts.caveats
    )


def test_finance_workflow_caveats_fall_back_to_generic_when_no_specific_signals():
    artifacts = build_workflow_artifacts(
        query="Show total deposits by segment.",
        answer="Deposits by segment are shown above.",
        answer_source="sql",
        data={"segment": ["retail", "corporate"], "total_deposits": [10.0, 20.0]},
        sources=[
            DataSource(
                datapoint_id="metric_total_deposits_bank_001",
                type="Business",
                name="Total Deposits",
                relevance_score=0.88,
            )
        ],
        validation_warnings=[],
        clarifying_questions=[],
        has_datapoints=True,
        workflow_mode="finance_variance_v1",
        sql="SELECT segment, total_deposits FROM t",
        retrieved_datapoints=[],
        used_datapoints=[],
    )

    assert artifacts is not None
    assert artifacts.caveats == ["Review source assumptions before sharing externally."]
